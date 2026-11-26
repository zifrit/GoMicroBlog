//go:build integration

package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/database"
	"MicroBlog/internal/events"
	"MicroBlog/internal/kafkatest"
	"MicroBlog/services/engagement/internal/repository"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerDeduplicatesRedelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := newConsumerTestPool(t, ctx)
	repo := repository.New(pool)
	_, broker := kafkatest.Start(t, ctx)
	producer := newKafkaTestProducer(t, ctx, broker)

	event, err := events.NewPostLiked(42, "alice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	firstPartition := publishEventually(t, ctx, producer, "post:42", value)
	secondPartition := publishEventually(t, ctx, producer, "post:42", value)
	if secondPartition != firstPartition {
		t.Fatalf("same key produced to different partitions: %d and %d", firstPartition, secondPartition)
	}

	groupID := "engagement-dedup-" + uuid.NewString()
	consumer, err := New([]string{broker}, repo, slog.Default(), Config{GroupID: groupID})
	if err != nil {
		t.Fatal(err)
	}
	stop, done, runErr := runConsumerForTest(t, ctx, consumer)
	if err := waitForConsumer(ctx, func() (bool, error) {
		count, found, err := repo.PostLikeCount(ctx, 42)
		return found && count == 1, err
	}); err != nil {
		stop()
		t.Fatalf("counter was not applied: %v", err)
	}
	admin := newAdminClient(t, broker)
	if err := waitForConsumer(ctx, func() (bool, error) {
		return committedAtLeast(ctx, admin, groupID, firstPartition, 2)
	}); err != nil {
		stop()
		t.Fatalf("consumer did not commit past both redeliveries: %v", err)
	}
	stop()
	select {
	case <-done:
		if err := runErr(); err != nil {
			t.Fatalf("consumer stopped with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop")
	}
	var processed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM engagement.processed_events WHERE event_id=$1`, event.EventID).Scan(&processed); err != nil || processed != 1 {
		t.Fatalf("processed event rows: %d, %v", processed, err)
	}
}

func TestConsumerDoesNotCommitPastFailedRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := newConsumerTestPool(t, ctx)
	repo := repository.New(pool)
	_, broker := kafkatest.Start(t, ctx)
	producer := newKafkaTestProducer(t, ctx, broker)

	firstEvent, err := events.NewPostLiked(42, "alice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	secondEvent, err := events.NewPostLiked(42, "bob", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	firstValue, _ := json.Marshal(firstEvent)
	secondValue, _ := json.Marshal(secondEvent)
	firstPartition := publishEventually(t, ctx, producer, "post:42", firstValue)
	if partition := publishEventually(t, ctx, producer, "post:42", secondValue); partition != firstPartition {
		t.Fatalf("same key produced to different partitions: %d and %d", firstPartition, partition)
	}
	_, err = pool.Exec(ctx, `
		CREATE FUNCTION reject_engagement_event() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'simulated event processing failure';
		END $$;
		CREATE TRIGGER reject_engagement_event
		BEFORE INSERT ON engagement.processed_events FOR EACH ROW
		EXECUTE FUNCTION reject_engagement_event();`)
	if err != nil {
		t.Fatal(err)
	}

	groupID := "engagement-failure-" + uuid.NewString()
	var logs safeBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	consumer, err := New([]string{broker}, repo, logger, Config{
		GroupID:          groupID,
		InitialBackoff:   100 * time.Millisecond,
		MaxBackoff:       500 * time.Millisecond,
		IdlePollInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	stop, done, runErr := runConsumerForTest(t, ctx, consumer)
	admin := newAdminClient(t, broker)
	if err := waitForConsumer(ctx, func() (bool, error) {
		return logs.contains("apply event failed"), nil
	}); err != nil {
		stop()
		t.Fatalf("consumer did not encounter the database error: %v", err)
	}
	if err := waitForConsumer(ctx, func() (bool, error) {
		return noCommittedOffset(ctx, admin, groupID, firstPartition)
	}); err != nil {
		stop()
		t.Fatalf("consumer committed an offset past a failed record: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER reject_engagement_event ON engagement.processed_events; DROP FUNCTION reject_engagement_event()`); err != nil {
		stop()
		t.Fatal(err)
	}
	if err := waitForConsumer(ctx, func() (bool, error) {
		count, found, err := repo.PostLikeCount(ctx, 42)
		return found && count == 2, err
	}); err != nil {
		stop()
		t.Fatalf("consumer did not process both events after recovery: %v", err)
	}
	stop()
	select {
	case <-done:
		if err := runErr(); err != nil {
			t.Fatalf("consumer stopped with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop")
	}
	if err := waitForConsumer(ctx, func() (bool, error) {
		return committedAtLeast(ctx, admin, groupID, firstPartition, 2)
	}); err != nil {
		t.Fatalf("consumer did not commit offsets after processing: %v", err)
	}
}

func newConsumerTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	container, err := postgres.Run(ctx, "postgres:17.9-alpine", postgres.WithDatabase("microblog"),
		postgres.WithUsername("microblog"), postgres.WithPassword("test-password"), postgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := database.OpenPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func newKafkaTestProducer(t *testing.T, ctx context.Context, broker string) *kgo.Client {
	t.Helper()
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(15*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	admin := kadm.NewClient(producer)
	for {
		responses, err := admin.CreateTopics(ctx, 3, 1, nil, TopicName)
		if err == nil {
			response, ok := responses[TopicName]
			if ok && (response.Err == nil || errors.Is(response.Err, kerr.TopicAlreadyExists)) {
				return producer
			}
			if ok {
				err = response.Err
			} else {
				err = errors.New("Kafka returned no create-topic response")
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("create Kafka topic: %v; last error: %v", ctx.Err(), err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func publishEventually(t *testing.T, ctx context.Context, producer *kgo.Client, key string, value []byte) int32 {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		produced, err := producer.ProduceSync(ctx, &kgo.Record{Topic: TopicName, Key: []byte(key), Value: value}).First()
		if err == nil {
			return produced.Partition
		}
		select {
		case <-ctx.Done():
			t.Fatalf("publish test event: %v; last error: %v", ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func newAdminClient(t *testing.T, broker string) *kadm.Client {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return kadm.NewClient(client)
}

func committedAtLeast(ctx context.Context, admin *kadm.Client, groupID string, partition int32, minimum int64) (bool, error) {
	offsets, err := admin.FetchOffsetsForTopics(ctx, groupID, TopicName)
	if err != nil {
		return false, err
	}
	response, found := offsets.Lookup(TopicName, partition)
	return found && response.At >= minimum, nil
}

func noCommittedOffset(ctx context.Context, admin *kadm.Client, groupID string, partition int32) (bool, error) {
	offsets, err := admin.FetchOffsetsForTopics(ctx, groupID, TopicName)
	if err != nil {
		return false, err
	}
	response, found := offsets.Lookup(TopicName, partition)
	return !found || response.At < 0, nil
}

func waitForConsumer(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := check()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func runConsumerForTest(t *testing.T, parent context.Context, consumer *Consumer) (context.CancelFunc, <-chan struct{}, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = consumer.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("consumer did not stop")
		}
		consumer.Close()
		if runErr != nil {
			t.Errorf("consumer: %v", runErr)
		}
	})
	return cancel, done, func() error { return runErr }
}

type safeBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *safeBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(value)
}

func (b *safeBuffer) contains(value string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Contains(b.buffer.Bytes(), []byte(fmt.Sprintf("%q", value)))
}
