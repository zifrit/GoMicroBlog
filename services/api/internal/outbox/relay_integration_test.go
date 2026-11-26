//go:build integration

package outbox

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"MicroBlog/db/migrations"
	"MicroBlog/internal/database"
	"MicroBlog/internal/events"
	"MicroBlog/internal/kafkatest"
	"MicroBlog/services/api/internal/kafka"
	"MicroBlog/services/api/internal/repository"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRelayRetriesAfterBrokerRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool := newRelayTestPool(t, ctx)
	db := repository.New(pool)

	brokerContainer, broker := kafkatest.Start(t, ctx)
	producer, err := kafka.NewProducer([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.EnsureTopic(ctx, TopicName); err != nil {
		t.Fatalf("create topic before broker outage: %v", err)
	}
	observedStore := &nextPendingObservedStore{Store: db, observed: make(chan struct{})}
	relay := NewRelay(observedStore, producer, slog.Default(), Config{
		Topic:            TopicName,
		InitialBackoff:   50 * time.Millisecond,
		MaxBackoff:       200 * time.Millisecond,
		IdlePollInterval: 20 * time.Millisecond,
		PublishTimeout:   250 * time.Millisecond,
		BacklogInterval:  30 * time.Second,
	})
	relayCtx, stopRelay := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Run(relayCtx) }()
	relayStopped := false
	defer func() {
		stopRelay()
		if !relayStopped {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("relay did not stop")
			}
		}
	}()
	select {
	case <-observedStore.observed:
	case <-ctx.Done():
		stopRelay()
		t.Fatal("relay did not finish topic setup")
	}
	stopTimeout := time.Second
	if err := brokerContainer.Stop(ctx, &stopTimeout); err != nil {
		stopRelay()
		t.Fatal(err)
	}
	if _, err := db.Register(ctx, "alice"); err != nil {
		stopRelay()
		t.Fatal(err)
	}

	if err := waitFor(ctx, func() (bool, error) {
		var attempts int
		err := pool.QueryRow(ctx, `SELECT attempts FROM api.outbox_events WHERE event_type='UserRegistered'`).Scan(&attempts)
		return attempts > 0, err
	}); err != nil {
		stopRelay()
		t.Fatalf("relay did not record a failed publish: %v", err)
	}
	if err := brokerContainer.Start(ctx); err != nil {
		stopRelay()
		t.Fatal(err)
	}
	if err := waitFor(ctx, func() (bool, error) {
		var publishedAt *time.Time
		err := pool.QueryRow(ctx, `SELECT published_at FROM api.outbox_events WHERE event_type='UserRegistered'`).Scan(&publishedAt)
		return publishedAt != nil, err
	}); err != nil {
		stopRelay()
		t.Fatalf("event was not published after broker recovery: %v", err)
	}
	stopRelay()
	if err := <-done; err != nil {
		t.Fatalf("relay stopped with error: %v", err)
	}
	relayStopped = true
}

func TestRelayReplaysAfterAckBeforeMarkPublished(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool := newRelayTestPool(t, ctx)
	if _, err := repository.New(pool).Register(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	_, broker := kafkatest.Start(t, ctx)
	producer, err := kafka.NewProducer([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	_, err = pool.Exec(ctx, `
		CREATE FUNCTION fail_first_outbox_publish_mark() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.published_at IS NULL AND OLD.attempts = 0 AND NEW.published_at IS NOT NULL THEN
				RAISE EXCEPTION 'simulated failure after Kafka acknowledgement';
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER fail_first_outbox_publish_mark
		BEFORE UPDATE ON api.outbox_events FOR EACH ROW
		EXECUTE FUNCTION fail_first_outbox_publish_mark();`)
	if err != nil {
		t.Fatal(err)
	}

	relay := NewRelay(repository.New(pool), producer, slog.Default(), Config{
		Topic:            TopicName,
		InitialBackoff:   20 * time.Millisecond,
		MaxBackoff:       50 * time.Millisecond,
		IdlePollInterval: 10 * time.Millisecond,
		PublishTimeout:   5 * time.Second,
		BacklogInterval:  30 * time.Second,
	})
	relayCtx, stopRelay := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Run(relayCtx) }()
	relayStopped := false
	defer func() {
		stopRelay()
		if !relayStopped {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("relay did not stop")
			}
		}
	}()
	if err := waitFor(ctx, func() (bool, error) {
		var publishedAt *time.Time
		var attempts int
		err := pool.QueryRow(ctx, `SELECT published_at, attempts FROM api.outbox_events WHERE event_type='UserRegistered'`).Scan(&publishedAt, &attempts)
		return publishedAt != nil && attempts == 1, err
	}); err != nil {
		stopRelay()
		t.Fatalf("event was not marked after one replay: %v", err)
	}
	stopRelay()
	if err := <-done; err != nil {
		t.Fatalf("relay stopped with error: %v", err)
	}
	relayStopped = true

	reader, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup("relay-replay-test"),
		kgo.ConsumeTopics(TopicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var received []events.Envelope
	for len(received) < 2 {
		fetches := reader.PollRecords(ctx, 2-len(received))
		if fetchErr := fetches.Err(); fetchErr != nil {
			t.Fatalf("read replayed messages: %v", fetchErr)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			event, decodeErr := events.Decode(record.Value)
			if decodeErr != nil {
				t.Errorf("decode Kafka event: %v", decodeErr)
				return
			}
			received = append(received, event)
		})
	}
	if received[0].EventID != received[1].EventID {
		t.Fatalf("replayed IDs differ: %s and %s", received[0].EventID, received[1].EventID)
	}
}

func newRelayTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
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

func waitFor(ctx context.Context, check func() (bool, error)) error {
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

type nextPendingObservedStore struct {
	Store
	observed chan struct{}
	called   bool
}

func (s *nextPendingObservedStore) NextPending(ctx context.Context) (repository.OutboxRecord, bool, error) {
	if !s.called {
		close(s.observed)
		s.called = true
	}
	return s.Store.NextPending(ctx)
}
