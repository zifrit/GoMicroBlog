//go:build !integration

package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"MicroBlog/internal/events"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPollContextExpiresWhenRetryDeadlineHasPassed(t *testing.T) {
	retryAt := time.Unix(1, 0)
	consumer := NewWithClient(&fakeKafkaClient{}, newMemoryStore(nil), nil, Config{
		Now: func() time.Time { return retryAt },
	})
	key := partitionKey{topic: TopicName, partition: 2}
	consumer.partitions[key] = partitionState{paused: true, retryAt: retryAt}

	pollContext, cancel, hasRetryDeadline := consumer.pollContext(context.Background())
	defer cancel()

	if !hasRetryDeadline {
		t.Fatal("poll context has no retry deadline")
	}
	if _, ok := pollContext.Deadline(); !ok {
		t.Fatal("poll context has no deadline")
	}
	if !errors.Is(pollContext.Err(), context.DeadlineExceeded) {
		t.Fatalf("poll context error: got %v, want %v", pollContext.Err(), context.DeadlineExceeded)
	}
}

func TestConsumerDeduplicatesRedelivery(t *testing.T) {
	event, err := events.NewPostLiked(42, "alice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	first := eventRecord(t, event, 0)
	second := eventRecord(t, event, 1)
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeKafkaClient{batches: [][]*kgo.Record{{first, second}}, cancelAfterCommits: 2, cancel: cancel}
	store := newMemoryStore(nil)
	consumer := NewWithClient(client, store, nil, Config{})
	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	if store.likeCounts[42] != 1 {
		t.Fatalf("like counter after redelivery: got %d, want 1", store.likeCounts[42])
	}
	if len(client.committed) != 2 || client.committed[0] != 0 || client.committed[1] != 1 {
		t.Fatalf("committed offsets: %v", client.committed)
	}
}

func TestConsumerDoesNotCommitPastFailedRecord(t *testing.T) {
	event, err := events.NewPostLiked(42, "alice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeKafkaClient{
		batches:       [][]*kgo.Record{{eventRecord(t, event, 0), eventRecord(t, event, 1)}},
		cancelOnPause: cancel,
	}
	store := newMemoryStore(errors.New("database unavailable"))
	consumer := NewWithClient(client, store, nil, Config{})
	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	if len(client.committed) != 0 {
		t.Fatalf("consumer committed offsets after failed event: %v", client.committed)
	}
	if store.applyCalls != 1 {
		t.Fatalf("processed %d records after first record failed, want 1", store.applyCalls)
	}
	if len(client.paused[TopicName]) != 1 || client.paused[TopicName][0] != 2 {
		t.Fatalf("paused partitions: %v", client.paused)
	}
	if client.offsets[TopicName][2].Offset != 0 {
		t.Fatalf("retry offset: got %+v, want offset 0", client.offsets[TopicName][2])
	}
}

func eventRecord(t *testing.T, event events.Envelope, offset int64) *kgo.Record {
	t.Helper()
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{
		Topic:       TopicName,
		Key:         []byte("post:42"),
		Value:       value,
		Partition:   2,
		Offset:      offset,
		LeaderEpoch: 1,
	}
}

type memoryStore struct {
	mu         sync.Mutex
	fail       error
	eventIDs   map[string]struct{}
	likeCounts map[int64]int64
	applyCalls int
}

func newMemoryStore(fail error) *memoryStore {
	return &memoryStore{fail: fail, eventIDs: make(map[string]struct{}), likeCounts: make(map[int64]int64)}
}

func (s *memoryStore) ApplyEvent(_ context.Context, event events.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyCalls++
	if s.fail != nil {
		return s.fail
	}
	if _, exists := s.eventIDs[event.EventID.String()]; exists {
		return nil
	}
	s.eventIDs[event.EventID.String()] = struct{}{}
	var payload events.PostLikedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	s.likeCounts[payload.PostID]++
	return nil
}

type fakeKafkaClient struct {
	mu                 sync.Mutex
	batches            [][]*kgo.Record
	pollCount          int
	committed          []int64
	paused             map[string][]int32
	offsets            map[string]map[int32]kgo.EpochOffset
	cancelAfterCommits int
	cancelOnPause      context.CancelFunc
	cancel             context.CancelFunc
}

func (c *fakeKafkaClient) PollRecords(ctx context.Context, _ int) ([]*kgo.Record, error) {
	c.mu.Lock()
	if c.pollCount < len(c.batches) {
		records := c.batches[c.pollCount]
		c.pollCount++
		c.mu.Unlock()
		return records, nil
	}
	c.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *fakeKafkaClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	c.mu.Lock()
	for _, record := range records {
		c.committed = append(c.committed, record.Offset)
	}
	shouldCancel := c.cancelAfterCommits > 0 && len(c.committed) >= c.cancelAfterCommits
	cancel := c.cancel
	c.mu.Unlock()
	if shouldCancel && cancel != nil {
		cancel()
	}
	return nil
}

func (c *fakeKafkaClient) PauseFetchPartitions(partitions map[string][]int32) {
	c.mu.Lock()
	c.paused = partitions
	cancel := c.cancelOnPause
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (*fakeKafkaClient) ResumeFetchPartitions(map[string][]int32) {}

func (c *fakeKafkaClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	c.mu.Lock()
	c.offsets = offsets
	c.mu.Unlock()
}

func (*fakeKafkaClient) Close() {}
