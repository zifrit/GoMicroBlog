package outbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"MicroBlog/internal/events"
	"MicroBlog/services/api/internal/repository"
)

func TestRelayMarksPublishedOnlyAfterAck(t *testing.T) {
	store := newFakeStore(testOutboxRecord(t))
	producer := &fakeProducer{publishStarted: make(chan struct{}), publishRelease: make(chan struct{})}
	relay := newTestRelay(store, producer, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	select {
	case <-producer.publishStarted:
	case <-time.After(time.Second):
		t.Fatal("relay did not try to publish")
	}
	if store.isPublished() {
		t.Fatal("outbox row was marked before broker acknowledgement")
	}
	close(producer.publishRelease)
	select {
	case <-store.marked:
	case <-time.After(time.Second):
		t.Fatal("relay did not mark the acknowledged event")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("relay stopped with error: %v", err)
	}
	if !store.isPublished() {
		t.Fatal("outbox row was not marked published")
	}
}

func TestRelayRetriesWithBackoff(t *testing.T) {
	store := newFakeStore(testOutboxRecord(t))
	producer := &fakeProducer{publishErrors: []error{errors.New("temporary broker failure")}}
	var delayMu sync.Mutex
	var delays []time.Duration
	relay := newTestRelay(store, producer, func(_ context.Context, delay time.Duration) error {
		delayMu.Lock()
		delays = append(delays, delay)
		delayMu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	select {
	case <-store.marked:
	case <-time.After(time.Second):
		t.Fatal("relay did not publish after transient failure")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("relay stopped with error: %v", err)
	}
	if producer.publishCount() != 2 || store.failureCount() != 1 {
		t.Fatalf("publish attempts=%d recorded failures=%d", producer.publishCount(), store.failureCount())
	}
	delayMu.Lock()
	defer delayMu.Unlock()
	if len(delays) == 0 || delays[0] != time.Millisecond {
		t.Fatalf("retry delays: got %v, want first delay %s", delays, time.Millisecond)
	}
}

func TestRelayLogsBacklog(t *testing.T) {
	store := newFakeStore(repository.OutboxRecord{})
	store.pending = false
	store.backlogCount = 3
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	relay := newTestRelay(store, &fakeProducer{}, nil)
	relay.logger = logger
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	select {
	case <-store.backlogRead:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("relay did not read the outbox backlog")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("relay stopped with error: %v", err)
	}
	if !strings.Contains(logs.String(), `"service":"api"`) ||
		!strings.Contains(logs.String(), `"pending_count":3`) ||
		!strings.Contains(logs.String(), `"oldest_pending_age_seconds":`) {
		t.Fatalf("backlog log does not include count: %s", logs.String())
	}
}

type fakeStore struct {
	mu           sync.Mutex
	record       repository.OutboxRecord
	pending      bool
	failures     int
	backlogCount int64
	marked       chan struct{}
	backlogRead  chan struct{}
}

func newFakeStore(record repository.OutboxRecord) *fakeStore {
	return &fakeStore{
		record:      record,
		pending:     true,
		marked:      make(chan struct{}, 1),
		backlogRead: make(chan struct{}, 1),
	}
}

func (s *fakeStore) NextPending(ctx context.Context) (repository.OutboxRecord, bool, error) {
	s.mu.Lock()
	if s.pending {
		record := s.record
		s.mu.Unlock()
		return record, true, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return repository.OutboxRecord{}, false, ctx.Err()
}

func (s *fakeStore) MarkPublished(context.Context, int64, time.Time) error {
	s.mu.Lock()
	s.pending = false
	s.mu.Unlock()
	s.marked <- struct{}{}
	return nil
}

func (s *fakeStore) RecordFailure(context.Context, int64, error) error {
	s.mu.Lock()
	s.failures++
	s.mu.Unlock()
	return nil
}

func (s *fakeStore) Backlog(context.Context) (int64, time.Time, error) {
	s.backlogRead <- struct{}{}
	return s.backlogCount, time.Now().Add(-time.Minute), nil
}

func (s *fakeStore) isPublished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.pending
}

func (s *fakeStore) failureCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}

type fakeProducer struct {
	mu             sync.Mutex
	publishErrors  []error
	publishStarted chan struct{}
	publishRelease chan struct{}
	published      int
}

func (p *fakeProducer) EnsureTopic(context.Context, string) error { return nil }

func (p *fakeProducer) Publish(ctx context.Context, _ string, _ string, _ []byte) (int32, error) {
	p.mu.Lock()
	p.published++
	attempt := p.published
	var err error
	if attempt <= len(p.publishErrors) {
		err = p.publishErrors[attempt-1]
	}
	p.mu.Unlock()
	if p.publishStarted != nil && attempt == 1 {
		close(p.publishStarted)
	}
	if p.publishRelease != nil && attempt == 1 {
		select {
		case <-p.publishRelease:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (p *fakeProducer) Close() error { return nil }

func (p *fakeProducer) publishCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.published
}

func newTestRelay(store Store, producer Producer, wait func(context.Context, time.Duration) error) *Relay {
	return NewRelay(store, producer, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), Config{
		InitialBackoff:   time.Millisecond,
		MaxBackoff:       2 * time.Millisecond,
		IdlePollInterval: time.Millisecond,
		BacklogInterval:  time.Hour,
		Topic:            TopicName,
		Wait:             wait,
	})
}

func testOutboxRecord(t *testing.T) repository.OutboxRecord {
	t.Helper()
	event, err := events.NewUserRegistered("alice", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	key, err := events.MessageKey(event)
	if err != nil {
		t.Fatal(err)
	}
	return repository.OutboxRecord{
		ID:         1,
		Event:      event,
		MessageKey: key,
		CreatedAt:  time.Now(),
	}
}
