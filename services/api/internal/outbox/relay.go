package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"MicroBlog/services/api/internal/repository"
)

const TopicName = "microblog.events"

type Store interface {
	NextPending(ctx context.Context) (repository.OutboxRecord, bool, error)
	MarkPublished(ctx context.Context, id int64, at time.Time) error
	RecordFailure(ctx context.Context, id int64, publishErr error) error
	Backlog(ctx context.Context) (count int64, oldestPendingAt time.Time, err error)
}

type Producer interface {
	EnsureTopic(ctx context.Context, topic string) error
	Publish(ctx context.Context, topic, key string, value []byte) (partition int32, err error)
	Close() error
}

type Config struct {
	Topic            string
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	PublishTimeout   time.Duration
	IdlePollInterval time.Duration
	BacklogInterval  time.Duration
	Wait             func(context.Context, time.Duration) error
	Now              func() time.Time
}

type Relay struct {
	store    Store
	producer Producer
	logger   *slog.Logger
	config   Config
}

func NewRelay(store Store, producer Producer, logger *slog.Logger, config Config) *Relay {
	if logger == nil {
		logger = slog.Default()
	}
	if config.Topic == "" {
		config.Topic = TopicName
	}
	if config.InitialBackoff <= 0 {
		config.InitialBackoff = time.Second
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = 30 * time.Second
	}
	if config.MaxBackoff < config.InitialBackoff {
		config.MaxBackoff = config.InitialBackoff
	}
	if config.PublishTimeout <= 0 {
		config.PublishTimeout = 10 * time.Second
	}
	if config.IdlePollInterval <= 0 {
		config.IdlePollInterval = 250 * time.Millisecond
	}
	if config.BacklogInterval <= 0 {
		config.BacklogInterval = 30 * time.Second
	}
	if config.Wait == nil {
		config.Wait = waitContext
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Relay{store: store, producer: producer, logger: logger, config: config}
}

func (r *Relay) Run(ctx context.Context) error {
	backoff := r.config.InitialBackoff
	lastBacklogLog := time.Time{}
	topicReady := false

	for {
		if ctx.Err() != nil {
			return nil
		}
		now := r.config.Now()
		if lastBacklogLog.IsZero() || now.Sub(lastBacklogLog) >= r.config.BacklogInterval {
			r.logBacklog(ctx)
			lastBacklogLog = now
		}

		if !topicReady {
			if err := r.producer.EnsureTopic(ctx, r.config.Topic); err != nil {
				r.logger.ErrorContext(ctx, "ensure Kafka topic failed", "service", "api", "topic", r.config.Topic, "error", err)
				if err := r.wait(ctx, backoff); err != nil {
					return err
				}
				backoff = nextBackoff(backoff, r.config.MaxBackoff)
				continue
			}
			topicReady = true
			backoff = r.config.InitialBackoff
		}

		record, found, err := r.store.NextPending(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.logger.ErrorContext(ctx, "read pending outbox event failed", "error", err)
			if err := r.wait(ctx, backoff); err != nil {
				return err
			}
			backoff = nextBackoff(backoff, r.config.MaxBackoff)
			continue
		}
		if !found {
			if err := r.wait(ctx, r.config.IdlePollInterval); err != nil {
				return err
			}
			continue
		}

		value, err := json.Marshal(record.Event)
		if err != nil {
			r.recordFailure(ctx, record, fmt.Errorf("encode outbox event: %w", err))
			if err := r.wait(ctx, backoff); err != nil {
				return err
			}
			backoff = nextBackoff(backoff, r.config.MaxBackoff)
			continue
		}
		publishCtx, cancel := context.WithTimeout(ctx, r.config.PublishTimeout)
		partition, err := r.producer.Publish(publishCtx, r.config.Topic, record.MessageKey, value)
		cancel()
		if err != nil {
			r.recordFailure(ctx, record, err)
			r.logger.ErrorContext(ctx, "publish outbox event failed", "service", "api", "event_id", record.Event.EventID,
				"event_type", record.Event.EventType, "attempts", record.Attempts+1, "topic", r.config.Topic,
				"partition", nil, "outbox_id", record.ID, "error", err)
			if waitErr := r.wait(ctx, backoff); waitErr != nil {
				return waitErr
			}
			backoff = nextBackoff(backoff, r.config.MaxBackoff)
			continue
		}
		// Сначала получаем подтверждение Kafka, затем помечаем событие опубликованным.
		// Если relay остановится между этими шагами, Kafka может получить дубликат.
		if err := r.store.MarkPublished(ctx, record.ID, r.config.Now().UTC()); err != nil {
			r.recordFailure(ctx, record, err)
			r.logger.ErrorContext(ctx, "mark acknowledged outbox event published failed", "service", "api",
				"event_id", record.Event.EventID, "event_type", record.Event.EventType,
				"attempts", record.Attempts+1, "topic", r.config.Topic, "partition", partition,
				"outbox_id", record.ID, "error", err)
			if waitErr := r.wait(ctx, backoff); waitErr != nil {
				return waitErr
			}
			backoff = nextBackoff(backoff, r.config.MaxBackoff)
			continue
		}
		r.logger.InfoContext(ctx, "outbox event published", "service", "api", "event_id", record.Event.EventID,
			"event_type", record.Event.EventType, "attempts", record.Attempts, "topic", r.config.Topic,
			"partition", partition, "error", nil)
		backoff = r.config.InitialBackoff
	}
}

func (r *Relay) recordFailure(ctx context.Context, record repository.OutboxRecord, cause error) {
	if err := r.store.RecordFailure(ctx, record.ID, cause); err != nil {
		r.logger.ErrorContext(ctx, "record outbox failure failed", "service", "api", "event_id", record.Event.EventID,
			"event_type", record.Event.EventType, "attempts", record.Attempts+1, "topic", r.config.Topic,
			"outbox_id", record.ID, "error", err)
	}
}

func (r *Relay) logBacklog(ctx context.Context) {
	count, oldest, err := r.store.Backlog(ctx)
	if err != nil {
		r.logger.ErrorContext(ctx, "read outbox backlog failed", "service", "api", "error", err)
		return
	}
	oldestPendingAge := 0.0
	if count > 0 {
		oldestPendingAge = r.config.Now().Sub(oldest).Seconds()
		if oldestPendingAge < 0 {
			oldestPendingAge = 0
		}
	}
	r.logger.InfoContext(ctx, "outbox backlog", "service", "api", "pending_count", count,
		"oldest_pending_age_seconds", oldestPendingAge)
}

func (r *Relay) wait(ctx context.Context, duration time.Duration) error {
	if err := r.config.Wait(ctx, duration); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

var _ Store = (*repository.Postgres)(nil)
