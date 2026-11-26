package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"MicroBlog/internal/events"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	TopicName      = "microblog.events"
	DefaultGroupID = "engagement-stats-v1"
)

type EventStore interface {
	ApplyEvent(ctx context.Context, event events.Envelope) error
}

type KafkaClient interface {
	PollRecords(ctx context.Context, maxRecords int) ([]*kgo.Record, error)
	CommitRecords(ctx context.Context, records ...*kgo.Record) error
	PauseFetchPartitions(topicPartitions map[string][]int32)
	ResumeFetchPartitions(topicPartitions map[string][]int32)
	SetOffsets(setOffsets map[string]map[int32]kgo.EpochOffset)
	Close()
}

type Config struct {
	Topic            string
	GroupID          string
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	IdlePollInterval time.Duration
	MaxPollRecords   int
	Wait             func(context.Context, time.Duration) error
	Now              func() time.Time
}

type Consumer struct {
	client     KafkaClient
	store      EventStore
	logger     *slog.Logger
	config     Config
	partitions map[partitionKey]partitionState
}

type partitionKey struct {
	topic     string
	partition int32
}

type partitionState struct {
	offset    kgo.EpochOffset
	backoff   time.Duration
	retryAt   time.Time
	attempts  int
	paused    bool
	permanent bool
}

type kafkaClient struct {
	client *kgo.Client
}

func New(brokers []string, store EventStore, logger *slog.Logger, config Config) (*Consumer, error) {
	seedBrokers := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			continue
		}
		seedBrokers = append(seedBrokers, broker)
	}
	if len(seedBrokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if config.Topic == "" {
		config.Topic = TopicName
	}
	if config.GroupID == "" {
		config.GroupID = DefaultGroupID
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.ConsumerGroup(config.GroupID),
		kgo.ConsumeTopics(config.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	return NewWithClient(&kafkaClient{client: client}, store, logger, config), nil
}

func NewWithClient(client KafkaClient, store EventStore, logger *slog.Logger, config Config) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	if config.Topic == "" {
		config.Topic = TopicName
	}
	if config.GroupID == "" {
		config.GroupID = DefaultGroupID
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
	if config.IdlePollInterval <= 0 {
		config.IdlePollInterval = 250 * time.Millisecond
	}
	if config.MaxPollRecords <= 0 {
		config.MaxPollRecords = 100
	}
	if config.Wait == nil {
		config.Wait = waitContext
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Consumer{
		client:     client,
		store:      store,
		logger:     logger,
		config:     config,
		partitions: make(map[partitionKey]partitionState),
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		c.resumeReadyPartitions()
		pollContext, cancelPoll, hasRetryDeadline := c.pollContext(ctx)
		records, pollErr := c.client.PollRecords(pollContext, c.config.MaxPollRecords)
		cancelPoll()
		if ctx.Err() != nil {
			return nil
		}
		if len(records) > 0 {
			c.processRecords(ctx, records)
		}
		if pollErr == nil {
			continue
		}
		if hasRetryDeadline && errors.Is(pollErr, context.DeadlineExceeded) {
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		c.logger.ErrorContext(ctx, "poll Kafka failed", "service", "engagement", "topic", c.config.Topic, "error", pollErr)
		if err := c.config.Wait(ctx, c.config.InitialBackoff); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
	}
}

func (c *Consumer) processRecords(ctx context.Context, records []*kgo.Record) {
	// Группируем записи по партициям и обрабатываем их по offset.
	partitions := make(map[partitionKey][]*kgo.Record)
	keys := make([]partitionKey, 0)
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		if _, exists := partitions[key]; !exists {
			keys = append(keys, key)
		}
		partitions[key] = append(partitions[key], record)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic == keys[j].topic {
			return keys[i].partition < keys[j].partition
		}
		return keys[i].topic < keys[j].topic
	})

	for _, key := range keys {
		state := c.partitions[key]
		if state.paused {
			continue
		}
		partitionRecords := partitions[key]
		sort.Slice(partitionRecords, func(i, j int) bool {
			return partitionRecords[i].Offset < partitionRecords[j].Offset
		})
		// Не обрабатываем следующие записи, пока текущая запись не будет разрешена.
		for _, record := range partitionRecords {
			if ctx.Err() != nil {
				return
			}
			event, err := events.Decode(record.Value)
			if err != nil {
				c.pausePermanently(record, err)
				break
			}
			if err := c.store.ApplyEvent(ctx, event); err != nil {
				c.pauseForRetry(record, event, "apply event failed", err)
				break
			}
			if err := c.client.CommitRecords(ctx, record); err != nil {
				c.pauseForRetry(record, event, "commit Kafka offset failed", err)
				break
			}
			state := c.partitions[key]
			state.backoff = c.config.InitialBackoff
			state.attempts = 0
			state.retryAt = time.Time{}
			c.partitions[key] = state
		}
	}
}

func (c *Consumer) pauseForRetry(record *kgo.Record, event events.Envelope, message string, cause error) {
	key := partitionKey{topic: record.Topic, partition: record.Partition}
	state := c.partitions[key]
	if state.backoff <= 0 {
		state.backoff = c.config.InitialBackoff
	}
	state.offset = kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
	state.retryAt = c.config.Now().Add(state.backoff)
	state.attempts++
	state.paused = true
	c.partitions[key] = state
	c.pausePartition(key, state.offset)
	c.logger.Error(message, "service", "engagement", "event_id", event.EventID,
		"event_type", event.EventType, "attempts", state.attempts, "topic", record.Topic,
		"partition", record.Partition, "offset", record.Offset, "error", cause)
}

func (c *Consumer) pausePermanently(record *kgo.Record, cause error) {
	key := partitionKey{topic: record.Topic, partition: record.Partition}
	state := c.partitions[key]
	state.offset = kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
	state.paused = true
	state.permanent = true
	c.partitions[key] = state
	c.pausePartition(key, state.offset)
	c.logger.Error("pause Kafka partition after invalid event", "service", "engagement",
		"topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", cause)
}

func (c *Consumer) pausePartition(key partitionKey, offset kgo.EpochOffset) {
	c.client.PauseFetchPartitions(map[string][]int32{key.topic: {key.partition}})
	c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{key.topic: {key.partition: offset}})
}

func (c *Consumer) resumeReadyPartitions() {
	now := c.config.Now()
	for key, state := range c.partitions {
		if !state.paused || state.permanent || now.Before(state.retryAt) {
			continue
		}
		c.client.ResumeFetchPartitions(map[string][]int32{key.topic: {key.partition}})
		state.paused = false
		state.retryAt = time.Time{}
		state.backoff = nextBackoff(state.backoff, c.config.MaxBackoff)
		c.partitions[key] = state
	}
}

func (c *Consumer) pollContext(ctx context.Context) (context.Context, context.CancelFunc, bool) {
	var earliest time.Time
	for _, state := range c.partitions {
		if state.paused && !state.permanent && (earliest.IsZero() || state.retryAt.Before(earliest)) {
			earliest = state.retryAt
		}
	}
	if earliest.IsZero() {
		return ctx, func() {}, false
	}
	delay := earliest.Sub(c.config.Now())
	pollContext, cancel := context.WithTimeout(ctx, delay)
	return pollContext, cancel, true
}

func (c *Consumer) Close() {
	c.client.Close()
}

func (c *kafkaClient) PollRecords(ctx context.Context, maxRecords int) ([]*kgo.Record, error) {
	fetches := c.client.PollRecords(ctx, maxRecords)
	records := make([]*kgo.Record, 0)
	fetches.EachRecord(func(record *kgo.Record) {
		records = append(records, record)
	})
	return records, fetches.Err()
}

func (c *kafkaClient) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}

func (c *kafkaClient) PauseFetchPartitions(topicPartitions map[string][]int32) {
	c.client.PauseFetchPartitions(topicPartitions)
}

func (c *kafkaClient) ResumeFetchPartitions(topicPartitions map[string][]int32) {
	c.client.ResumeFetchPartitions(topicPartitions)
}

func (c *kafkaClient) SetOffsets(setOffsets map[string]map[int32]kgo.EpochOffset) {
	c.client.SetOffsets(setOffsets)
}

func (c *kafkaClient) Close() {
	c.client.Close()
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
	if current <= 0 {
		return maximum
	}
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

var _ KafkaClient = (*kafkaClient)(nil)
