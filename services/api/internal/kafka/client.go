package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	TopicPartitions        = 3
	TopicReplicationFactor = 1
)

type Producer struct {
	client *kgo.Client
	admin  *kadm.Client
}

func NewProducer(brokers []string) (*Producer, error) {
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
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(15*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Producer{client: client, admin: kadm.NewClient(client)}, nil
}

func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) (int32, error) {
	record := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	produced, err := p.client.ProduceSync(ctx, record).First()
	if err != nil {
		return 0, fmt.Errorf("publish Kafka record: %w", err)
	}
	if produced == nil {
		return 0, errors.New("Kafka returned no produced record")
	}
	return produced.Partition, nil
}

func (p *Producer) EnsureTopic(ctx context.Context, topic string) error {
	retentionMS := "-1"
	created, err := p.admin.CreateTopics(ctx, TopicPartitions, TopicReplicationFactor, map[string]*string{
		"retention.ms": &retentionMS,
	}, topic)
	if err != nil {
		return fmt.Errorf("create Kafka topic %q: %w", topic, err)
	}
	response, ok := created[topic]
	if !ok {
		return fmt.Errorf("create Kafka topic %q: broker returned no topic response", topic)
	}
	if response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create Kafka topic %q: %w", topic, response.Err)
	}
	altered, err := p.admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{{
		Op:    kadm.SetConfig,
		Name:  "retention.ms",
		Value: &retentionMS,
	}}, topic)
	if err != nil {
		return fmt.Errorf("set Kafka topic %q retention: %w", topic, err)
	}
	retentionConfigured := false
	for _, alteration := range altered {
		if alteration.Name != topic {
			continue
		}
		retentionConfigured = true
		if alteration.Err != nil {
			return fmt.Errorf("set Kafka topic %q retention: %w", topic, alteration.Err)
		}
	}
	if !retentionConfigured {
		return fmt.Errorf("set Kafka topic %q retention: broker returned no topic response", topic)
	}

	descriptions, err := p.admin.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("describe Kafka topic %q: %w", topic, err)
	}
	description, ok := descriptions[topic]
	if !ok {
		return fmt.Errorf("describe Kafka topic %q: broker returned no topic metadata", topic)
	}
	if description.Err != nil {
		return fmt.Errorf("describe Kafka topic %q: %w", topic, description.Err)
	}
	if len(description.Partitions) != TopicPartitions {
		return fmt.Errorf("Kafka topic %q has %d partitions, want %d", topic, len(description.Partitions), TopicPartitions)
	}
	for partitionID, partition := range description.Partitions {
		if partition.Err != nil {
			return fmt.Errorf("describe Kafka topic %q partition %d: %w", topic, partitionID, partition.Err)
		}
		if len(partition.Replicas) != TopicReplicationFactor {
			return fmt.Errorf("Kafka topic %q partition %d has replication factor %d, want %d", topic, partitionID, len(partition.Replicas), TopicReplicationFactor)
		}
	}
	return nil
}

func (p *Producer) Close() error {
	p.client.Close()
	return nil
}
