//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"MicroBlog/internal/kafkatest"
)

const testTopicName = "microblog.events"

func TestEnsureTopicDisablesRetention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, broker := kafkatest.Start(t, ctx)

	producer, err := NewProducer([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = producer.Close() })
	if err := producer.EnsureTopic(ctx, testTopicName); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	configs, err := producer.admin.DescribeTopicConfigs(ctx, testTopicName)
	if err != nil {
		t.Fatalf("describe topic configs: %v", err)
	}
	for _, resource := range configs {
		if resource.Name != testTopicName {
			continue
		}
		if resource.Err != nil {
			t.Fatalf("describe topic config: %v", resource.Err)
		}
		for _, config := range resource.Configs {
			if config.Key == "retention.ms" && config.MaybeValue() == "-1" {
				return
			}
		}
		t.Fatalf("topic %q retention.ms is not unlimited", testTopicName)
	}
	t.Fatalf("Kafka returned no config for topic %q", testTopicName)
}
