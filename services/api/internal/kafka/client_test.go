package kafka

import "testing"

func TestNewProducerRequiresBrokers(t *testing.T) {
	if _, err := NewProducer(nil); err == nil {
		t.Fatal("producer was created without brokers")
	}
}

func TestNewProducerDoesNotWaitForKafkaAvailability(t *testing.T) {
	producer, err := NewProducer([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatalf("create producer without an available broker: %v", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("close producer: %v", err)
	}
}
