package transport

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/raviqlahadi/pulsecheck/internal/domain"
)

// Producer publishes Check messages to a Kafka topic.
type Producer interface {
	Publish(ctx context.Context, check domain.Check) error
	Close() error
}

// Consumer reads Check messages from a Kafka topic.
type Consumer interface {
	Subscribe(ctx context.Context, handler func(check domain.Check) error) error
	Close() error
}

// KafkaProducer is a stub Kafka producer that satisfies the Producer interface.
// Replace the body of Publish with a real Kafka client (e.g. franz-go or confluent-kafka-go).
type KafkaProducer struct {
	brokers []string
	topic   string
}

// NewKafkaProducer creates a new KafkaProducer.
func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{brokers: brokers, topic: topic}
}

// Publish serialises check and sends it to the configured Kafka topic.
func (p *KafkaProducer) Publish(_ context.Context, check domain.Check) error {
	payload, err := json.Marshal(check)
	if err != nil {
		return fmt.Errorf("kafka producer: marshal check: %w", err)
	}
	// TODO: send payload to Kafka topic p.topic using a real client.
	_ = payload
	return nil
}

// Close releases any resources held by the producer.
func (p *KafkaProducer) Close() error {
	return nil
}

// KafkaConsumer is a stub Kafka consumer that satisfies the Consumer interface.
// Replace the body of Subscribe with a real Kafka client.
type KafkaConsumer struct {
	brokers []string
	topic   string
	groupID string
}

// NewKafkaConsumer creates a new KafkaConsumer.
func NewKafkaConsumer(brokers []string, topic, groupID string) *KafkaConsumer {
	return &KafkaConsumer{brokers: brokers, topic: topic, groupID: groupID}
}

// Subscribe begins consuming messages from the topic, calling handler for each one.
func (c *KafkaConsumer) Subscribe(ctx context.Context, handler func(domain.Check) error) error {
	// TODO: poll messages from Kafka using a real client and call handler for each.
	<-ctx.Done()
	return ctx.Err()
}

// Close releases any resources held by the consumer.
func (c *KafkaConsumer) Close() error {
	return nil
}
