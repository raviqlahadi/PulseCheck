package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/raviqlahadi/pulsecheck/internal/domain"
	"github.com/segmentio/kafka-go"
)

// Producer publishes Check messages to a Kafka topic.
type Producer interface {
	Publish(ctx context.Context, check domain.HealthCheck) error
	Close() error
}

// Consumer reads Check messages from a Kafka topic.
type Consumer interface {
	Subscribe(ctx context.Context, handler func(check domain.HealthCheck) error) error
	Close() error
}

// KafkaProducer is a stub Kafka producer.
type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, check domain.HealthCheck) error {
	payload, err := json.Marshal(check)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(check.URL), // Use URL as the key for partitioning
		Value: payload,
	})
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

// KafkaConsumer is a stub Kafka consumer.
type KafkaConsumer struct {
	reader *kafka.Reader
}

func NewKafkaConsumer(brokers []string, topic string, groupID string) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
			// Optional: add a small wait to avoid aggressive CPU usage on empty topics
			MaxWait: 1 * time.Second,
		}),
	}
}

func (c *KafkaConsumer) Subscribe(ctx context.Context, handler func(check domain.HealthCheck) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			m, err := c.reader.ReadMessage(ctx)
			if err != nil {
				return fmt.Errorf("kafka consumer: read: %w", err)
			}

			var check domain.HealthCheck
			if err := json.Unmarshal(m.Value, &check); err != nil {
				continue // Skip malformed messages
			}

			if err := handler(check); err != nil {
				fmt.Printf("Handler error: %v\n", err)
			}
		}
	}

}

func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}
