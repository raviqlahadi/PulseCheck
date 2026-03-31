package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/raviqlahadi/pulsecheck/internal/domain"
	"github.com/raviqlahadi/pulsecheck/internal/transport"
)

func main() {
	brokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	topic := envOrDefault("KAFKA_TOPIC", "pulse-checks")

	producer := transport.NewKafkaProducer(strings.Split(brokers, ","), topic)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("producer close: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("producer started (brokers=%s topic=%s)", brokers, topic)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("producer shutting down")
			return
		case <-ticker.C:
			check := domain.Check{
				ID:  "example-check",
				URL: "https://example.com",
			}
			if err := producer.Publish(ctx, check); err != nil {
				log.Printf("publish error: %v", err)
			} else {
				log.Printf("published check id=%s url=%s", check.ID, check.URL)
			}
		}
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
