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
	"github.com/raviqlahadi/pulsecheck/internal/storage"
	"github.com/raviqlahadi/pulsecheck/internal/transport"
)

func main() {
	brokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	topic := envOrDefault("KAFKA_TOPIC", "pulse-checks")
	groupID := envOrDefault("KAFKA_GROUP_ID", "pulsecheck-consumer")
	redisAddr := envOrDefault("REDIS_ADDR", "localhost:6379")
	redisTTL := 24 * time.Hour

	consumer := transport.NewKafkaConsumer(strings.Split(brokers, ","), topic, groupID)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("consumer close: %v", err)
		}
	}()

	store := storage.NewRedisStore(redisAddr, redisTTL)
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("store close: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("consumer started (brokers=%s topic=%s group=%s redis=%s)",
		brokers, topic, groupID, redisAddr)

	if err := consumer.Subscribe(ctx, func(check domain.Check) error {
		if err := store.Save(ctx, check); err != nil {
			log.Printf("store save error: %v", err)
			return err
		}
		log.Printf("stored check id=%s url=%s status=%s", check.ID, check.URL, check.Status)
		return nil
	}); err != nil && err != context.Canceled {
		log.Printf("consumer error: %v", err)
	}

	log.Println("consumer shutting down")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
