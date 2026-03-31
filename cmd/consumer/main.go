package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/raviqlahadi/pulsecheck/internal/config"
	"github.com/raviqlahadi/pulsecheck/internal/domain"
	"github.com/raviqlahadi/pulsecheck/internal/storage"
	"github.com/raviqlahadi/pulsecheck/internal/transport"
)

func main() {
	cfg := config.Load()

	repo := storage.NewRedisStore(cfg)
	log.Printf("Connected to Redis at %s", cfg.RedisAddr)

	consumer := transport.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, "pulsecheck-consumers")
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Consumer close error: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("Consumer started. Listening to: %v | Topic: %s", cfg.KafkaBrokers, cfg.KafkaTopic)

	handler := func(check domain.HealthCheck) error {
		if err := repo.SaveLatestCheck(ctx, check); err != nil {
			return err
		}

		if check.Status == domain.StatusDown {
			if err := repo.IncrementFailureCount(ctx, check.URL); err != nil {
				log.Printf("Failed to increment failure for %s: %v", check.URL, err)
			}
			log.Printf("ALERT: %s is DOWN (Status: %d)", check.URL, check.StatusCode)
		} else {
			log.Printf("OK: %s (%dms)", check.URL, check.ResponseTimeMs)
		}

		return nil
	}

	if err := consumer.Subscribe(ctx, handler); err != nil {
		if err == context.Canceled {
			log.Printf("Consumer shutting down gracefully...")
		} else {
			log.Fatalf("Consumer loop error: %v", err)
		}
	}
}
