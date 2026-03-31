package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/raviqlahadi/pulsecheck/internal/config"
	"github.com/raviqlahadi/pulsecheck/internal/domain"
	"github.com/raviqlahadi/pulsecheck/internal/transport"
)

func main() {
	cfg := config.Load()

	producer := transport.NewKafkaProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("producer close: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("Producer started. Broking to: %v | Topic: %s", cfg.KafkaBrokers, cfg.KafkaTopic)

	urls := []string{
		"https://google.com",
		"https://github.com",
		"https://go.dev",
	}

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Producer shutting down...")
			return
		case <-ticker.C:
			for _, url := range urls {
				check := pingURL(url)
				if err := producer.Publish(ctx, check); err != nil {
					log.Printf("Publish error for %s: %v", url, err)
				} else {
					log.Printf("[%s] Status: %s | Latency: %dms", check.URL, check.Status, check.ResponseTimeMs)
				}
			}
		}
	}

}

func pingURL(url string) domain.HealthCheck {
	start := time.Now()

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	duration := time.Since(start).Microseconds()
	check := domain.HealthCheck{
		URL:            url,
		CheckedAt:      time.Now(),
		ResponseTimeMs: duration,
		Status:         domain.StatusDown,
	}

	if err != nil {
		check.Error = err.Error()
		return check
	}

	defer resp.Body.Close()

	check.StatusCode = resp.StatusCode
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		check.Status = domain.StatusUp
	}

	return check
}
