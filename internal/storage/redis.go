package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/raviqlahadi/pulsecheck/internal/config"
	"github.com/raviqlahadi/pulsecheck/internal/domain"
	"github.com/redis/go-redis/v9"
)

// Store defines the persistence operations for Check results.
type Store interface {
	SaveLatestCheck(ctx context.Context, check domain.HealthCheck) error
	IncrementFailureCount(ctx context.Context, url string) error
	GetLatestStatus(ctx context.Context) (map[string]domain.HealthCheck, error)
	Close() error
}

// RedisStore is a stub Redis-backed Store.
// Replace the field and method bodies with a real Redis client (e.g. go-redis).
type RedisStore struct {
	client *redis.Client // uncomment when using a real Redis client
}

// NewRedisStore creates a new RedisStore.
func NewRedisStore(cfg *config.Config) *RedisStore {
	return &RedisStore{
		client: redis.NewClient(&redis.Options{
			Addr: cfg.RedisAddr,
		}),
	}
}

// SaveLatestCheck updates the current status of a service in a Hash
func (r *RedisStore) SaveLatestCheck(ctx context.Context, check domain.HealthCheck) error {
	data, err := json.Marshal(check)
	if err != nil {
		return fmt.Errorf("redis store: marshal check %q: %w", check.URL, err)
	}

	return r.client.HSet(ctx, "pulsecheck:latest", check.URL, data).Err()
}

// IncrementFailureCount adds to a persistent failure counter for a specific URL
func (r *RedisStore) IncrementFailureCount(ctx context.Context, url string) error {
	return r.client.Incr(ctx, "pulsecheck:failures:"+url).Err()
}

// GetLatestStatus retrieves all latest checks for the dashboard
func (r *RedisStore) GetLatestStatus(ctx context.Context) (map[string]domain.HealthCheck, error) {
	result, err := r.client.HGetAll(ctx, "pulsecheck:latest").Result()
	if err != nil {
		return nil, fmt.Errorf("redis store: get latest status: %w", err)
	}

	checks := make(map[string]domain.HealthCheck)
	for url, data := range result {
		var check domain.HealthCheck
		if err := json.Unmarshal([]byte(data), &check); err != nil {
			return nil, fmt.Errorf("redis store: unmarshal check for url %q: %w", url, err)
		}
		checks[url] = check
	}

	return checks, nil
}

// Close releases the Redis connection pool.
func (r *RedisStore) Close() error {
	return r.client.Close()
}
