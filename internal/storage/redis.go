package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/raviqlahadi/pulsecheck/internal/domain"
)

// Store defines the persistence operations for Check results.
type Store interface {
	Save(ctx context.Context, check domain.Check) error
	Get(ctx context.Context, id string) (domain.Check, error)
	Close() error
}

// RedisStore is a stub Redis-backed Store.
// Replace the field and method bodies with a real Redis client (e.g. go-redis).
type RedisStore struct {
	addr string
	ttl  time.Duration
	// client *redis.Client  // uncomment when using a real Redis client
}

// NewRedisStore creates a new RedisStore.
func NewRedisStore(addr string, ttl time.Duration) *RedisStore {
	return &RedisStore{addr: addr, ttl: ttl}
}

// Save serialises check and writes it to Redis under the key "check:<id>".
func (s *RedisStore) Save(_ context.Context, check domain.Check) error {
	payload, err := json.Marshal(check)
	if err != nil {
		return fmt.Errorf("redis store: marshal check: %w", err)
	}
	// TODO: s.client.Set(ctx, "check:"+check.ID, payload, s.ttl)
	_ = payload
	return nil
}

// Get retrieves a Check from Redis by its ID.
func (s *RedisStore) Get(_ context.Context, id string) (domain.Check, error) {
	// TODO: val, err := s.client.Get(ctx, "check:"+id).Result()
	return domain.Check{}, fmt.Errorf("redis store: get %q: not implemented", id)
}

// Close releases the Redis connection pool.
func (s *RedisStore) Close() error {
	return nil
}
