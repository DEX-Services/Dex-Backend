// Package sessions tracks active login sessions in Redis so a logout (or
// future admin-initiated revocation) can invalidate a JWT before its natural
// expiry, which a stateless JWT alone cannot do (M8).
package sessions

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	client *redis.Client
}

func New(redisURL string) (*Store, error) {
	if redisURL == "" {
		return nil, fmt.Errorf("sessions: REDIS_SERVICE_URI not set")
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("sessions: parse redis url: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("sessions: redis ping: %w", err)
	}
	return &Store{client: client}, nil
}

func key(sessionID string) string {
	return "session:active:" + sessionID
}

// Activate marks sessionID valid until ttl elapses (matching the JWT's own
// expiry), so an untouched key naturally falls out of Redis at the same time
// the token would have expired anyway.
func (s *Store) Activate(ctx context.Context, sessionID string, ttl time.Duration) error {
	return s.client.Set(ctx, key(sessionID), "1", ttl).Err()
}

// Revoke removes sessionID immediately, e.g. on logout.
func (s *Store) Revoke(ctx context.Context, sessionID string) error {
	return s.client.Del(ctx, key(sessionID)).Err()
}

// IsActive reports whether sessionID is still valid. Callers should treat a
// lookup error as "not active" (fail closed) rather than letting a Redis
// outage silently disable revocation checks.
func (s *Store) IsActive(ctx context.Context, sessionID string) (bool, error) {
	n, err := s.client.Exists(ctx, key(sessionID)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
