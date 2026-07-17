package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	nonceTTL      = 5 * time.Minute
	nonceMax      = 100_000
	nonceReapEvery = 1 * time.Minute
)

type nonceEntry struct {
	value     string
	expiresAt time.Time
}

// NonceStore issues and consumes one-time sign-in challenges per wallet address.
// A background reaper evicts expired entries so the map cannot grow unbounded.
type NonceStore struct {
	mu      sync.Mutex
	entries map[string]nonceEntry
}

func NewNonceStore() *NonceStore {
	return &NonceStore{entries: make(map[string]nonceEntry)}
}

// Run periodically removes expired nonces until ctx is cancelled.
func (s *NonceStore) Run(ctx context.Context) {
	t := time.NewTicker(nonceReapEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reap()
		}
	}
}

func (s *NonceStore) reap() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.entries {
		if now.After(e.expiresAt) {
			delete(s.entries, k)
		}
	}
}

func (s *NonceStore) Create(address string) (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	nonce := hex.EncodeToString(buf)

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) >= nonceMax {
		// Evict any expired entries to make room; if none expired, refuse.
		now := time.Now()
		for k, e := range s.entries {
			if now.After(e.expiresAt) {
				delete(s.entries, k)
			}
		}
		if len(s.entries) >= nonceMax {
			return "", fmt.Errorf("nonce store at capacity")
		}
	}
	s.entries[strings.ToLower(address)] = nonceEntry{value: nonce, expiresAt: time.Now().Add(nonceTTL)}
	return nonce, nil
}

// Consume returns the nonce for address and deletes it, or an error if missing/expired.
func (s *NonceStore) Consume(address string) (string, error) {
	key := strings.ToLower(address)

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	delete(s.entries, key)
	if !ok {
		return "", fmt.Errorf("no pending nonce for address")
	}
	if time.Now().After(entry.expiresAt) {
		return "", fmt.Errorf("nonce expired")
	}
	return entry.value, nil
}

// SignMessage builds the exact message the wallet must sign.
func SignMessage(address, nonce string) string {
	return fmt.Sprintf("Sign this message to log in to Dex-AI.\n\nAddress: %s\nNonce: %s", address, nonce)
}
