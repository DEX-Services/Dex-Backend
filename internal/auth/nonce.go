package auth

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

const nonceTTL = 5 * time.Minute

type nonceEntry struct {
	value     string
	expiresAt time.Time
}

// NonceStore issues and consumes one-time sign-in challenges per wallet address.
type NonceStore struct {
	mu      sync.Mutex
	entries map[string]nonceEntry
}

func NewNonceStore() *NonceStore {
	return &NonceStore{entries: make(map[string]nonceEntry)}
}

func (s *NonceStore) Create(address string) (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	nonce := hex.EncodeToString(buf)

	s.mu.Lock()
	defer s.mu.Unlock()
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
