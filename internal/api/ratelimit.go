package api

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// limiterStore holds one token bucket per key (client IP), created lazily on
// first use and reaped periodically so a long-running process doesn't
// accumulate one bucket per distinct IP forever (a real concern for a
// public-facing auth endpoint under scanning/abuse traffic).
type limiterStore struct {
	mu       sync.Mutex
	limiters map[string]*rateEntry
	r        rate.Limit
	b        int
}

type rateEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// newLimiterStore builds a store where each key gets its own token bucket
// refilling at r tokens/sec with burst capacity b.
func newLimiterStore(r rate.Limit, b int) *limiterStore {
	s := &limiterStore{limiters: make(map[string]*rateEntry), r: r, b: b}
	go s.reapLoop()
	return s
}

func (s *limiterStore) allow(key string) bool {
	s.mu.Lock()
	entry, ok := s.limiters[key]
	if !ok {
		entry = &rateEntry{limiter: rate.NewLimiter(s.r, s.b)}
		s.limiters[key] = entry
	}
	entry.lastSeen = time.Now()
	limiter := entry.limiter
	s.mu.Unlock()
	return limiter.Allow()
}

// reapLoop drops buckets idle for more than 10 minutes, so memory stays
// bounded to roughly the number of distinct IPs seen in the last 10 minutes
// rather than growing for the lifetime of the process.
func (s *limiterStore) reapLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-10 * time.Minute)
		s.mu.Lock()
		for k, e := range s.limiters {
			if e.lastSeen.Before(cutoff) {
				delete(s.limiters, k)
			}
		}
		s.mu.Unlock()
	}
}

// RateLimiter wraps next with a per-client-IP token-bucket limiter. general
// governs most routes; strict (tighter) is applied additionally to the
// path prefixes in strictPaths — currently /auth/ and /admin/login, the
// endpoints an attacker would flood to exhaust the nonce cache or brute-force
// a password. A request that exceeds either bucket gets 429 with a
// Retry-After hint instead of reaching the handler.
//
// In-memory, per-process: this deployment runs one instance per service (see
// run.sh), so a shared/distributed limiter (Redis-backed) would be
// unnecessary complexity for no real benefit right now — revisit if this
// service is ever horizontally scaled.
type RateLimiter struct {
	general *limiterStore
	strict  *limiterStore
	ipFunc  func(*http.Request) string
}

// NewRateLimiter builds a limiter using ipFunc to key buckets — pass
// (*Server).clientIP so the same trusted-proxy-aware IP resolution used for
// session logging is used here too, instead of raw RemoteAddr.
func NewRateLimiter(ipFunc func(*http.Request) string) *RateLimiter {
	return &RateLimiter{
		// General: 20 requests/sec sustained, burst of 40 — generous enough
		// that no legitimate user of this API (wallet balance polling, order
		// placement, etc.) would ever notice it.
		general: newLimiterStore(20, 40),
		// Strict: 1 request every 2 seconds sustained, burst of 5 — enough
		// for a real user to retry a typo'd password or a dropped nonce
		// request a few times in a row, but nowhere near enough to flood the
		// nonce cache or brute-force a login.
		strict: newLimiterStore(0.5, 5),
		ipFunc: ipFunc,
	}
}

var strictPathPrefixes = []string{"/auth/", "/admin/login"}

func isStrictPath(path string) bool {
	for _, p := range strictPathPrefixes {
		if len(path) >= len(p) && path[:len(p)] == p {
			return true
		}
	}
	return false
}

func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := rl.ipFunc(r)
		if host, _, err := net.SplitHostPort(key); err == nil {
			key = host // bucket per IP, not per IP:port
		}
		if isStrictPath(r.URL.Path) && !rl.strict.allow(key) {
			w.Header().Set("Retry-After", "2")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		if !rl.general.allow(key) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
