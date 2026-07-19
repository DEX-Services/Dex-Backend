package api

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// RateLimiter is a per-client token-bucket limiter. Buckets are keyed by
// client IP (honoring the trusted-proxy X-Forwarded-For rules) and refill at
// rate tokens per interval up to burst.
type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rate     float64 // tokens added per second
	burst    float64
	lastGC   time.Time
	trusted  string
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewRateLimiter allows `requests` per `per` with a burst of `burst`.
// trustedProxy mirrors Server.TrustedProxy for X-Forwarded-For handling.
func NewRateLimiter(requests int, per time.Duration, burst int, trustedProxy string) *RateLimiter {
	return &RateLimiter{
		buckets: make(map[string]*bucket),
		rate:    float64(requests) / per.Seconds(),
		burst:   float64(burst),
		lastGC:  time.Now(),
		trusted: trustedProxy,
	}
}

func (rl *RateLimiter) key(r *http.Request) string {
	addr := r.RemoteAddr
	if rl.trusted != "" && (addr == rl.trusted || strings.HasPrefix(addr, rl.trusted)) {
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			return strings.TrimSpace(strings.Split(fwd, ",")[0])
		}
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// Allow consumes a token for the request's client, reporting whether the
// request may proceed.
func (rl *RateLimiter) Allow(r *http.Request) bool {
	key := rl.key(r)
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Periodically drop idle buckets so the map cannot grow unboundedly.
	if now.Sub(rl.lastGC) > 10*time.Minute {
		for k, b := range rl.buckets {
			if now.Sub(b.last) > 10*time.Minute {
				delete(rl.buckets, k)
			}
		}
		rl.lastGC = now
	}

	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: rl.burst}
		rl.buckets[key] = b
	} else {
		b.tokens += now.Sub(b.last).Seconds() * rl.rate
		if b.tokens > rl.burst {
			b.tokens = rl.burst
		}
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Wrap applies the limiter to a handler, returning 429 when exhausted.
func (rl *RateLimiter) Wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !rl.Allow(r) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next(w, r)
	}
}
