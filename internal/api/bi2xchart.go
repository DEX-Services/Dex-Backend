// Package api: BI2X chart datafeed proxy.
//
// The BI2X data feed (https://bitdx-feed-jk3y.onrender.com) exposes a
// TradingView UDF-compatible datafeed at /api/datafeed/* — exactly the
// protocol the licensed Advanced Charting Library's Datafeeds.UDFCompatibleDatafeed
// adapter expects, so the frontend needs no custom datafeed code for BI2X the
// way it does for Binance (see src/lib/binanceDatafeed.ts).
//
// The one problem: that server sends no Access-Control-Allow-Origin header at
// all (confirmed via a real preflight request, 2026-09-12) — every request
// the browser makes to it directly is silently blocked by CORS, with nothing
// visible in this app's UI beyond a blank chart and a console error. We do
// not control that server, so the fix lives here instead: this backend
// forwards /bi2x-chart/* to the real feed server-to-server (no CORS applies
// between two servers) and the frontend points its UDF datafeed at THIS
// backend's URL instead of the feed's own domain. Nothing else about the UDF
// protocol changes — this is a transparent path+query passthrough.
//
// The upstream is also genuinely flaky under real traffic (confirmed live,
// 2026-09-23): the same /history request observed returning, at different
// times, a clean 200, a 500, a 503, a 429 with a Cloudflare "managed
// challenge" HTML page (it fronts the feed with bot protection that a plain
// server-to-server client can never pass), and outright connection timeouts
// on a cold Render instance. None of that is something we can fix upstream,
// so this proxy retries transient failures and caches successful responses
// briefly — both to ride out a flaky moment and to send fewer requests at an
// upstream that appears to rate-limit/challenge based on request volume.
package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	// bi2xFeedBaseURL is the real feed server this proxies to. Not made
	// configurable via env var: this is specific to one third-party
	// dependency for one specific asset, not a general integration point.
	bi2xFeedBaseURL = "https://bitdx-feed-jk3y.onrender.com"

	// bi2xProxyPrefix is the path prefix this backend exposes to the
	// frontend; everything after it is forwarded verbatim (path + query) to
	// bi2xFeedBaseURL + "/api/datafeed" + that remainder.
	bi2xProxyPrefix = "/bi2x-chart"

	// bi2xUpstreamTimeout is per-attempt. The feed is a free-tier Render
	// service: after idle it cold-starts, and the first request can take
	// 20s+ (observed live). A single 10s timeout turned every cold start
	// into an immediate failure, so use a longer window and retry below.
	bi2xUpstreamTimeout = 20 * time.Second

	// bi2xAttempts covers both transport failures (timeout, connection
	// refused) and a 5xx/429 response from upstream — both are treated as
	// transient given the flakiness observed above, and GET is safe to
	// re-issue.
	bi2xAttempts      = 3
	bi2xRetryDelay    = 700 * time.Millisecond
	bi2xCacheTTL      = 20 * time.Second
	bi2xCacheMaxEntry = 200
)

// bi2xCache holds recent successful (2xx) upstream responses keyed by the
// full upstream URL (path+query — history requests differ by resolution/
// countback/to, so each distinct query is cached separately). Every request
// for the SAME symbol/resolution/range within the TTL is served from here
// instead of hitting the flaky upstream again, which both rides out a
// flaky moment for other viewers of the same chart and reduces the request
// volume that appears to trigger the upstream's own rate limiting/
// Cloudflare challenge. Not correctness-critical if lost (process restart,
// eviction) — a cache miss just means the normal upstream fetch path runs.
type bi2xCache struct {
	mu      sync.Mutex
	entries map[string]bi2xCacheEntry
}

type bi2xCacheEntry struct {
	status      int
	contentType string
	body        []byte
	expiresAt   time.Time
}

func newBI2XCache() *bi2xCache {
	return &bi2xCache{entries: make(map[string]bi2xCacheEntry)}
}

func (c *bi2xCache) get(key string) (bi2xCacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		return bi2xCacheEntry{}, false
	}
	return e, true
}

func (c *bi2xCache) set(key string, e bi2xCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= bi2xCacheMaxEntry {
		// Simplest possible bound: drop everything rather than track LRU for
		// what is, at 200 entries × a TTL of 20s, a self-limiting cache
		// anyway — a full clear costs at most one extra round of upstream
		// fetches for whatever's in flight at that moment.
		c.entries = make(map[string]bi2xCacheEntry)
	}
	c.entries[key] = e
}

// BI2XChartProxy forwards GET/OPTIONS requests under /bi2x-chart/* to the
// BI2X feed's real UDF datafeed at {bi2xFeedBaseURL}/api/datafeed/*, purely
// to route around that server's missing CORS headers (see package doc
// comment). It does not interpret, cache eviction aside, or modify the UDF
// response body — whatever shape the upstream feed returns (config/time/
// symbols/search/history) passes through unchanged, so this proxy needs no
// changes if the feed's own response shapes evolve.
func BI2XChartProxy(log *slog.Logger) http.HandlerFunc {
	return newBI2XChartProxy(bi2xFeedBaseURL, log)
}

// newBI2XChartProxy is the testable constructor: upstreamBase is injectable
// so tests can point it at an httptest server instead of the real feed.
func newBI2XChartProxy(upstreamBase string, log *slog.Logger) http.HandlerFunc {
	client := &http.Client{Timeout: bi2xUpstreamTimeout}
	cache := newBI2XCache()

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeError(w, http.StatusMethodNotAllowed, "GET only")
			return
		}

		// Strip our prefix, keep everything after it (including a leading
		// slash for the specific sub-route: /config, /time, /symbols, /search,
		// /history) and re-root it under the upstream's own /api/datafeed
		// path. TrimPrefix rather than a routing table because the UDF
		// protocol's own set of sub-routes is what defines what's valid here —
		// duplicating that list would just be another place for it to drift.
		remainder := strings.TrimPrefix(r.URL.Path, bi2xProxyPrefix)
		upstreamURL := upstreamBase + "/api/datafeed" + remainder
		if r.URL.RawQuery != "" {
			upstreamURL += "?" + r.URL.RawQuery
		}

		if e, ok := cache.get(upstreamURL); ok {
			if e.contentType != "" {
				w.Header().Set("Content-Type", e.contentType)
			}
			w.WriteHeader(e.status)
			_, _ = w.Write(e.body)
			return
		}

		var (
			status      int
			contentType string
			body        []byte
			lastErr     error
		)
		for attempt := 0; attempt < bi2xAttempts; attempt++ {
			if attempt > 0 {
				time.Sleep(bi2xRetryDelay)
			}
			ctx, cancel := context.WithTimeout(r.Context(), bi2xUpstreamTimeout)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
			if err != nil {
				cancel()
				writeError(w, http.StatusInternalServerError, "build upstream request: "+err.Error())
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				cancel()
				lastErr = err
				log.Warn("bi2x chart proxy: upstream request failed", "path", remainder, "attempt", attempt+1, "error", err)
				continue
			}
			b, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			cancel()
			if readErr != nil {
				lastErr = readErr
				log.Warn("bi2x chart proxy: reading upstream body failed", "path", remainder, "attempt", attempt+1, "error", readErr)
				continue
			}
			status = resp.StatusCode
			contentType = resp.Header.Get("Content-Type")
			body = b
			lastErr = nil
			// A 5xx or 429 is treated the same as a transport failure here —
			// both are the transient upstream flakiness this retry exists
			// for (see package doc comment) — everything else (2xx, and any
			// other 4xx, which is a genuine client-side/query problem, not
			// transient) is final.
			if status >= 500 || status == http.StatusTooManyRequests {
				log.Warn("bi2x feed returned a retryable error", "path", remainder, "attempt", attempt+1, "status", status)
				continue
			}
			break
		}

		if lastErr != nil {
			log.Warn("bi2x chart proxy: all attempts failed", "path", remainder, "error", lastErr)
			writeError(w, http.StatusBadGateway, "bi2x feed unavailable")
			return
		}
		if status >= 500 || status == http.StatusTooManyRequests {
			// Exhausted retries on a retryable status — pass the last
			// response through rather than masking it as a different error.
			log.Warn("bi2x feed still erroring after retries", "path", remainder, "status", status)
		}

		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		if _, err := w.Write(body); err != nil {
			log.Warn("bi2x chart proxy: writing response failed", "path", remainder, "error", err)
		}

		// Cache only genuine success — a cached error would keep serving
		// that error to every viewer of this chart for the TTL, which is
		// worse than just re-fetching (see history's transient-error
		// framing above).
		if status >= 200 && status < 300 {
			cache.set(upstreamURL, bi2xCacheEntry{
				status: status, contentType: contentType, body: body,
				expiresAt: time.Now().Add(bi2xCacheTTL),
			})
		}
	}
}
