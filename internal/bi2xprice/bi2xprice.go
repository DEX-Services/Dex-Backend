// Package bi2xprice reads BI2X's current BIUSDB price for the fee-tier
// purchase flow (see FEE-TIER-SYSTEM-PLAN.md). BI2X floats against BIUSDB —
// confirmed live (currently ~3.75, moving continuously) — so a tier's fixed
// BIUSDB value must be converted to a BI2X quantity using a fresh price at
// purchase time, never a hardcoded or cached-indefinitely number.
//
// This calls the BI2X feed's own plain tick endpoint directly (the same
// upstream internal/api/bi2xchart.go proxies for charting) rather than
// standing up a Redis client in this service purely for one read — Redis
// would duplicate Price-Fetcher's own polling loop for no benefit here.
package bi2xprice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

// feedURL is the BI2X feed's plain tick endpoint (see
// BI2X-DATAFEED-CHART-SPEC.md) — not the UDF chart path, which is shaped for
// TradingView, not a single current-price read.
const feedURL = "https://bitdx-feed-jk3y.onrender.com/tick"

// maxAge bounds how stale a price reading may be before it's rejected rather
// than used to compute a purchase — see FEE-TIER-SYSTEM-PLAN.md's guard
// against purchasing against a stale price.
const maxAge = 30 * time.Second

// tickResponse mirrors the feed's /tick response shape. Timestamp is sent as
// a JSON string (confirmed live, e.g. "1789196273000"), not a number.
type tickResponse struct {
	Rate      string `json:"rate"`
	Timestamp string `json:"timestamp"` // unix millis, as a string
}

// Reader fetches BI2X's current price on demand. A tiny interface (not just
// a bare function) so callers can substitute a fake in tests without a live
// network call.
type Reader interface {
	CurrentPrice(ctx context.Context) (decimal.Decimal, error)
}

// HTTPReader is the real Reader, backed by an http.Client with a short
// timeout — a purchase request should fail fast rather than hang if the
// third-party feed is slow or down.
type HTTPReader struct {
	Client *http.Client
}

// NewHTTPReader creates a Reader with a sensible request timeout.
func NewHTTPReader() *HTTPReader {
	return &HTTPReader{Client: &http.Client{Timeout: 5 * time.Second}}
}

// CurrentPrice fetches BI2X's current BIUSDB price. Returns an error if the
// feed is unreachable, returns a malformed response, or the reading is
// older than maxAge — a purchase must never be computed against a stale or
// missing price.
func (h *HTTPReader) CurrentPrice(ctx context.Context) (decimal.Decimal, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return decimal.Zero, fmt.Errorf("build BI2X price request: %w", err)
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return decimal.Zero, fmt.Errorf("fetch BI2X price: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return decimal.Zero, fmt.Errorf("BI2X price feed returned status %d", resp.StatusCode)
	}
	var tick tickResponse
	if err := json.NewDecoder(resp.Body).Decode(&tick); err != nil {
		return decimal.Zero, fmt.Errorf("decode BI2X price response: %w", err)
	}
	rate, err := decimal.NewFromString(tick.Rate)
	if err != nil || !rate.IsPositive() {
		return decimal.Zero, fmt.Errorf("BI2X price feed returned an invalid rate %q", tick.Rate)
	}
	timestampMs, err := strconv.ParseInt(tick.Timestamp, 10, 64)
	if err != nil {
		return decimal.Zero, fmt.Errorf("BI2X price feed returned an invalid timestamp %q", tick.Timestamp)
	}
	age := time.Since(time.UnixMilli(timestampMs))
	if age > maxAge || age < -maxAge {
		return decimal.Zero, fmt.Errorf("BI2X price reading is stale (age %s)", age)
	}
	return rate, nil
}
