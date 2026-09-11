package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// The whole point of this proxy: the frontend calls OUR origin, we forward
// server-to-server, and the exact path/query the UDF adapter asked for
// arrives at the upstream unchanged.
func TestBI2XChartProxy_ForwardsPathAndQuery(t *testing.T) {
	var gotPath, gotQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"s":"ok","t":[1],"o":[1],"h":[1],"l":[1],"c":[1],"v":[1]}`))
	}))
	defer upstream.Close()

	handler := newBI2XChartProxy(upstream.URL, testLogger())
	req := httptest.NewRequest(http.MethodGet, "/bi2x-chart/history?symbol=BI2X&resolution=1&from=1&to=2", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotPath != "/api/datafeed/history" {
		t.Errorf("upstream path = %q, want /api/datafeed/history", gotPath)
	}
	if gotQuery != "symbol=BI2X&resolution=1&from=1&to=2" {
		t.Errorf("upstream query = %q", gotQuery)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json (passed through)", ct)
	}
}

// The /config sub-route (no query params) must map to exactly
// /api/datafeed/config, not something with a stray trailing slash or
// double prefix.
func TestBI2XChartProxy_ConfigRoute(t *testing.T) {
	var gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"supports_search":true}`))
	}))
	defer upstream.Close()

	handler := newBI2XChartProxy(upstream.URL, testLogger())
	req := httptest.NewRequest(http.MethodGet, "/bi2x-chart/config", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if gotPath != "/api/datafeed/config" {
		t.Errorf("upstream path = %q, want /api/datafeed/config", gotPath)
	}
	if rec.Body.String() != `{"supports_search":true}` {
		t.Errorf("body = %q, want passthrough of upstream response", rec.Body.String())
	}
}

// TradingView's own "no data" convention is a 200 with a body the UDF
// adapter inspects (s:"no_data"), not an HTTP error — the proxy must not
// mistake that for a failure and must pass the 200 + body through exactly.
func TestBI2XChartProxy_PassesThroughNoDataResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"s":"no_data","nextTime":1788647843}`))
	}))
	defer upstream.Close()

	handler := newBI2XChartProxy(upstream.URL, testLogger())
	req := httptest.NewRequest(http.MethodGet, "/bi2x-chart/history?symbol=BI2X&resolution=1&from=0&to=1", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (no_data is not an HTTP error)", rec.Code)
	}
	if rec.Body.String() != `{"s":"no_data","nextTime":1788647843}` {
		t.Errorf("body = %q, want the no_data envelope passed through unchanged", rec.Body.String())
	}
}

// A real upstream outage (connection refused, DNS failure, etc.) must
// surface as 502, not be silently swallowed as an empty 200.
func TestBI2XChartProxy_UpstreamUnreachable(t *testing.T) {
	// A closed server guarantees connection refused.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	upstream.Close()

	handler := newBI2XChartProxy(upstream.URL, testLogger())
	req := httptest.NewRequest(http.MethodGet, "/bi2x-chart/time", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 on upstream failure", rec.Code)
	}
}

// A genuine 5xx from the feed itself (as opposed to a connection failure)
// must also pass through as-is, not be masked as a 200.
func TestBI2XChartProxy_UpstreamServerError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("down for maintenance"))
	}))
	defer upstream.Close()

	handler := newBI2XChartProxy(upstream.URL, testLogger())
	req := httptest.NewRequest(http.MethodGet, "/bi2x-chart/symbols?symbol=BI2X", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 passed through from upstream", rec.Code)
	}
}

// Non-GET methods must be rejected — this proxy has no business accepting
// writes to a third-party read-only feed.
func TestBI2XChartProxy_RejectsNonGET(t *testing.T) {
	handler := newBI2XChartProxy("http://unused.invalid", testLogger())
	req := httptest.NewRequest(http.MethodPost, "/bi2x-chart/history", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 for POST", rec.Code)
	}
}
