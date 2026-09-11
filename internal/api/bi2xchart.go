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
package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
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

	bi2xUpstreamTimeout = 10 * time.Second
)

// BI2XChartProxy forwards GET/OPTIONS requests under /bi2x-chart/* to the
// BI2X feed's real UDF datafeed at {bi2xFeedBaseURL}/api/datafeed/*, purely
// to route around that server's missing CORS headers (see package doc
// comment). It does not interpret, cache, or modify the UDF response body —
// whatever shape the upstream feed returns (config/time/symbols/search/
// history) passes through unchanged, so this proxy needs no changes if the
// feed's own response shapes evolve.
func BI2XChartProxy(log *slog.Logger) http.HandlerFunc {
	return newBI2XChartProxy(bi2xFeedBaseURL, log)
}

// newBI2XChartProxy is the testable constructor: upstreamBase is injectable
// so tests can point it at an httptest server instead of the real feed.
func newBI2XChartProxy(upstreamBase string, log *slog.Logger) http.HandlerFunc {
	client := &http.Client{Timeout: bi2xUpstreamTimeout}

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

		ctx, cancel := context.WithTimeout(r.Context(), bi2xUpstreamTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "build upstream request: "+err.Error())
			return
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Warn("bi2x chart proxy: upstream request failed", "path", remainder, "error", err)
			writeError(w, http.StatusBadGateway, "bi2x feed unavailable")
			return
		}
		defer resp.Body.Close()

		// Passed through verbatim: the UDF adapter checks Content-Type itself
		// (JSON vs the feed's own CSV support, which this proxy also doesn't
		// need to know about) and TradingView's own error-shape convention
		// ({"s":"no_data",...} / {"s":"error",...}) is a 200 with a body the
		// library inspects, not an HTTP error status — so a non-2xx from
		// upstream is a genuine outage, not a normal "no data" response, and
		// gets logged as one.
		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Warn("bi2x chart proxy: copying upstream body failed", "path", remainder, "error", err)
		}
		if resp.StatusCode >= 500 {
			log.Warn("bi2x feed returned a server error", "path", remainder, "status", resp.StatusCode)
		}
	}
}
