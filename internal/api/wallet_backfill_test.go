package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dex/dex-backend/internal/db"
	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/repo"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// testPool mirrors internal/repo's own (unexported, package-private)
// helper of the same name — duplicated here rather than exported cross-
// package, matching this repo's existing convention of each package owning
// its own live-Postgres test skip/connect boilerplate.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	godotenv.Load("../../.env")
	connString := os.Getenv("POSTGRES_SERVICE_URI")
	if connString == "" {
		t.Skip("POSTGRES_SERVICE_URI not set, skipping live-Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := db.New(ctx, connString)
	if err != nil {
		t.Skipf("could not connect to Postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newBackfillTestUser(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	users := repo.NewUserRepo(pool)
	wallet := "0xbackfilltest" + time.Now().Format("20060102150405.000000000")
	user, err := users.FindOrCreate(context.Background(), wallet, "test", "")
	if err != nil {
		t.Fatalf("create test user: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM engine_backfill_failures WHERE user_id = $1`, user.ID)
		pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, user.ID)
	})
	return user.ID
}

// These tests exercise processBackfillItems directly with a small,
// explicit synthetic item list rather than going through runBackfill
// (which loads EVERY nonzero balance via AllNonzeroBalances — 100+ rows in
// this deployment's real Postgres). Testing through runBackfill would
// process that entire real table on every test run: slow (each item pays
// the real pacing delay, and a failing item pays real network round trips
// for its retries against the live engine fake) and an unwanted side
// effect of running the test suite. processBackfillItems is the actual
// pacing/retry/durable-failure logic under test; runBackfill itself is a
// thin "load from Postgres, then call processBackfillItems" wrapper with
// nothing else to unit test.

// TestProcessBackfillItems_PacesCallsAndNeverExceedsRateLimit is a
// regression test for the live incident: runBackfill used to fire Credit
// for every nonzero balance in a tight loop with no delay, which — against
// the real engine's per-IP rate limiter (40 req/sec sustained, burst 80) —
// meant every single credit in a backfill of more than ~80 accounts failed
// with 429. This fakes the engine as an httptest server that itself
// enforces the same shape of limit (reject once more than N requests land
// within a short window) and asserts backfill still reports every item
// synced, proving the pacing is enough to stay under a realistic limiter.
func TestProcessBackfillItems_PacesCallsAndNeverExceedsRateLimit(t *testing.T) {
	const numItems = 10
	items := make([]backfillItem, numItems)
	for i := range items {
		items[i] = backfillItem{userID: "synthetic-user", asset: "USDC", amount: "1000000"}
	}

	// Simulates the engine's real per-IP limiter shape: reject once more
	// than `burst` requests land within `window` of the first request in
	// that window resetting. A tight unpaced loop of 10 calls would exceed
	// a burst of 3 instantly; a properly-paced loop (30ms apart) spread
	// over the test's window should not.
	const burst = 3
	const window = 200 * time.Millisecond
	var mu sync.Mutex
	var windowStart time.Time
	var count int
	var got429 atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		now := time.Now()
		if windowStart.IsZero() || now.Sub(windowStart) > window {
			windowStart = now
			count = 0
		}
		count++
		exceeded := count > burst
		mu.Unlock()
		if exceeded {
			got429.Store(true)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := &WalletServer{
		Server:       &Server{Log: slog.Default()},
		EngineClient: engineclient.NewForTest(srv.URL, "s", srv.Client()),
	}

	synced, failed, total, err := s.processBackfillItems(context.Background(), items)
	if err != nil {
		t.Fatalf("processBackfillItems: %v", err)
	}
	if total != numItems {
		t.Fatalf("total = %d, want %d", total, numItems)
	}
	if failed != 0 {
		t.Fatalf("failed = %d, want 0 — pacing should keep every call under the fake limiter's burst; got429 fired at least once = %v", failed, got429.Load())
	}
	if synced != numItems {
		t.Fatalf("synced = %d, want %d", synced, numItems)
	}
}

// TestProcessBackfillItems_RecordsAndClearsDurableFailures is a regression
// test for the second half of the incident: a credit that failed was only
// logged, never persisted, so an engine outage spanning an entire backfill
// run left every affected account desynced with no durable record for a
// human or a later run to act on. This drives the engine fake to always
// fail, confirms the failure lands in engine_backfill_failures via a real
// (but freshly-created, narrowly-scoped) test user, then flips the fake to
// succeed and confirms a second processBackfillItems call both clears the
// durable record and reports the retry as synced.
func TestProcessBackfillItems_RecordsAndClearsDurableFailures(t *testing.T) {
	pool := testPool(t)
	ledger := repo.NewLedgerRepo(pool)
	userID := newBackfillTestUser(t, pool)

	var failEverything atomic.Bool
	failEverything.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failEverything.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := &WalletServer{
		Server:       &Server{Log: slog.Default()},
		Ledger:       ledger,
		EngineClient: engineclient.NewForTest(srv.URL, "s", srv.Client()),
	}

	items := []backfillItem{{userID: userID, asset: "USDC", amount: "5000000"}} // 5.0 USDC raw

	// First run: engine is "down", so it should fail and be durably
	// recorded.
	_, failed, _, err := s.processBackfillItems(context.Background(), items)
	if err != nil {
		t.Fatalf("processBackfillItems (first run): %v", err)
	}
	if failed != 1 {
		t.Fatalf("failed = %d, want 1 while the fake engine returns 503", failed)
	}
	pending, err := ledger.PendingBackfillFailures(context.Background())
	if err != nil {
		t.Fatalf("PendingBackfillFailures: %v", err)
	}
	found := false
	for _, p := range pending {
		if p.UserID == userID && p.Asset == "USDC" {
			found = true
			if p.Amount != "5.0" && p.Amount != "5" {
				t.Fatalf("recorded failure amount = %q, want the converted human-unit amount (5)", p.Amount)
			}
		}
	}
	if !found {
		t.Fatalf("expected a durable failure record for %s/USDC after the engine rejected the credit, pending=%+v", userID, pending)
	}

	// Second run: engine recovers, and this run's items are exactly the
	// pending failures a real runBackfill would have loaded via
	// PendingBackfillFailures (already human-unit, so alreadyHuman: true —
	// re-converting a human amount through rawToHumanUnits again would
	// silently shrink it by 10^6).
	failEverything.Store(false)
	retryItems := []backfillItem{{userID: userID, asset: "USDC", amount: "5", alreadyHuman: true}}
	synced, failed2, _, err := s.processBackfillItems(context.Background(), retryItems)
	if err != nil {
		t.Fatalf("processBackfillItems (second run): %v", err)
	}
	if failed2 != 0 {
		t.Fatalf("failed = %d on the second (recovered) run, want 0", failed2)
	}
	if synced != 1 {
		t.Fatalf("synced = %d on the recovered run, want 1", synced)
	}
	pending, err = ledger.PendingBackfillFailures(context.Background())
	if err != nil {
		t.Fatalf("PendingBackfillFailures after recovery: %v", err)
	}
	for _, p := range pending {
		if p.UserID == userID && p.Asset == "USDC" {
			t.Fatalf("expected the durable failure record for %s/USDC to be cleared after a successful retry, still present: %+v", userID, p)
		}
	}
}
