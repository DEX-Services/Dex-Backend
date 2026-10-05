package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/repo"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newSipSwpTestServer builds a SipSwpServer against a live Postgres test
// pool and a fake engine HTTP server. Trade.Ledger is deliberately left nil
// — submitOrderReconciled's reconcileOrderBalance returns nil immediately
// when s.Ledger is nil (see trade.go), the same no-op path a real
// LedgerClient-less TradeServer takes, so these tests isolate the SIP/SWP
// worker's own scheduling/bookkeeping logic from the (separately, already
// well-tested) reconcile path.
func newSipSwpTestServer(t *testing.T, pool *pgxpool.Pool, engineHandler http.Handler) (*SipSwpServer, *repo.SipSwpRepo) {
	t.Helper()
	plans := repo.NewSipSwpRepo(pool)
	srv := httptest.NewServer(engineHandler)
	t.Cleanup(srv.Close)
	engine := engineclient.NewForTest(srv.URL, "s", srv.Client())
	trade := &TradeServer{Server: &Server{Log: slog.Default()}, Engine: engine, Ledger: nil}
	return &SipSwpServer{Server: &Server{Log: slog.Default()}, Trade: trade, Plans: plans}, plans
}

func fakeEngine(t *testing.T, midPrice string, orderShouldFail bool) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ticker":
			json.NewEncoder(w).Encode(map[string]string{"midPrice": midPrice})
		case "/order":
			if orderShouldFail {
				http.Error(w, "insufficient balance", http.StatusConflict)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"orderId": "fake-order-1", "status": "filled", "filled": "1", "trades": 1})
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
}

func TestExecuteOnePlan_Completed_PlacesOrderAndAdvances(t *testing.T) {
	pool := testPool(t)
	srv, plans := newSipSwpTestServer(t, pool, fakeEngine(t, "10", false))
	ctx := context.Background()
	// newBackfillTestUser (wallet_backfill_test.go, same package) gives a
	// real users row this plan can reference via its FK.
	realUserID := newBackfillTestUser(t, pool)

	dayOfPeriod := 1
	plan, err := plans.CreatePlan(ctx, realUserID, "SIP", "Test SIP", "BI2X", "BI2XUSD", "1000000", "DAILY", &dayOfPeriod, time.Now().UTC().Format("2006-01-02"), nil)
	if err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	due, err := plans.DuePlans(ctx, time.Now())
	if err != nil {
		t.Fatalf("DuePlans: %v", err)
	}
	found := false
	for _, p := range due {
		if p.ID == plan.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected newly created plan %s to be due immediately (startDate=today)", plan.ID)
	}

	srv.executeOnePlan(ctx, *plan)

	updated, err := plans.GetPlan(ctx, realUserID, plan.ID)
	if err != nil {
		t.Fatalf("GetPlan after execution: %v", err)
	}
	if updated.ExecutionsCompleted != 1 {
		t.Fatalf("executionsCompleted = %d, want 1", updated.ExecutionsCompleted)
	}
	if updated.TotalUsdRaw != "1000000" {
		t.Fatalf("totalUsdRaw = %q, want 1000000 (a completed cycle should add its full amount)", updated.TotalUsdRaw)
	}
	if updated.NextRunDate == plan.NextRunDate {
		t.Fatalf("nextRunDate did not advance past the cycle just executed: still %s", updated.NextRunDate)
	}

	executions, err := plans.ListExecutions(ctx, realUserID, plan.ID, 10)
	if err != nil {
		t.Fatalf("ListExecutions: %v", err)
	}
	if len(executions) != 1 || executions[0].Status != "completed" {
		t.Fatalf("executions = %+v, want exactly one 'completed' entry", executions)
	}
	if executions[0].OrderID == nil || *executions[0].OrderID != "fake-order-1" {
		t.Fatalf("execution orderId = %v, want fake-order-1", executions[0].OrderID)
	}
}

func TestExecuteOnePlan_EngineRejects_SkipsAndStillAdvances(t *testing.T) {
	pool := testPool(t)
	srv, plans := newSipSwpTestServer(t, pool, fakeEngine(t, "10", true))
	ctx := context.Background()
	realUserID := newBackfillTestUser(t, pool)

	dayOfPeriod := 1
	plan, err := plans.CreatePlan(ctx, realUserID, "SIP", "Test SIP", "BI2X", "BI2XUSD", "1000000", "DAILY", &dayOfPeriod, time.Now().UTC().Format("2006-01-02"), nil)
	if err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	srv.executeOnePlan(ctx, *plan)

	updated, err := plans.GetPlan(ctx, realUserID, plan.ID)
	if err != nil {
		t.Fatalf("GetPlan after execution: %v", err)
	}
	// Confirmed product decision: a failed cycle is skipped and the plan
	// stays active, trying again next cycle — never paused, and never
	// retried again the SAME day (executionsCompleted/nextRunDate still
	// advance exactly like a completed cycle would).
	if updated.Status != "active" {
		t.Fatalf("status = %q after a failed cycle, want still 'active' (skip, don't pause)", updated.Status)
	}
	if updated.ExecutionsCompleted != 1 {
		t.Fatalf("executionsCompleted = %d, want 1 (a skipped cycle still advances)", updated.ExecutionsCompleted)
	}
	if updated.TotalUsdRaw != "0" {
		t.Fatalf("totalUsdRaw = %q, want 0 (nothing actually moved on a skipped cycle)", updated.TotalUsdRaw)
	}
	if updated.NextRunDate == plan.NextRunDate {
		t.Fatalf("nextRunDate did not advance after a skipped cycle: still %s", updated.NextRunDate)
	}

	executions, err := plans.ListExecutions(ctx, realUserID, plan.ID, 10)
	if err != nil {
		t.Fatalf("ListExecutions: %v", err)
	}
	if len(executions) != 1 || executions[0].Status != "skipped" {
		t.Fatalf("executions = %+v, want exactly one 'skipped' entry", executions)
	}
	if executions[0].SkipReason == nil || *executions[0].SkipReason == "" {
		t.Fatalf("skipped execution has no skipReason recorded")
	}
}
