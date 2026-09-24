package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dex/dex-backend/internal/auth"
	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/repo"
)

// These are a regression test for a real production incident: WalletServer.
// Swap (USDT/USDC <-> BI2XUSD) used to write ONLY to Postgres via
// LedgerRepo.SwapBalance, never calling the matching engine at all. Every
// single swap into BI2XUSD left the engine's in-memory balance for that
// asset unchanged — a 100% gap, not a small drift — so a user who swapped
// into BI2XUSD and then tried to trade got "risk: insufficient BI2XUSD:
// available=0" even though Postgres correctly showed the full amount.
// reconcileOrderBalance's 2% safety cap (trade.go) can never self-heal a
// 100% delta by design, so this had to be fixed at the source: Swap must
// push both legs into the engine synchronously, the same way every other
// balance-adjusting path (AdjustUserBalance, deposits) already does.

func swapSessionRequest(t *testing.T, jwt *auth.JWTIssuer, userID, body string) *http.Request {
	t.Helper()
	token, _, err := jwt.Issue(userID, "0xswaptest", "")
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/wallet/swap", strings.NewReader(body))
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
	return req
}

// TestSwap_SyncsBothLegsIntoEngineLedger drives a real USDT -> BI2XUSD swap
// (no fee direction) through the real Swap handler and a real Postgres
// LedgerRepo, and asserts the fake engine received exactly one debit
// (source asset, full amount) and one credit (destination asset, net
// amount) in human-decimal units — not raw, matching the exact prior
// incident (RawToHumanUnits' doc comment) where passing raw units directly
// inflated every amount 1,000,000x.
func TestSwap_SyncsBothLegsIntoEngineLedger(t *testing.T) {
	pool := testPool(t)
	ledger := repo.NewLedgerRepo(pool)
	userID := newBackfillTestUser(t, pool)
	ctx := context.Background()

	if err := ledger.CreditBalance(ctx, userID, "USDT", "10000000"); err != nil { // 10.0 USDT raw
		t.Fatalf("seed USDT balance: %v", err)
	}

	type syncCall struct {
		AccountID string `json:"accountId"`
		Asset     string `json:"asset"`
		Amount    string `json:"amount"`
		Direction string `json:"direction"`
	}
	var mu sync.Mutex
	var calls []syncCall
	engineSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var c syncCall
		_ = json.NewDecoder(r.Body).Decode(&c)
		mu.Lock()
		calls = append(calls, c)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer engineSrv.Close()

	jwt := auth.NewJWTIssuer("test-secret", time.Hour)
	base := &Server{JWT: jwt, Log: slog.Default()}
	engineClient := engineclient.NewForTest(engineSrv.URL, "shared-secret", engineSrv.Client())
	s := &WalletServer{Server: base, Ledger: ledger, EngineClient: engineClient}

	body := `{"amount":"10000000","sourceAsset":"USDT","destinationAsset":"BI2XUSD"}`
	req := swapSessionRequest(t, jwt, userID, body)
	w := httptest.NewRecorder()
	s.Swap(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("swap status = %d, body = %s", w.Code, w.Body.String())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("engine sync calls = %d, want 2 (one debit, one credit) — got %+v", len(calls), calls)
	}

	var debit, credit *syncCall
	for i := range calls {
		switch calls[i].Direction {
		case "debit":
			debit = &calls[i]
		case "credit":
			credit = &calls[i]
		}
	}
	if debit == nil {
		t.Fatalf("no debit call reached the engine, calls=%+v", calls)
	}
	if debit.AccountID != userID || debit.Asset != "USDT" || debit.Amount != "10" {
		t.Fatalf("debit call = %+v, want accountId=%s asset=USDT amount=10 (human units, not raw)", *debit, userID)
	}
	if credit == nil {
		t.Fatalf("no credit call reached the engine, calls=%+v", calls)
	}
	if credit.AccountID != userID || credit.Asset != "BI2XUSD" || credit.Amount != "10" {
		t.Fatalf("credit call = %+v, want accountId=%s asset=BI2XUSD amount=10 (no fee on this direction)", *credit, userID)
	}
}

// TestSwap_EngineSyncFailureDoesNotFailTheSwap confirms the fix is
// best-effort, matching AdjustUserBalance's convention: if the engine is
// unreachable, the swap must still succeed (Postgres is the source of
// truth), and only a warning is logged — never a failed response for a
// swap that already committed correctly to Postgres.
func TestSwap_EngineSyncFailureDoesNotFailTheSwap(t *testing.T) {
	pool := testPool(t)
	ledger := repo.NewLedgerRepo(pool)
	userID := newBackfillTestUser(t, pool)
	ctx := context.Background()

	if err := ledger.CreditBalance(ctx, userID, "USDT", "5000000"); err != nil { // 5.0 USDT raw
		t.Fatalf("seed USDT balance: %v", err)
	}

	jwt := auth.NewJWTIssuer("test-secret", time.Hour)
	base := &Server{JWT: jwt, Log: slog.Default()}
	// Unreachable engine (closed server) — every Credit/Debit call fails.
	deadSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadSrv.Close()
	engineClient := engineclient.NewForTest(deadSrv.URL, "shared-secret", deadSrv.Client())
	s := &WalletServer{Server: base, Ledger: ledger, EngineClient: engineClient}

	body := `{"amount":"5000000","sourceAsset":"USDT","destinationAsset":"BI2XUSD"}`
	req := swapSessionRequest(t, jwt, userID, body)
	w := httptest.NewRecorder()
	s.Swap(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("swap must still succeed when the engine is unreachable (Postgres is the source of truth), got status %d body=%s", w.Code, w.Body.String())
	}

	// Postgres itself must reflect the swap regardless of the engine outcome.
	balances, err := ledger.BalancesFor(ctx, userID)
	if err != nil {
		t.Fatalf("load balances: %v", err)
	}
	if balances["BI2XUSD"] != "5000000" {
		t.Fatalf("BI2XUSD balance = %s, want 5000000 (swap must commit to Postgres even if the engine sync fails)", balances["BI2XUSD"])
	}
}
