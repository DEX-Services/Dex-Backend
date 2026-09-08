package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dex/dex-backend/internal/auth"
	"github.com/dex/dex-backend/internal/engineclient"
)

// newTestTradeServer wires a TradeServer whose engine calls hit a local
// httptest server, so tests can assert exactly which account ID Dex-Backend
// forwards to the engine for a given caller's session - the thing that
// actually enforces "changing a request parameter cannot read or trade
// another account" (plan.md Phase 1 / 1.2 exit criterion).
func newTestTradeServer(t *testing.T, engineHandler http.HandlerFunc) (*TradeServer, *auth.JWTIssuer) {
	t.Helper()
	engineSrv := httptest.NewServer(engineHandler)
	t.Cleanup(engineSrv.Close)

	jwt := auth.NewJWTIssuer("test-secret", time.Hour)
	base := &Server{JWT: jwt, Log: slog.Default()}
	client := engineclient.NewForTest(engineSrv.URL, "shared-secret", engineSrv.Client())
	return &TradeServer{Server: base, Engine: client}, jwt
}

func sessionRequest(t *testing.T, jwt *auth.JWTIssuer, userID string, method, target string, body string) *http.Request {
	t.Helper()
	token, _, err := jwt.Issue(userID, "0xabc")
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
	return req
}

// TestOrders_UsesSessionAccountNotRequestParameter is the cross-account
// denial test plan.md 1.2 item 6 calls for: two different users' sessions
// must each see the engine called with their own account ID, and a
// browser-supplied account-like value in the request body/query must never
// override it - because the handlers never read one in the first place.
func TestOrders_UsesSessionAccountNotRequestParameter(t *testing.T) {
	var gotAccounts []string
	ts, jwt := newTestTradeServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAccounts = append(gotAccounts, r.URL.Query().Get("account"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orders": []}`))
	})

	// User A's session.
	reqA := sessionRequest(t, jwt, "user-A", http.MethodGet, "/trade/orders?account=user-B", "")
	wA := httptest.NewRecorder()
	ts.Orders(wA, reqA)
	if wA.Code != http.StatusOK {
		t.Fatalf("user A: status = %d, body = %s", wA.Code, wA.Body.String())
	}

	// User B's session, same handler.
	reqB := sessionRequest(t, jwt, "user-B", http.MethodGet, "/trade/orders", "")
	wB := httptest.NewRecorder()
	ts.Orders(wB, reqB)
	if wB.Code != http.StatusOK {
		t.Fatalf("user B: status = %d, body = %s", wB.Code, wB.Body.String())
	}

	if len(gotAccounts) != 2 || gotAccounts[0] != "user-A" || gotAccounts[1] != "user-B" {
		t.Fatalf("expected engine to see [user-A user-B], got %#v (a spoofed ?account=user-B on user A's request must not reach the engine)", gotAccounts)
	}
}

func TestPositions_UnauthenticatedRequestDenied(t *testing.T) {
	ts, _ := newTestTradeServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("engine must not be called for an unauthenticated request")
	})
	req := httptest.NewRequest(http.MethodGet, "/trade/positions", nil)
	w := httptest.NewRecorder()
	ts.Positions(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestBalance_UnauthenticatedRequestDenied(t *testing.T) {
	ts, _ := newTestTradeServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("engine must not be called for an unauthenticated request")
	})
	req := httptest.NewRequest(http.MethodGet, "/trade/balance?asset=USDC", nil)
	w := httptest.NewRecorder()
	ts.Balance(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

// TestOrder_RequestBodyCannotSupplyAccount confirms the order-placement DTO
// has no account field at all - a browser cannot even attempt to set one -
// and that whatever account ID is used to call the engine comes only from
// the verified session, never from the JSON body.
func TestOrder_RequestBodyCannotSupplyAccount(t *testing.T) {
	var gotAccount string
	ts, jwt := newTestTradeServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAccount = r.URL.Query().Get("account")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orderId":"o1","status":"OPEN","filled":"0","trades":0}`))
	})

	// Body attempts to smuggle an "account" field alongside a valid order;
	// tradeOrderRequest has no such field, so json.Decode silently drops it.
	body := `{"account":"user-B","symbol":"BTC-USDT","market":"SPOT","side":"BUY","type":"LIMIT","qty":"1","price":"100"}`
	req := sessionRequest(t, jwt, "user-A", http.MethodPost, "/trade/order", body)
	w := httptest.NewRecorder()
	ts.Order(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if gotAccount != "user-A" {
		t.Fatalf("engine saw account=%q, want user-A (session identity must win over any body-supplied account)", gotAccount)
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func TestCancel_UsesSessionAccount(t *testing.T) {
	var gotAccount string
	ts, jwt := newTestTradeServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAccount = r.URL.Query().Get("account")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orderId":"o1","status":"CANCELLED","filled":"0"}`))
	})
	body := `{"symbol":"BTC-USDT","market":"SPOT","orderId":"o1"}`
	req := sessionRequest(t, jwt, "user-A", http.MethodPost, "/trade/cancel", body)
	w := httptest.NewRecorder()
	ts.Cancel(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if gotAccount != "user-A" {
		t.Fatalf("engine saw account=%q, want user-A", gotAccount)
	}
}
