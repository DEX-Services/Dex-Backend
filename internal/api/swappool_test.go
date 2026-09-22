package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// These cover the cheap, DB-independent guard checks (method-not-allowed,
// unauthenticated, invalid input) that don't need a live Postgres — the
// actual pool-bookkeeping behavior (60/40 split, capped debit, admin-only
// top-up, concurrency) is covered end-to-end against a real database in
// internal/repo/swappool_test.go, following the same split established
// elsewhere in this package (e.g. fees_test.go's FeeServer tests vs. the
// live-Postgres tests in internal/repo).

func TestSwapPoolMax_RejectsUnauthenticated(t *testing.T) {
	srv := &WalletServer{Server: &Server{}}
	req := httptest.NewRequest(http.MethodGet, "/wallet/swap/max?asset=USDT", nil)
	w := httptest.NewRecorder()
	srv.SwapPoolMax(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestSwapPoolStatus_MethodNotAllowed(t *testing.T) {
	srv := &AdminServer{Server: &Server{}}
	req := httptest.NewRequest(http.MethodPost, "/admin/swap-pool", nil)
	w := httptest.NewRecorder()
	srv.SwapPoolStatus(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestSwapPoolStatus_RejectsUnauthenticated(t *testing.T) {
	srv := &AdminServer{Server: &Server{}}
	req := httptest.NewRequest(http.MethodGet, "/admin/swap-pool", nil)
	w := httptest.NewRecorder()
	srv.SwapPoolStatus(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestAdminTopUpSwapPool_MethodNotAllowed(t *testing.T) {
	srv := &AdminServer{Server: &Server{}}
	req := httptest.NewRequest(http.MethodGet, "/admin/swap-pool/topup", nil)
	w := httptest.NewRecorder()
	srv.AdminTopUpSwapPool(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestAdminTopUpSwapPool_RejectsUnauthenticated(t *testing.T) {
	srv := &AdminServer{Server: &Server{}}
	body, _ := json.Marshal(swapPoolTopUpRequest{Asset: "USDT", Amount: "10"})
	req := httptest.NewRequest(http.MethodPost, "/admin/swap-pool/topup", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.AdminTopUpSwapPool(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}
