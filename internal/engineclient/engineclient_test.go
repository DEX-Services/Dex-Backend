package engineclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNew_DisabledWhenUnconfigured(t *testing.T) {
	t.Setenv("MATCHING_ENGINE_URL", "")
	t.Setenv("ENGINE_SHARED_SECRET", "")

	c := New()
	if c.Enabled() {
		t.Fatal("expected disabled client when env vars are unset")
	}
	if err := c.Credit(context.Background(), "u1", "USDC", "40"); err != nil {
		t.Fatalf("disabled Credit should no-op, got %v", err)
	}
	if err := c.Debit(context.Background(), "u1", "USDC", "40"); err != nil {
		t.Fatalf("disabled Debit should no-op, got %v", err)
	}
}

func TestNew_EnabledWhenConfigured(t *testing.T) {
	t.Setenv("MATCHING_ENGINE_URL", "http://localhost:9999")
	t.Setenv("ENGINE_SHARED_SECRET", "secret")

	c := New()
	if !c.Enabled() {
		t.Fatal("expected enabled client when both env vars are set")
	}
}

func TestClient_Credit_SuccessSendsCorrectRequest(t *testing.T) {
	var gotMethod, gotPath, gotSecret string
	var gotBody syncReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotSecret = r.Header.Get("X-Engine-Secret")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s3cr3t", http: srv.Client()}
	if err := c.Credit(context.Background(), "DEXUSER_1", "USDC", "40"); err != nil {
		t.Fatalf("Credit: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %s, want POST", gotMethod)
	}
	if gotPath != "/internal/ledger/sync" {
		t.Fatalf("path = %s, want /internal/ledger/sync", gotPath)
	}
	if gotSecret != "s3cr3t" {
		t.Fatalf("X-Engine-Secret = %q, want s3cr3t", gotSecret)
	}
	if gotBody.AccountID != "DEXUSER_1" || gotBody.Asset != "USDC" || gotBody.Amount != "40" || gotBody.Direction != "credit" {
		t.Fatalf("body = %+v, want {DEXUSER_1 USDC 40 credit}", gotBody)
	}
}

func TestClient_Debit_SendsDebitDirection(t *testing.T) {
	var gotBody syncReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s", http: srv.Client()}
	if err := c.Debit(context.Background(), "u1", "USDC", "10"); err != nil {
		t.Fatalf("Debit: %v", err)
	}
	if gotBody.Direction != "debit" {
		t.Fatalf("direction = %q, want debit", gotBody.Direction)
	}
}

func TestClient_NonOKStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "insufficient balance", http.StatusConflict)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s", http: srv.Client()}
	if err := c.Debit(context.Background(), "u1", "USDC", "500"); err == nil {
		t.Fatal("expected error on non-200 status, got nil")
	}
}

func TestClient_Unreachable_ReturnsError(t *testing.T) {
	c := &Client{baseURL: "http://127.0.0.1:1", secret: "s", http: &http.Client{Timeout: time.Second}}
	if err := c.Credit(context.Background(), "u1", "USDC", "1"); err == nil {
		t.Fatal("expected error calling an unreachable engine, got nil")
	}
}

// TestClient_Credit_RetriesOn429AndEventuallySucceeds is a regression test
// for a live incident: runBackfill fired Credit in a tight, unpaced loop
// over every nonzero balance, blowing straight through the engine's per-IP
// rate limiter (40 req/sec sustained, burst 80) — every single credit in
// the run failed with status 429, and Credit made no attempt to retry it,
// so backfill "succeeded" for zero users after a restart. Credit must now
// retry a 429 a few times before giving up.
func TestClient_Credit_RetriesOn429AndEventuallySucceeds(t *testing.T) {
	var attempts int
	var gotRequestIDs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body syncReq
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotRequestIDs = append(gotRequestIDs, body.RequestID)
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s", http: srv.Client()}
	if err := c.Credit(context.Background(), "u1", "USDC", "10"); err != nil {
		t.Fatalf("Credit should have succeeded after retries, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (two 429s then a success)", attempts)
	}
	for i, id := range gotRequestIDs {
		if id != gotRequestIDs[0] {
			t.Fatalf("attempt %d used a different requestId (%q) than attempt 0 (%q) — retries of the same call must reuse the same id for the engine's dedup to work", i, id, gotRequestIDs[0])
		}
	}
}

// TestClient_Credit_GivesUpAfterRepeated429s confirms the retry is bounded,
// not infinite — a sustained rate-limit condition must eventually surface
// as an error to the caller (so runBackfill can durably record it) rather
// than hanging or looping forever.
func TestClient_Credit_GivesUpAfterRepeated429s(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s", http: srv.Client()}
	err := c.Credit(context.Background(), "u1", "USDC", "10")
	if err == nil {
		t.Fatal("expected an error after exhausting retries against a sustained 429, got nil")
	}
	if attempts != callInternalRetryAttempts {
		t.Fatalf("attempts = %d, want exactly %d (callInternalRetryAttempts)", attempts, callInternalRetryAttempts)
	}
}

// TestClient_Credit_DoesNotRetryNonRetryableStatus confirms a genuine
// client error (e.g. 409 insufficient-balance-shaped conflict) fails fast
// on the first attempt instead of wasting callInternalRetryAttempts'
// worth of delay retrying a request that will never succeed as-is.
func TestClient_Credit_DoesNotRetryNonRetryableStatus(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s", http: srv.Client()}
	if err := c.Credit(context.Background(), "u1", "USDC", "10"); err == nil {
		t.Fatal("expected an error for a 409 response, got nil")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want exactly 1 (a non-retryable status must not be retried)", attempts)
	}
}

// TestStatusError_Retryable pins down exactly which statuses Credit/Debit
// will retry — 429 and 5xx, nothing else — independent of the HTTP
// round-trip, so this classification stays correct even if callWithRetry's
// wiring changes.
func TestStatusError_Retryable(t *testing.T) {
	cases := []struct {
		status int
		want   bool
	}{
		{http.StatusOK, false}, // never actually constructed for 200, but pins the boundary
		{http.StatusBadRequest, false},
		{http.StatusForbidden, false},
		{http.StatusConflict, false},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
	}
	for _, tc := range cases {
		e := &StatusError{StatusCode: tc.status}
		if got := e.Retryable(); got != tc.want {
			t.Errorf("StatusError{%d}.Retryable() = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestAsync_RunsFnWithoutBlockingCaller(t *testing.T) {
	done := make(chan struct{})
	start := time.Now()
	Async("test-op", func(ctx context.Context) error {
		defer close(done)
		return fmt.Errorf("simulated failure")
	})
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Async blocked the caller for %s, want near-instant return", elapsed)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Async did not execute fn within timeout")
	}
}
