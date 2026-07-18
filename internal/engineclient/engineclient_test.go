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

func TestClient_SyncSendsIdempotencyKey(t *testing.T) {
	var gotHeader string
	var gotBody syncReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("Idempotency-Key")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := &Client{baseURL: srv.URL, secret: "s", http: srv.Client()}
	if err := c.Sync(context.Background(), "event-123", "u1", "USDC", "10", "debit"); err != nil {
		t.Fatal(err)
	}
	if gotHeader != "event-123" || gotBody.EventID != "event-123" {
		t.Fatalf("idempotency key header/body = %q/%q", gotHeader, gotBody.EventID)
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
