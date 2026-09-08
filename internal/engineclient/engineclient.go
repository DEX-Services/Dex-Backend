// Package engineclient calls matching-engine's /internal/ledger/sync endpoint
// so that the engine's in-memory risk.Ledger stays in step with real balance
// changes recorded in Postgres (deposits, approved withdrawals). Postgres
// remains the durable source of truth; this client is a best-effort push so
// the engine's risk checks see the same balance a user sees in their wallet.
package engineclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// Client calls matching-engine's internal ledger-sync endpoint. A nil/zero-
// value Client (created when MATCHING_ENGINE_URL or ENGINE_SHARED_SECRET is
// unset) no-ops every call so Dex-Backend runs unaffected when the bridge is
// disabled.
type Client struct {
	baseURL string
	secret  string
	http    *http.Client
}

// New builds a Client from MATCHING_ENGINE_URL / ENGINE_SHARED_SECRET env
// vars. If either is unset, the returned Client is disabled: every method
// becomes a no-op that returns nil, and a warning is logged once.
func New() *Client {
	base := os.Getenv("MATCHING_ENGINE_URL")
	secret := os.Getenv("ENGINE_SHARED_SECRET")
	if base == "" || secret == "" {
		slog.Warn("MATCHING_ENGINE_URL or ENGINE_SHARED_SECRET not set, engine ledger-sync bridge disabled")
		return &Client{}
	}
	return &Client{
		baseURL: base,
		secret:  secret,
		// 10s (was 5s): /attached-order does strictly more engine-side work
		// than a plain /order — entry submit, then registry activation, then
		// up to two leg placements, each touching the shared, deliberately
		// small Postgres pools (see internal/db/db.go) — and under bursty
		// concurrent load (many attached orders at once) 5s was tight enough
		// to produce client-visible 502s even though the engine eventually
		// completed the work correctly with no incorrect state. This raises
		// the ceiling for every trade call, not just attached orders, since
		// they share one client; 10s is still a hard bound, not unbounded.
		//
		// This is a ceiling on ONE call, not a queueing budget: TradeServer's
		// per-account lock (see acctLocks) means a burst of orders from the
		// SAME account is processed one at a time, so raising this further
		// would just let a deep same-account queue exceed it anyway — see
		// TradeServer.Order's queue-depth guard, which is the actual fix for
		// that case instead of an ever-larger timeout here.
		//
		// 20s (was 10s): measured against the live Aiven Postgres instance
		// this deploys against, a single lock/settle round-trip alone can
		// take 4-6s (real network RTT to the hosted DB, not application
		// logic), and one order does several such round-trips in sequence
		// (reconcileOrderBalance's two reads, the lock, the settle). 10s
		// produced client-visible "trading service unavailable" errors on
		// perfectly normal, non-concurrent single orders — a correctness
		// non-issue turned into a false failure by too tight a ceiling.
		http: &http.Client{Timeout: 20 * time.Second},
	}
}

// Enabled reports whether this client will actually call the engine.
func (c *Client) Enabled() bool {
	return c != nil && c.baseURL != ""
}

// NewForTest builds a Client pointed at an arbitrary base URL/secret/http
// client, for other packages' tests (e.g. internal/api) that need to stub
// the matching engine without depending on unexported fields or env vars.
func NewForTest(baseURL, secret string, httpClient *http.Client) *Client {
	return &Client{baseURL: baseURL, secret: secret, http: httpClient}
}

type syncReq struct {
	AccountID string `json:"accountId"`
	Asset     string `json:"asset"`
	Amount    string `json:"amount"`
	Direction string `json:"direction"`
}

// Credit tells the engine to add amount to accountID's asset balance.
func (c *Client) Credit(ctx context.Context, accountID, asset, amount string) error {
	return c.call(ctx, accountID, asset, amount, "credit")
}

// Debit tells the engine to subtract amount from accountID's asset balance.
func (c *Client) Debit(ctx context.Context, accountID, asset, amount string) error {
	return c.call(ctx, accountID, asset, amount, "debit")
}

func (c *Client) call(ctx context.Context, accountID, asset, amount, direction string) error {
	if !c.Enabled() {
		return nil
	}
	body, err := json.Marshal(syncReq{AccountID: accountID, Asset: asset, Amount: amount, Direction: direction})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/ledger/sync", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.secret)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("engineclient sync: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("engineclient sync: status %d", resp.StatusCode)
	}
	return nil
}

// Async runs fn in a goroutine with a fresh timeout context, logging failures
// instead of propagating them. Use so deposit/withdrawal confirmation never
// blocks on engine availability.
func Async(op string, fn func(ctx context.Context) error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := fn(ctx); err != nil {
			slog.Error("engineclient async call failed", "op", op, "error", err)
		}
	}()
}
