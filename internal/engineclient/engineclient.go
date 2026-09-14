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
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"
)

// RawUnitScale is this platform's fixed-point scale: every asset column in
// user_balances is a raw integer (amount × 10^RawUnitScale). Credit/Debit
// below expect HUMAN-decimal amounts (matching the engine's own risk.Ledger,
// which is entirely human-unit) — never pass a raw integer straight through,
// see RawToHumanUnits' doc comment for the exact incident this constant and
// helper were extracted to prevent a second occurrence of.
const RawUnitScale = 6

// RawToHumanUnits converts a raw balance string (as stored in Postgres —
// see RawUnitScale) to the human-decimal string Credit/Debit expect.
//
// Added 2026-09-14 after a live incident: internal/chain/listener.go's
// handleDeposit passed a real on-chain deposit's raw amount (e.g.
// "31000040", i.e. 31.00004 tokens at 6-decimal scale) directly to
// EngineClient.Credit with NO conversion, while correctly converting it for
// Postgres via Ledger.InsertDeposit. The engine's in-memory ledger ended up
// believing the account held 31,000,040 BI2XUSD — 1,000,000× the real
// amount — while Postgres correctly showed the real, much smaller figure.
// This exact class of bug had already been found and fixed once before, in
// runBackfill's own raw→human conversion (see that function's comment:
// "Passing the raw integer through here inflated every restored balance by
// one million after a restart") — but the fix was local to that one call
// site rather than extracted, so the chain listener's separate, equally
// wrong call site was never caught. This helper exists so there is exactly
// one conversion implementation for every Credit/Debit call site to share.
func RawToHumanUnits(raw string) (string, error) {
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return "", fmt.Errorf("invalid raw token amount %q", raw)
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(RawUnitScale), nil)
	r := new(big.Rat).SetFrac(n, scale)
	return strings.TrimRight(strings.TrimRight(r.FloatString(RawUnitScale), "0"), "."), nil
}

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
		// A custom Transport is required here: Go's http.DefaultTransport caps
		// MaxIdleConnsPerHost at 2, so under concurrent trading (every order,
		// balance check, and reconcile call goes through this one Client to
		// the same engine host) most requests were opening a brand-new TCP+TLS
		// connection instead of reusing an idle one — pure overhead stacked on
		// top of the already-slow Aiven round trips described below. Raising
		// the per-host idle pool removes that overhead; it does not change
		// what each individual call actually waits on.
		http: &http.Client{
			Timeout: 20 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
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
