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
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
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
	// RequestID lets the engine recognize a retried call as the same
	// operation instead of double-applying it (M4) — see
	// matching-engine's /internal/ledger/sync handler. Generated once per
	// logical Credit/Debit call, not once per HTTP attempt: retries of the
	// SAME call must reuse the SAME id, or the dedup is pointless.
	RequestID string `json:"requestId"`
}

// StatusError wraps a non-200 HTTP response from the engine so callers can
// distinguish a retryable rate-limit response from a genuine failure
// (a bad request, an auth error, engine unreachable) without parsing the
// error string. Added after a live incident: runBackfill's tight,
// unpaced loop over every nonzero balance blew straight through the
// engine's per-IP rate limiter (cmd/engine/ratelimit.go, 40 req/sec
// sustained / burst 80) — every single credit in the run failed with
// status 429, and the failure was indistinguishable from a real error by
// the caller, so nothing retried it. See StatusError.Retryable.
type StatusError struct {
	StatusCode int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("engineclient sync: status %d", e.StatusCode)
}

// Retryable reports whether this status is worth retrying automatically:
// 429 (rate-limited — the request itself was fine, just throttled) and 5xx
// (transient server-side failure) are; a 4xx like 400/403/409 reflects a
// genuinely bad or conflicting request that retrying verbatim would only
// repeat.
func (e *StatusError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

// Credit tells the engine to add amount to accountID's asset balance. A
// fresh request ID is generated per call, so calling this directly (outside
// Async) always dedup-registers as a distinct operation — as intended for a
// genuinely new credit. For a call that may be retried, see CreditAsync.
// Automatically retries a few times on a retryable StatusError (429/5xx) —
// see StatusError's doc comment for the incident this closes — reusing the
// SAME request ID across attempts so the engine's own dedup treats them as
// one logical call, exactly like Async's retry loop does.
func (c *Client) Credit(ctx context.Context, accountID, asset, amount string) error {
	return c.callWithRetry(ctx, accountID, asset, amount, "credit", uuid.NewString())
}

// Debit tells the engine to subtract amount from accountID's asset balance.
// See Credit's doc comment on request IDs, retry behavior, and DebitAsync.
func (c *Client) Debit(ctx context.Context, accountID, asset, amount string) error {
	return c.callWithRetry(ctx, accountID, asset, amount, "debit", uuid.NewString())
}

// callInternalRetryAttempts/Delay bound Credit/Debit's own built-in retry
// for a retryable status (429/5xx), independent of and in addition to the
// separate Async wrapper's retry loop (Async retries ANY error, including
// a non-retryable one, on a longer 2s cadence for background/fire-and-
// forget calls; this one is specifically for a synchronous caller like
// runBackfill that needs a prompt, bounded wait before giving up on one
// item and moving to the next).
const (
	callInternalRetryAttempts = 3
	callInternalRetryDelay    = 250 * time.Millisecond
)

func (c *Client) callWithRetry(ctx context.Context, accountID, asset, amount, direction, requestID string) error {
	var err error
	for attempt := 0; attempt < callInternalRetryAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(callInternalRetryDelay * time.Duration(attempt)):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		err = c.call(ctx, accountID, asset, amount, direction, requestID)
		if err == nil {
			return nil
		}
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || !statusErr.Retryable() {
			return err
		}
	}
	return err
}

func (c *Client) call(ctx context.Context, accountID, asset, amount, direction, requestID string) error {
	if !c.Enabled() {
		return nil
	}
	body, err := json.Marshal(syncReq{AccountID: accountID, Asset: asset, Amount: amount, Direction: direction, RequestID: requestID})
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
		return &StatusError{StatusCode: resp.StatusCode}
	}
	return nil
}

// asyncRetryAttempts/asyncRetryDelay match matching-engine's own
// internal/backendclient.Async (the reverse-direction sync, engine -> this
// service), which already retries the same class of call. This function
// previously made exactly one attempt with no retry at all: a single
// transient failure (a network blip, the engine mid-restart) left the
// engine's in-memory ledger silently out of sync with Postgres — for a
// withdrawal debit specifically, that meant Postgres said "confirmed" while
// the engine still showed the withdrawn funds as spendable, with nothing to
// notice or correct it. Retrying a few times over several seconds absorbs
// exactly that kind of one-off blip without turning this into a full
// durable outbox (a real gap that remains: an outage longer than the retry
// window still desyncs the two ledgers with no automatic recovery — see
// M3/M4's other findings).
const (
	asyncRetryAttempts = 3
	asyncRetryDelay    = 2 * time.Second
)

// CreditAsync/DebitAsync run Credit/Debit via Async, generating ONE request
// ID up front and reusing it across every retry attempt (M4) — Async's fn
// closure runs fresh on each attempt, so generating the ID inside it would
// give every retry a distinct ID and defeat the engine's dedup entirely.
// This is the routine way to call Credit/Debit from a background/fire-and-
// forget path; call Credit/Debit directly only when not going through Async.
func (c *Client) CreditAsync(op, accountID, asset, amount string) {
	requestID := uuid.NewString()
	Async(op, func(ctx context.Context) error {
		return c.call(ctx, accountID, asset, amount, "credit", requestID)
	})
}

func (c *Client) DebitAsync(op, accountID, asset, amount string) {
	requestID := uuid.NewString()
	Async(op, func(ctx context.Context) error {
		return c.call(ctx, accountID, asset, amount, "debit", requestID)
	})
}

// Async runs fn in a goroutine with a fresh timeout context per attempt,
// retrying a few times before giving up and only logging. Use so deposit/
// withdrawal confirmation never blocks on engine availability, for calls
// whose failure shouldn't undo work already committed to Postgres.
func Async(op string, fn func(ctx context.Context) error) {
	go func() {
		var err error
		for attempt := 0; attempt < asyncRetryAttempts; attempt++ {
			if attempt > 0 {
				time.Sleep(asyncRetryDelay)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = fn(ctx)
			cancel()
			if err == nil {
				return
			}
			slog.Warn("engineclient async call failed, retrying", "op", op, "attempt", attempt+1, "error", err)
		}
		slog.Error("engineclient async call failed after retries", "op", op, "attempts", asyncRetryAttempts, "error", err)
	}()
}
