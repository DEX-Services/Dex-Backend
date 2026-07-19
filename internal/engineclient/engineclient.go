// Package engineclient calls matching-engine's /internal/ledger/sync endpoint
// so that the engine's in-memory risk.Ledger stays in step with real balance
// changes recorded in Postgres (deposits, approved withdrawals). Postgres
// remains the durable source of truth. Sync intents are recorded in a durable
// outbox table and retried until acknowledged, so an engine outage can no
// longer cause silent, permanent balance drift.
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

	"github.com/jackc/pgx/v5/pgxpool"
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
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

// Enabled reports whether this client will actually call the engine.
func (c *Client) Enabled() bool {
	return c != nil && c.baseURL != ""
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

// ── Durable outbox ────────────────────────────────────────────────────────────

// Outbox persists engine sync intents in Postgres and retries them until the
// engine acknowledges. Enqueue is transactional with the caller's ledger write
// when the caller passes its own tx via EnqueueTx.
type Outbox struct {
	pool   *pgxpool.Pool
	client *Client
	log    *slog.Logger
}

const outboxSchema = `
CREATE TABLE IF NOT EXISTS engine_sync_outbox (
	id BIGSERIAL PRIMARY KEY,
	account_id TEXT NOT NULL,
	asset TEXT NOT NULL,
	amount NUMERIC(38,0) NOT NULL,
	direction TEXT NOT NULL CHECK (direction IN ('credit', 'debit')),
	attempts INT NOT NULL DEFAULT 0,
	last_error TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_engine_sync_outbox_due ON engine_sync_outbox (next_attempt_at);
`

// NewOutbox ensures the outbox table exists and returns a ready Outbox.
func NewOutbox(ctx context.Context, pool *pgxpool.Pool, client *Client) (*Outbox, error) {
	if _, err := pool.Exec(ctx, outboxSchema); err != nil {
		return nil, fmt.Errorf("engine sync outbox schema: %w", err)
	}
	return &Outbox{pool: pool, client: client, log: slog.Default()}, nil
}

// Enqueue durably records a sync intent. The background worker delivers it.
func (o *Outbox) Enqueue(ctx context.Context, accountID, asset, amount, direction string) error {
	_, err := o.pool.Exec(ctx,
		`INSERT INTO engine_sync_outbox (account_id, asset, amount, direction) VALUES ($1, $2, $3, $4)`,
		accountID, asset, amount, direction)
	return err
}

// EnqueueCredit / EnqueueDebit are convenience wrappers. On enqueue failure
// they fall back to a direct engine call so the sync is not lost outright,
// and log loudly either way.
func (o *Outbox) EnqueueCredit(ctx context.Context, accountID, asset, amount string) {
	o.enqueueOrFallback(ctx, accountID, asset, amount, "credit")
}

func (o *Outbox) EnqueueDebit(ctx context.Context, accountID, asset, amount string) {
	o.enqueueOrFallback(ctx, accountID, asset, amount, "debit")
}

func (o *Outbox) enqueueOrFallback(ctx context.Context, accountID, asset, amount, direction string) {
	if err := o.Enqueue(ctx, accountID, asset, amount, direction); err != nil {
		o.log.Error("engine sync outbox enqueue failed; attempting direct call",
			"direction", direction, "accountId", accountID, "asset", asset, "err", err)
		callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var cerr error
		if direction == "credit" {
			cerr = o.client.Credit(callCtx, accountID, asset, amount)
		} else {
			cerr = o.client.Debit(callCtx, accountID, asset, amount)
		}
		if cerr != nil {
			o.log.Error("engine sync direct fallback also failed; balances WILL drift until backfill",
				"direction", direction, "accountId", accountID, "asset", asset, "err", cerr)
		}
	}
}

// Run delivers pending outbox rows until ctx is cancelled. Rows are retried
// with exponential backoff (capped at 5 minutes) and removed on success.
// Delivery order is per-row FIFO by id for equal next_attempt_at.
func (o *Outbox) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.deliverDue(ctx)
		}
	}
}

func (o *Outbox) deliverDue(ctx context.Context) {
	rows, err := o.pool.Query(ctx,
		`SELECT id, account_id, asset, amount::text, direction, attempts
		 FROM engine_sync_outbox
		 WHERE next_attempt_at <= now()
		 ORDER BY id
		 LIMIT 100`)
	if err != nil {
		o.log.Error("engine sync outbox query failed", "err", err)
		return
	}
	type row struct {
		id                                 int64
		accountID, asset, amount, dir      string
		attempts                           int
	}
	var due []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.accountID, &r.asset, &r.amount, &r.dir, &r.attempts); err != nil {
			o.log.Error("engine sync outbox scan failed", "err", err)
			rows.Close()
			return
		}
		due = append(due, r)
	}
	rows.Close()

	for _, r := range due {
		var cerr error
		if r.dir == "credit" {
			cerr = o.client.Credit(ctx, r.accountID, r.asset, r.amount)
		} else {
			cerr = o.client.Debit(ctx, r.accountID, r.asset, r.amount)
		}
		if cerr == nil {
			if _, err := o.pool.Exec(ctx, `DELETE FROM engine_sync_outbox WHERE id = $1`, r.id); err != nil {
				o.log.Error("engine sync outbox delete failed", "id", r.id, "err", err)
			}
			continue
		}
		backoff := time.Duration(1<<min(r.attempts, 8)) * time.Second // 1s..256s
		if backoff > 5*time.Minute {
			backoff = 5 * time.Minute
		}
		if _, err := o.pool.Exec(ctx,
			`UPDATE engine_sync_outbox
			 SET attempts = attempts + 1, last_error = $2, next_attempt_at = now() + $3::interval
			 WHERE id = $1`,
			r.id, cerr.Error(), fmt.Sprintf("%d seconds", int(backoff.Seconds()))); err != nil {
			o.log.Error("engine sync outbox reschedule failed", "id", r.id, "err", err)
		}
		o.log.Warn("engine sync delivery failed; will retry",
			"id", r.id, "direction", r.dir, "accountId", r.accountID, "attempts", r.attempts+1, "err", cerr)
	}
}

// Async runs fn in a goroutine with a fresh timeout context, logging failures
// instead of propagating them.
//
// Deprecated: use Outbox for balance syncs — Async gives no durability and
// failures here silently drift engine balances. Retained only for callers
// that genuinely don't need delivery guarantees.
func Async(op string, fn func(ctx context.Context) error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := fn(ctx); err != nil {
			slog.Error("engineclient async call failed", "op", op, "error", err)
		}
	}()
}
