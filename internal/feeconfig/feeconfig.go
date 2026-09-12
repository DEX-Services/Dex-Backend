// Package feeconfig reads the platform's base fee rates and per-user
// discounts from Postgres. The fee_config/fee_tiers/user_fee_subscriptions
// tables are owned by matching-engine (see its internal/feeconfig and
// internal/discounts packages, which create the schema and seed defaults);
// this package is a read-only client sharing the same Postgres instance —
// Dex-Backend never creates or migrates these tables itself.
//
// Unlike matching-engine's settlement hot path, P2P and swap requests here
// are one-off HTTP calls, not per-fill matching-loop code, so a direct
// per-request Postgres read is fine — no in-memory hot-reload cache is
// needed on this side (see FEE-TIER-SYSTEM-PLAN.md §1a).
package feeconfig

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// Fee config keys, mirroring matching-engine's internal/feeconfig constants.
const (
	KeyP2PBuyer  = "p2p.buyer"
	KeyP2PSeller = "p2p.seller"
	KeySwapIn    = "swap.in"
	KeySwapOut   = "swap.out"
)

// Client reads fee_config/discount rows on demand.
type Client struct {
	pool *pgxpool.Pool
}

// New creates a Client backed by pool.
func New(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

// BaseRate returns the current base rate for key, or zero if unconfigured
// (e.g. the row hasn't been seeded yet, or Postgres is unreachable — callers
// should treat zero as "no fee charged" the same way the pre-existing
// hardcoded-constant code did before this feature, rather than failing the
// request outright over a fee-lookup hiccup).
func (c *Client) BaseRate(ctx context.Context, key string) decimal.Decimal {
	var rateStr string
	err := c.pool.QueryRow(ctx, `SELECT rate FROM fee_config WHERE key = $1`, key).Scan(&rateStr)
	if err != nil {
		return decimal.Zero
	}
	rate, err := decimal.NewFromString(rateStr)
	if err != nil {
		return decimal.Zero
	}
	return rate
}

// ActiveDiscountFor returns userID's current fee-tier discount as a fraction
// (0.15 for 15% off), or zero if they have no active, unexpired subscription.
func (c *Client) ActiveDiscountFor(ctx context.Context, userID string) decimal.Decimal {
	var pctStr string
	err := c.pool.QueryRow(ctx, `
		SELECT t.discount_pct
		FROM user_fee_subscriptions s
		JOIN fee_tiers t ON t.tier = s.tier
		WHERE s.user_id = $1 AND s.status = 'active' AND s.expires_at > now()
		ORDER BY t.discount_pct DESC
		LIMIT 1`, userID).Scan(&pctStr)
	if err != nil {
		return decimal.Zero
	}
	pct, err := decimal.NewFromString(pctStr)
	if err != nil {
		return decimal.Zero
	}
	return pct.Div(decimal.NewFromInt(100))
}

// EffectiveRate returns key's base rate discounted by userID's active
// fee-tier subscription (effective_rate = base_rate * (1 - discount) — see
// FEE-TIER-SYSTEM-PLAN.md). This is the one call site normally used;
// BaseRate/ActiveDiscountFor are exposed separately for callers (like the
// admin API) that need to display them independently.
func (c *Client) EffectiveRate(ctx context.Context, key, userID string) decimal.Decimal {
	base := c.BaseRate(ctx, key)
	discount := c.ActiveDiscountFor(ctx, userID)
	if discount.IsZero() {
		return base
	}
	return base.Mul(decimal.NewFromInt(1).Sub(discount))
}

// ErrNoActivePrice/ErrInsufficientBalance are used by the fee-tier purchase
// flow (see internal/api/fees.go); kept here since they describe conditions
// specific to reading fee/discount state.
var ErrNoActivePrice = fmt.Errorf("no live BI2X price available")

// TierInfo describes one row of fee_tiers.
type TierInfo struct {
	Tier        int
	BIUSDBValue decimal.Decimal
	DiscountPct decimal.Decimal
	Active      bool
}

// Tier returns the fee_tiers row for the given tier number, or
// (TierInfo{}, false) if it doesn't exist or isn't active.
func (c *Client) Tier(ctx context.Context, tier int) (TierInfo, bool) {
	var t TierInfo
	var valueStr, pctStr string
	err := c.pool.QueryRow(ctx,
		`SELECT tier, biusdb_value, discount_pct, active FROM fee_tiers WHERE tier = $1`, tier,
	).Scan(&t.Tier, &valueStr, &pctStr, &t.Active)
	if err != nil || !t.Active {
		return TierInfo{}, false
	}
	t.BIUSDBValue, _ = decimal.NewFromString(valueStr)
	t.DiscountPct, _ = decimal.NewFromString(pctStr)
	return t, true
}

// AllTiers returns every fee_tiers row (including inactive ones), ordered by
// tier ascending. Used by the tier-selection UI and the admin listing.
func (c *Client) AllTiers(ctx context.Context) ([]TierInfo, error) {
	rows, err := c.pool.Query(ctx, `SELECT tier, biusdb_value, discount_pct, active FROM fee_tiers ORDER BY tier ASC`)
	if err != nil {
		return nil, fmt.Errorf("query fee_tiers: %w", err)
	}
	defer rows.Close()

	var out []TierInfo
	for rows.Next() {
		var t TierInfo
		var valueStr, pctStr string
		if err := rows.Scan(&t.Tier, &valueStr, &pctStr, &t.Active); err != nil {
			return nil, fmt.Errorf("scan fee_tiers row: %w", err)
		}
		t.BIUSDBValue, _ = decimal.NewFromString(valueStr)
		t.DiscountPct, _ = decimal.NewFromString(pctStr)
		out = append(out, t)
	}
	return out, rows.Err()
}

// AllRates returns every configured fee_config row. Used by the admin API.
func (c *Client) AllRates(ctx context.Context) (map[string]decimal.Decimal, error) {
	rows, err := c.pool.Query(ctx, `SELECT key, rate FROM fee_config`)
	if err != nil {
		return nil, fmt.Errorf("query fee_config: %w", err)
	}
	defer rows.Close()

	out := make(map[string]decimal.Decimal)
	for rows.Next() {
		var key, rateStr string
		if err := rows.Scan(&key, &rateStr); err != nil {
			return nil, fmt.Errorf("scan fee_config row: %w", err)
		}
		rate, err := decimal.NewFromString(rateStr)
		if err != nil {
			continue
		}
		out[key] = rate
	}
	return out, rows.Err()
}

// SetRate updates one fee_config row (admin edit path). Returns
// pgx.ErrNoRows if key does not exist — callers should validate key against
// a known set (see ValidKeys) before calling, so this is a defensive
// backstop, not the primary validation.
func (c *Client) SetRate(ctx context.Context, key string, rate decimal.Decimal, updatedBy string) error {
	tag, err := c.pool.Exec(ctx,
		`UPDATE fee_config SET rate = $2, updated_by = $3, updated_at = now() WHERE key = $1`,
		key, rate.String(), updatedBy)
	if err != nil {
		return fmt.Errorf("update fee_config: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// ValidKeys lists every recognized fee_config key, for admin API validation
// — mirrors matching-engine's internal/feeconfig.ValidKeys.
func ValidKeys() []string {
	return []string{
		"spot.maker", "spot.taker",
		"futures.maker", "futures.taker",
		KeyP2PBuyer, KeyP2PSeller,
		KeySwapIn, KeySwapOut,
		"liquidation",
	}
}

// MinRate/MaxRate bound what an admin may set any fee_config rate to,
// per the confirmed product decision to clamp 0%-5% (0%-10% for
// liquidation) rather than trust free-form input — a single fat-fingered
// edit (e.g. "45" instead of "0.45") should be rejected, not silently
// applied to every trade on the platform.
func RateBounds(key string) (min, max decimal.Decimal) {
	if key == "liquidation" {
		return decimal.Zero, decimal.NewFromFloat(0.10)
	}
	return decimal.Zero, decimal.NewFromFloat(0.05)
}
