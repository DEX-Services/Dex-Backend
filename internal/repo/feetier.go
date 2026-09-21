package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/dex/dex-backend/internal/feeconfig"
	"github.com/shopspring/decimal"
)

// FeeTierRepo handles fee-tier discount subscription purchases: debiting the
// user's BI2X balance and recording the subscription atomically. See
// FEE-TIER-SYSTEM-PLAN.md for the full design.
type FeeTierRepo struct {
	ledger *LedgerRepo
	fees   *feeconfig.Client
}

// NewFeeTierRepo creates a FeeTierRepo sharing the same pool as ledger/fees.
func NewFeeTierRepo(ledger *LedgerRepo, fees *feeconfig.Client) *FeeTierRepo {
	return &FeeTierRepo{ledger: ledger, fees: fees}
}

// ErrTierNotFound is returned when the requested tier number doesn't exist
// or has been deactivated by an admin.
var ErrTierNotFound = fmt.Errorf("fee tier not found or inactive")

// ErrDiscountsExcluded is returned for a user ID that must never receive a
// fee-tier discount, regardless of who or what tries to subscribe it.
var ErrDiscountsExcluded = fmt.Errorf("this account is not eligible for fee-tier discounts")

// propFirmMasterUserID is the same fixed master/omnibus account ID BitDX
// Prop Firm's own backend uses for every funded trader's real order
// (liveengine.MasterAccountID in that service — duplicated here as a
// literal since it's a different Go module, not importable). This account
// must NEVER trade at a discounted rate (PROP_FIRM_PLAN.md section 11):
// funded traders pay the exchange's real, full fee on every live trade, and
// discounting it would understate what a live account actually costs the
// platform. In practice nothing currently calls Subscribe for this account
// (it never logs in through the normal JWT flow this endpoint requires),
// but this guard makes that structurally true rather than merely
// coincidental.
const propFirmMasterUserID = "propfirm-master"

// isExcludedFromDiscounts reports whether userID must never hold an active
// fee-tier subscription. A single hardcoded ID today; if more
// discount-ineligible account classes appear later, this is the one place
// to extend rather than duplicating the check at every call site.
func isExcludedFromDiscounts(userID string) bool {
	return userID == propFirmMasterUserID
}

// Subscription describes one purchased (or admin-granted) fee-tier row.
type Subscription struct {
	ID                int64
	UserID            string
	Tier              int
	DiscountPct       decimal.Decimal
	BI2XPriceSnapshot decimal.Decimal
	BI2XAmountPaid    string // raw units
	PurchasedAt       time.Time
	ExpiresAt         time.Time
	Status            string
}

// Subscribe purchases tier for userID at currentBI2XPrice (the live BI2XUSD
// price of one BI2X, read by the caller from bi2xprice.Reader immediately
// before calling this — never a cached or hardcoded number, since BI2X
// floats). The required BI2X quantity is computed as
// tier.bi2xusd_value / currentBI2XPrice, debited from the user's BI2X
// balance, and both the debit and the new subscription row are committed in
// one transaction: either both happen or neither does.
//
// Any existing active subscription for userID is marked 'superseded' in the
// same transaction (confirmed product decision: a new purchase always
// replaces the old one and restarts the 1-year clock, regardless of whether
// the new tier is higher or lower).
func (r *FeeTierRepo) Subscribe(ctx context.Context, userID string, tier int, currentBI2XPrice decimal.Decimal) (*Subscription, error) {
	if isExcludedFromDiscounts(userID) {
		return nil, ErrDiscountsExcluded
	}
	if !currentBI2XPrice.IsPositive() {
		return nil, fmt.Errorf("invalid BI2X price")
	}
	tx, err := r.ledger.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var bi2xusdValueStr, discountPctStr string
	var active bool
	if err := tx.QueryRow(ctx, `SELECT bi2xusd_value, discount_pct, active FROM fee_tiers WHERE tier = $1 FOR UPDATE`, tier).
		Scan(&bi2xusdValueStr, &discountPctStr, &active); err != nil {
		return nil, ErrTierNotFound
	}
	if !active {
		return nil, ErrTierNotFound
	}
	bi2xusdValue, err := decimal.NewFromString(bi2xusdValueStr)
	if err != nil {
		return nil, fmt.Errorf("corrupt fee_tiers row for tier %d", tier)
	}
	discountPct, err := decimal.NewFromString(discountPctStr)
	if err != nil {
		return nil, fmt.Errorf("corrupt fee_tiers row for tier %d", tier)
	}

	// bi2xAmount = bi2xusdValue / currentBI2XPrice, rounded down to whole raw
	// units (never round up what we ask the user to pay).
	bi2xAmount := bi2xusdValue.Div(currentBI2XPrice)
	bi2xAmountRaw := toRawUnitsDecimal(bi2xAmount)
	if bi2xAmountRaw == "0" {
		return nil, fmt.Errorf("computed BI2X amount rounds to zero")
	}

	if err := r.ledger.debitBalanceTx(ctx, tx, userID, "BI2X", bi2xAmountRaw); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE user_fee_subscriptions SET status = 'superseded'
		WHERE user_id = $1 AND status = 'active'`, userID); err != nil {
		return nil, fmt.Errorf("supersede prior subscription: %w", err)
	}

	expiresAt := time.Now().AddDate(1, 0, 0)
	var sub Subscription
	if err := tx.QueryRow(ctx, `
		INSERT INTO user_fee_subscriptions (user_id, tier, bi2x_price_snapshot, bi2x_amount_paid, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, user_id, tier, bi2x_price_snapshot, bi2x_amount_paid::text, purchased_at, expires_at, status`,
		userID, tier, currentBI2XPrice.String(), bi2xAmountRaw, expiresAt,
	).Scan(&sub.ID, &sub.UserID, &sub.Tier, &sub.BI2XPriceSnapshot, &sub.BI2XAmountPaid, &sub.PurchasedAt, &sub.ExpiresAt, &sub.Status); err != nil {
		return nil, fmt.Errorf("insert subscription: %w", err)
	}
	sub.DiscountPct = discountPct

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &sub, nil
}

// ActiveSubscription returns userID's current active, unexpired subscription
// (with its tier's discount joined in), or nil if they have none.
func (r *FeeTierRepo) ActiveSubscription(ctx context.Context, userID string) (*Subscription, error) {
	var sub Subscription
	var priceStr, amountStr, pctStr string
	err := r.ledger.pool.QueryRow(ctx, `
		SELECT s.id, s.user_id, s.tier, t.discount_pct, s.bi2x_price_snapshot, s.bi2x_amount_paid::text,
		       s.purchased_at, s.expires_at, s.status
		FROM user_fee_subscriptions s
		JOIN fee_tiers t ON t.tier = s.tier
		WHERE s.user_id = $1 AND s.status = 'active' AND s.expires_at > now()
		ORDER BY t.discount_pct DESC
		LIMIT 1`, userID,
	).Scan(&sub.ID, &sub.UserID, &sub.Tier, &pctStr, &priceStr, &amountStr, &sub.PurchasedAt, &sub.ExpiresAt, &sub.Status)
	if err != nil {
		return nil, nil // nolint:nilerr — "no active subscription" is not an error condition for callers
	}
	sub.DiscountPct, _ = decimal.NewFromString(pctStr)
	sub.BI2XPriceSnapshot, _ = decimal.NewFromString(priceStr)
	sub.BI2XAmountPaid = amountStr
	return &sub, nil
}

// SubscriptionHistory returns every subscription row for userID, newest
// first — used by the admin lookup endpoint.
func (r *FeeTierRepo) SubscriptionHistory(ctx context.Context, userID string) ([]Subscription, error) {
	rows, err := r.ledger.pool.Query(ctx, `
		SELECT s.id, s.user_id, s.tier, t.discount_pct, s.bi2x_price_snapshot, s.bi2x_amount_paid::text,
		       s.purchased_at, s.expires_at, s.status
		FROM user_fee_subscriptions s
		JOIN fee_tiers t ON t.tier = s.tier
		WHERE s.user_id = $1
		ORDER BY s.purchased_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Subscription
	for rows.Next() {
		var sub Subscription
		var priceStr, amountStr, pctStr string
		if err := rows.Scan(&sub.ID, &sub.UserID, &sub.Tier, &pctStr, &priceStr, &amountStr, &sub.PurchasedAt, &sub.ExpiresAt, &sub.Status); err != nil {
			return nil, err
		}
		sub.DiscountPct, _ = decimal.NewFromString(pctStr)
		sub.BI2XPriceSnapshot, _ = decimal.NewFromString(priceStr)
		sub.BI2XAmountPaid = amountStr
		out = append(out, sub)
	}
	return out, rows.Err()
}

// toRawUnitsDecimal truncates a decimal BI2X amount down to a whole raw-unit
// integer string. BI2X uses the same 6-decimal raw-unit scale as every other
// asset in user_balances (see assetColumns) — truncating (not rounding) means
// a purchase never asks for fractionally more than the computed BI2XUSD value.
func toRawUnitsDecimal(amount decimal.Decimal) string {
	raw := amount.Mul(decimal.New(1, 6)).Truncate(0)
	if raw.IsNegative() {
		return "0"
	}
	return raw.String()
}
