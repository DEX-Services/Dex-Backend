package repo

import (
	"context"
	"testing"

	"github.com/dex/dex-backend/internal/feeconfig"
	"github.com/shopspring/decimal"
)

func TestFeeTierSubscribe_DebitsExactBI2XAmountAndRecordsSubscription(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ledger := NewLedgerRepo(pool)
	fees := feeconfig.New(pool)
	tiers := NewFeeTierRepo(ledger, fees)
	userID := newTestUser(t, pool)

	// Tier 1 costs 500 BIUSDB worth of BI2X. At a price of 5 BIUSDB per BI2X,
	// that's exactly 100 BI2X. Credit 1000 raw units (BI2X uses the same
	// 6-decimal raw scale as every other asset, so amounts here are in whole
	// BI2X, not raw units — CreditBalance's amountRaw param is misleadingly
	// named for a caller passing a human decimal string; see toRawUnits).
	if err := ledger.CreditBalance(ctx, userID, "BI2X", "1000000000"); err != nil {
		t.Fatalf("credit BI2X: %v", err)
	}
	price := decimal.NewFromInt(5)
	sub, err := tiers.Subscribe(ctx, userID, 1, price)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if sub.Tier != 1 || !sub.DiscountPct.Equal(decimal.NewFromInt(5)) {
		t.Fatalf("subscription tier/discount = %d/%s, want 1/5", sub.Tier, sub.DiscountPct)
	}
	if sub.BI2XAmountPaid != "100000000" { // 100 BI2X at 6-decimal raw scale
		t.Fatalf("BI2X amount paid = %s, want 100000000 (100 BI2X)", sub.BI2XAmountPaid)
	}

	remaining, err := ledger.BalanceFor(ctx, userID, "BI2X")
	if err != nil {
		t.Fatalf("load remaining BI2X: %v", err)
	}
	if remaining != "900000000" { // 1000 - 100 = 900 BI2X, raw
		t.Fatalf("remaining BI2X = %s, want 900000000 (900 BI2X)", remaining)
	}

	active, err := tiers.ActiveSubscription(ctx, userID)
	if err != nil {
		t.Fatalf("load active subscription: %v", err)
	}
	if active == nil || active.Tier != 1 {
		t.Fatalf("active subscription = %+v, want tier 1", active)
	}
}

func TestFeeTierSubscribe_InsufficientBalanceRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ledger := NewLedgerRepo(pool)
	fees := feeconfig.New(pool)
	tiers := NewFeeTierRepo(ledger, fees)
	userID := newTestUser(t, pool)

	// No BI2X credited at all — any positive price should fail with
	// insufficient balance, and critically, no subscription row should be
	// left behind (the debit and the insert must be atomic).
	if _, err := tiers.Subscribe(ctx, userID, 1, decimal.NewFromInt(5)); err == nil {
		t.Fatal("expected insufficient-balance error")
	}
	active, err := tiers.ActiveSubscription(ctx, userID)
	if err != nil {
		t.Fatalf("load active subscription: %v", err)
	}
	if active != nil {
		t.Fatalf("expected no subscription after failed purchase, got %+v", active)
	}
}

func TestFeeTierSubscribe_RepurchaseSupersedesPrior(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ledger := NewLedgerRepo(pool)
	fees := feeconfig.New(pool)
	tiers := NewFeeTierRepo(ledger, fees)
	userID := newTestUser(t, pool)

	// Tier 1 needs 100 BI2X, tier 3 needs 1000 BI2X (both at price 5) — the
	// second purchase happens after the first has already been debited, so
	// credit enough for both up front (raw units, 6-decimal scale).
	if err := ledger.CreditBalance(ctx, userID, "BI2X", "1100000000"); err != nil {
		t.Fatalf("credit BI2X: %v", err)
	}
	price := decimal.NewFromInt(5)
	if _, err := tiers.Subscribe(ctx, userID, 1, price); err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	sub2, err := tiers.Subscribe(ctx, userID, 3, price)
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}
	if sub2.Tier != 3 {
		t.Fatalf("second subscription tier = %d, want 3", sub2.Tier)
	}

	active, err := tiers.ActiveSubscription(ctx, userID)
	if err != nil {
		t.Fatalf("load active subscription: %v", err)
	}
	// Exactly one active subscription should exist, and it must be the
	// second (higher tier), not the first.
	if active == nil || active.Tier != 3 {
		t.Fatalf("active subscription = %+v, want tier 3 (repurchase must supersede the prior one)", active)
	}

	history, err := tiers.SubscriptionHistory(ctx, userID)
	if err != nil {
		t.Fatalf("load subscription history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("subscription history length = %d, want 2 (both purchases kept, one superseded)", len(history))
	}
	activeCount := 0
	for _, h := range history {
		if h.Status == "active" {
			activeCount++
		}
	}
	if activeCount != 1 {
		t.Fatalf("active rows in history = %d, want exactly 1", activeCount)
	}
}

func TestFeeTierSubscribe_UnknownTierRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ledger := NewLedgerRepo(pool)
	fees := feeconfig.New(pool)
	tiers := NewFeeTierRepo(ledger, fees)
	userID := newTestUser(t, pool)

	if err := ledger.CreditBalance(ctx, userID, "BI2X", "1000000"); err != nil {
		t.Fatalf("credit BI2X: %v", err)
	}
	if _, err := tiers.Subscribe(ctx, userID, 99, decimal.NewFromInt(5)); err != ErrTierNotFound {
		t.Fatalf("subscribe to unknown tier: err = %v, want ErrTierNotFound", err)
	}
}
