package repo

import (
	"context"
	"math/big"
	"testing"
	"time"
)

// TestAccruedInterest_OneYearAtFullAPR is the simplest sanity check: a full
// year at 500 bps (5%) on 1,000,000 raw units earns exactly 50,000.
func TestAccruedInterest_OneYearAtFullAPR(t *testing.T) {
	principal := big.NewInt(1_000_000)
	started := time.Now().Add(-365 * 24 * time.Hour)
	got := AccruedInterest(principal, 500, started, started.Add(365*24*time.Hour))
	want := big.NewInt(50_000)
	if got.Cmp(want) != 0 {
		t.Fatalf("interest = %s, want %s", got, want)
	}
}

// TestAccruedInterest_OneHour confirms the hourly-credit framing: one hour
// of interest on a round number should be a small, correctly-scaled amount,
// not zero and not the full year's worth.
func TestAccruedInterest_OneHour(t *testing.T) {
	principal := big.NewInt(876_000_000) // chosen so the answer divides evenly: 876,000,000 * 500 * 3600 / (10000 * 31536000) = 5000
	started := time.Now()
	got := AccruedInterest(principal, 500, started, started.Add(time.Hour))
	want := big.NewInt(5000)
	if got.Cmp(want) != 0 {
		t.Fatalf("one hour of interest = %s, want %s", got, want)
	}
}

// TestAccruedInterest_ZeroOrNegativeElapsedIsZero confirms no interest
// accrues for zero or negative elapsed time (e.g. a clock skew edge case),
// rather than erroring or returning something nonsensical.
func TestAccruedInterest_ZeroOrNegativeElapsedIsZero(t *testing.T) {
	principal := big.NewInt(1_000_000)
	now := time.Now()
	if got := AccruedInterest(principal, 500, now, now); got.Sign() != 0 {
		t.Fatalf("zero elapsed interest = %s, want 0", got)
	}
	if got := AccruedInterest(principal, 500, now, now.Add(-time.Hour)); got.Sign() != 0 {
		t.Fatalf("negative elapsed interest = %s, want 0", got)
	}
}

// TestAccruedInterest_ZeroPrincipalOrAprIsZero confirms the degenerate
// inputs are handled without dividing by zero or panicking.
func TestAccruedInterest_ZeroPrincipalOrAprIsZero(t *testing.T) {
	now := time.Now()
	if got := AccruedInterest(big.NewInt(0), 500, now.Add(-time.Hour), now); got.Sign() != 0 {
		t.Fatalf("zero principal interest = %s, want 0", got)
	}
	if got := AccruedInterest(big.NewInt(1_000_000), 0, now.Add(-time.Hour), now); got.Sign() != 0 {
		t.Fatalf("zero APR interest = %s, want 0", got)
	}
}

// fundStakingWallet is the test helper every staking test uses to get BI2X
// into the staking wallet, Phase 3's replacement for crediting
// user_balances directly: Stake now draws from staking_wallet_balances, not
// the main wallet, so a test must fund that wallet first via
// FundStakingWalletAsset, mirroring how a real user would.
func fundStakingWallet(t *testing.T, ctx context.Context, ledger *LedgerRepo, staking *StakingRepo, userID, amountRaw string) {
	t.Helper()
	if err := ledger.CreditBalance(ctx, userID, "BI2X", amountRaw); err != nil {
		t.Fatalf("credit main wallet: %v", err)
	}
	if _, _, err := staking.FundStakingWalletAsset(ctx, userID, amountRaw, "test-fund-"+userID); err != nil {
		t.Fatalf("fund staking wallet: %v", err)
	}
}

// TestStakeAndRedeem_FullCycle is an end-to-end integration test against a
// real Postgres instance: stake moves funds from the staking wallet into a
// position, the position exists, redeeming the full amount pays back
// principal (interest may be 0 if the test runs faster than a second,
// which is fine and expected) and closes the position, and the staking
// wallet balance reflects the payout.
func TestStakeAndRedeem_FullCycle(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	fundStakingWallet(t, ctx, ledger, staking, userID, "10000000000") // 10,000 BI2X at raw scale 6

	pos, err := staking.Stake(ctx, userID, "5000000000") // 5,000 BI2X
	if err != nil {
		t.Fatalf("stake: %v", err)
	}
	if pos.Status != "active" || pos.PrincipalRaw != "5000000000" {
		t.Fatalf("unexpected position after stake: %+v", pos)
	}

	wallet, err := staking.WalletBalance(ctx, userID)
	if err != nil {
		t.Fatalf("wallet balance: %v", err)
	}
	if wallet.AvailableRaw != "5000000000" {
		t.Fatalf("staking wallet available after staking 5000 of 10000 = %s, want 5000000000 remaining", wallet.AvailableRaw)
	}
	if wallet.ReservedRaw != "5000000000" {
		t.Fatalf("staking wallet reserved after staking 5000 = %s, want 5000000000", wallet.ReservedRaw)
	}

	positions, err := staking.Positions(ctx, userID)
	if err != nil {
		t.Fatalf("positions: %v", err)
	}
	if len(positions) != 1 || positions[0].ID != pos.ID {
		t.Fatalf("expected exactly the one stake just created, got %+v", positions)
	}

	result, err := staking.Redeem(ctx, userID, pos.ID, "5000000000")
	if err != nil {
		t.Fatalf("redeem: %v", err)
	}
	if result.Position.Status != "redeemed" {
		t.Fatalf("position status after full redeem = %s, want redeemed", result.Position.Status)
	}
	if result.PrincipalRaw != "5000000000" {
		t.Fatalf("redeemed principal = %s, want 5000000000", result.PrincipalRaw)
	}

	walletAfter, err := staking.WalletBalance(ctx, userID)
	if err != nil {
		t.Fatalf("wallet balance after redeem: %v", err)
	}
	if walletAfter.ReservedRaw != "0" {
		t.Fatalf("staking wallet reserved after full redeem = %s, want 0", walletAfter.ReservedRaw)
	}
	// Should be back to at least 10000000000 available (the interest earned
	// in this near-instant test is likely 0, but must never be negative or
	// lost).
	got, _ := new(big.Int).SetString(walletAfter.AvailableRaw, 10)
	want, _ := new(big.Int).SetString("10000000000", 10)
	if got.Cmp(want) < 0 {
		t.Fatalf("staking wallet available after full redeem = %s, want >= %s (principal fully returned, plus any interest)", got, want)
	}
}

// TestPartialRedeem_KeepsRemainderStartTime is a regression test for the
// specific product decision: after a partial redemption, the remaining
// principal must keep accruing from its ORIGINAL start time, not reset.
func TestPartialRedeem_KeepsRemainderStartTime(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	fundStakingWallet(t, ctx, ledger, staking, userID, "5000000000")
	pos, err := staking.Stake(ctx, userID, "5000000000")
	if err != nil {
		t.Fatalf("stake: %v", err)
	}
	originalStart := pos.StartedAt

	result, err := staking.Redeem(ctx, userID, pos.ID, "2000000000") // partial: redeem 2000 of 5000
	if err != nil {
		t.Fatalf("partial redeem: %v", err)
	}
	if result.Position.Status != "active" {
		t.Fatalf("position status after partial redeem = %s, want still active", result.Position.Status)
	}
	if result.Position.PrincipalRaw != "3000000000" {
		t.Fatalf("remaining principal after partial redeem = %s, want 3000000000", result.Position.PrincipalRaw)
	}
	if !result.Position.StartedAt.Equal(originalStart) {
		t.Fatalf("StartedAt changed after partial redeem: was %v, now %v (must stay unchanged)", originalStart, result.Position.StartedAt)
	}
}

// TestRedeem_RejectsAmountExceedingPrincipal confirms a redeem request
// larger than the position's current principal is rejected outright.
func TestRedeem_RejectsAmountExceedingPrincipal(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	fundStakingWallet(t, ctx, ledger, staking, userID, "1000000000")
	pos, err := staking.Stake(ctx, userID, "1000000000")
	if err != nil {
		t.Fatalf("stake: %v", err)
	}

	if _, err := staking.Redeem(ctx, userID, pos.ID, "2000000000"); err == nil {
		t.Fatal("expected redeem to reject an amount exceeding the position's principal")
	}
}

// TestRedeem_RejectsAlreadyRedeemedPosition confirms a fully-redeemed
// position cannot be redeemed again.
func TestRedeem_RejectsAlreadyRedeemedPosition(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	fundStakingWallet(t, ctx, ledger, staking, userID, "1000000000")
	pos, err := staking.Stake(ctx, userID, "1000000000")
	if err != nil {
		t.Fatalf("stake: %v", err)
	}
	if _, err := staking.Redeem(ctx, userID, pos.ID, "1000000000"); err != nil {
		t.Fatalf("first redeem: %v", err)
	}
	if _, err := staking.Redeem(ctx, userID, pos.ID, "1000000000"); err == nil {
		t.Fatal("expected redeeming an already-redeemed position to fail")
	}
}

// TestStake_RejectsInsufficientBalance confirms staking more than the
// staking wallet holds is rejected, exactly like an ordinary debit would be.
func TestStake_RejectsInsufficientBalance(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	fundStakingWallet(t, ctx, ledger, staking, userID, "100")
	if _, err := staking.Stake(ctx, userID, "200"); err == nil {
		t.Fatal("expected stake to reject an amount exceeding the staking wallet's balance")
	}
}

// TestStake_RejectsWhenStakingWalletNeverFunded confirms Stake fails (not
// panics, not succeeds by falling back to the main wallet) for an account
// that has never called FundStakingWalletAsset — Phase 3's whole point is
// that Stake can no longer reach the main wallet at all.
func TestStake_RejectsWhenStakingWalletNeverFunded(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	// Fund the MAIN wallet only — never the staking wallet.
	if err := ledger.CreditBalance(ctx, userID, "BI2X", "10000000000"); err != nil {
		t.Fatalf("credit main wallet: %v", err)
	}
	if _, err := staking.Stake(ctx, userID, "100"); err == nil {
		t.Fatal("expected stake to reject when the staking wallet was never funded, even though the main wallet has plenty")
	}
}

// TestFundStakingWalletAsset_Idempotent confirms a retried fund request
// with the same idempotency key is a safe no-op (mirrors
// P2PRepo.FundWalletAsset's own guarantee).
func TestFundStakingWalletAsset_Idempotent(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	if err := ledger.CreditBalance(ctx, userID, "BI2X", "1000000000"); err != nil {
		t.Fatalf("credit main wallet: %v", err)
	}
	key := "fund-key-1"
	b1, moved1, err := staking.FundStakingWalletAsset(ctx, userID, "500000000", key)
	if err != nil {
		t.Fatalf("first fund: %v", err)
	}
	if !moved1 {
		t.Fatal("expected first fund call to report moved=true")
	}
	b2, moved2, err := staking.FundStakingWalletAsset(ctx, userID, "500000000", key)
	if err != nil {
		t.Fatalf("retried fund: %v", err)
	}
	if moved2 {
		t.Fatal("expected retried fund call with the same key to report moved=false")
	}
	if b1.AvailableRaw != b2.AvailableRaw {
		t.Fatalf("retried fund changed the balance: %s -> %s", b1.AvailableRaw, b2.AvailableRaw)
	}
	// A different amount under the same key must fail outright, not silently
	// apply the new amount.
	if _, _, err := staking.FundStakingWalletAsset(ctx, userID, "999", key); err == nil {
		t.Fatal("expected reusing the same idempotency key with a different amount to fail")
	}
}

// TestUnfundStakingWalletAsset_RespectsAvailableNotReserved confirms
// unfunding can only draw on available, not reserved (staked) funds —
// mirrors how every other balance-reducing operation in this system
// behaves.
func TestUnfundStakingWalletAsset_RespectsAvailableNotReserved(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	fundStakingWallet(t, ctx, ledger, staking, userID, "1000000000")
	if _, err := staking.Stake(ctx, userID, "900000000"); err != nil {
		t.Fatalf("stake: %v", err)
	}
	// Only 100000000 remains available; unfunding more than that must fail.
	if _, _, err := staking.UnfundStakingWalletAsset(ctx, userID, "200000000", "unfund-key-1"); err == nil {
		t.Fatal("expected unfund exceeding available (not total) balance to fail")
	}
	b, _, err := staking.UnfundStakingWalletAsset(ctx, userID, "100000000", "unfund-key-2")
	if err != nil {
		t.Fatalf("unfund exactly available amount: %v", err)
	}
	if b.AvailableRaw != "0" {
		t.Fatalf("staking wallet available after unfunding exactly what was available = %s, want 0", b.AvailableRaw)
	}
}

// TestEvents_RecordsInterestPaidPerRedemption is a regression test for the
// staking history feature: Events must return a 'redeem' entry carrying the
// exact interest actually paid on that specific redemption, distinct from
// the 'stake' entry that opened the position (which always carries 0
// interest — see AddHistoryEntry/Stake).
func TestEvents_RecordsInterestPaidPerRedemption(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	staking := NewStakingRepo(pool, ledger)
	userID := newTestUser(t, pool)
	ctx := context.Background()

	fundStakingWallet(t, ctx, ledger, staking, userID, "1000000000")
	pos, err := staking.Stake(ctx, userID, "1000000000")
	if err != nil {
		t.Fatalf("stake: %v", err)
	}
	result, err := staking.Redeem(ctx, userID, pos.ID, "1000000000")
	if err != nil {
		t.Fatalf("redeem: %v", err)
	}

	events, err := staking.Events(ctx, userID, 10)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var sawStake, sawRedeem bool
	for _, e := range events {
		if e.PositionID != pos.ID {
			continue
		}
		switch e.Kind {
		case "stake":
			sawStake = true
			if e.InterestRaw != "0" {
				t.Fatalf("stake event interest = %s, want 0", e.InterestRaw)
			}
			if e.PrincipalRaw != "1000000000" {
				t.Fatalf("stake event principal = %s, want 1000000000", e.PrincipalRaw)
			}
		case "redeem":
			sawRedeem = true
			if e.InterestRaw != result.InterestRaw {
				t.Fatalf("redeem event interest = %s, want %s (matching Redeem's own return value)", e.InterestRaw, result.InterestRaw)
			}
			if e.PrincipalRaw != result.PrincipalRaw {
				t.Fatalf("redeem event principal = %s, want %s", e.PrincipalRaw, result.PrincipalRaw)
			}
		}
	}
	if !sawStake {
		t.Fatal("expected a 'stake' event for this position")
	}
	if !sawRedeem {
		t.Fatal("expected a 'redeem' event for this position")
	}
}
