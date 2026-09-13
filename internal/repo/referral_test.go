package repo

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestUserRepo(pool *pgxpool.Pool) (*UserRepo, *ReferralRepo, *LedgerRepo) {
	ledger := NewLedgerRepo(pool)
	referrals := NewReferralRepo(pool, ledger)
	users := NewUserRepo(pool)
	users.SetReferrals(referrals)
	return users, referrals, ledger
}

// testWallet returns a fresh, never-before-used fake wallet address, same
// convention newTestUser (ledger_test.go) already uses for its own users —
// duplicated here since these tests create several distinct users per test
// (referrer + referred, owner + joined-via-link) rather than just one.
func testWallet() string {
	return fmt.Sprintf("0xtest%d", time.Now().UnixNano())
}

// cleanupUser deletes userID at test end. user_referral_links/referral_codes/
// affiliate_links all cascade or reference users with ON DELETE CASCADE/
// RESTRICT as appropriate, so deleting the user is enough for the referrer/
// referred rows; affiliate_links (owner_user_id ... ON DELETE RESTRICT) is
// deleted explicitly first since an owner can't be deleted while a link
// referencing them still exists.
func cleanupUser(t *testing.T, pool *pgxpool.Pool, userID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		pool.Exec(ctx, `DELETE FROM affiliate_links WHERE owner_user_id = $1`, userID)
		pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	})
}

func TestReferralSignup_NewUserWithReferralCodeIsLinkedPermanently(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users, referrals, _ := newTestUserRepo(pool)

	referrer, err := users.FindOrCreate(ctx, testWallet(), "test", "")
	if err != nil {
		t.Fatalf("create referrer: %v", err)
	}
	cleanupUser(t, pool, referrer.ID)
	code, err := referrals.MyReferralCode(ctx, referrer.ID)
	if err != nil {
		t.Fatalf("get referral code: %v", err)
	}

	referred, err := users.FindOrCreate(ctx, testWallet(), "test", code)
	if err != nil {
		t.Fatalf("create referred user: %v", err)
	}
	cleanupUser(t, pool, referred.ID)

	count, err := referrals.ReferredCount(ctx, referrer.ID)
	if err != nil {
		t.Fatalf("referred count: %v", err)
	}
	if count != 1 {
		t.Fatalf("referred count = %d, want 1", count)
	}

	// A returning user (same wallet, logging in again) must never be
	// re-linked, even if a different code is supplied the second time.
	otherReferrer, err := users.FindOrCreate(ctx, testWallet(), "test", "")
	if err != nil {
		t.Fatalf("create other referrer: %v", err)
	}
	cleanupUser(t, pool, otherReferrer.ID)
	otherCode, err := referrals.MyReferralCode(ctx, otherReferrer.ID)
	if err != nil {
		t.Fatalf("get other referral code: %v", err)
	}
	again, err := users.FindOrCreate(ctx, referred.WalletAddress, "test", otherCode)
	if err != nil {
		t.Fatalf("re-login referred user: %v", err)
	}
	if again.ID != referred.ID {
		t.Fatalf("re-login returned a different user id")
	}
	count, err = referrals.ReferredCount(ctx, otherReferrer.ID)
	if err != nil {
		t.Fatalf("other referrer count: %v", err)
	}
	if count != 0 {
		t.Fatalf("other referrer count = %d, want 0 (returning user must never be re-linked)", count)
	}
	count, err = referrals.ReferredCount(ctx, referrer.ID)
	if err != nil {
		t.Fatalf("original referrer count: %v", err)
	}
	if count != 1 {
		t.Fatalf("original referrer count = %d, want 1 (link must still hold)", count)
	}
}

func TestReferralSignup_UnknownCodeLinksNothing(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users, referrals, ledger := newTestUserRepo(pool)

	user, err := users.FindOrCreate(ctx, testWallet(), "test", "NOT-A-REAL-CODE")
	if err != nil {
		t.Fatalf("create user with bogus code: %v", err)
	}
	cleanupUser(t, pool, user.ID)
	// An unrecognized code must not create any user_referral_links row —
	// verified indirectly: settling a fee for this user should route 100%
	// to the treasury (no beneficiary found), same as having no code at all.
	if err := referrals.SettleFee(ctx, user.ID, "BI2XUSD", "1000000", "", "spot"); err != nil {
		t.Fatalf("settle fee: %v", err)
	}
	balance, err := ledger.BalanceFor(ctx, user.ID, "BI2XUSD")
	if err != nil {
		t.Fatalf("load user balance: %v", err)
	}
	if balance != "0" {
		t.Fatalf("user balance = %s, want 0 (unknown code must not link a beneficiary)", balance)
	}
}

func TestReferralSettleFee_SplitsBetweenReferrerAndTreasury(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users, referrals, ledger := newTestUserRepo(pool)

	referrer, err := users.FindOrCreate(ctx, testWallet(), "test", "")
	if err != nil {
		t.Fatalf("create referrer: %v", err)
	}
	cleanupUser(t, pool, referrer.ID)
	code, err := referrals.MyReferralCode(ctx, referrer.ID)
	if err != nil {
		t.Fatalf("get referral code: %v", err)
	}
	trader, err := users.FindOrCreate(ctx, testWallet(), "test", code)
	if err != nil {
		t.Fatalf("create referred trader: %v", err)
	}
	cleanupUser(t, pool, trader.ID)

	before, err := referrals.ReferralSharePct(ctx)
	if err != nil {
		t.Fatalf("load referral share pct: %v", err)
	}
	if before != "20.00" {
		t.Fatalf("default referral share pct = %s, want 20.00", before)
	}

	// A $10 (as 10_000_000 raw at 6 decimals) fee, 20% to the referrer.
	if err := referrals.SettleFee(ctx, trader.ID, "BI2XUSD", "10000000", "trade-1", "spot"); err != nil {
		t.Fatalf("settle fee: %v", err)
	}

	referrerBalance, err := ledger.BalanceFor(ctx, referrer.ID, "BI2XUSD")
	if err != nil {
		t.Fatalf("load referrer balance: %v", err)
	}
	if referrerBalance != "2000000" {
		t.Fatalf("referrer balance = %s, want 2000000 (20%% of 10000000)", referrerBalance)
	}

	earnings, err := referrals.ReferralEarnings(ctx, referrer.ID)
	if err != nil {
		t.Fatalf("load referral earnings: %v", err)
	}
	if earnings != "2000000" {
		t.Fatalf("referral earnings = %s, want 2000000", earnings)
	}
}

func TestReferralSettleFee_NoSourceGoesEntirelyToTreasury(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users, referrals, ledger := newTestUserRepo(pool)

	trader, err := users.FindOrCreate(ctx, testWallet(), "test", "")
	if err != nil {
		t.Fatalf("create trader: %v", err)
	}
	cleanupUser(t, pool, trader.ID)

	if err := referrals.SettleFee(ctx, trader.ID, "BI2XUSD", "5000000", "trade-2", "futures"); err != nil {
		t.Fatalf("settle fee: %v", err)
	}

	// Trader themselves must never receive any part of their own fee.
	traderBalance, err := ledger.BalanceFor(ctx, trader.ID, "BI2XUSD")
	if err != nil {
		t.Fatalf("load trader balance: %v", err)
	}
	if traderBalance != "0" {
		t.Fatalf("trader balance = %s, want 0", traderBalance)
	}
}

func TestAffiliateLink_CreateAndSplit(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users, referrals, ledger := newTestUserRepo(pool)

	owner, err := users.FindOrCreate(ctx, testWallet(), "test", "")
	if err != nil {
		t.Fatalf("create affiliate owner: %v", err)
	}
	cleanupUser(t, pool, owner.ID)
	link, err := referrals.CreateAffiliateLink(ctx, owner.ID, "35.00", "admin")
	if err != nil {
		t.Fatalf("create affiliate link: %v", err)
	}
	if link.SharePct != "35.00" {
		t.Fatalf("link share pct = %s, want 35.00", link.SharePct)
	}

	joined, err := users.FindOrCreate(ctx, testWallet(), "test", link.Code)
	if err != nil {
		t.Fatalf("create user via affiliate link: %v", err)
	}
	cleanupUser(t, pool, joined.ID)

	if err := referrals.SettleFee(ctx, joined.ID, "BI2XUSD", "10000000", "trade-3", "spot"); err != nil {
		t.Fatalf("settle fee: %v", err)
	}
	ownerBalance, err := ledger.BalanceFor(ctx, owner.ID, "BI2XUSD")
	if err != nil {
		t.Fatalf("load owner balance: %v", err)
	}
	if ownerBalance != "3500000" {
		t.Fatalf("owner balance = %s, want 3500000 (35%% of 10000000)", ownerBalance)
	}

	links, err := referrals.AffiliateLinksForOwner(ctx, owner.ID)
	if err != nil {
		t.Fatalf("load owner's links: %v", err)
	}
	if len(links) != 1 || links[0].JoinedCount != 1 {
		t.Fatalf("owner links = %+v, want exactly 1 link with joinedCount 1", links)
	}

	// Deactivating stops NEW signups but never changes existing links' terms.
	if err := referrals.SetAffiliateLinkActive(ctx, link.ID, false); err != nil {
		t.Fatalf("deactivate link: %v", err)
	}
	postDeactivation, err := users.FindOrCreate(ctx, testWallet(), "test", link.Code)
	if err != nil {
		t.Fatalf("signup after deactivation should still create a user (just unlinked): %v", err)
	}
	cleanupUser(t, pool, postDeactivation.ID)
}

// TestFeeRevenueTotals_BreaksDownByCategory verifies the admin Fee Revenue
// page's totals: each SettleFee/CreditTreasuryFee call's category is
// reflected in FeeRevenueTotals as a delta, and TotalRaw sums every
// category (P2P included, from its own separate table).
func TestFeeRevenueTotals_BreaksDownByCategory(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users, referrals, _ := newTestUserRepo(pool)

	trader, err := users.FindOrCreate(ctx, testWallet(), "test", "")
	if err != nil {
		t.Fatalf("create trader: %v", err)
	}
	cleanupUser(t, pool, trader.ID)

	before, err := referrals.FeeRevenueTotals(ctx, nil)
	if err != nil {
		t.Fatalf("load fee revenue totals: %v", err)
	}

	if err := referrals.SettleFee(ctx, trader.ID, "BI2XUSD", "1000000", "cat-spot", "spot"); err != nil {
		t.Fatalf("settle spot fee: %v", err)
	}
	if err := referrals.SettleFee(ctx, trader.ID, "BI2XUSD", "2000000", "cat-futures", "futures"); err != nil {
		t.Fatalf("settle futures fee: %v", err)
	}
	if err := referrals.SettleFee(ctx, trader.ID, "BI2XUSD", "3000000", "cat-liq", "liquidation"); err != nil {
		t.Fatalf("settle liquidation fee: %v", err)
	}
	if err := referrals.CreditTreasuryFee(ctx, "BI2XUSD", "4000000", trader.ID, "cat-swap", "swap"); err != nil {
		t.Fatalf("credit swap fee: %v", err)
	}

	after, err := referrals.FeeRevenueTotals(ctx, nil)
	if err != nil {
		t.Fatalf("load fee revenue totals: %v", err)
	}

	delta := func(afterVal, beforeVal string) string {
		a, _ := new(big.Int).SetString(afterVal, 10)
		b, _ := new(big.Int).SetString(beforeVal, 10)
		return new(big.Int).Sub(a, b).String()
	}
	if d := delta(after.SpotRaw, before.SpotRaw); d != "1000000" {
		t.Fatalf("spot delta = %s, want 1000000", d)
	}
	if d := delta(after.FuturesRaw, before.FuturesRaw); d != "2000000" {
		t.Fatalf("futures delta = %s, want 2000000", d)
	}
	if d := delta(after.LiquidationRaw, before.LiquidationRaw); d != "3000000" {
		t.Fatalf("liquidation delta = %s, want 3000000", d)
	}
	if d := delta(after.SwapRaw, before.SwapRaw); d != "4000000" {
		t.Fatalf("swap delta = %s, want 4000000", d)
	}
	if d := delta(after.TotalRaw, before.TotalRaw); d != "10000000" {
		t.Fatalf("total delta = %s, want 10000000 (sum of all four)", d)
	}
}

// TestFeeRevenueTotals_SinceFiltersOlderEntries verifies the admin page's
// range selector: with `since` set to just before this test's own fee is
// settled, the totals must include it; with `since` set to just after
// (before it's written) they must not — bracketing the one write with two
// timestamps taken around it, rather than comparing against the table's
// full history (which other tests/runs also populate).
func TestFeeRevenueTotals_SinceFiltersOlderEntries(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users, referrals, _ := newTestUserRepo(pool)

	trader, err := users.FindOrCreate(ctx, testWallet(), "test", "")
	if err != nil {
		t.Fatalf("create trader: %v", err)
	}
	cleanupUser(t, pool, trader.ID)

	justBefore := time.Now()
	time.Sleep(10 * time.Millisecond) // ensure created_at strictly follows justBefore
	if err := referrals.SettleFee(ctx, trader.ID, "BI2XUSD", "7000000", "cat-since", "spot"); err != nil {
		t.Fatalf("settle spot fee: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	justAfter := time.Now()

	includedTotals, err := referrals.FeeRevenueTotals(ctx, &justBefore)
	if err != nil {
		t.Fatalf("load fee revenue totals (since=justBefore): %v", err)
	}
	excludedTotals, err := referrals.FeeRevenueTotals(ctx, &justAfter)
	if err != nil {
		t.Fatalf("load fee revenue totals (since=justAfter): %v", err)
	}

	// since=justBefore includes every entry from justBefore onward, i.e. at
	// least our one fee; since=justAfter includes only entries from
	// justAfter onward, i.e. none of them (our fee was written strictly
	// before justAfter). So included - excluded must be exactly our fee.
	included, _ := new(big.Int).SetString(includedTotals.SpotRaw, 10)
	excluded, _ := new(big.Int).SetString(excludedTotals.SpotRaw, 10)
	if diff := new(big.Int).Sub(included, excluded); diff.String() != "7000000" {
		t.Fatalf("spot total (since=justBefore) - (since=justAfter) = %s, want 7000000", diff.String())
	}
	if excludedTotals.SpotRaw != "0" {
		t.Fatalf("spot total with since=justAfter = %s, want 0 (fee was written before, not after, justAfter)", excludedTotals.SpotRaw)
	}
}
