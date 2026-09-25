package repo

import (
	"context"
	"math/big"
	"testing"
	"time"
)

func TestProfitPoolForDay_SumsTreasuryAndP2PWithinDayBounds(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users, _, _ := newTestUserRepo(pool)
	partners := NewPartnerRepo(pool)

	trader, err := users.FindOrCreate(ctx, testWallet(), "test", "")
	if err != nil {
		t.Fatalf("create trader: %v", err)
	}
	cleanupUser(t, pool, trader.ID)

	// Anchor the test day far in the past so no real production traffic can
	// land inside its window and inflate the sum this test asserts on.
	day := time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)

	before, err := partners.ProfitPoolForDay(ctx, day)
	if err != nil {
		t.Fatalf("profit pool before: %v", err)
	}

	// Backdate a treasury entry into the test day directly (CreditTreasuryFee
	// always stamps created_at = now(), so there's no repo method to insert
	// one in the past — this is the same kind of direct-SQL backdate this
	// package's other range-filter tests use when they need control over
	// created_at).
	if _, err := pool.Exec(ctx, `
		INSERT INTO platform_treasury_entries (kind, asset, amount_raw, account_id, trade_ref, category, created_at)
		VALUES ('trading_fee', 'BI2XUSD', 5000000, $1, 'partner-test-spot', 'spot', $2)`,
		trader.ID, day.Add(2*time.Hour)); err != nil {
		t.Fatalf("backdate treasury entry: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM platform_treasury_entries WHERE trade_ref = 'partner-test-spot'`)
	})

	// One entry just before the day starts and one just after it ends, to
	// confirm the range is exclusive on both sides, not just inclusive of
	// everything before "now".
	if _, err := pool.Exec(ctx, `
		INSERT INTO platform_treasury_entries (kind, asset, amount_raw, account_id, trade_ref, category, created_at)
		VALUES ('trading_fee', 'BI2XUSD', 9000000, $1, 'partner-test-outside-before', 'spot', $2)`,
		trader.ID, day.Add(-time.Minute)); err != nil {
		t.Fatalf("backdate out-of-range entry (before): %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO platform_treasury_entries (kind, asset, amount_raw, account_id, trade_ref, category, created_at)
		VALUES ('trading_fee', 'BI2XUSD', 9000000, $1, 'partner-test-outside-after', 'spot', $2)`,
		trader.ID, day.Add(24*time.Hour)); err != nil {
		t.Fatalf("backdate out-of-range entry (after): %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM platform_treasury_entries WHERE trade_ref IN ('partner-test-outside-before', 'partner-test-outside-after')`)
	})

	after, err := partners.ProfitPoolForDay(ctx, day)
	if err != nil {
		t.Fatalf("profit pool after: %v", err)
	}

	beforeN, ok := new(big.Int).SetString(before, 10)
	if !ok {
		t.Fatalf("invalid before total %q", before)
	}
	afterN, ok := new(big.Int).SetString(after, 10)
	if !ok {
		t.Fatalf("invalid after total %q", after)
	}
	delta := new(big.Int).Sub(afterN, beforeN)
	if delta.String() != "5000000" {
		t.Fatalf("expected only the in-range 5000000 entry to count, got delta %s (before=%s after=%s)", delta.String(), before, after)
	}
}

func TestRecordDailySplit_DividesEvenlyAndGivesRemainderToFirstPartner(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	partners := NewPartnerRepo(pool)

	day := time.Date(2020, 2, 20, 0, 0, 0, 0, time.UTC)
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM partner_profit_splits WHERE profit_date = $1`, day)
	})

	// 100 raw units split 3 ways: 33, 33, 34 (remainder to the
	// alphabetically-first login_id, which is partner1).
	if err := partners.RecordDailySplit(ctx, day, "100"); err != nil {
		t.Fatalf("record daily split: %v", err)
	}

	history1, err := partners.History(ctx, "partner1", nil)
	if err != nil {
		t.Fatalf("history partner1: %v", err)
	}
	history2, err := partners.History(ctx, "partner2", nil)
	if err != nil {
		t.Fatalf("history partner2: %v", err)
	}

	var share1, share2 string
	for _, h := range history1 {
		if h.ProfitDate.Equal(day) {
			share1 = h.ShareRaw
		}
	}
	for _, h := range history2 {
		if h.ProfitDate.Equal(day) {
			share2 = h.ShareRaw
		}
	}
	if share1 != "34" {
		t.Fatalf("expected partner1 (first alphabetically) to absorb the remainder and get 34, got %q", share1)
	}
	if share2 != "33" {
		t.Fatalf("expected partner2 to get an even 33, got %q", share2)
	}
}

func TestRecordDailySplit_IsRejectedAsDuplicateOnSecondCallForSameDay(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	partners := NewPartnerRepo(pool)

	day := time.Date(2020, 3, 10, 0, 0, 0, 0, time.UTC)
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM partner_profit_splits WHERE profit_date = $1`, day)
	})

	if err := partners.RecordDailySplit(ctx, day, "60"); err != nil {
		t.Fatalf("first split: %v", err)
	}
	already, err := partners.AlreadySplit(ctx, day)
	if err != nil {
		t.Fatalf("already split check: %v", err)
	}
	if !already {
		t.Fatalf("expected AlreadySplit to report true after a successful split")
	}

	if err := partners.RecordDailySplit(ctx, day, "60"); err == nil {
		t.Fatalf("expected a second RecordDailySplit for the same day to fail on the unique constraint")
	}

	// A rejected re-run must not have left a partial/duplicate row behind —
	// exactly one row per partner for this day, from the first successful
	// call only.
	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM partner_profit_splits WHERE profit_date = $1`, day).Scan(&rowCount); err != nil {
		t.Fatalf("count rows for day: %v", err)
	}
	if rowCount != 3 {
		t.Fatalf("expected exactly 3 rows (one per seeded partner) for %v, got %d", day, rowCount)
	}
}
