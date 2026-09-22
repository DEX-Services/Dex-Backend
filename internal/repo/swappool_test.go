package repo

import (
	"context"
	"math/big"
	"sync"
	"testing"
)

// delta computes after-before for two raw big.Int strings — same helper
// shape as referral_test.go's inline delta func, extracted here since every
// test in this file needs it against the shared, never-reset
// swap_pool_balances table (a global row per asset, not a per-user row —
// see TestFeeRevenueTotals_BreaksDownByCategory in referral_test.go for the
// established "read before, apply, read after, assert the delta" pattern
// this file follows for the same reason: tests can't assume a zero starting
// balance on a table shared with real deployment data).
func swapPoolDelta(after, before string) string {
	a, _ := new(big.Int).SetString(after, 10)
	b, _ := new(big.Int).SetString(before, 10)
	return new(big.Int).Sub(a, b).String()
}

func TestSplitSixtyForty_SumsExactlyWithNoRoundingLoss(t *testing.T) {
	cases := []string{"100", "101", "1", "3", "7", "1000000", "999999999999"}
	for _, amount := range cases {
		total, ok := new(big.Int).SetString(amount, 10)
		if !ok {
			t.Fatalf("bad test input %q", amount)
		}
		swappable, reserve := splitSixtyForty(total)
		sum := new(big.Int).Add(swappable, reserve)
		if sum.Cmp(total) != 0 {
			t.Fatalf("splitSixtyForty(%s) = (%s, %s), sum %s != total %s", amount, swappable, reserve, sum, amount)
		}
		if swappable.Sign() < 0 || reserve.Sign() < 0 {
			t.Fatalf("splitSixtyForty(%s) produced a negative leg: swappable=%s reserve=%s", amount, swappable, reserve)
		}
	}
	// The specific incident-relevant case from the plan: 101 raw units must
	// split as 60 + 41 (not 60 + 40 with one unit silently lost).
	swappable, reserve := splitSixtyForty(big.NewInt(101))
	if swappable.String() != "60" || reserve.String() != "41" {
		t.Fatalf("splitSixtyForty(101) = (%s, %s), want (60, 41)", swappable, reserve)
	}
}

func TestCreditSwapPoolSplit_AppliesExact60_40Split(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	ctx := context.Background()

	beforeSwappable, beforeReserve, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load balance before: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ledger.CreditSwapPoolSplit(ctx, tx, "USDT", "101", ""); err != nil {
		t.Fatalf("CreditSwapPoolSplit: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	afterSwappable, afterReserve, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load balance after: %v", err)
	}
	if got := swapPoolDelta(afterSwappable, beforeSwappable); got != "60" {
		t.Fatalf("swappable delta = %s, want 60", got)
	}
	if got := swapPoolDelta(afterReserve, beforeReserve); got != "41" {
		t.Fatalf("reserve delta = %s, want 41", got)
	}
}

func TestDebitSwapPoolCapped_SucceedsWithinPoolAndTouchesOnlySwappable(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	ctx := context.Background()

	// Ensure there's enough to debit regardless of whatever the shared pool
	// currently holds, by crediting first via the same split path (so this
	// test is self-sufficient and doesn't depend on prior test/manual state).
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := ledger.CreditSwapPoolSplit(ctx, tx, "USDC", "1000", ""); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("seed credit: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	// 1000 split 60/40 -> 600 swappable, 400 reserve.

	beforeSwappable, beforeReserve, err := ledger.SwapPoolBalance(ctx, "USDC")
	if err != nil {
		t.Fatalf("load balance before debit: %v", err)
	}

	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer func() { _ = tx2.Rollback(ctx) }()
	if err := ledger.DebitSwapPoolCapped(ctx, tx2, "USDC", "500", ""); err != nil {
		t.Fatalf("DebitSwapPoolCapped within pool should succeed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit debit: %v", err)
	}

	afterSwappable, afterReserve, err := ledger.SwapPoolBalance(ctx, "USDC")
	if err != nil {
		t.Fatalf("load balance after debit: %v", err)
	}
	if got := swapPoolDelta(afterSwappable, beforeSwappable); got != "-500" {
		t.Fatalf("swappable delta after debit = %s, want -500", got)
	}
	if got := swapPoolDelta(afterReserve, beforeReserve); got != "0" {
		t.Fatalf("reserve delta after debit = %s, want 0 (debit must never touch reserve)", got)
	}
}

func TestDebitSwapPoolCapped_RejectsAndLeavesBalanceUntouchedWhenExceedingPool(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	ctx := context.Background()

	swappableRaw, _, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load balance: %v", err)
	}
	swappable, ok := new(big.Int).SetString(swappableRaw, 10)
	if !ok {
		t.Fatalf("bad swappable balance %q", swappableRaw)
	}
	tooMuch := new(big.Int).Add(swappable, big.NewInt(1)).String() // guaranteed to exceed whatever's currently there

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	err = ledger.DebitSwapPoolCapped(ctx, tx, "USDT", tooMuch, "")
	if err != ErrSwapPoolInsufficient {
		t.Fatalf("DebitSwapPoolCapped(%s, exceeding pool) error = %v, want ErrSwapPoolInsufficient", tooMuch, err)
	}
	// tx is rolled back via defer, never committed — confirm the balance
	// really is unchanged (not just that this one call returned an error
	// while secretly still mutating something).
	tx.Rollback(ctx)

	afterSwappableRaw, _, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load balance after rejected debit: %v", err)
	}
	if afterSwappableRaw != swappableRaw {
		t.Fatalf("swappable balance changed after a rejected debit: before=%s after=%s", swappableRaw, afterSwappableRaw)
	}
}

func TestDebitSwapPoolCapped_ConcurrentRaceNeverOverdraws(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	ctx := context.Background()

	// Seed exactly 1000 swappable via a fresh credit (600+400 split of
	// 1666.67 isn't exact, so seed with an amount chosen for a clean 60%:
	// 1000/0.6 rounds awkwardly — instead top up directly to a known figure
	// by crediting a total that splits to exactly 1000 swappable: 1000/60*100
	// isn't integral either, so just record the pool's actual swappable
	// figure after a credit of "5000" raw units (5000 * 60% = 3000 exactly)
	// and race against THAT known amount rather than a suspiciously round
	// number that depends on the pool's pre-existing state.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	if err := ledger.CreditSwapPoolSplit(ctx, tx, "USDC", "5000", ""); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("seed credit: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed: %v", err)
	}

	beforeSwappableRaw, _, err := ledger.SwapPoolBalance(ctx, "USDC")
	if err != nil {
		t.Fatalf("load balance before race: %v", err)
	}
	beforeSwappable, _ := new(big.Int).SetString(beforeSwappableRaw, 10)

	// Fire far more concurrent debits of 100 each than the pool (whatever it
	// currently is, now known exactly) can cover, and confirm the number of
	// successful debits times 100 never exceeds what was actually available
	// — mirrors TestLockBalance_ConcurrentRaceNeverOverLocks's shape
	// (ledger_test.go) for the identical "no read-then-write race" property,
	// just against the new pool table instead of a per-user locked column.
	const attempts = 50
	const amountEach = "100"
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			tx, err := pool.Begin(ctx)
			if err != nil {
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if err := ledger.DebitSwapPoolCapped(ctx, tx, "USDC", amountEach, ""); err != nil {
				return
			}
			if err := tx.Commit(ctx); err != nil {
				return
			}
			mu.Lock()
			successes++
			mu.Unlock()
		}()
	}
	wg.Wait()

	afterSwappableRaw, _, err := ledger.SwapPoolBalance(ctx, "USDC")
	if err != nil {
		t.Fatalf("load balance after race: %v", err)
	}
	afterSwappable, _ := new(big.Int).SetString(afterSwappableRaw, 10)

	consumed := new(big.Int).Sub(beforeSwappable, afterSwappable)
	expectedConsumed := new(big.Int).Mul(big.NewInt(int64(successes)), big.NewInt(100))
	if consumed.Cmp(expectedConsumed) != 0 {
		t.Fatalf("consumed %s raw units but %d successful debits of 100 each implies %s — a mismatch means the race overdrew or double-counted", consumed, successes, expectedConsumed)
	}
	if afterSwappable.Sign() < 0 {
		t.Fatalf("swappable balance went negative after the race: %s", afterSwappable)
	}
	maxPossibleSuccesses := new(big.Int).Div(beforeSwappable, big.NewInt(100)).Int64()
	if int64(successes) > maxPossibleSuccesses {
		t.Fatalf("successes = %d, but only %d could possibly fit in %s available — pool was overdrawn", successes, maxPossibleSuccesses, beforeSwappable)
	}
}

func TestAdminTopUpSwapPool_OnlyChangesSwappableNeverReserve(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	ctx := context.Background()

	beforeSwappable, beforeReserve, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load balance before topup: %v", err)
	}

	if err := ledger.AdminTopUpSwapPool(ctx, "USDT", "12345", ""); err != nil {
		t.Fatalf("AdminTopUpSwapPool: %v", err)
	}

	afterSwappable, afterReserve, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load balance after topup: %v", err)
	}
	if got := swapPoolDelta(afterSwappable, beforeSwappable); got != "12345" {
		t.Fatalf("swappable delta = %s, want 12345", got)
	}
	if got := swapPoolDelta(afterReserve, beforeReserve); got != "0" {
		t.Fatalf("reserve delta = %s, want 0 (admin top-up must never touch reserve)", got)
	}
}

func TestSwapBalance_UsdtToBi2xusdSplitsIntoSwapPool(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	user := newTestUser(t, pool)
	ctx := context.Background()

	if err := ledger.CreditBalance(ctx, user, "USDT", "1000"); err != nil {
		t.Fatalf("seed USDT balance: %v", err)
	}

	beforeSwappable, beforeReserve, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load pool before swap: %v", err)
	}

	if err := ledger.SwapBalance(ctx, user, "USDT", "1000", "BI2XUSD", "1000"); err != nil {
		t.Fatalf("SwapBalance USDT->BI2XUSD: %v", err)
	}

	afterSwappable, afterReserve, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load pool after swap: %v", err)
	}
	if got := swapPoolDelta(afterSwappable, beforeSwappable); got != "600" {
		t.Fatalf("swappable delta after USDT->BI2XUSD swap of 1000 = %s, want 600", got)
	}
	if got := swapPoolDelta(afterReserve, beforeReserve); got != "400" {
		t.Fatalf("reserve delta after USDT->BI2XUSD swap of 1000 = %s, want 400", got)
	}
}

func TestSwapBalance_Bi2xusdToUsdtRejectedAndRolledBackWhenPoolInsufficient(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	user := newTestUser(t, pool)
	ctx := context.Background()

	swappableRaw, _, err := ledger.SwapPoolBalance(ctx, "USDT")
	if err != nil {
		t.Fatalf("load pool balance: %v", err)
	}
	swappable, _ := new(big.Int).SetString(swappableRaw, 10)
	tooMuch := new(big.Int).Add(swappable, big.NewInt(1000)).String()

	if err := ledger.CreditBalance(ctx, user, "BI2XUSD", tooMuch); err != nil {
		t.Fatalf("seed BI2XUSD balance: %v", err)
	}
	beforeBalances, err := ledger.BalancesFor(ctx, user)
	if err != nil {
		t.Fatalf("load user balances before swap: %v", err)
	}

	err = ledger.SwapBalance(ctx, user, "BI2XUSD", tooMuch, "USDT", tooMuch)
	if err != ErrSwapPoolInsufficient {
		t.Fatalf("SwapBalance exceeding the pool error = %v, want ErrSwapPoolInsufficient", err)
	}

	afterBalances, err := ledger.BalancesFor(ctx, user)
	if err != nil {
		t.Fatalf("load user balances after rejected swap: %v", err)
	}
	if afterBalances["BI2XUSD"] != beforeBalances["BI2XUSD"] {
		t.Fatalf("user's BI2XUSD balance changed after a rejected swap (transaction did not roll back cleanly): before=%s after=%s", beforeBalances["BI2XUSD"], afterBalances["BI2XUSD"])
	}
	if afterBalances["USDT"] != beforeBalances["USDT"] {
		t.Fatalf("user's USDT balance changed after a rejected swap: before=%s after=%s", beforeBalances["USDT"], afterBalances["USDT"])
	}
}
