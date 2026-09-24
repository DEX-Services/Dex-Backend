package repo

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dex/dex-backend/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	godotenv.Load("../../.env")
	connString := os.Getenv("POSTGRES_SERVICE_URI")
	if connString == "" {
		t.Skip("POSTGRES_SERVICE_URI not set, skipping live-Postgres integration test")
	}
	// The full migration sequence run inside db.New (35+ statements against
	// the real Aiven instance, ~1-2s each) genuinely takes ~12s end to end —
	// confirmed directly (timed at 12.09s) after this budget started
	// intermittently timing out test runs mid-migration with no actual lock
	// contention (pg_stat_activity showed only one idle, unrelated
	// connection at the time). 10s was too tight even before this session's
	// swap-pool migration added two more statements to the list; 30s leaves
	// real headroom for the list to keep growing and for ordinary network
	// jitter against a real, non-local Postgres instance.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := db.New(ctx, connString)
	if err != nil {
		t.Skipf("could not connect to Postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newTestUser(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	users := NewUserRepo(pool)
	wallet := fmt.Sprintf("0xtest%d", time.Now().UnixNano())
	user, err := users.FindOrCreate(context.Background(), wallet, "test", "")
	if err != nil {
		t.Fatalf("create test user: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, user.ID)
	})
	return user.ID
}

func TestLockBalance_InsufficientFundsRejected(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	userID := newTestUser(t, pool)

	if err := ledger.CreditBalance(context.Background(), userID, "USDC", "100"); err != nil {
		t.Fatalf("credit: %v", err)
	}
	if err := ledger.LockBalance(context.Background(), userID, "USDC", "101"); err == nil {
		t.Fatal("expected LockBalance to fail when amount exceeds available balance")
	}
}

func TestLockBalance_SuccessTracksAvailable(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	userID := newTestUser(t, pool)

	if err := ledger.CreditBalance(context.Background(), userID, "USDC", "500"); err != nil {
		t.Fatalf("credit: %v", err)
	}
	// Locking exactly up to the available balance should succeed.
	if err := ledger.LockBalance(context.Background(), userID, "USDC", "500"); err != nil {
		t.Fatalf("lock up to full balance: %v", err)
	}
	locked, err := ledger.LockedBalancesFor(context.Background(), userID)
	if err != nil {
		t.Fatalf("locked balances: %v", err)
	}
	if locked["USDC"] != "500" {
		t.Fatalf("locked USDC = %s, want 500", locked["USDC"])
	}
	// Nothing left available, so any further lock must fail.
	if err := ledger.LockBalance(context.Background(), userID, "USDC", "1"); err == nil {
		t.Fatal("expected LockBalance to fail once fully locked")
	}
}

func TestUnlockBalance_FloorsAtZero(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	userID := newTestUser(t, pool)

	if err := ledger.CreditBalance(context.Background(), userID, "USDC", "200"); err != nil {
		t.Fatalf("credit: %v", err)
	}
	if err := ledger.LockBalance(context.Background(), userID, "USDC", "100"); err != nil {
		t.Fatalf("lock: %v", err)
	}
	// Unlocking more than is locked should not error and must floor at zero.
	if err := ledger.UnlockBalance(context.Background(), userID, "USDC", "9999"); err != nil {
		t.Fatalf("unlock more than locked: %v", err)
	}
	locked, err := ledger.LockedBalancesFor(context.Background(), userID)
	if err != nil {
		t.Fatalf("locked balances: %v", err)
	}
	if locked["USDC"] != "0" {
		t.Fatalf("locked USDC after over-unlock = %s, want 0", locked["USDC"])
	}
}

func TestSettleLockedDebit_AtomicDecrementAndInsufficientRejected(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	userID := newTestUser(t, pool)

	if err := ledger.CreditBalance(context.Background(), userID, "USDC", "300"); err != nil {
		t.Fatalf("credit: %v", err)
	}
	if err := ledger.LockBalance(context.Background(), userID, "USDC", "300"); err != nil {
		t.Fatalf("lock: %v", err)
	}
	// Settle guards on balance, not locked amount: settling less than locked but
	// within balance should succeed and decrement both balance and locked.
	if err := ledger.SettleLockedDebit(context.Background(), userID, "USDC", "100"); err != nil {
		t.Fatalf("settle: %v", err)
	}
	balance, err := ledger.BalanceFor(context.Background(), userID, "USDC")
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != "200" {
		t.Fatalf("balance after settle = %s, want 200", balance)
	}
	locked, err := ledger.LockedBalancesFor(context.Background(), userID)
	if err != nil {
		t.Fatalf("locked balances: %v", err)
	}
	if locked["USDC"] != "200" {
		t.Fatalf("locked USDC after settle = %s, want 200", locked["USDC"])
	}

	// Settling more than the remaining balance must fail.
	if err := ledger.SettleLockedDebit(context.Background(), userID, "USDC", "9999"); err == nil {
		t.Fatal("expected SettleLockedDebit to fail when amount exceeds balance")
	}
}

func TestLockBalance_ConcurrentRaceNeverOverLocks(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	userID := newTestUser(t, pool)

	if err := ledger.CreditBalance(context.Background(), userID, "USDC", "1000"); err != nil {
		t.Fatalf("credit: %v", err)
	}

	const attempts = 20
	const amountEach = "100" // 20 * 100 = 2000, double the available 1000
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			if err := ledger.LockBalance(context.Background(), userID, "USDC", amountEach); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if successes != 10 {
		t.Fatalf("successful locks = %d, want exactly 10 (1000 available / 100 each)", successes)
	}
	locked, err := ledger.LockedBalancesFor(context.Background(), userID)
	if err != nil {
		t.Fatalf("locked balances: %v", err)
	}
	if locked["USDC"] != "1000" {
		t.Fatalf("locked USDC after race = %s, want 1000 (never exceeds real balance)", locked["USDC"])
	}
}

// TestSettleSpotTrade_MovesBothLegsAndConsumesLocks covers the merge of
// SettleSpotTrade's four UPDATEs (two debit-locked + two later credit) into
// two (each side's debit-locked and credit combined in one statement) — added
// alongside that change to pin down that both balances end up exactly where
// they did before, including the debit-locked amount actually leaving
// `locked`, not just `available`.
func TestSettleSpotTrade_MovesBothLegsAndConsumesLocks(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	buyer := newTestUser(t, pool)
	seller := newTestUser(t, pool)
	ctx := context.Background()

	// Buyer holds quote (BI2XUSD) and will pay it for base (BI2X); seller
	// holds base and will receive quote. Buyer's quote and seller's base are
	// reserved up front, exactly as the matching engine's Lock call would
	// have already done before settlement runs.
	if err := ledger.CreditBalance(ctx, buyer, "BI2XUSD", "1000"); err != nil {
		t.Fatalf("credit buyer quote: %v", err)
	}
	if err := ledger.LockBalance(ctx, buyer, "BI2XUSD", "300"); err != nil {
		t.Fatalf("lock buyer quote: %v", err)
	}
	if err := ledger.CreditBalance(ctx, seller, "BI2X", "50"); err != nil {
		t.Fatalf("credit seller base: %v", err)
	}
	if err := ledger.LockBalance(ctx, seller, "BI2X", "10"); err != nil {
		t.Fatalf("lock seller base: %v", err)
	}

	// Trade: 10 BI2X for 300 BI2XUSD, seller nets 299 after a 1-unit fee.
	if err := ledger.SettleSpotTrade(ctx, buyer, seller, "BI2X", "BI2XUSD", "10", "300", "299"); err != nil {
		t.Fatalf("settle: %v", err)
	}

	buyerBals, err := ledger.BalancesFor(ctx, buyer)
	if err != nil {
		t.Fatalf("buyer balances: %v", err)
	}
	buyerLocked, err := ledger.LockedBalancesFor(ctx, buyer)
	if err != nil {
		t.Fatalf("buyer locked: %v", err)
	}
	if buyerBals["BI2XUSD"] != "700" {
		t.Fatalf("buyer BI2XUSD available = %s, want 700 (1000 - 300 paid)", buyerBals["BI2XUSD"])
	}
	if buyerLocked["BI2XUSD"] != "0" {
		t.Fatalf("buyer BI2XUSD locked = %s, want 0 (reservation consumed by settle)", buyerLocked["BI2XUSD"])
	}
	if buyerBals["BI2X"] != "10" {
		t.Fatalf("buyer BI2X available = %s, want 10 (credited)", buyerBals["BI2X"])
	}

	sellerBals, err := ledger.BalancesFor(ctx, seller)
	if err != nil {
		t.Fatalf("seller balances: %v", err)
	}
	sellerLocked, err := ledger.LockedBalancesFor(ctx, seller)
	if err != nil {
		t.Fatalf("seller locked: %v", err)
	}
	if sellerBals["BI2X"] != "40" {
		t.Fatalf("seller BI2X available = %s, want 40 (50 - 10 sold)", sellerBals["BI2X"])
	}
	if sellerLocked["BI2X"] != "0" {
		t.Fatalf("seller BI2X locked = %s, want 0 (reservation consumed by settle)", sellerLocked["BI2X"])
	}
	if sellerBals["BI2XUSD"] != "299" {
		t.Fatalf("seller BI2XUSD available = %s, want 299 (credited net of fee)", sellerBals["BI2XUSD"])
	}
}

// TestSettleSpotTrade_InsufficientLockedRejectsWholeTrade covers the merged
// UPDATE's WHERE clause still gating on the locked/available check before any
// column is written — a failed debit-check must not partially apply the
// credit side folded into the same statement.
func TestSettleSpotTrade_InsufficientLockedRejectsWholeTrade(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	buyer := newTestUser(t, pool)
	seller := newTestUser(t, pool)
	ctx := context.Background()

	// Buyer never locks anything, so the buyer-side UPDATE's WHERE clause
	// (available/locked >= amount) must reject the whole trade.
	if err := ledger.CreditBalance(ctx, seller, "BI2X", "50"); err != nil {
		t.Fatalf("credit seller base: %v", err)
	}
	if err := ledger.LockBalance(ctx, seller, "BI2X", "10"); err != nil {
		t.Fatalf("lock seller base: %v", err)
	}

	if err := ledger.SettleSpotTrade(ctx, buyer, seller, "BI2X", "BI2XUSD", "10", "300", "299"); err == nil {
		t.Fatal("expected settle to fail: buyer has no locked BI2XUSD")
	}

	// Nothing should have moved on either side — the transaction must have
	// rolled back in full, including the seller-side statement that never ran.
	sellerBals, err := ledger.BalancesFor(ctx, seller)
	if err != nil {
		t.Fatalf("seller balances: %v", err)
	}
	sellerLocked, err := ledger.LockedBalancesFor(ctx, seller)
	if err != nil {
		t.Fatalf("seller locked: %v", err)
	}
	if sellerBals["BI2X"] != "50" || sellerLocked["BI2X"] != "10" {
		t.Fatalf("seller balances changed despite failed settle: available=%s locked=%s", sellerBals["BI2X"], sellerLocked["BI2X"])
	}
}

// newUnprovisionedDeskWallet returns a market-maker desk wallet id with no
// users row, cleaning up whatever the call under test creates. This is the
// exact state a desk can be left in after a database restore/clear or a
// cleanup pass, and the state that used to make desk enable fail: the ledger's
// user_balances INSERT is a create-on-demand, but user_balances.user_id has a
// FOREIGN KEY to users(id), so without the users row Postgres raised
//
//	insert or update on table "user_balances" violates foreign key
//	constraint "user_balances_reordered_user_id_fkey" (SQLSTATE 23503)
//
// and every balance call — release-locks, replace-locks, lock — returned that
// raw driver error instead of doing its job.
func newUnprovisionedDeskWallet(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	wallet := fmt.Sprintf("mm:TEST%d:spot", time.Now().UnixNano())
	if _, err := pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, wallet); err != nil {
		t.Fatalf("clear desk user: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM user_balances WHERE user_id = $1`, wallet)
		pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, wallet)
	})
	return wallet
}

// TestReleaseLocksFor_UnprovisionedDeskWalletSucceeds covers the desk-enable
// path: bots' mm.Service.recreditDesk releases a desk's stale quote locks
// before starting it, and a desk can reach that point with no users row.
func TestReleaseLocksFor_UnprovisionedDeskWalletSucceeds(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	wallet := newUnprovisionedDeskWallet(t, pool)
	ctx := context.Background()

	if err := ledger.ReleaseLocksFor(ctx, wallet, "BI2XUSD"); err != nil {
		t.Fatalf("ReleaseLocksFor on an unprovisioned desk wallet: %v", err)
	}
	// The call must leave a usable account behind, not just avoid the error.
	bal, err := ledger.BalanceFor(ctx, wallet, "BI2XUSD")
	if err != nil {
		t.Fatalf("BalanceFor after release: %v", err)
	}
	if bal != "0" {
		t.Fatalf("BI2XUSD balance = %s, want 0", bal)
	}
}

// TestReplaceLocksFor_UnprovisionedDeskWalletFailsOnFundsNotForeignKey pins
// the other half of the same bug: the engine's ladder replacement calls
// replace-locks for a funded desk, and the only failure a caller should ever
// see there is a genuine "insufficient balance", never a raw FK violation.
func TestReplaceLocksFor_UnprovisionedDeskWalletFailsOnFundsNotForeignKey(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	wallet := newUnprovisionedDeskWallet(t, pool)
	ctx := context.Background()

	err := ledger.ReplaceLocksFor(ctx, wallet, map[string]string{"BI2XUSD": "1000"})
	if err == nil {
		t.Fatal("expected replace-locks to fail on an unfunded desk")
	}
	if !strings.Contains(err.Error(), "insufficient") {
		t.Fatalf("replace-locks error = %v, want an insufficient-balance error (not a foreign key violation)", err)
	}
}

// TestReplaceLocksFor_UnprovisionedDeskWalletSucceedsWhenFunded is the same
// call with real funds behind it: it must now succeed outright.
func TestReplaceLocksFor_UnprovisionedDeskWalletSucceedsWhenFunded(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	wallet := newUnprovisionedDeskWallet(t, pool)
	ctx := context.Background()

	if err := ledger.CreditBalance(ctx, wallet, "BI2XUSD", "5000"); err != nil {
		t.Fatalf("credit desk wallet: %v", err)
	}
	if err := ledger.ReplaceLocksFor(ctx, wallet, map[string]string{"BI2XUSD": "4000"}); err != nil {
		t.Fatalf("ReplaceLocksFor on a funded unprovisioned desk wallet: %v", err)
	}
	locked, err := ledger.LockedBalancesFor(ctx, wallet)
	if err != nil {
		t.Fatalf("locked balances: %v", err)
	}
	if locked["BI2XUSD"] != "4000" {
		t.Fatalf("locked BI2XUSD = %s, want 4000", locked["BI2XUSD"])
	}
}

// TestLockBalances_ProvisionsMultipleUserIDs covers the batched variant used
// by spot settlement and swaps, where both sides of a trade are locked in one
// statement and either may be a synthetic desk id.
func TestLockBalances_ProvisionsMultipleUserIDs(t *testing.T) {
	pool := testPool(t)
	ledger := NewLedgerRepo(pool)
	first := newUnprovisionedDeskWallet(t, pool)
	second := newUnprovisionedDeskWallet(t, pool)
	ctx := context.Background()

	for _, id := range []string{first, second} {
		if err := ledger.CreditBalance(ctx, id, "BI2XUSD", "250"); err != nil {
			t.Fatalf("credit %s: %v", id, err)
		}
	}
	// SettleSpotTrade is the real caller of the batched path; it locks buyer
	// and seller together, so a missing users row on either would abort it.
	if err := ledger.SettleSpotTrade(ctx, first, second, "BI2X", "BI2XUSD", "1", "10", "9"); err == nil {
		t.Fatal("expected settle to fail: neither side has a locked BI2X leg")
	}
	// The failure must be the business one, not an FK violation.
	locked, err := ledger.LockedBalancesFor(ctx, first)
	if err != nil {
		t.Fatalf("locked balances: %v", err)
	}
	if locked["BI2XUSD"] != "0" {
		t.Fatalf("locked BI2XUSD = %s, want 0 after rollback", locked["BI2XUSD"])
	}
}
