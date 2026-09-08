package repo

import (
	"context"
	"fmt"
	"os"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	user, err := users.FindOrCreate(context.Background(), wallet, "test")
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
