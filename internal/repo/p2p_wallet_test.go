package repo

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dex/dex-backend/internal/db"
	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func TestP2PWalletEscrowSuccessAndRefund(t *testing.T) {
	pool := p2pTestPool(t)
	ctx := context.Background()
	p2p := NewP2PRepo(pool)
	ledger := NewLedgerRepo(pool)
	sellerID := newTestUser(t, pool)
	buyerID := newTestUser(t, pool)

	if err := ledger.CreditBalance(ctx, sellerID, "USDC", "20000000"); err != nil {
		t.Fatalf("credit seller main wallet: %v", err)
	}
	balance, moved, err := p2p.FundWallet(ctx, sellerID, "20000000", "fund-test-0001")
	if err != nil || !moved {
		t.Fatalf("fund P2P wallet: moved=%v err=%v", moved, err)
	}
	assertWallet(t, balance, "20000000", "0", "20000000")
	mainBalance, err := ledger.BalanceFor(ctx, sellerID, "USDC")
	if err != nil || mainBalance != "0" {
		t.Fatalf("seller main balance = %q err=%v, want 0", mainBalance, err)
	}

	// A retry must not debit the main wallet twice.
	_, moved, err = p2p.FundWallet(ctx, sellerID, "20000000", "fund-test-0001")
	if err != nil || moved {
		t.Fatalf("idempotent fund retry: moved=%v err=%v", moved, err)
	}

	listing, err := p2p.CreateListing(ctx, sellerID, "20000000", "UPI")
	if err != nil {
		t.Fatalf("create listing: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_orders WHERE listing_id=$1`, listing.ID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_listings WHERE id=$1`, listing.ID)
	})
	balance, err = p2p.WalletBalance(ctx, sellerID)
	if err != nil {
		t.Fatalf("load reserved wallet: %v", err)
	}
	assertWallet(t, balance, "0", "20000000", "20000000")

	order, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "5000000", "order-test-success")
	if err != nil {
		t.Fatalf("create success order: %v", err)
	}
	if order.Status != P2PStatusPendingPayment || order.EscrowRaw != "5000000" {
		t.Fatalf("new order status/escrow = %s/%s", order.Status, order.EscrowRaw)
	}
	if _, err = p2p.MarkPaid(ctx, buyerID, order.ID); err != nil {
		t.Fatalf("mark paid: %v", err)
	}
	completed, err := p2p.ReleaseOrder(ctx, sellerID, order.ID)
	if err != nil {
		t.Fatalf("release order: %v", err)
	}
	if completed.Status != P2PStatusCompleted || completed.EscrowRaw != "0" || completed.AmountRaw != "5000000" {
		t.Fatalf("completed order status/escrow/amount = %s/%s/%s", completed.Status, completed.EscrowRaw, completed.AmountRaw)
	}
	buyerWallet, err := p2p.WalletBalance(ctx, buyerID)
	if err != nil {
		t.Fatalf("load buyer wallet: %v", err)
	}
	assertWallet(t, buyerWallet, "5000000", "0", "5000000")

	// Releasing the same order concurrently/repeatedly remains exactly once.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, releaseErr := p2p.ReleaseOrder(context.Background(), sellerID, order.ID); releaseErr != nil {
				t.Errorf("idempotent release: %v", releaseErr)
			}
		}()
	}
	wg.Wait()
	buyerWallet, _ = p2p.WalletBalance(ctx, buyerID)
	assertWallet(t, buyerWallet, "5000000", "0", "5000000")

	failedOrder, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "5000000", "order-test-refund")
	if err != nil {
		t.Fatalf("create refundable order: %v", err)
	}
	cancelled, err := p2p.CancelOrder(ctx, buyerID, failedOrder.ID)
	if err != nil {
		t.Fatalf("cancel order: %v", err)
	}
	if cancelled.Status != P2PStatusCancelled || cancelled.EscrowRaw != "0" {
		t.Fatalf("cancelled order status/escrow = %s/%s", cancelled.Status, cancelled.EscrowRaw)
	}
	sellerWallet, err := p2p.WalletBalance(ctx, sellerID)
	if err != nil {
		t.Fatalf("load refunded seller wallet: %v", err)
	}
	assertWallet(t, sellerWallet, "0", "15000000", "15000000")

	expiring, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "5000000", "order-test-expiry")
	if err != nil {
		t.Fatalf("create expiring order: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE p2p_orders SET expires_at=now()-interval '1 second' WHERE id=$1`, expiring.ID); err != nil {
		t.Fatalf("expire order fixture: %v", err)
	}
	if err = p2p.ExpirePendingOrders(ctx, 50); err != nil {
		t.Fatalf("process failed order: %v", err)
	}
	failed, err := scanOrder(pool.QueryRow(ctx, orderSelect+` WHERE id=$1`, expiring.ID))
	if err != nil {
		t.Fatalf("load failed order: %v", err)
	}
	if failed.Status != P2PStatusCancelled || failed.EscrowRaw != "0" {
		t.Fatalf("failed order status/escrow = %s/%s", failed.Status, failed.EscrowRaw)
	}
	sellerWallet, _ = p2p.WalletBalance(ctx, sellerID)
	assertWallet(t, sellerWallet, "0", "15000000", "15000000")
}

func TestP2PWalletUSDBEscrowSuccess(t *testing.T) {
	pool := p2pTestPool(t)
	ctx := context.Background()
	// Reproduce the constraint left by an older USDC-only database, then run
	// startup migrations again. Existing installations must be upgraded too.
	if _, err := pool.Exec(ctx, `ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_asset_check; ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_asset_check CHECK (asset = 'USDC')`); err != nil {
		t.Fatalf("install legacy USDC order constraint: %v", err)
	}
	migratedPool, err := db.New(ctx, pool.Config().ConnString())
	if err != nil {
		t.Fatalf("migrate legacy P2P order constraint: %v", err)
	}
	migratedPool.Close()
	p2p := NewP2PRepo(pool)
	ledger := NewLedgerRepo(pool)
	sellerID := newTestUser(t, pool)
	buyerID := newTestUser(t, pool)

	if err := ledger.CreditBalance(ctx, sellerID, "USDB", "10000000"); err != nil {
		t.Fatalf("credit seller USDB: %v", err)
	}
	balance, moved, err := p2p.FundWalletAsset(ctx, sellerID, "USDB", "10000000", "fund-usdb-test-0001")
	if err != nil || !moved {
		t.Fatalf("fund USDB P2P wallet: moved=%v err=%v", moved, err)
	}
	assertWallet(t, balance, "10000000", "0", "10000000")

	listing, err := p2p.CreateListingForAsset(ctx, sellerID, "USDB", "10000000", "UPI")
	if err != nil {
		t.Fatalf("create USDB listing: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_orders WHERE listing_id=$1`, listing.ID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_listings WHERE id=$1`, listing.ID)
	})

	order, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "10000000", "order-usdb-success")
	if err != nil {
		t.Fatalf("create USDB order: %v", err)
	}
	if order.Asset != "USDB" || order.EscrowRaw != "10000000" {
		t.Fatalf("USDB order asset/escrow = %s/%s", order.Asset, order.EscrowRaw)
	}
	if _, err = p2p.MarkPaid(ctx, buyerID, order.ID); err != nil {
		t.Fatalf("mark USDB order paid: %v", err)
	}
	completed, err := p2p.ReleaseOrder(ctx, sellerID, order.ID)
	if err != nil {
		t.Fatalf("release USDB order: %v", err)
	}
	if completed.Asset != "USDB" || completed.EscrowRaw != "0" {
		t.Fatalf("completed USDB order asset/escrow = %s/%s", completed.Asset, completed.EscrowRaw)
	}
	buyerUSDB, err := p2p.WalletBalanceForAsset(ctx, buyerID, "USDB")
	if err != nil {
		t.Fatalf("load buyer USDB wallet: %v", err)
	}
	assertWallet(t, buyerUSDB, "10000000", "0", "10000000")
	buyerUSDC, err := p2p.WalletBalanceForAsset(ctx, buyerID, "USDC")
	if err != nil {
		t.Fatalf("load buyer USDC wallet: %v", err)
	}
	assertWallet(t, buyerUSDC, "0", "0", "0")
}

func assertWallet(t *testing.T, got *models.P2PWalletBalance, available, reserved, total string) {
	t.Helper()
	if got.AvailableRaw != available || got.ReservedRaw != reserved || got.TotalRaw != total {
		t.Fatalf("P2P wallet = available %s reserved %s total %s; want %s/%s/%s", got.AvailableRaw, got.ReservedRaw, got.TotalRaw, available, reserved, total)
	}
}

// p2pTestPool uses its own PostgreSQL schema so a running local backend cannot
// deadlock these migration-heavy integration tests by serving the public schema.
func p2pTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	_ = godotenv.Load("../../.env")
	connString := os.Getenv("POSTGRES_SERVICE_URI")
	if connString == "" {
		t.Skip("POSTGRES_SERVICE_URI not set, skipping live-Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Skipf("could not connect to Postgres: %v", err)
	}
	schemaName := fmt.Sprintf("p2p_test_%d", time.Now().UnixNano())
	schemaSQL := pgx.Identifier{schemaName}.Sanitize()
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schemaSQL); err != nil {
		admin.Close()
		t.Skipf("could not create isolated test schema: %v", err)
	}
	u, err := url.Parse(connString)
	if err != nil {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schemaSQL+` CASCADE`)
		admin.Close()
		t.Fatalf("parse Postgres URL: %v", err)
	}
	query := u.Query()
	query.Set("options", "-c search_path="+schemaName)
	u.RawQuery = query.Encode()
	pool, err := db.New(ctx, u.String())
	if err != nil {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schemaSQL+` CASCADE`)
		admin.Close()
		t.Fatalf("initialize isolated P2P schema: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schemaSQL+` CASCADE`)
		admin.Close()
	})
	return pool
}
