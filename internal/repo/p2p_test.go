package repo

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestP2PEscrowLifecycleAndIdempotency(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	p2p := NewP2PRepo(pool)
	ledger := NewLedgerRepo(pool)

	oldPrice, oldPriceErr := p2p.TodayPrice(ctx)
	if _, err := p2p.SetTodayPrice(ctx, "100"); err != nil {
		t.Fatalf("set test price: %v", err)
	}
	t.Cleanup(func() {
		if oldPriceErr == nil {
			_, _ = p2p.SetTodayPrice(context.Background(), oldPrice.Price)
		} else {
			_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_price_history
				WHERE asset='USDC' AND fiat_currency='INR' AND price_date=(now() AT TIME ZONE 'Asia/Kolkata')::date`)
		}
	})

	sellerID := newTestUser(t, pool)
	buyerID := newTestUser(t, pool)
	if err := ledger.CreditBalance(ctx, sellerID, "USDC", "10000000"); err != nil {
		t.Fatalf("credit seller: %v", err)
	}
	listing, err := p2p.CreateListing(ctx, sellerID, "10000000", "UPI")
	if err != nil {
		t.Fatalf("create listing: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_engine_outbox WHERE order_id IN (SELECT id FROM p2p_orders WHERE listing_id=$1)`, listing.ID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_orders WHERE listing_id=$1`, listing.ID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_listings WHERE id=$1`, listing.ID)
	})

	order, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "5000000", "test-order-key-0001")
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	if order.Status != P2PStatusPendingPayment {
		t.Fatalf("status = %s, want %s", order.Status, P2PStatusPendingPayment)
	}
	if time.Until(order.ExpiresAt) <= 0 {
		t.Fatal("new order must have a future payment deadline")
	}

	retry, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "5000000", "test-order-key-0001")
	if err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if retry.ID != order.ID {
		t.Fatalf("retry created order %s, want original %s", retry.ID, order.ID)
	}
	assertP2PBalance(t, pool, sellerID, "10000000", "10000000")
	assertP2PBalance(t, pool, buyerID, "0", "0")

	paid, err := p2p.MarkPaid(ctx, buyerID, order.ID)
	if err != nil {
		t.Fatalf("mark paid: %v", err)
	}
	if paid.Status != P2PStatusPaymentMade {
		t.Fatalf("status = %s, want %s", paid.Status, P2PStatusPaymentMade)
	}
	assertP2PBalance(t, pool, sellerID, "10000000", "10000000")
	assertP2PBalance(t, pool, buyerID, "0", "0")

	completed, err := p2p.ReleaseOrder(ctx, sellerID, order.ID)
	if err != nil {
		t.Fatalf("release order: %v", err)
	}
	if completed.Status != P2PStatusCompleted {
		t.Fatalf("status = %s, want %s", completed.Status, P2PStatusCompleted)
	}
	assertP2PBalance(t, pool, sellerID, "5000000", "5000000")
	assertP2PBalance(t, pool, buyerID, "5000000", "0")

	var outboxCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM p2p_engine_outbox WHERE order_id=$1`, order.ID).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	if outboxCount != 2 {
		t.Fatalf("outbox event count = %d, want 2", outboxCount)
	}
}

func assertP2PBalance(t *testing.T, pool *pgxpool.Pool, userID, wantTotal, wantLocked string) {
	t.Helper()
	var total, locked string
	if err := pool.QueryRow(context.Background(), `SELECT COALESCE(b."USDC",0)::text,COALESCE(b."USDC_locked",0)::text
		FROM users u LEFT JOIN user_balances b ON b.user_id=u.id WHERE u.id=$1`, userID).Scan(&total, &locked); err != nil {
		t.Fatalf("load balance for %s: %v", userID, err)
	}
	if total != wantTotal || locked != wantLocked {
		t.Fatalf("balance for %s = total %s locked %s, want total %s locked %s", userID, total, locked, wantTotal, wantLocked)
	}
}
