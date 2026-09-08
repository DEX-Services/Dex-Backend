package repo

import (
	"context"
	"errors"
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

	if err := ledger.CreditBalance(ctx, sellerID, "BIUSD", "20200000"); err != nil {
		t.Fatalf("credit seller main wallet: %v", err)
	}
	balance, moved, err := p2p.FundWallet(ctx, sellerID, "20200000", "fund-test-0001")
	if err != nil || !moved {
		t.Fatalf("fund P2P wallet: moved=%v err=%v", moved, err)
	}
	assertWallet(t, balance, "20200000", "0", "20200000")
	mainBalance, err := ledger.BalanceFor(ctx, sellerID, "BIUSD")
	if err != nil || mainBalance != "0" {
		t.Fatalf("seller main balance = %q err=%v, want 0", mainBalance, err)
	}

	// A retry must not debit the main wallet twice.
	_, moved, err = p2p.FundWallet(ctx, sellerID, "20200000", "fund-test-0001")
	if err != nil || moved {
		t.Fatalf("idempotent fund retry: moved=%v err=%v", moved, err)
	}

	if _, err = p2p.EstablishP2PUsername(ctx, sellerID, "seller_success"); err != nil {
		t.Fatalf("establish seller username: %v", err)
	}
	if _, err = p2p.UpsertPaymentAccount(ctx, sellerID, "UPI", "Seller Success", "seller@upi", "", "", ""); err != nil {
		t.Fatalf("configure seller payment account: %v", err)
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
	assertWallet(t, balance, "0", "20200000", "20200000")

	order, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "5000000", "order-test-success")
	if err != nil {
		t.Fatalf("create success order: %v", err)
	}
	if order.Status != P2PStatusPendingPayment || order.EscrowRaw != "5050000" || order.BuyerFeeRaw != "50000" || order.SellerFeeRaw != "50000" {
		t.Fatalf("new order status/escrow = %s/%s", order.Status, order.EscrowRaw)
	}
	if _, err = p2p.AddOrderProof(ctx, buyerID, order.ID, "proof.png", "image/png", []byte("proof")); err != nil {
		t.Fatalf("upload payment proof: %v", err)
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
	assertWallet(t, buyerWallet, "4950000", "0", "4950000")
	assertAdminP2PWallet(t, pool, "100000")

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
	assertWallet(t, buyerWallet, "4950000", "0", "4950000")
	assertAdminP2PWallet(t, pool, "100000")

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
	assertWallet(t, sellerWallet, "0", "15150000", "15150000")

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
	assertWallet(t, sellerWallet, "0", "15150000", "15150000")
}

func TestP2PWalletBIUSDEscrowSuccess(t *testing.T) {
	pool := p2pTestPool(t)
	ctx := context.Background()
	// Reproduce constraints left by older database versions, then run startup
	// migrations again. Existing installations must be upgraded too.
	if _, err := pool.Exec(ctx, `
		ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_asset_check;
		ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_asset_check CHECK (asset = 'USDC');
		ALTER TABLE p2p_orders ADD COLUMN initiator_id TEXT NOT NULL;
		ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_payment_methods_check;
		ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_payment_methods_check CHECK (
			payment_methods <@ ARRAY['UPI','Bank Transfer','NEFT','IMPS']::TEXT[]
		);
	`); err != nil {
		t.Fatalf("install legacy P2P constraints: %v", err)
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

	if err := ledger.CreditBalance(ctx, sellerID, "BIUSD", "10100000"); err != nil {
		t.Fatalf("credit seller BIUSD: %v", err)
	}
	balance, moved, err := p2p.FundWalletAsset(ctx, sellerID, "BIUSD", "10100000", "fund-biusd-test-0001")
	if err != nil || !moved {
		t.Fatalf("fund BIUSD P2P wallet: moved=%v err=%v", moved, err)
	}
	assertWallet(t, balance, "10100000", "0", "10100000")

	if _, err = p2p.EstablishP2PUsername(ctx, sellerID, "seller_biusd"); err != nil {
		t.Fatalf("establish seller username: %v", err)
	}
	if _, err = p2p.UpsertPaymentAccount(ctx, sellerID, "Bank Transfer", "Seller BIUSD", "payment-id", "", "", ""); err == nil {
		t.Fatal("expected bank name and IFSC to be required for Bank Transfer")
	}
	for _, method := range []string{"UPI", "Bank Transfer", "MPESN", "NEFT", "IMPS"} {
		bankName, ifscCode := "", ""
		if method == "Bank Transfer" || method == "NEFT" || method == "IMPS" {
			bankName, ifscCode = "Test Bank", "TEST0123456"
		}
		if _, err = p2p.UpsertPaymentAccount(ctx, sellerID, method, "Seller BIUSD", "payment-id", "", bankName, ifscCode); err != nil {
			t.Fatalf("configure %s account: %v", method, err)
		}
	}
	listing, err := p2p.CreateListingWithDetails(ctx, sellerID, "SELL", "BIUSD", "10000000", []string{"UPI", "Bank Transfer", "MPESN", "NEFT", "IMPS"}, "")
	if err != nil {
		t.Fatalf("create BIUSD listing: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_orders WHERE listing_id=$1`, listing.ID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM p2p_listings WHERE id=$1`, listing.ID)
	})

	order, err := p2p.CreateOrderWithPayment(ctx, buyerID, listing.ID, "10000000", "Bank Transfer", "order-biusd-success")
	if err != nil {
		t.Fatalf("create BIUSD order: %v", err)
	}
	if order.Asset != "BIUSD" || order.EscrowRaw != "10100000" {
		t.Fatalf("BIUSD order asset/escrow = %s/%s", order.Asset, order.EscrowRaw)
	}
	if order.PaymentBankName != "Test Bank" || order.PaymentIFSCCode != "TEST0123456" {
		t.Fatalf("bank snapshot = %q/%q", order.PaymentBankName, order.PaymentIFSCCode)
	}
	if _, err = p2p.AddOrderProof(ctx, buyerID, order.ID, "proof.png", "image/png", []byte("proof")); err != nil {
		t.Fatalf("upload BIUSD payment proof: %v", err)
	}
	if _, err = p2p.MarkPaid(ctx, buyerID, order.ID); err != nil {
		t.Fatalf("mark BIUSD order paid: %v", err)
	}
	completed, err := p2p.ReleaseOrder(ctx, sellerID, order.ID)
	if err != nil {
		t.Fatalf("release BIUSD order: %v", err)
	}
	if completed.Asset != "BIUSD" || completed.EscrowRaw != "0" {
		t.Fatalf("completed BIUSD order asset/escrow = %s/%s", completed.Asset, completed.EscrowRaw)
	}
	buyerBIUSD, err := p2p.WalletBalanceForAsset(ctx, buyerID, "BIUSD")
	if err != nil {
		t.Fatalf("load buyer BIUSD wallet: %v", err)
	}
	assertWallet(t, buyerBIUSD, "9900000", "0", "9900000")
	assertAdminP2PWallet(t, pool, "200000")
}

func TestP2PBuyAdUsesTakerAsSeller(t *testing.T) {
	pool := p2pTestPool(t)
	ctx := context.Background()
	p2p := NewP2PRepo(pool)
	ledger := NewLedgerRepo(pool)
	creatorBuyerID := newTestUser(t, pool)
	takerSellerID := newTestUser(t, pool)

	if err := ledger.CreditBalance(ctx, takerSellerID, "BIUSD", "5050000"); err != nil {
		t.Fatalf("credit taker seller: %v", err)
	}
	if _, moved, err := p2p.FundWalletAsset(ctx, takerSellerID, "BIUSD", "5050000", "fund-buy-ad-seller"); err != nil || !moved {
		t.Fatalf("fund taker P2P wallet: moved=%v err=%v", moved, err)
	}
	if _, err := p2p.EstablishP2PUsername(ctx, creatorBuyerID, "buyer_ad_creator"); err != nil {
		t.Fatalf("establish creator username: %v", err)
	}
	for _, method := range []string{"UPI", "Bank Transfer"} {
		bankName, ifscCode := "", ""
		if method == "Bank Transfer" || method == "NEFT" || method == "IMPS" {
			bankName, ifscCode = "Test Bank", "TEST0123456"
		}
		if _, err := p2p.UpsertPaymentAccount(ctx, takerSellerID, method, "Taker Seller", "payment-id", "", bankName, ifscCode); err != nil {
			t.Fatalf("configure taker %s account: %v", method, err)
		}
	}
	if _, err := p2p.EstablishP2PUsername(ctx, creatorBuyerID, "different_name"); err == nil {
		t.Fatal("expected established P2P username to be immutable")
	}

	listing, err := p2p.CreateListingWithDetails(ctx, creatorBuyerID, "BUY", "BIUSD", "5000000", []string{"UPI", "Bank Transfer"}, "")
	if err != nil {
		t.Fatalf("create buy ad: %v", err)
	}
	if _, err = p2p.CreateOrderWithPayment(ctx, creatorBuyerID, listing.ID, "1000000", "UPI", "self-trade"); !errors.Is(err, ErrP2PSelfPurchase) {
		t.Fatalf("self trade error = %v, want %v", err, ErrP2PSelfPurchase)
	}

	cancelledOrder, err := p2p.CreateOrderWithPayment(ctx, takerSellerID, listing.ID, "2000000", "UPI", "take-buy-ad-cancel")
	if err != nil {
		t.Fatalf("take refundable buy ad: %v", err)
	}
	if _, err = p2p.CancelOrder(ctx, creatorBuyerID, cancelledOrder.ID); err != nil {
		t.Fatalf("cancel buy-ad order: %v", err)
	}
	refundedBalance, err := p2p.WalletBalance(ctx, takerSellerID)
	if err != nil {
		t.Fatalf("load refunded taker balance: %v", err)
	}
	assertWallet(t, refundedBalance, "5050000", "0", "5050000")

	order, err := p2p.CreateOrderWithPayment(ctx, takerSellerID, listing.ID, "5000000", "Bank Transfer", "take-buy-ad")
	if err != nil {
		t.Fatalf("take buy ad: %v", err)
	}
	if order.SellerID != takerSellerID || order.BuyerID != creatorBuyerID || order.PaymentMethod != "Bank Transfer" {
		t.Fatalf("buy-ad roles/method = seller %s buyer %s method %s", order.SellerID, order.BuyerID, order.PaymentMethod)
	}
	if _, err = p2p.AddOrderProof(ctx, creatorBuyerID, order.ID, "proof.png", "image/png", []byte("proof")); err != nil {
		t.Fatalf("upload buy-ad payment proof: %v", err)
	}
	takerBalance, err := p2p.WalletBalance(ctx, takerSellerID)
	if err != nil {
		t.Fatalf("load taker balance: %v", err)
	}
	assertWallet(t, takerBalance, "0", "0", "0")

	if _, err = p2p.MarkPaid(ctx, creatorBuyerID, order.ID); err != nil {
		t.Fatalf("creator buyer marks paid: %v", err)
	}
	if _, err = p2p.ReleaseOrder(ctx, takerSellerID, order.ID); err != nil {
		t.Fatalf("taker seller releases: %v", err)
	}
	creatorBalance, err := p2p.WalletBalance(ctx, creatorBuyerID)
	if err != nil {
		t.Fatalf("load creator buyer balance: %v", err)
	}
	assertWallet(t, creatorBalance, "4950000", "0", "4950000")
	assertAdminP2PWallet(t, pool, "100000")
}

func TestP2POrderEvidenceChatAndAppealResolution(t *testing.T) {
	pool := p2pTestPool(t)
	ctx := context.Background()
	p2p := NewP2PRepo(pool)
	ledger := NewLedgerRepo(pool)
	sellerID := newTestUser(t, pool)
	buyerID := newTestUser(t, pool)
	outsiderID := newTestUser(t, pool)

	if err := ledger.CreditBalance(ctx, sellerID, "BIUSD", "20200000"); err != nil {
		t.Fatalf("credit seller: %v", err)
	}
	if _, moved, err := p2p.FundWalletAsset(ctx, sellerID, "BIUSD", "20200000", "workflow-fund"); err != nil || !moved {
		t.Fatalf("fund seller P2P wallet: moved=%v err=%v", moved, err)
	}
	if _, err := p2p.EstablishP2PUsername(ctx, sellerID, "workflow_seller"); err != nil {
		t.Fatalf("establish seller username: %v", err)
	}
	if _, err := p2p.EstablishP2PUsername(ctx, buyerID, "workflow_buyer"); err != nil {
		t.Fatalf("establish buyer username: %v", err)
	}
	if _, err := p2p.UpsertPaymentAccount(ctx, sellerID, "UPI", "Original Seller", "original@upi", "Pay once", "", ""); err != nil {
		t.Fatalf("configure seller payment account: %v", err)
	}

	createOrder := func(key string) *models.P2POrder {
		listing, err := p2p.CreateListingWithDetails(ctx, sellerID, "SELL", "BIUSD", "10000000", []string{"UPI"}, "")
		if err != nil {
			t.Fatalf("create listing: %v", err)
		}
		order, err := p2p.CreateOrderWithPayment(ctx, buyerID, listing.ID, "10000000", "UPI", key)
		if err != nil {
			t.Fatalf("create order: %v", err)
		}
		return order
	}

	releaseOrder := createOrder("workflow-release")
	if releaseOrder.PaymentAccountName != "Original Seller" || releaseOrder.PaymentAccountID != "original@upi" {
		t.Fatalf("payment snapshot = %q/%q", releaseOrder.PaymentAccountName, releaseOrder.PaymentAccountID)
	}
	if _, err := p2p.UpsertPaymentAccount(ctx, sellerID, "UPI", "Updated Seller", "updated@upi", "Updated", "", ""); err != nil {
		t.Fatalf("update seller payment account: %v", err)
	}
	snapshot, err := p2p.Order(ctx, buyerID, releaseOrder.ID)
	if err != nil || snapshot.PaymentAccountID != "original@upi" {
		t.Fatalf("order payment snapshot changed: order=%+v err=%v", snapshot, err)
	}
	if _, err = p2p.MarkPaidWithAttestation(ctx, buyerID, releaseOrder.ID, true); err == nil {
		t.Fatal("expected payment proof to be required")
	}
	if _, err = p2p.AddOrderProof(ctx, outsiderID, releaseOrder.ID, "outsider.png", "image/png", []byte("proof")); !errors.Is(err, ErrP2PForbidden) {
		t.Fatalf("outsider proof error = %v, want forbidden", err)
	}
	proof, err := p2p.AddOrderProof(ctx, buyerID, releaseOrder.ID, "receipt.png", "image/png", []byte("proof"))
	if err != nil {
		t.Fatalf("upload proof: %v", err)
	}
	if _, err = p2p.OrderProofFile(ctx, sellerID, proof.ID); err != nil {
		t.Fatalf("seller reads participant proof: %v", err)
	}
	if _, err = p2p.AddOrderMessage(ctx, outsiderID, releaseOrder.ID, "not allowed"); !errors.Is(err, ErrP2PForbidden) {
		t.Fatalf("outsider chat error = %v, want forbidden", err)
	}
	if _, err = p2p.AddOrderMessage(ctx, buyerID, releaseOrder.ID, "Payment sent"); err != nil {
		t.Fatalf("add participant message: %v", err)
	}
	if messages, err := p2p.OrderMessages(ctx, sellerID, releaseOrder.ID); err != nil || len(messages) < 2 {
		t.Fatalf("seller chat messages = %d err=%v", len(messages), err)
	}
	if _, err = p2p.MarkPaidWithAttestation(ctx, buyerID, releaseOrder.ID, false); err == nil {
		t.Fatal("expected own-account attestation to be required")
	}
	if _, err = p2p.MarkPaidWithAttestation(ctx, buyerID, releaseOrder.ID, true); err != nil {
		t.Fatalf("mark paid: %v", err)
	}
	if _, err = p2p.AppealOrder(ctx, buyerID, releaseOrder.ID, "Seller has not released"); err == nil {
		t.Fatal("expected appeal delay to be enforced")
	}
	if _, err = pool.Exec(ctx, `UPDATE p2p_orders SET appeal_available_at=now()-interval '1 second' WHERE id=$1`, releaseOrder.ID); err != nil {
		t.Fatalf("open appeal window fixture: %v", err)
	}
	appealed, err := p2p.AppealOrder(ctx, buyerID, releaseOrder.ID, "Seller has not released")
	if err != nil || appealed.Status != P2PStatusAppeal {
		t.Fatalf("appeal order: status=%v err=%v", appealed, err)
	}
	if _, err = p2p.ReleaseOrder(ctx, sellerID, releaseOrder.ID); !errors.Is(err, ErrP2PInvalidState) {
		t.Fatalf("seller release during appeal error = %v, want invalid state", err)
	}
	if orders, err := p2p.AppealedOrders(ctx); err != nil || len(orders) != 1 {
		t.Fatalf("admin appealed orders = %d err=%v", len(orders), err)
	}
	if proofs, err := p2p.AdminOrderProofs(ctx, releaseOrder.ID); err != nil || len(proofs) != 1 {
		t.Fatalf("admin proofs = %d err=%v", len(proofs), err)
	}
	completed, err := p2p.ResolveAppeal(ctx, releaseOrder.ID, "RELEASE")
	if err != nil || completed.Status != P2PStatusCompleted {
		t.Fatalf("admin release: status=%v err=%v", completed, err)
	}
	buyerWallet, err := p2p.WalletBalance(ctx, buyerID)
	if err != nil {
		t.Fatalf("load buyer wallet: %v", err)
	}
	assertWallet(t, buyerWallet, "9900000", "0", "9900000")
	assertAdminP2PWallet(t, pool, "200000")

	refundOrder := createOrder("workflow-refund")
	if _, err = p2p.AddOrderProof(ctx, buyerID, refundOrder.ID, "refund.png", "image/png", []byte("proof")); err != nil {
		t.Fatalf("upload refund proof: %v", err)
	}
	if _, err = p2p.MarkPaid(ctx, buyerID, refundOrder.ID); err != nil {
		t.Fatalf("mark refund order paid: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE p2p_orders SET appeal_available_at=now()-interval '1 second' WHERE id=$1`, refundOrder.ID); err != nil {
		t.Fatalf("open refund appeal window fixture: %v", err)
	}
	if _, err = p2p.AppealOrder(ctx, sellerID, refundOrder.ID, "Payment was not received"); err != nil {
		t.Fatalf("seller appeal: %v", err)
	}
	cancelled, err := p2p.ResolveAppeal(ctx, refundOrder.ID, "REFUND")
	if err != nil || cancelled.Status != P2PStatusCancelled || cancelled.EscrowRaw != "0" {
		t.Fatalf("admin refund: order=%+v err=%v", cancelled, err)
	}
	sellerWallet, err := p2p.WalletBalance(ctx, sellerID)
	if err != nil {
		t.Fatalf("load refunded seller wallet: %v", err)
	}
	assertWallet(t, sellerWallet, "0", "10100000", "10100000")
	assertAdminP2PWallet(t, pool, "200000")
}

func TestP2PListingLimitsAndAdvertiserStatistics(t *testing.T) {
	pool := p2pTestPool(t)
	ctx := context.Background()
	p2p := NewP2PRepo(pool)
	ledger := NewLedgerRepo(pool)
	sellerID := newTestUser(t, pool)
	buyerID := newTestUser(t, pool)

	if err := ledger.CreditBalance(ctx, sellerID, "BIUSD", "10100000"); err != nil {
		t.Fatalf("credit seller: %v", err)
	}
	if _, moved, err := p2p.FundWalletAsset(ctx, sellerID, "BIUSD", "10100000", "limits-fund"); err != nil || !moved {
		t.Fatalf("fund seller P2P wallet: moved=%v err=%v", moved, err)
	}
	if _, err := p2p.EstablishP2PUsername(ctx, sellerID, "limits_seller"); err != nil {
		t.Fatalf("establish seller username: %v", err)
	}
	if _, err := p2p.UpsertPaymentAccount(ctx, sellerID, "UPI", "Limits Seller", "limits@upi", "", "", ""); err != nil {
		t.Fatalf("configure payment account: %v", err)
	}
	listing, err := p2p.CreateListingWithLimits(ctx, sellerID, "SELL", "BIUSD", "10000000", []string{"UPI"}, "", "200.00", "500.00")
	if err != nil {
		t.Fatalf("create limited listing: %v", err)
	}
	if listing.MinOrderFiat != "200.00000000" || listing.MaxOrderFiat != "500.00000000" {
		t.Fatalf("listing limits = %s-%s", listing.MinOrderFiat, listing.MaxOrderFiat)
	}
	if _, err = p2p.CreateOrder(ctx, buyerID, listing.ID, "1000000", "limits-below"); err == nil {
		t.Fatal("expected below-minimum order to be rejected")
	}
	if _, err = p2p.CreateOrder(ctx, buyerID, listing.ID, "6000000", "limits-above"); err == nil {
		t.Fatal("expected above-maximum order to be rejected")
	}

	completedOrder, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "5000000", "limits-complete")
	if err != nil {
		t.Fatalf("create valid order: %v", err)
	}
	if _, err = p2p.AddOrderProof(ctx, buyerID, completedOrder.ID, "limits.png", "image/png", []byte("proof")); err != nil {
		t.Fatalf("add proof: %v", err)
	}
	if _, err = p2p.MarkPaid(ctx, buyerID, completedOrder.ID); err != nil {
		t.Fatalf("mark valid order paid: %v", err)
	}
	if _, err = p2p.ReleaseOrder(ctx, sellerID, completedOrder.ID); err != nil {
		t.Fatalf("complete valid order: %v", err)
	}

	if _, err = p2p.CreateOrder(ctx, buyerID, listing.ID, "4000000", "limits-dust"); err == nil {
		t.Fatal("expected an order leaving less than the minimum to be rejected")
	}
	cancelledOrder, err := p2p.CreateOrder(ctx, buyerID, listing.ID, "2000000", "limits-cancel")
	if err != nil {
		t.Fatalf("create cancellable order: %v", err)
	}
	if _, err = p2p.CancelOrderWithReason(ctx, buyerID, cancelledOrder.ID, "Buyer changed payment method"); err != nil {
		t.Fatalf("cancel order: %v", err)
	}

	listings, err := p2p.Listings(ctx, sellerID, false)
	if err != nil || len(listings) != 1 {
		t.Fatalf("load listing statistics: count=%d err=%v", len(listings), err)
	}
	got := listings[0]
	if got.CompletedOrders != 1 || got.CompletedAmountRaw != "5000000" {
		t.Fatalf("ad completion = %d orders/%s raw", got.CompletedOrders, got.CompletedAmountRaw)
	}
	if got.CompletedOrders30d != 1 || got.RatedOrders30d != 1 || got.CompletionRate30d != "100.00" {
		t.Fatalf("advertiser 30d statistics = completed %d rated %d rate %s", got.CompletedOrders30d, got.RatedOrders30d, got.CompletionRate30d)
	}
	var cancelledBy string
	if err = pool.QueryRow(ctx, `SELECT cancelled_by FROM p2p_orders WHERE id=$1`, cancelledOrder.ID).Scan(&cancelledBy); err != nil || cancelledBy != buyerID {
		t.Fatalf("cancel attribution = %q err=%v", cancelledBy, err)
	}
}

func assertAdminP2PWallet(t *testing.T, pool *pgxpool.Pool, want string) {
	t.Helper()
	var got string
	if err := pool.QueryRow(context.Background(), `SELECT available_raw::text FROM p2p_admin_wallet_balances WHERE asset='BIUSD'`).Scan(&got); err != nil {
		t.Fatalf("load admin P2P wallet: %v", err)
	}
	if got != want {
		t.Fatalf("admin P2P wallet = %s, want %s", got, want)
	}
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
