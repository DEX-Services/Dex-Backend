package repo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	P2PStatusPendingPayment = "pending_payment"
	P2PStatusPaymentMade    = "payment_made"
	P2PStatusCompleted      = "completed"
	P2PStatusCancelled      = "cancelled"
	p2pFundingWallet        = "P2P_WALLET"
	p2pFundingLegacyMain    = "MAIN_WALLET_LEGACY"
)

var (
	ErrP2PNotFound       = errors.New("p2p listing not found")
	ErrP2POrderNotFound  = errors.New("p2p order not found")
	ErrP2PSelfPurchase   = errors.New("seller cannot buy their own listing")
	ErrP2PUnavailable    = errors.New("requested amount is no longer available")
	ErrP2PInvalidState   = errors.New("order is not in the required state")
	ErrP2PForbidden      = errors.New("not authorized for this p2p order")
	ErrP2PExpired        = errors.New("payment window has expired")
	ErrP2PIdempotencyKey = errors.New("idempotency key was already used for another request")
)

var validP2PPaymentMethods = map[string]bool{"UPI": true, "Bank Transfer": true, "NEFT": true, "IMPS": true}

type P2PRepo struct {
	pool   *pgxpool.Pool
	ledger *LedgerRepo
}

func NewP2PRepo(pool *pgxpool.Pool) *P2PRepo {
	return &P2PRepo{pool: pool, ledger: NewLedgerRepo(pool)}
}

func (r *P2PRepo) TodayPrice(ctx context.Context) (*models.P2PPrice, error) {
	return r.PriceFor(ctx, "USDC")
}

func normalizeP2PAsset(asset string) (string, error) {
	asset = strings.ToUpper(strings.TrimSpace(asset))
	if asset != "USDC" && asset != "USDB" {
		return "", fmt.Errorf("P2P asset must be USDC or USDB")
	}
	return asset, nil
}

func (r *P2PRepo) PriceFor(ctx context.Context, asset string) (*models.P2PPrice, error) {
	asset, err := normalizeP2PAsset(asset)
	if err != nil {
		return nil, err
	}
	var p models.P2PPrice
	err = r.pool.QueryRow(ctx, `SELECT asset,fiat_currency,price::text,price_date::text,created_at FROM p2p_price_history WHERE asset=$1 AND fiat_currency='INR' AND price_date=CURRENT_DATE ORDER BY created_at DESC LIMIT 1`, asset).Scan(&p.Asset, &p.FiatCurrency, &p.Price, &p.PriceDate, &p.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("today's %s/INR P2P price has not been entered", asset)
	}
	return &p, err
}

func validateP2PAmount(raw string) error {
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok || n.Sign() <= 0 {
		return fmt.Errorf("amount must be a positive raw asset integer")
	}
	return nil
}

func validateIdempotencyKey(key string, required bool) (string, error) {
	key = strings.TrimSpace(key)
	if required && key == "" {
		return "", fmt.Errorf("idempotencyKey is required")
	}
	if len(key) > 128 {
		return "", fmt.Errorf("idempotencyKey must be at most 128 characters")
	}
	return key, nil
}

func (r *P2PRepo) todayPriceTx(ctx context.Context, tx pgx.Tx, asset string) (string, error) {
	var price string
	err := tx.QueryRow(ctx, `SELECT price::text FROM p2p_price_history WHERE asset=$1 AND fiat_currency='INR' AND price_date=CURRENT_DATE LIMIT 1`, asset).Scan(&price)
	if err == pgx.ErrNoRows {
		return "", fmt.Errorf("today's %s/INR P2P price has not been entered", asset)
	}
	return price, err
}

func (r *P2PRepo) lockWallet(ctx context.Context, tx pgx.Tx, userID, asset string) error {
	if _, err := tx.Exec(ctx, `INSERT INTO p2p_wallet_balances(user_id,asset) VALUES($1,$2) ON CONFLICT(user_id,asset) DO NOTHING`, userID, asset); err != nil {
		return err
	}
	var one int
	return tx.QueryRow(ctx, `SELECT 1 FROM p2p_wallet_balances WHERE user_id=$1 AND asset=$2 FOR UPDATE`, userID, asset).Scan(&one)
}

func scanWallet(row pgx.Row) (*models.P2PWalletBalance, error) {
	var b models.P2PWalletBalance
	err := row.Scan(&b.Asset, &b.AvailableRaw, &b.ReservedRaw, &b.TotalRaw)
	return &b, err
}

func (r *P2PRepo) walletTx(ctx context.Context, tx pgx.Tx, userID, asset string) (*models.P2PWalletBalance, error) {
	return scanWallet(tx.QueryRow(ctx, `SELECT asset,available_raw::text,reserved_raw::text,(available_raw+reserved_raw)::text FROM p2p_wallet_balances WHERE user_id=$1 AND asset=$2`, userID, asset))
}

func (r *P2PRepo) WalletBalance(ctx context.Context, userID string) (*models.P2PWalletBalance, error) {
	return r.WalletBalanceForAsset(ctx, userID, "USDC")
}

func (r *P2PRepo) WalletBalanceForAsset(ctx context.Context, userID, asset string) (*models.P2PWalletBalance, error) {
	asset, err := normalizeP2PAsset(asset)
	if err != nil {
		return nil, err
	}
	b, err := scanWallet(r.pool.QueryRow(ctx, `SELECT asset,available_raw::text,reserved_raw::text,(available_raw+reserved_raw)::text FROM p2p_wallet_balances WHERE user_id=$1 AND asset=$2`, userID, asset))
	if err == pgx.ErrNoRows {
		return &models.P2PWalletBalance{Asset: asset, AvailableRaw: "0", ReservedRaw: "0", TotalRaw: "0"}, nil
	}
	return b, err
}

func (r *P2PRepo) WalletBalances(ctx context.Context, userID string) ([]models.P2PWalletBalance, error) {
	out := make([]models.P2PWalletBalance, 0, 2)
	for _, asset := range []string{"USDC", "USDB"} {
		balance, err := r.WalletBalanceForAsset(ctx, userID, asset)
		if err != nil {
			return nil, err
		}
		out = append(out, *balance)
	}
	return out, nil
}

// FundWallet moves available main-wallet USDC into the P2P wallet atomically.
// moved=false means an identical idempotent request was already applied.
func (r *P2PRepo) FundWallet(ctx context.Context, userID, amountRaw, idempotencyKey string) (*models.P2PWalletBalance, bool, error) {
	return r.FundWalletAsset(ctx, userID, "USDC", amountRaw, idempotencyKey)
}

func (r *P2PRepo) FundWalletAsset(ctx context.Context, userID, asset, amountRaw, idempotencyKey string) (*models.P2PWalletBalance, bool, error) {
	asset, err := normalizeP2PAsset(asset)
	if err != nil {
		return nil, false, err
	}
	if err := validateP2PAmount(amountRaw); err != nil {
		return nil, false, err
	}
	key, err := validateIdempotencyKey(idempotencyKey, true)
	if err != nil {
		return nil, false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = r.ledger.lockBalance(ctx, tx, userID); err != nil {
		return nil, false, err
	}
	if err = r.lockWallet(ctx, tx, userID, asset); err != nil {
		return nil, false, err
	}

	var prior string
	var priorAsset string
	err = tx.QueryRow(ctx, `SELECT asset,amount_raw::text FROM p2p_wallet_entries WHERE user_id=$1 AND kind='main_to_p2p' AND idempotency_key=$2`, userID, key).Scan(&priorAsset, &prior)
	if err == nil {
		if priorAsset != asset || prior != amountRaw {
			return nil, false, ErrP2PIdempotencyKey
		}
		b, e := r.walletTx(ctx, tx, userID, asset)
		if e != nil {
			return nil, false, e
		}
		return b, false, tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return nil, false, err
	}

	_, column, err := normalizeAsset(asset)
	if err != nil {
		return nil, false, err
	}
	lockedColumn := lockedColumns[asset]
	pending, err := r.ledger.pendingWithdrawalHoldTx(ctx, tx, userID, asset)
	if err != nil {
		return nil, false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE user_balances SET `+column+`=`+column+`-$2::numeric,updated_at=now() WHERE user_id=$1 AND `+column+`-`+lockedColumn+`-$3::numeric >= $2::numeric`, userID, amountRaw, pending.String())
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() != 1 {
		return nil, false, fmt.Errorf("insufficient available %s in main wallet", asset)
	}
	if _, err = tx.Exec(ctx, `UPDATE p2p_wallet_balances SET available_raw=available_raw+$3::numeric,updated_at=now() WHERE user_id=$1 AND asset=$2`, userID, asset, amountRaw); err != nil {
		return nil, false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_wallet_entries(user_id,kind,asset,amount_raw,idempotency_key) VALUES($1,'main_to_p2p',$2,$3,$4)`, userID, asset, amountRaw, key); err != nil {
		return nil, false, err
	}
	b, err := r.walletTx(ctx, tx, userID, asset)
	if err != nil {
		return nil, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (r *P2PRepo) CreateListing(ctx context.Context, sellerID, amountRaw, method string) (*models.P2PListing, error) {
	return r.CreateListingForAsset(ctx, sellerID, "USDC", amountRaw, method)
}

func (r *P2PRepo) CreateListingForAsset(ctx context.Context, sellerID, asset, amountRaw, method string) (*models.P2PListing, error) {
	asset, err := normalizeP2PAsset(asset)
	if err != nil {
		return nil, err
	}
	if err := validateP2PAmount(amountRaw); err != nil {
		return nil, err
	}
	if !validP2PPaymentMethods[method] {
		return nil, fmt.Errorf("unsupported payment method")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	price, err := r.todayPriceTx(ctx, tx, asset)
	if err != nil {
		return nil, err
	}
	if err = r.lockWallet(ctx, tx, sellerID, asset); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `UPDATE p2p_wallet_balances SET available_raw=available_raw-$3::numeric,reserved_raw=reserved_raw+$3::numeric,updated_at=now() WHERE user_id=$1 AND asset=$2 AND available_raw >= $3::numeric`, sellerID, asset, amountRaw)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("insufficient available %s in P2P wallet", asset)
	}
	var l models.P2PListing
	err = tx.QueryRow(ctx, `INSERT INTO p2p_listings(seller_id,asset,amount_raw,remaining_raw,price,fiat_currency,payment_method,funding_source) VALUES($1,$2,$3,$3,$4,'INR',$5,$6) RETURNING id,seller_id,'',asset,amount_raw::text,remaining_raw::text,price::text,fiat_currency,payment_method,status,created_at,updated_at`, sellerID, asset, amountRaw, price, method, p2pFundingWallet).Scan(&l.ID, &l.SellerID, &l.SellerAddress, &l.Asset, &l.AmountRaw, &l.RemainingRaw, &l.Price, &l.FiatCurrency, &l.PaymentMethod, &l.Status, &l.CreatedAt, &l.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_wallet_entries(user_id,listing_id,kind,asset,amount_raw) VALUES($1,$2,'listing_reserve',$3,$4)`, sellerID, l.ID, asset, amountRaw); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &l, nil
}

const listingSelect = `SELECT l.id,l.seller_id,u.wallet_address,l.asset,l.amount_raw::text,l.remaining_raw::text,l.price::text,l.fiat_currency,l.payment_method,l.status,l.created_at,l.updated_at FROM p2p_listings l JOIN users u ON u.id=l.seller_id`

func scanListing(row pgx.Row) (*models.P2PListing, error) {
	var l models.P2PListing
	err := row.Scan(&l.ID, &l.SellerID, &l.SellerAddress, &l.Asset, &l.AmountRaw, &l.RemainingRaw, &l.Price, &l.FiatCurrency, &l.PaymentMethod, &l.Status, &l.CreatedAt, &l.UpdatedAt)
	return &l, err
}

func (r *P2PRepo) Listings(ctx context.Context, sellerID string, activeOnly bool) ([]models.P2PListing, error) {
	query, args := listingSelect+` WHERE 1=1`, []any{}
	if sellerID != "" {
		args = append(args, sellerID)
		query += fmt.Sprintf(" AND l.seller_id=$%d", len(args))
	}
	if activeOnly {
		query += ` AND l.status='ACTIVE' AND l.remaining_raw>0`
	}
	rows, err := r.pool.Query(ctx, query+` ORDER BY l.created_at DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2PListing{}
	for rows.Next() {
		l, e := scanListing(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, *l)
	}
	return out, rows.Err()
}

const orderSelect = `SELECT id,listing_id,seller_id,buyer_id,asset,amount_raw::text,escrow_raw::text,price::text,fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,seller_receivable::text,payment_method,status,expires_at,updated_at,COALESCE(cancellation_reason,''),completed_at,created_at FROM p2p_orders`

func scanOrder(row pgx.Row) (*models.P2POrder, error) {
	var o models.P2POrder
	err := row.Scan(&o.ID, &o.ListingID, &o.SellerID, &o.BuyerID, &o.Asset, &o.AmountRaw, &o.EscrowRaw, &o.Price, &o.FiatCurrency, &o.GrossAmount, &o.BuyerFee, &o.SellerFee, &o.BuyerPayable, &o.SellerReceivable, &o.PaymentMethod, &o.Status, &o.ExpiresAt, &o.UpdatedAt, &o.CancellationReason, &o.CompletedAt, &o.CreatedAt)
	return &o, err
}

func (r *P2PRepo) CreateOrder(ctx context.Context, buyerID, listingID, amountRaw, idempotencyKey string) (*models.P2POrder, error) {
	if err := validateP2PAmount(amountRaw); err != nil {
		return nil, err
	}
	key, err := validateIdempotencyKey(idempotencyKey, false)
	if err != nil {
		return nil, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = r.ledger.lockUser(ctx, tx, buyerID); err != nil {
		return nil, err
	}
	if key != "" {
		existing, e := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE buyer_id=$1 AND idempotency_key=$2`, buyerID, key))
		if e == nil {
			if existing.ListingID != listingID || existing.AmountRaw != amountRaw {
				return nil, ErrP2PIdempotencyKey
			}
			if err = tx.Commit(ctx); err != nil {
				return nil, err
			}
			return existing, nil
		}
		if e != pgx.ErrNoRows {
			return nil, e
		}
	}

	var sellerID, asset, remaining, price, fiat, method, status, source string
	err = tx.QueryRow(ctx, `SELECT seller_id,asset,remaining_raw::text,price::text,fiat_currency,payment_method,status,funding_source FROM p2p_listings WHERE id=$1 FOR UPDATE`, listingID).Scan(&sellerID, &asset, &remaining, &price, &fiat, &method, &status, &source)
	if err == pgx.ErrNoRows {
		return nil, ErrP2PNotFound
	}
	if err != nil {
		return nil, err
	}
	if buyerID == sellerID {
		return nil, ErrP2PSelfPurchase
	}
	have, _ := new(big.Int).SetString(remaining, 10)
	want, _ := new(big.Int).SetString(amountRaw, 10)
	if status != "ACTIVE" || have.Cmp(want) < 0 {
		return nil, ErrP2PUnavailable
	}

	legacyDebit := false
	if source == p2pFundingWallet {
		if err = r.lockWallet(ctx, tx, sellerID, asset); err != nil {
			return nil, err
		}
		tag, e := tx.Exec(ctx, `UPDATE p2p_wallet_balances SET reserved_raw=reserved_raw-$3::numeric,updated_at=now() WHERE user_id=$1 AND asset=$2 AND reserved_raw >= $3::numeric`, sellerID, asset, amountRaw)
		if e != nil {
			return nil, e
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("seller P2P %s is not reserved for this listing", asset)
		}
	} else if source == p2pFundingLegacyMain {
		if err = r.ledger.lockBalance(ctx, tx, sellerID); err != nil {
			return nil, err
		}
		_, column, columnErr := normalizeAsset(asset)
		if columnErr != nil {
			return nil, columnErr
		}
		lockedColumn := lockedColumns[asset]
		tag, e := tx.Exec(ctx, `UPDATE user_balances SET `+column+`=`+column+`-$2::numeric,`+lockedColumn+`=`+lockedColumn+`-$2::numeric,updated_at=now() WHERE user_id=$1 AND `+column+`>=$2::numeric AND `+lockedColumn+`>=$2::numeric`, sellerID, amountRaw)
		if e != nil {
			return nil, e
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("seller %s is not reserved for this legacy listing", asset)
		}
		legacyDebit = true
	} else {
		return nil, fmt.Errorf("unsupported P2P listing funding source")
	}

	if _, err = tx.Exec(ctx, `UPDATE p2p_listings SET remaining_raw=remaining_raw-$2::numeric,status=CASE WHEN remaining_raw-$2::numeric=0 THEN 'FILLED' ELSE status END,updated_at=now() WHERE id=$1`, listingID, amountRaw); err != nil {
		return nil, err
	}
	order, err := r.insertOrder(ctx, tx, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method, key)
	if err != nil {
		return nil, err
	}
	kind := "p2p_to_order"
	if legacyDebit {
		kind = "legacy_main_to_order"
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_wallet_entries(user_id,listing_id,order_id,kind,asset,amount_raw) VALUES($1,$2,$3,$4,$5,$6)`, sellerID, listingID, order.ID, kind, asset, amountRaw); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	order.LegacyMainDebit = legacyDebit
	return order, nil
}

func (r *P2PRepo) insertOrder(ctx context.Context, tx pgx.Tx, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method, key string) (*models.P2POrder, error) {
	var dbKey any
	if key != "" {
		dbKey = key
	}
	q := `WITH amounts AS (SELECT round(($5::numeric/1000000)*$6::numeric,8) AS gross)
	INSERT INTO p2p_orders(listing_id,seller_id,buyer_id,asset,amount_raw,escrow_raw,price,fiat_currency,gross_amount,buyer_fee,seller_fee,buyer_payable,seller_receivable,payment_method,buyer_credit,seller_debit,fiat_amount,buyer_fee_fiat,seller_fee_fiat,buyer_pays_fiat,seller_receives_fiat,status,idempotency_key,expires_at)
	SELECT $1,$2,$3,$4,$5,$5,$6,$7,gross,round(gross*.01,8),round(gross*.01,8),round(gross*1.01,8),round(gross*.99,8),$8,$5,$5,gross,round(gross*.01,8),round(gross*.01,8),round(gross*1.01,8),round(gross*.99,8),'pending_payment',$9,$10 FROM amounts
	RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,escrow_raw::text,price::text,fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,seller_receivable::text,payment_method,status,expires_at,updated_at,COALESCE(cancellation_reason,''),completed_at,created_at`
	return scanOrder(tx.QueryRow(ctx, q, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method, dbKey, time.Now().Add(15*time.Minute)))
}

func (r *P2PRepo) Orders(ctx context.Context, userID string) ([]models.P2POrder, error) {
	if err := r.ExpirePendingOrders(ctx, 50); err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, orderSelect+` WHERE buyer_id=$1 OR seller_id=$1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrder{}
	for rows.Next() {
		o, e := scanOrder(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

func (r *P2PRepo) MarkPaid(ctx context.Context, buyerID, orderID string) (*models.P2POrder, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	o, err := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1 FOR UPDATE`, orderID))
	if err == pgx.ErrNoRows {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if o.BuyerID != buyerID {
		return nil, ErrP2PForbidden
	}
	if o.Status == P2PStatusPaymentMade {
		return o, tx.Commit(ctx)
	}
	if o.Status != P2PStatusPendingPayment {
		return nil, ErrP2PInvalidState
	}
	if !time.Now().Before(o.ExpiresAt) {
		if _, err = r.refundTx(ctx, tx, o, P2PStatusCancelled, "payment window expired"); err != nil {
			return nil, err
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, ErrP2PExpired
	}
	o, err = scanOrder(tx.QueryRow(ctx, `UPDATE p2p_orders SET status='payment_made',updated_at=now() WHERE id=$1 RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,escrow_raw::text,price::text,fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,seller_receivable::text,payment_method,status,expires_at,updated_at,COALESCE(cancellation_reason,''),completed_at,created_at`, orderID))
	if err != nil {
		return nil, err
	}
	return o, tx.Commit(ctx)
}

// ReleaseOrder is the success path: empty escrow and credit the buyer's P2P wallet.
func (r *P2PRepo) ReleaseOrder(ctx context.Context, sellerID, orderID string) (*models.P2POrder, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	o, err := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1 FOR UPDATE`, orderID))
	if err == pgx.ErrNoRows {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if o.SellerID != sellerID {
		return nil, ErrP2PForbidden
	}
	if o.Status == P2PStatusCompleted {
		return o, tx.Commit(ctx)
	}
	if o.Status != P2PStatusPaymentMade || o.EscrowRaw == "0" {
		return nil, ErrP2PInvalidState
	}
	if err = r.lockWallet(ctx, tx, o.BuyerID, o.Asset); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `UPDATE p2p_wallet_balances SET available_raw=available_raw+$3::numeric,updated_at=now() WHERE user_id=$1 AND asset=$2`, o.BuyerID, o.Asset, o.EscrowRaw); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_wallet_entries(user_id,listing_id,order_id,kind,asset,amount_raw) VALUES($1,$2,$3,'order_to_buyer',$4,$5)`, o.BuyerID, o.ListingID, o.ID, o.Asset, o.EscrowRaw); err != nil {
		return nil, err
	}
	o, err = scanOrder(tx.QueryRow(ctx, `UPDATE p2p_orders SET escrow_raw=0,status='completed',completed_at=now(),updated_at=now() WHERE id=$1 RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,escrow_raw::text,price::text,fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,seller_receivable::text,payment_method,status,expires_at,updated_at,COALESCE(cancellation_reason,''),completed_at,created_at`, orderID))
	if err != nil {
		return nil, err
	}
	return o, tx.Commit(ctx)
}

func (r *P2PRepo) CancelOrder(ctx context.Context, buyerID, orderID string) (*models.P2POrder, error) {
	return r.cancelOrder(ctx, buyerID, orderID, false, "cancelled by buyer")
}

func (r *P2PRepo) cancelOrder(ctx context.Context, actorID, orderID string, system bool, reason string) (*models.P2POrder, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	o, err := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1 FOR UPDATE`, orderID))
	if err == pgx.ErrNoRows {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if !system && o.BuyerID != actorID {
		return nil, ErrP2PForbidden
	}
	if o.Status == P2PStatusCancelled {
		return o, tx.Commit(ctx)
	}
	if o.Status != P2PStatusPendingPayment {
		return nil, ErrP2PInvalidState
	}
	o, err = r.refundTx(ctx, tx, o, P2PStatusCancelled, reason)
	if err != nil {
		return nil, err
	}
	return o, tx.Commit(ctx)
}

// refundTx is the failure path: empty escrow and return it to the seller's P2P wallet.
func (r *P2PRepo) refundTx(ctx context.Context, tx pgx.Tx, o *models.P2POrder, finalStatus, reason string) (*models.P2POrder, error) {
	if o.EscrowRaw == "0" {
		return nil, ErrP2PInvalidState
	}
	var listingStatus, source string
	if err := tx.QueryRow(ctx, `SELECT status,funding_source FROM p2p_listings WHERE id=$1 FOR UPDATE`, o.ListingID).Scan(&listingStatus, &source); err != nil {
		return nil, err
	}
	if err := r.lockWallet(ctx, tx, o.SellerID, o.Asset); err != nil {
		return nil, err
	}
	if source == p2pFundingWallet && listingStatus != "CANCELLED" {
		if _, err := tx.Exec(ctx, `UPDATE p2p_wallet_balances SET reserved_raw=reserved_raw+$3::numeric,updated_at=now() WHERE user_id=$1 AND asset=$2`, o.SellerID, o.Asset, o.EscrowRaw); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE p2p_listings SET remaining_raw=remaining_raw+$2::numeric,status='ACTIVE',updated_at=now() WHERE id=$1`, o.ListingID, o.EscrowRaw); err != nil {
			return nil, err
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE p2p_wallet_balances SET available_raw=available_raw+$3::numeric,updated_at=now() WHERE user_id=$1 AND asset=$2`, o.SellerID, o.Asset, o.EscrowRaw); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO p2p_wallet_entries(user_id,listing_id,order_id,kind,asset,amount_raw) VALUES($1,$2,$3,'order_refund',$4,$5)`, o.SellerID, o.ListingID, o.ID, o.Asset, o.EscrowRaw); err != nil {
		return nil, err
	}
	return scanOrder(tx.QueryRow(ctx, `UPDATE p2p_orders SET escrow_raw=0,status=$2,cancellation_reason=$3,updated_at=now() WHERE id=$1 RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,escrow_raw::text,price::text,fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,seller_receivable::text,payment_method,status,expires_at,updated_at,COALESCE(cancellation_reason,''),completed_at,created_at`, o.ID, finalStatus, reason))
}

func (r *P2PRepo) ExpirePendingOrders(ctx context.Context, limit int) error {
	rows, err := r.pool.Query(ctx, `SELECT id::text FROM p2p_orders WHERE status='pending_payment' AND expires_at<=now() ORDER BY expires_at LIMIT $1`, limit)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err = r.cancelOrder(ctx, "", id, true, "payment window expired"); err != nil && !errors.Is(err, ErrP2PInvalidState) {
			return err
		}
	}
	return nil
}

func (r *P2PRepo) CancelListing(ctx context.Context, sellerID, listingID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var asset, remaining, source string
	err = tx.QueryRow(ctx, `SELECT asset,remaining_raw::text,funding_source FROM p2p_listings WHERE id=$1 AND seller_id=$2 AND status='ACTIVE' FOR UPDATE`, listingID, sellerID).Scan(&asset, &remaining, &source)
	if err == pgx.ErrNoRows {
		return ErrP2PNotFound
	}
	if err != nil {
		return err
	}
	if source == p2pFundingWallet {
		if err = r.lockWallet(ctx, tx, sellerID, asset); err != nil {
			return err
		}
		tag, e := tx.Exec(ctx, `UPDATE p2p_wallet_balances SET reserved_raw=reserved_raw-$3::numeric,available_raw=available_raw+$3::numeric,updated_at=now() WHERE user_id=$1 AND asset=$2 AND reserved_raw >= $3::numeric`, sellerID, asset, remaining)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("seller P2P reservation is inconsistent")
		}
		if remaining != "0" {
			if _, err = tx.Exec(ctx, `INSERT INTO p2p_wallet_entries(user_id,listing_id,kind,asset,amount_raw) VALUES($1,$2,'listing_release',$3,$4)`, sellerID, listingID, asset, remaining); err != nil {
				return err
			}
		}
	} else {
		if err = r.ledger.lockBalance(ctx, tx, sellerID); err != nil {
			return err
		}
		_, _, columnErr := normalizeAsset(asset)
		if columnErr != nil {
			return columnErr
		}
		lockedColumn := lockedColumns[asset]
		tag, e := tx.Exec(ctx, `UPDATE user_balances SET `+lockedColumn+`=`+lockedColumn+`-$2::numeric,updated_at=now() WHERE user_id=$1 AND `+lockedColumn+`>=$2::numeric`, sellerID, remaining)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("seller legacy %s reservation is inconsistent", asset)
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE p2p_listings SET status='CANCELLED',updated_at=now() WHERE id=$1`, listingID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
