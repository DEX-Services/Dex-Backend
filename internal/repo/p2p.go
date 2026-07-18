package repo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/models"
	"github.com/dex/dex-backend/internal/tokenunits"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	P2PStatusPendingPayment = "pending_payment"
	P2PStatusPaymentMade    = "payment_made"
	P2PStatusCompleted      = "completed"
	P2PStatusCancelled      = "cancelled"
	P2PStatusAppeal         = "appeal"
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
var validP2PPrice = regexp.MustCompile(`^[0-9]{1,30}(\.[0-9]{1,8})?$`)

type P2PRepo struct {
	pool   *pgxpool.Pool
	ledger *LedgerRepo
}

func NewP2PRepo(pool *pgxpool.Pool) *P2PRepo {
	return &P2PRepo{pool: pool, ledger: NewLedgerRepo(pool)}
}

func (r *P2PRepo) TodayPrice(ctx context.Context) (*models.P2PPrice, error) {
	var p models.P2PPrice
	err := r.pool.QueryRow(ctx, `SELECT asset,fiat_currency,price::text,price_date::text,created_at
		FROM p2p_price_history
		WHERE asset='USDC' AND fiat_currency='INR' AND price_date=(now() AT TIME ZONE 'Asia/Kolkata')::date
		ORDER BY created_at DESC LIMIT 1`).Scan(&p.Asset, &p.FiatCurrency, &p.Price, &p.PriceDate, &p.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("today's USDC/INR P2P price has not been entered")
	}
	return &p, err
}

func (r *P2PRepo) SetTodayPrice(ctx context.Context, price string) (*models.P2PPrice, error) {
	if strings.TrimSpace(price) != price || !validP2PPrice.MatchString(price) {
		return nil, fmt.Errorf("price must be a positive decimal with up to 30 integer and 8 fractional digits")
	}
	value, ok := new(big.Rat).SetString(price)
	if !ok || value.Sign() <= 0 {
		return nil, fmt.Errorf("price must be greater than zero")
	}
	var p models.P2PPrice
	err := r.pool.QueryRow(ctx, `INSERT INTO p2p_price_history(asset,fiat_currency,price,price_date)
		VALUES('USDC','INR',$1::numeric,(now() AT TIME ZONE 'Asia/Kolkata')::date)
		ON CONFLICT(asset,fiat_currency,price_date)
		DO UPDATE SET price=EXCLUDED.price,created_at=now()
		RETURNING asset,fiat_currency,price::text,price_date::text,created_at`, price).
		Scan(&p.Asset, &p.FiatCurrency, &p.Price, &p.PriceDate, &p.CreatedAt)
	return &p, err
}

func validateP2PAmount(raw string) error {
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok || n.Sign() <= 0 {
		return fmt.Errorf("amount must be a positive raw USDC integer")
	}
	return nil
}

func (r *P2PRepo) todayPriceTx(ctx context.Context, tx pgx.Tx) (string, error) {
	var p string
	err := tx.QueryRow(ctx, `SELECT price::text FROM p2p_price_history
		WHERE asset='USDC' AND fiat_currency='INR' AND price_date=(now() AT TIME ZONE 'Asia/Kolkata')::date LIMIT 1`).Scan(&p)
	if err == pgx.ErrNoRows {
		return "", fmt.Errorf("today's USDC/INR P2P price has not been entered")
	}
	return p, err
}

func (r *P2PRepo) CreateListing(ctx context.Context, sellerID, amountRaw, paymentMethod string) (*models.P2PListing, error) {
	if err := validateP2PAmount(amountRaw); err != nil {
		return nil, err
	}
	if !validP2PPaymentMethods[paymentMethod] {
		return nil, fmt.Errorf("unsupported payment method")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	price, err := r.todayPriceTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err = r.ledger.lockBalance(ctx, tx, sellerID); err != nil {
		return nil, err
	}
	pending, err := r.ledger.pendingWithdrawalHoldTx(ctx, tx, sellerID, "USDC")
	if err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `UPDATE user_balances
		SET "USDC_locked"="USDC_locked"+$2::numeric,updated_at=now()
		WHERE user_id=$1 AND "USDC"-"USDC_locked"-$3::numeric >= $2::numeric`, sellerID, amountRaw, pending.String())
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("insufficient USDC available balance")
	}
	var l models.P2PListing
	err = tx.QueryRow(ctx, `INSERT INTO p2p_listings(seller_id,asset,amount_raw,remaining_raw,price,fiat_currency,payment_method)
		VALUES($1,'USDC',$2,$2,$3,'INR',$4)
		RETURNING id,seller_id,'',asset,amount_raw::text,remaining_raw::text,price::text,fiat_currency,payment_method,status,created_at,updated_at`,
		sellerID, amountRaw, price, paymentMethod).
		Scan(&l.ID, &l.SellerID, &l.SellerAddress, &l.Asset, &l.AmountRaw, &l.RemainingRaw, &l.Price, &l.FiatCurrency, &l.PaymentMethod, &l.Status, &l.CreatedAt, &l.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &l, nil
}

const listingSelect = `SELECT l.id,l.seller_id,u.wallet_address,l.asset,l.amount_raw::text,
	l.remaining_raw::text,l.price::text,l.fiat_currency,l.payment_method,l.status,l.created_at,l.updated_at
	FROM p2p_listings l JOIN users u ON u.id=l.seller_id`

func scanListing(row pgx.Row) (*models.P2PListing, error) {
	var l models.P2PListing
	err := row.Scan(&l.ID, &l.SellerID, &l.SellerAddress, &l.Asset, &l.AmountRaw, &l.RemainingRaw, &l.Price, &l.FiatCurrency, &l.PaymentMethod, &l.Status, &l.CreatedAt, &l.UpdatedAt)
	return &l, err
}

func (r *P2PRepo) Listings(ctx context.Context, sellerID string, activeOnly bool) ([]models.P2PListing, error) {
	query := listingSelect + ` WHERE 1=1`
	args := []any{}
	if sellerID != "" {
		args = append(args, sellerID)
		query += fmt.Sprintf(" AND l.seller_id=$%d", len(args))
	}
	if activeOnly {
		query += ` AND l.status='ACTIVE' AND l.remaining_raw>0`
	}
	query += ` ORDER BY l.created_at DESC`
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2PListing{}
	for rows.Next() {
		l, err := scanListing(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *l)
	}
	return out, rows.Err()
}

const orderSelect = `SELECT id,listing_id,seller_id,buyer_id,asset,amount_raw::text,price::text,
	fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,
	seller_receivable::text,payment_method,status,expires_at,updated_at,
	COALESCE(cancellation_reason,''),completed_at,created_at FROM p2p_orders`

func scanOrder(row pgx.Row) (*models.P2POrder, error) {
	var o models.P2POrder
	err := row.Scan(&o.ID, &o.ListingID, &o.SellerID, &o.BuyerID, &o.Asset, &o.AmountRaw,
		&o.Price, &o.FiatCurrency, &o.GrossAmount, &o.BuyerFee, &o.SellerFee,
		&o.BuyerPayable, &o.SellerReceivable, &o.PaymentMethod, &o.Status, &o.ExpiresAt,
		&o.UpdatedAt, &o.CancellationReason, &o.CompletedAt, &o.CreatedAt)
	return &o, err
}

func validateIdempotencyKey(key string) error {
	if len(key) < 8 || len(key) > 128 || strings.TrimSpace(key) != key {
		return fmt.Errorf("idempotencyKey must contain 8 to 128 non-space characters")
	}
	return nil
}

func (r *P2PRepo) CreateOrder(ctx context.Context, buyerID, listingID, amountRaw, idempotencyKey string) (*models.P2POrder, error) {
	if err := validateP2PAmount(amountRaw); err != nil {
		return nil, err
	}
	if err := validateIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	lockKey := fmt.Sprintf("%d:%s%s", len(buyerID), buyerID, idempotencyKey)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return nil, err
	}

	existing, err := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE buyer_id=$1 AND idempotency_key=$2`, buyerID, idempotencyKey))
	if err == nil {
		if existing.ListingID != listingID || existing.AmountRaw != amountRaw {
			return nil, ErrP2PIdempotencyKey
		}
		return existing, nil
	}
	if err != pgx.ErrNoRows {
		return nil, err
	}
	if _, err = r.todayPriceTx(ctx, tx); err != nil {
		return nil, err
	}

	var sellerID, asset, remaining, price, fiat, method, status string
	err = tx.QueryRow(ctx, `SELECT seller_id,asset,remaining_raw::text,price::text,fiat_currency,payment_method,status
		FROM p2p_listings WHERE id=$1 FOR UPDATE`, listingID).
		Scan(&sellerID, &asset, &remaining, &price, &fiat, &method, &status)
	if err == pgx.ErrNoRows {
		return nil, ErrP2PNotFound
	}
	if err != nil {
		return nil, err
	}
	if buyerID == sellerID {
		return nil, ErrP2PSelfPurchase
	}
	available, _ := new(big.Int).SetString(remaining, 10)
	wanted, _ := new(big.Int).SetString(amountRaw, 10)
	if status != "ACTIVE" || available.Cmp(wanted) < 0 {
		return nil, ErrP2PUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE p2p_listings
		SET remaining_raw=remaining_raw-$2::numeric,
			status=CASE WHEN remaining_raw-$2::numeric=0 THEN 'FILLED' ELSE status END,
			updated_at=now() WHERE id=$1`, listingID, amountRaw); err != nil {
		return nil, err
	}
	order, err := r.insertOrder(ctx, tx, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return order, nil
}

func (r *P2PRepo) insertOrder(ctx context.Context, tx pgx.Tx, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method, idempotencyKey string) (*models.P2POrder, error) {
	units, err := tokenunits.Get(asset)
	if err != nil {
		return nil, err
	}
	query := `WITH amounts AS (
		SELECT round(($5::numeric/$10::numeric)*$6::numeric,8) AS gross
	)
	INSERT INTO p2p_orders(
		listing_id,seller_id,buyer_id,asset,amount_raw,price,fiat_currency,
		gross_amount,buyer_fee,seller_fee,buyer_payable,seller_receivable,payment_method,
		buyer_credit,seller_debit,fiat_amount,buyer_fee_fiat,seller_fee_fiat,
		buyer_pays_fiat,seller_receives_fiat,status,idempotency_key,expires_at)
	SELECT $1,$2,$3,$4,$5,$6,$7,
		gross,round(gross*.01,8),round(gross*.01,8),round(gross*1.01,8),round(gross*.99,8),$8,
		$5,$5,gross,round(gross*.01,8),round(gross*.01,8),
		round(gross*1.01,8),round(gross*.99,8),'pending_payment',$9,now()+interval '15 minutes'
	FROM amounts
	RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,price::text,
		fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,
		seller_receivable::text,payment_method,status,expires_at,updated_at,
		COALESCE(cancellation_reason,''),completed_at,created_at`
	return scanOrder(tx.QueryRow(ctx, query, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method, idempotencyKey, units.ScaleRaw))
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
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

func (r *P2PRepo) AppealOrders(ctx context.Context) ([]models.P2POrder, error) {
	rows, err := r.pool.Query(ctx, orderSelect+` WHERE status='appeal' ORDER BY updated_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrder{}
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

func (r *P2PRepo) MarkPaid(ctx context.Context, buyerID, orderID string) (*models.P2POrder, error) {
	order, err := scanOrder(r.pool.QueryRow(ctx, orderSelect+` WHERE id=$1 AND buyer_id=$2`, orderID, buyerID))
	if err == pgx.ErrNoRows {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if order.Status == P2PStatusPaymentMade {
		return order, nil
	}
	if order.Status != P2PStatusPendingPayment {
		return nil, ErrP2PInvalidState
	}
	if time.Now().After(order.ExpiresAt) {
		_, _ = r.cancelOrder(ctx, orderID, buyerID, true, "payment window expired")
		return nil, ErrP2PExpired
	}
	return scanOrder(r.pool.QueryRow(ctx, `UPDATE p2p_orders SET status='payment_made',updated_at=now()
		WHERE id=$1 AND buyer_id=$2 AND status='pending_payment' AND expires_at>now()
		RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,price::text,
		fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,
		seller_receivable::text,payment_method,status,expires_at,updated_at,
		COALESCE(cancellation_reason,''),completed_at,created_at`, orderID, buyerID))
}

func (r *P2PRepo) ReleaseOrder(ctx context.Context, sellerID, orderID string) (*models.P2POrder, error) {
	return r.releaseOrder(ctx, sellerID, orderID, false)
}

func (r *P2PRepo) releaseOrder(ctx context.Context, actorID, orderID string, admin bool) (*models.P2POrder, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	order, err := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1 FOR UPDATE`, orderID))
	if err == pgx.ErrNoRows {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if !admin && order.SellerID != actorID {
		return nil, ErrP2PForbidden
	}
	if order.Status == P2PStatusCompleted {
		return order, nil
	}
	if order.Status != P2PStatusPaymentMade && !(admin && order.Status == P2PStatusAppeal) {
		return nil, ErrP2PInvalidState
	}
	ids := []string{order.SellerID, order.BuyerID}
	if strings.Compare(ids[0], ids[1]) > 0 {
		ids[0], ids[1] = ids[1], ids[0]
	}
	if err = r.ledger.lockBalances(ctx, tx, ids); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `UPDATE user_balances
		SET "USDC"="USDC"-$2::numeric,"USDC_locked"="USDC_locked"-$2::numeric,updated_at=now()
		WHERE user_id=$1 AND "USDC">=$2::numeric AND "USDC_locked">=$2::numeric`, order.SellerID, order.AmountRaw)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("seller USDC is not reserved for this order")
	}
	if err = r.ledger.creditBalanceTx(ctx, tx, order.BuyerID, order.Asset, order.AmountRaw); err != nil {
		return nil, err
	}
	order, err = scanOrder(tx.QueryRow(ctx, `UPDATE p2p_orders
		SET status='completed',completed_at=now(),updated_at=now()
		WHERE id=$1
		RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,price::text,
		fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,
		seller_receivable::text,payment_method,status,expires_at,updated_at,
		COALESCE(cancellation_reason,''),completed_at,created_at`, orderID))
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO p2p_engine_outbox(order_id,sequence,direction,user_id,asset,amount_raw)
		VALUES($1,1,'debit',$2,$4,$5),($1,2,'credit',$3,$4,$5)
		ON CONFLICT(order_id,sequence) DO NOTHING`, order.ID, order.SellerID, order.BuyerID, order.Asset, order.AmountRaw); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return order, nil
}

func (r *P2PRepo) CancelOrder(ctx context.Context, buyerID, orderID string) (*models.P2POrder, error) {
	return r.cancelOrder(ctx, orderID, buyerID, false, "cancelled by buyer")
}

func (r *P2PRepo) cancelOrder(ctx context.Context, orderID, actorID string, system bool, reason string) (*models.P2POrder, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	order, err := scanOrder(tx.QueryRow(ctx, orderSelect+` WHERE id=$1 FOR UPDATE`, orderID))
	if err == pgx.ErrNoRows {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if !system && order.BuyerID != actorID {
		return nil, ErrP2PForbidden
	}
	if order.Status == P2PStatusCancelled {
		return order, nil
	}
	if order.Status != P2PStatusPendingPayment && !(system && order.Status == P2PStatusAppeal) {
		return nil, ErrP2PInvalidState
	}
	var listingStatus string
	if err = tx.QueryRow(ctx, `SELECT status FROM p2p_listings WHERE id=$1 FOR UPDATE`, order.ListingID).Scan(&listingStatus); err != nil {
		return nil, err
	}
	if listingStatus == "CANCELLED" {
		if err = r.ledger.lockBalance(ctx, tx, order.SellerID); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `UPDATE user_balances
			SET "USDC_locked"="USDC_locked"-$2::numeric,updated_at=now()
			WHERE user_id=$1 AND "USDC_locked">=$2::numeric`, order.SellerID, order.AmountRaw); err != nil {
			return nil, err
		}
	} else {
		if _, err = tx.Exec(ctx, `UPDATE p2p_listings
			SET remaining_raw=remaining_raw+$2::numeric,status='ACTIVE',updated_at=now() WHERE id=$1`,
			order.ListingID, order.AmountRaw); err != nil {
			return nil, err
		}
	}
	order, err = scanOrder(tx.QueryRow(ctx, `UPDATE p2p_orders
		SET status='cancelled',cancellation_reason=$2,updated_at=now() WHERE id=$1
		RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,price::text,
		fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,
		seller_receivable::text,payment_method,status,expires_at,updated_at,
		COALESCE(cancellation_reason,''),completed_at,created_at`, orderID, reason))
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return order, nil
}

func (r *P2PRepo) AppealOrder(ctx context.Context, userID, orderID string) (*models.P2POrder, error) {
	order, err := scanOrder(r.pool.QueryRow(ctx, orderSelect+` WHERE id=$1`, orderID))
	if err == pgx.ErrNoRows {
		return nil, ErrP2POrderNotFound
	}
	if err != nil {
		return nil, err
	}
	if userID != order.BuyerID && userID != order.SellerID {
		return nil, ErrP2PForbidden
	}
	if order.Status == P2PStatusAppeal {
		return order, nil
	}
	if order.Status != P2PStatusPaymentMade {
		return nil, ErrP2PInvalidState
	}
	return scanOrder(r.pool.QueryRow(ctx, `UPDATE p2p_orders SET status='appeal',updated_at=now()
		WHERE id=$1 AND status='payment_made'
		RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,price::text,
		fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,
		seller_receivable::text,payment_method,status,expires_at,updated_at,
		COALESCE(cancellation_reason,''),completed_at,created_at`, orderID))
}

func (r *P2PRepo) ResolveAppeal(ctx context.Context, orderID, action string) (*models.P2POrder, error) {
	switch action {
	case "release":
		return r.releaseOrder(ctx, "", orderID, true)
	case "cancel":
		return r.cancelOrder(ctx, orderID, "", true, "cancelled by admin after appeal")
	default:
		return nil, fmt.Errorf("action must be release or cancel")
	}
}

func (r *P2PRepo) ExpirePendingOrders(ctx context.Context, limit int) error {
	rows, err := r.pool.Query(ctx, `SELECT id::text FROM p2p_orders
		WHERE status='pending_payment' AND expires_at<=now()
		ORDER BY expires_at LIMIT $1`, limit)
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
		if _, err = r.cancelOrder(ctx, id, "", true, "payment window expired"); err != nil && !errors.Is(err, ErrP2PInvalidState) {
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
	var remaining string
	err = tx.QueryRow(ctx, `UPDATE p2p_listings SET status='CANCELLED',updated_at=now()
		WHERE id=$1 AND seller_id=$2 AND status='ACTIVE' RETURNING remaining_raw::text`, listingID, sellerID).Scan(&remaining)
	if err == pgx.ErrNoRows {
		return ErrP2PNotFound
	}
	if err != nil {
		return err
	}
	if err = r.ledger.lockBalance(ctx, tx, sellerID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE user_balances SET "USDC_locked"="USDC_locked"-$2::numeric,updated_at=now()
		WHERE user_id=$1 AND "USDC_locked">=$2::numeric`, sellerID, remaining)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("seller USDC reservation is inconsistent")
	}
	return tx.Commit(ctx)
}
