package repo

import (
	"context"
	"errors"
	"fmt"
	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"math/big"
	"strings"
)

var (
	ErrP2PNotFound     = errors.New("p2p listing not found")
	ErrP2PSelfPurchase = errors.New("seller cannot buy their own listing")
	ErrP2PUnavailable  = errors.New("requested amount is no longer available")
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
	var p models.P2PPrice
	err := r.pool.QueryRow(ctx, `SELECT asset,fiat_currency,price::text,price_date::text,created_at FROM p2p_price_history WHERE asset='USDC' AND fiat_currency='INR' AND price_date=CURRENT_DATE ORDER BY created_at DESC LIMIT 1`).Scan(&p.Asset, &p.FiatCurrency, &p.Price, &p.PriceDate, &p.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("today's USDC/INR P2P price has not been entered")
	}
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
	err := tx.QueryRow(ctx, `SELECT price::text FROM p2p_price_history WHERE asset='USDC' AND fiat_currency='INR' AND price_date=CURRENT_DATE LIMIT 1`).Scan(&p)
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
	tag, err := tx.Exec(ctx, `UPDATE user_balances SET "USDC_locked"="USDC_locked"+$2::numeric,updated_at=now() WHERE user_id=$1 AND "USDC"-"USDC_locked"-$3::numeric >= $2::numeric`, sellerID, amountRaw, pending.String())
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("insufficient USDC available balance")
	}
	var l models.P2PListing
	err = tx.QueryRow(ctx, `INSERT INTO p2p_listings(seller_id,asset,amount_raw,remaining_raw,price,fiat_currency,payment_method) VALUES($1,'USDC',$2,$2,$3,'INR',$4) RETURNING id,seller_id,'',asset,amount_raw::text,remaining_raw::text,price::text,fiat_currency,payment_method,status,created_at,updated_at`, sellerID, amountRaw, price, paymentMethod).Scan(&l.ID, &l.SellerID, &l.SellerAddress, &l.Asset, &l.AmountRaw, &l.RemainingRaw, &l.Price, &l.FiatCurrency, &l.PaymentMethod, &l.Status, &l.CreatedAt, &l.UpdatedAt)
	if err != nil {
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

func (r *P2PRepo) Buy(ctx context.Context, buyerID, listingID, amountRaw string) (*models.P2POrder, error) {
	if err := validateP2PAmount(amountRaw); err != nil {
		return nil, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var sellerID, asset, remaining, price, fiat, method, status string
	err = tx.QueryRow(ctx, `SELECT seller_id,asset,remaining_raw::text,price::text,fiat_currency,payment_method,status FROM p2p_listings WHERE id=$1 FOR UPDATE`, listingID).Scan(&sellerID, &asset, &remaining, &price, &fiat, &method, &status)
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
	ids := []string{sellerID, buyerID}
	if strings.Compare(ids[0], ids[1]) > 0 {
		ids[0], ids[1] = ids[1], ids[0]
	}
	if err = r.ledger.lockBalances(ctx, tx, ids); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `UPDATE user_balances SET "USDC"="USDC"-$2::numeric,"USDC_locked"="USDC_locked"-$2::numeric,updated_at=now() WHERE user_id=$1 AND "USDC">=$2::numeric AND "USDC_locked">=$2::numeric`, sellerID, amountRaw)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("seller USDC is not reserved for this listing")
	}
	if err = r.ledger.creditBalanceTx(ctx, tx, buyerID, asset, amountRaw); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `UPDATE p2p_listings SET remaining_raw=remaining_raw-$2::numeric,status=CASE WHEN remaining_raw-$2::numeric=0 THEN 'FILLED' ELSE status END,updated_at=now() WHERE id=$1`, listingID, amountRaw)
	if err != nil {
		return nil, err
	}
	return r.insertOrder(ctx, tx, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method)
}

func (r *P2PRepo) insertOrder(ctx context.Context, tx pgx.Tx, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method string) (*models.P2POrder, error) {
	var o models.P2POrder
	err := tx.QueryRow(ctx, `WITH amounts AS (
		SELECT round(($5::numeric/1000000)*$6::numeric,8) AS gross
	)
	INSERT INTO p2p_orders(
		listing_id,seller_id,buyer_id,asset,amount_raw,price,fiat_currency,
		gross_amount,buyer_fee,seller_fee,buyer_payable,seller_receivable,payment_method,
		buyer_credit,seller_debit,fiat_amount,buyer_fee_fiat,seller_fee_fiat,
		buyer_pays_fiat,seller_receives_fiat,status)
	SELECT $1,$2,$3,$4,$5,$6,$7,
		gross,round(gross*.01,8),round(gross*.01,8),round(gross*1.01,8),round(gross*.99,8),$8,
		$5,$5,gross,round(gross*.01,8),round(gross*.01,8),
		round(gross*1.01,8),round(gross*.99,8),'completed'
	FROM amounts
	RETURNING id,listing_id,seller_id,buyer_id,asset,amount_raw::text,price::text,fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,seller_receivable::text,payment_method,status,created_at`, listingID, sellerID, buyerID, asset, amountRaw, price, fiat, method).Scan(&o.ID, &o.ListingID, &o.SellerID, &o.BuyerID, &o.Asset, &o.AmountRaw, &o.Price, &o.FiatCurrency, &o.GrossAmount, &o.BuyerFee, &o.SellerFee, &o.BuyerPayable, &o.SellerReceivable, &o.PaymentMethod, &o.Status, &o.CreatedAt)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *P2PRepo) Orders(ctx context.Context, userID string) ([]models.P2POrder, error) {
	rows, err := r.pool.Query(ctx, `SELECT id,listing_id,seller_id,buyer_id,asset,amount_raw::text,price::text,fiat_currency,gross_amount::text,buyer_fee::text,seller_fee::text,buyer_payable::text,seller_receivable::text,payment_method,status,created_at FROM p2p_orders WHERE buyer_id=$1 OR seller_id=$1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.P2POrder{}
	for rows.Next() {
		var o models.P2POrder
		if err := rows.Scan(&o.ID, &o.ListingID, &o.SellerID, &o.BuyerID, &o.Asset, &o.AmountRaw, &o.Price, &o.FiatCurrency, &o.GrossAmount, &o.BuyerFee, &o.SellerFee, &o.BuyerPayable, &o.SellerReceivable, &o.PaymentMethod, &o.Status, &o.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (r *P2PRepo) CancelListing(ctx context.Context, sellerID, listingID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var remaining string
	err = tx.QueryRow(ctx, `UPDATE p2p_listings SET status='CANCELLED',updated_at=now() WHERE id=$1 AND seller_id=$2 AND status='ACTIVE' RETURNING remaining_raw::text`, listingID, sellerID).Scan(&remaining)
	if err == pgx.ErrNoRows {
		return ErrP2PNotFound
	}
	if err != nil {
		return err
	}
	if err = r.ledger.lockBalance(ctx, tx, sellerID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE user_balances SET "USDC_locked"=GREATEST(0,"USDC_locked"-$2::numeric),updated_at=now() WHERE user_id=$1`, sellerID, remaining)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
