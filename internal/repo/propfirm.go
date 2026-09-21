package repo

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dex/dex-backend/internal/models"
)

// PropFirmPurchaseRepo backs prop_firm_purchases — the exchange's own
// durable record of a BitDX Prop Firm challenge purchase (see
// db.ensurePropFirmPurchasesTable for why this is separate from the
// prop-firm backend's own pf_purchases table).
type PropFirmPurchaseRepo struct {
	pool *pgxpool.Pool
}

func NewPropFirmPurchaseRepo(pool *pgxpool.Pool) *PropFirmPurchaseRepo {
	return &PropFirmPurchaseRepo{pool: pool}
}

// Create records a new purchase as 'pending' BEFORE the wallet debit and
// provisioning call happen — see the table's own doc comment for why this
// ordering matters (a durable row to retry against, not a debit with no
// trace of what it paid for).
func (r *PropFirmPurchaseRepo) Create(ctx context.Context, userID, packageID, priceRaw string) (*models.PropFirmPurchase, error) {
	var p models.PropFirmPurchase
	err := r.pool.QueryRow(ctx, `
		INSERT INTO prop_firm_purchases (user_id, package_id, price_bi2xusd)
		VALUES ($1, $2, $3::numeric)
		RETURNING id, user_id, package_id, price_bi2xusd::text, status, prop_firm_account_id, fail_reason, created_at, updated_at
	`, userID, packageID, priceRaw).Scan(
		&p.ID, &p.UserID, &p.PackageID, &p.PriceBI2XUSD, &p.Status, &p.PropFirmAccountID, &p.FailReason, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *PropFirmPurchaseRepo) Get(ctx context.Context, id string) (*models.PropFirmPurchase, error) {
	var p models.PropFirmPurchase
	err := r.pool.QueryRow(ctx, `
		SELECT id, user_id, package_id, price_bi2xusd::text, status, prop_firm_account_id, fail_reason, created_at, updated_at
		FROM prop_firm_purchases WHERE id = $1
	`, id).Scan(
		&p.ID, &p.UserID, &p.PackageID, &p.PriceBI2XUSD, &p.Status, &p.PropFirmAccountID, &p.FailReason, &p.CreatedAt, &p.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// MarkFulfilled records that provisioning succeeded — accountID is the
// prop-firm backend's own account id from its provisionResponse.
func (r *PropFirmPurchaseRepo) MarkFulfilled(ctx context.Context, id, accountID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE prop_firm_purchases SET status = 'fulfilled', prop_firm_account_id = $2, updated_at = now() WHERE id = $1
	`, id, accountID)
	return err
}

// MarkRefundNeeded records that the wallet debit succeeded but provisioning
// never completed — the purchase must be refunded (or retried) rather than
// silently forgotten. Per PROP_FIRM_STATUS.md, an automatic refund credit
// is not yet wired; this is the durable record that makes a manual refund
// possible in the meantime.
func (r *PropFirmPurchaseRepo) MarkRefundNeeded(ctx context.Context, id, reason string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE prop_firm_purchases SET status = 'refund_needed', fail_reason = $2, updated_at = now() WHERE id = $1
	`, id, reason)
	return err
}

// ListByUser returns a user's purchase history, most recent first — for a
// future "my prop-firm purchases" view; not yet exposed via any handler.
func (r *PropFirmPurchaseRepo) ListByUser(ctx context.Context, userID string) ([]models.PropFirmPurchase, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, user_id, package_id, price_bi2xusd::text, status, prop_firm_account_id, fail_reason, created_at, updated_at
		FROM prop_firm_purchases WHERE user_id = $1 ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.PropFirmPurchase
	for rows.Next() {
		var p models.PropFirmPurchase
		if err := rows.Scan(&p.ID, &p.UserID, &p.PackageID, &p.PriceBI2XUSD, &p.Status, &p.PropFirmAccountID, &p.FailReason, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
