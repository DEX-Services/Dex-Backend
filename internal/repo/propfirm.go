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

const propFirmPurchaseColumns = `id, user_id, package_id, price_bi2xusd::text, status, prop_firm_account_id, fail_reason, retry_count, last_retry_at, created_at, updated_at`

func scanPropFirmPurchase(row interface {
	Scan(dest ...interface{}) error
}) (*models.PropFirmPurchase, error) {
	var p models.PropFirmPurchase
	err := row.Scan(
		&p.ID, &p.UserID, &p.PackageID, &p.PriceBI2XUSD, &p.Status, &p.PropFirmAccountID, &p.FailReason,
		&p.RetryCount, &p.LastRetryAt, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// Create records a new purchase as 'pending' BEFORE the wallet debit and
// provisioning call happen — see the table's own doc comment for why this
// ordering matters (a durable row to retry against, not a debit with no
// trace of what it paid for).
func (r *PropFirmPurchaseRepo) Create(ctx context.Context, userID, packageID, priceRaw string) (*models.PropFirmPurchase, error) {
	row := r.pool.QueryRow(ctx, `
		INSERT INTO prop_firm_purchases (user_id, package_id, price_bi2xusd)
		VALUES ($1, $2, $3::numeric)
		RETURNING `+propFirmPurchaseColumns, userID, packageID, priceRaw)
	return scanPropFirmPurchase(row)
}

func (r *PropFirmPurchaseRepo) Get(ctx context.Context, id string) (*models.PropFirmPurchase, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+propFirmPurchaseColumns+` FROM prop_firm_purchases WHERE id = $1`, id)
	p, err := scanPropFirmPurchase(row)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return p, nil
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
// never completed — the purchase must be retried (see IncrementRetryAttempt)
// or eventually refunded (see MarkRefunded), never silently forgotten.
func (r *PropFirmPurchaseRepo) MarkRefundNeeded(ctx context.Context, id, reason string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE prop_firm_purchases SET status = 'refund_needed', fail_reason = $2, updated_at = now() WHERE id = $1
	`, id, reason)
	return err
}

// ClaimForRefund atomically transitions a purchase from 'refund_needed' to
// 'refunded' and reports whether THIS call was the one that made the
// transition (claimed=false means it was already refunded, or was in some
// other status — either way, the caller must NOT credit the wallet again).
// This is the guard against a double-refund race: the wallet credit must
// only ever happen after a successful claim, never before, so two
// concurrent retry-job passes (or a retry racing a manual admin action)
// can't both credit the same purchase.
func (r *PropFirmPurchaseRepo) ClaimForRefund(ctx context.Context, id string) (claimed bool, err error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE prop_firm_purchases SET status = 'refunded', updated_at = now()
		WHERE id = $1 AND status = 'refund_needed'
	`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// IncrementRetryAttempt bumps retry_count and last_retry_at for one more
// provisioning retry attempt — called by the background retry job
// regardless of whether the retry itself succeeds, so retry_count always
// reflects how many times a real attempt was made.
func (r *PropFirmPurchaseRepo) IncrementRetryAttempt(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE prop_firm_purchases SET retry_count = retry_count + 1, last_retry_at = now(), updated_at = now() WHERE id = $1
	`, id)
	return err
}

// ListRefundNeeded returns every purchase currently in 'refund_needed'
// status with fewer than maxRetries retry attempts so far, oldest first —
// the background retry job's work queue. A purchase that has exhausted
// maxRetries is deliberately excluded here: the caller (the retry job)
// checks retry_count itself and refunds instead of retrying once the cap is
// reached, so this list only ever contains purchases still worth retrying.
func (r *PropFirmPurchaseRepo) ListRefundNeeded(ctx context.Context, maxRetries, limit int) ([]models.PropFirmPurchase, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+propFirmPurchaseColumns+`
		FROM prop_firm_purchases
		WHERE status = 'refund_needed' AND retry_count < $1
		ORDER BY created_at ASC
		LIMIT $2
	`, maxRetries, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.PropFirmPurchase
	for rows.Next() {
		p, err := scanPropFirmPurchase(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

// ListExhaustedRetries returns every 'refund_needed' purchase that has
// already hit maxRetries attempts — these are no longer retried, only
// refunded, on the retry job's next pass.
func (r *PropFirmPurchaseRepo) ListExhaustedRetries(ctx context.Context, maxRetries, limit int) ([]models.PropFirmPurchase, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+propFirmPurchaseColumns+`
		FROM prop_firm_purchases
		WHERE status = 'refund_needed' AND retry_count >= $1
		ORDER BY created_at ASC
		LIMIT $2
	`, maxRetries, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.PropFirmPurchase
	for rows.Next() {
		p, err := scanPropFirmPurchase(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

// ListByUser returns a user's purchase history, most recent first — for a
// future "my prop-firm purchases" view; not yet exposed via any handler.
func (r *PropFirmPurchaseRepo) ListByUser(ctx context.Context, userID string) ([]models.PropFirmPurchase, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+propFirmPurchaseColumns+` FROM prop_firm_purchases WHERE user_id = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.PropFirmPurchase
	for rows.Next() {
		p, err := scanPropFirmPurchase(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}
