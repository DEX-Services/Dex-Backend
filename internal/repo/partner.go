package repo

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PartnerRepo backs the partner profit-sharing feature: a fixed set of
// "partner" admin logins (see partner_accounts, ensurePartnerProfitShareTables
// in internal/db/db.go) who can only ever view their own daily share of the
// platform's profit, computed and recorded once per UTC day.
type PartnerRepo struct {
	pool *pgxpool.Pool
}

func NewPartnerRepo(pool *pgxpool.Pool) *PartnerRepo {
	return &PartnerRepo{pool: pool}
}

type PartnerAccount struct {
	LoginID      string
	PasswordHash string
	Name         string
}

// Account looks up a partner login by id. Returns pgx.ErrNoRows if the
// login_id isn't a known partner — callers treat that the same as a wrong
// password, so as not to leak which login IDs exist.
func (r *PartnerRepo) Account(ctx context.Context, loginID string) (PartnerAccount, error) {
	var a PartnerAccount
	err := r.pool.QueryRow(ctx,
		`SELECT login_id, password_hash, name FROM partner_accounts WHERE login_id = $1`, loginID,
	).Scan(&a.LoginID, &a.PasswordHash, &a.Name)
	return a, err
}

// ProfitPoolForDay sums the same revenue streams as ReferralRepo.
// FeeRevenueTotals (platform_treasury_entries by category, plus
// p2p_admin_wallet_entries) but bounded to a single UTC calendar day
// [dayStart, dayStart+24h) rather than an open-ended "since" cutoff — this
// is the source figure the daily partner split divides three ways.
func (r *PartnerRepo) ProfitPoolForDay(ctx context.Context, dayStart time.Time) (string, error) {
	dayEnd := dayStart.Add(24 * time.Hour)

	var treasuryRaw string
	if err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount_raw), 0)::text
		FROM platform_treasury_entries
		WHERE category IS NOT NULL AND created_at >= $1 AND created_at < $2`,
		dayStart, dayEnd,
	).Scan(&treasuryRaw); err != nil {
		return "", fmt.Errorf("query treasury profit for day: %w", err)
	}

	var p2pRaw string
	if err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(buyer_fee_raw + seller_fee_raw), 0)::text
		FROM p2p_admin_wallet_entries
		WHERE created_at >= $1 AND created_at < $2`,
		dayStart, dayEnd,
	).Scan(&p2pRaw); err != nil {
		return "", fmt.Errorf("query p2p profit for day: %w", err)
	}

	treasury, ok := new(big.Int).SetString(treasuryRaw, 10)
	if !ok {
		return "", fmt.Errorf("invalid treasury total %q", treasuryRaw)
	}
	p2p, ok := new(big.Int).SetString(p2pRaw, 10)
	if !ok {
		return "", fmt.Errorf("invalid p2p total %q", p2pRaw)
	}
	return treasury.Add(treasury, p2p).String(), nil
}

// AlreadySplit reports whether profit_date has already been recorded for
// every current partner account — used by the daily job to stay a safe
// no-op if an hourly ticker fires again after a successful run, or if a
// partner account is added later and needs its own historical rows (it
// won't — AlreadySplit only guards re-running the SAME day, not backfilling
// a new partner, which this feature deliberately doesn't support: a partner
// added after day N never gets a day-N share).
func (r *PartnerRepo) AlreadySplit(ctx context.Context, profitDate time.Time) (bool, error) {
	var count, partnerCount int
	if err := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM partner_profit_splits WHERE profit_date = $1`, profitDate,
	).Scan(&count); err != nil {
		return false, err
	}
	if err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM partner_accounts`).Scan(&partnerCount); err != nil {
		return false, err
	}
	return partnerCount > 0 && count >= partnerCount, nil
}

// RecordDailySplit divides totalRaw evenly across every current partner
// account and writes one row per partner for profitDate, inside a single
// transaction so a crash mid-write can't leave a day partially split. Any
// remainder from the integer division (e.g. 100 / 3 = 33 remainder 1) is
// given to the first partner in login_id order — a fixed, deterministic
// rule, not a rotation, so the same day's numbers never change on replay.
// Safe to call more than once for the same day: the (partner_id,
// profit_date) unique constraint makes a duplicate row a conflict, and the
// whole transaction is rolled back if any row hits it (callers should check
// AlreadySplit first to avoid this in the normal case; this is a backstop
// against a race, not the primary guard).
func (r *PartnerRepo) RecordDailySplit(ctx context.Context, profitDate time.Time, totalRaw string) error {
	total, ok := new(big.Int).SetString(totalRaw, 10)
	if !ok {
		return fmt.Errorf("invalid total %q", totalRaw)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `SELECT login_id FROM partner_accounts ORDER BY login_id`)
	if err != nil {
		return err
	}
	var partnerIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		partnerIDs = append(partnerIDs, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if len(partnerIDs) == 0 {
		return nil
	}

	count := big.NewInt(int64(len(partnerIDs)))
	share := new(big.Int).Div(total, count)
	remainder := new(big.Int).Mod(total, count)

	for i, partnerID := range partnerIDs {
		amount := new(big.Int).Set(share)
		if i == 0 {
			amount.Add(amount, remainder)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO partner_profit_splits (partner_id, profit_date, share_raw, source_total_raw, partner_count)
			VALUES ($1, $2, $3, $4, $5)`,
			partnerID, profitDate, amount.String(), total.String(), len(partnerIDs),
		); err != nil {
			return fmt.Errorf("insert partner split for %s: %w", partnerID, err)
		}
	}
	return tx.Commit(ctx)
}

type PartnerProfitSplit struct {
	ProfitDate     time.Time
	ShareRaw       string
	SourceTotalRaw string
	PartnerCount   int
}

// History returns partnerID's own recorded daily shares, most recent first,
// optionally bounded to entries on or after `since` (nil means all-time) —
// mirrors FeeRevenueTotals' since-cutoff convention on the admin fee page.
func (r *PartnerRepo) History(ctx context.Context, partnerID string, since *time.Time) ([]PartnerProfitSplit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT profit_date, share_raw, source_total_raw, partner_count
		FROM partner_profit_splits
		WHERE partner_id = $1 AND ($2::timestamptz IS NULL OR created_at >= $2)
		ORDER BY profit_date DESC`, partnerID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PartnerProfitSplit
	for rows.Next() {
		var s PartnerProfitSplit
		if err := rows.Scan(&s.ProfitDate, &s.ShareRaw, &s.SourceTotalRaw, &s.PartnerCount); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// CumulativeShare sums every recorded share for partnerID — the "total
// earned so far" figure, independent of any range filter.
func (r *PartnerRepo) CumulativeShare(ctx context.Context, partnerID string) (string, error) {
	var total string
	err := r.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(share_raw), 0)::text FROM partner_profit_splits WHERE partner_id = $1`, partnerID,
	).Scan(&total)
	return total, err
}
