package repo

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// secondsPerYear is the fixed denominator for APR -> per-second rate
// conversion (365-day year, no leap-year adjustment — standard simple
// convention for this kind of fixed-APR product, matching how APR is
// generally quoted rather than compounding daily/annually differently).
const secondsPerYear = 365 * 24 * 60 * 60

// StakingRepo backs the BI2X staking feature: 5% APR simple interest, no
// lock-up period, BI2X only. See db.ensureStakingTables for the schema and
// LedgerRepo for the balance debit/credit this wraps.
type StakingRepo struct {
	pool   *pgxpool.Pool
	ledger *LedgerRepo
}

func NewStakingRepo(pool *pgxpool.Pool, ledger *LedgerRepo) *StakingRepo {
	return &StakingRepo{pool: pool, ledger: ledger}
}

// AccruedInterest computes simple interest for principalRaw at aprBps basis
// points, accrued from startedAt through asOf (asOf must not be before
// startedAt — callers pass time.Now()). Truncates (floors) to a whole raw
// integer, same convention as every other balance figure in this API — a
// user is never credited a fractional raw unit, and flooring (rather than
// rounding) means the platform never pays out a hair more than what's
// exactly earned.
//
//	interest = principal * aprBps * secondsElapsed / (10000 * secondsPerYear)
func AccruedInterest(principalRaw *big.Int, aprBps int, startedAt, asOf time.Time) *big.Int {
	elapsed := asOf.Sub(startedAt)
	if elapsed <= 0 || principalRaw.Sign() <= 0 || aprBps <= 0 {
		return big.NewInt(0)
	}
	elapsedSeconds := new(big.Int).SetInt64(int64(elapsed.Seconds()))
	numerator := new(big.Int).Mul(principalRaw, big.NewInt(int64(aprBps)))
	numerator.Mul(numerator, elapsedSeconds)
	denominator := new(big.Int).Mul(big.NewInt(10000), big.NewInt(secondsPerYear))
	interest := new(big.Rat).SetFrac(numerator, denominator)
	// Truncate toward zero (floor, since interest is always non-negative here).
	quo := new(big.Int).Quo(interest.Num(), interest.Denom())
	return quo
}

// Positions returns every staking position for userID, most recently
// started first.
func (r *StakingRepo) Positions(ctx context.Context, userID string) ([]models.StakingPosition, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, asset, principal_raw::text, apr_bps, started_at, status, closed_at
		FROM staking_positions
		WHERE user_id = $1
		ORDER BY started_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.StakingPosition
	for rows.Next() {
		var p models.StakingPosition
		if err := rows.Scan(&p.ID, &p.Asset, &p.PrincipalRaw, &p.AprBps, &p.StartedAt, &p.Status, &p.ClosedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Events returns userID's staking history (every stake and redeem action),
// most recent first, capped at limit — this is what actually shows the
// interest paid on each redemption, since a position's own current
// principal_raw doesn't retain that (Redeem already computes it, but only
// staking_events keeps a permanent per-action record of it).
func (r *StakingRepo) Events(ctx context.Context, userID string, limit int) ([]models.StakingEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, position_id, kind, principal_raw::text, interest_raw::text, created_at
		FROM staking_events
		WHERE user_id = $1
		ORDER BY created_at DESC, id DESC
		LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.StakingEvent
	for rows.Next() {
		var e models.StakingEvent
		if err := rows.Scan(&e.ID, &e.PositionID, &e.Kind, &e.PrincipalRaw, &e.InterestRaw, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Stake debits amountRaw of BI2X from userID's wallet balance and opens a
// new staking position for it, in one transaction — mirrors
// LedgerRepo.SwapBalance's shape (debit then insert, single tx, single
// user, no extra app-level lock needed beyond the row lock debitBalanceTx
// already takes on user_balances).
func (r *StakingRepo) Stake(ctx context.Context, userID, amountRaw string) (models.StakingPosition, error) {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return models.StakingPosition{}, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return models.StakingPosition{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.ledger.debitBalanceTx(ctx, tx, userID, "BI2X", amountRaw); err != nil {
		return models.StakingPosition{}, err
	}

	var p models.StakingPosition
	if err := tx.QueryRow(ctx, `
		INSERT INTO staking_positions (user_id, asset, principal_raw)
		VALUES ($1, 'BI2X', $2::numeric)
		RETURNING id, asset, principal_raw::text, apr_bps, started_at, status, closed_at`,
		userID, amountRaw).
		Scan(&p.ID, &p.Asset, &p.PrincipalRaw, &p.AprBps, &p.StartedAt, &p.Status, &p.ClosedAt); err != nil {
		return models.StakingPosition{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO staking_events (position_id, user_id, kind, principal_raw, interest_raw)
		VALUES ($1, $2, 'stake', $3::numeric, 0)`, p.ID, userID, amountRaw); err != nil {
		return models.StakingPosition{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return models.StakingPosition{}, err
	}
	return p, nil
}

// RedeemResult is what Redeem returns: the interest actually paid out (the
// backend's own authoritative recalculation, not whatever the frontend's
// live estimate showed) and the position's state afterward.
type RedeemResult struct {
	Position     models.StakingPosition
	PrincipalRaw string // the amount of principal just redeemed
	InterestRaw  string // the interest just paid out on that amount
	TotalRaw     string // PrincipalRaw + InterestRaw, credited to the wallet
}

// Redeem redeems amountRaw of principal from an active position owned by
// userID (full redeem if amountRaw equals the position's current
// principal, partial otherwise). Recomputes accrued interest authoritatively
// at this exact moment via AccruedInterest — never trusts a client-supplied
// interest figure — pro-rated to the fraction of principal being redeemed,
// credits principal+interest back to the wallet, and either closes the
// position (full redeem) or reduces its principal (partial redeem), leaving
// started_at UNCHANGED for the remainder so it keeps accruing from its
// original start time uninterrupted (product decision, 2026-09-16).
func (r *StakingRepo) Redeem(ctx context.Context, userID, positionID, amountRaw string) (RedeemResult, error) {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return RedeemResult{}, err
	}
	redeemAmount, _ := new(big.Int).SetString(amountRaw, 10) // validated above

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return RedeemResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the position row for the duration of this transaction so two
	// concurrent redeem requests against the same position can't both read
	// the same principal and both succeed past the amount check below,
	// together over-redeeming it.
	var principalRaw string
	var aprBps int
	var startedAt time.Time
	var status string
	if err := tx.QueryRow(ctx, `
		SELECT principal_raw::text, apr_bps, started_at, status
		FROM staking_positions
		WHERE id = $1 AND user_id = $2
		FOR UPDATE`, positionID, userID).
		Scan(&principalRaw, &aprBps, &startedAt, &status); err != nil {
		return RedeemResult{}, fmt.Errorf("staking position not found: %w", err)
	}
	if status != "active" {
		return RedeemResult{}, fmt.Errorf("staking position is not active")
	}
	principal, _ := new(big.Int).SetString(principalRaw, 10)
	if redeemAmount.Cmp(principal) > 0 {
		return RedeemResult{}, fmt.Errorf("redeem amount exceeds staked principal")
	}

	// Authoritative interest, computed server-side right now — the amount
	// actually earned on the PORTION being redeemed, from the position's
	// real started_at to this exact moment.
	totalAccrued := AccruedInterest(principal, aprBps, startedAt, time.Now())
	interest := new(big.Int).Mul(totalAccrued, redeemAmount)
	interest.Quo(interest, principal) // pro-rate: interest * (redeemAmount / principal), floored

	payout := new(big.Int).Add(redeemAmount, interest)

	if err := r.ledger.creditBalanceTx(ctx, tx, userID, "BI2X", payout.String()); err != nil {
		return RedeemResult{}, err
	}

	remaining := new(big.Int).Sub(principal, redeemAmount)
	var result models.StakingPosition
	if remaining.Sign() == 0 {
		if err := tx.QueryRow(ctx, `
			UPDATE staking_positions
			SET status = 'redeemed', closed_at = now(), updated_at = now()
			WHERE id = $1
			RETURNING id, asset, principal_raw::text, apr_bps, started_at, status, closed_at`,
			positionID).
			Scan(&result.ID, &result.Asset, &result.PrincipalRaw, &result.AprBps, &result.StartedAt, &result.Status, &result.ClosedAt); err != nil {
			return RedeemResult{}, err
		}
	} else {
		// started_at is deliberately NOT touched: the remaining principal
		// keeps accruing from its original start time.
		if err := tx.QueryRow(ctx, `
			UPDATE staking_positions
			SET principal_raw = $2::numeric, updated_at = now()
			WHERE id = $1
			RETURNING id, asset, principal_raw::text, apr_bps, started_at, status, closed_at`,
			positionID, remaining.String()).
			Scan(&result.ID, &result.Asset, &result.PrincipalRaw, &result.AprBps, &result.StartedAt, &result.Status, &result.ClosedAt); err != nil {
			return RedeemResult{}, err
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO staking_events (position_id, user_id, kind, principal_raw, interest_raw)
		VALUES ($1, $2, 'redeem', $3::numeric, $4::numeric)`,
		positionID, userID, redeemAmount.String(), interest.String()); err != nil {
		return RedeemResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return RedeemResult{}, err
	}
	return RedeemResult{
		Position:     result,
		PrincipalRaw: redeemAmount.String(),
		InterestRaw:  interest.String(),
		TotalRaw:     payout.String(),
	}, nil
}
