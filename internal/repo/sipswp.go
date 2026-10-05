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

var (
	// ErrSipSwpNotFound is returned when a plan lookup/action targets a
	// plan id that doesn't exist (or exists but belongs to another user —
	// see GetPlan's doc comment on why those two cases aren't distinguished
	// to the caller).
	ErrSipSwpNotFound = errors.New("SIP/SWP plan not found")
	// ErrSipSwpInvalidKind/Frequency/DayOfPeriod/Amount/Dates are returned
	// by CreatePlan for a request that fails validation before anything is
	// written — distinct error values so the API layer can map each to a
	// specific 400 message instead of one generic "invalid request".
	ErrSipSwpInvalidKind        = errors.New("kind must be SIP or SWP")
	ErrSipSwpInvalidFrequency   = errors.New("frequency must be DAILY, WEEKLY, MONTHLY, or YEARLY")
	ErrSipSwpInvalidDayOfPeriod = errors.New("dayOfPeriod must be between 1 and 31 for MONTHLY/YEARLY frequency")
	ErrSipSwpInvalidAmount      = errors.New("amountUsdRaw must be a positive integer")
	ErrSipSwpInvalidDates       = errors.New("endDate must be after startDate")
	// ErrSipSwpNotActive is returned by actions that only make sense
	// against an active (Pause) or paused (Resume) plan.
	ErrSipSwpNotActive = errors.New("plan is not in a state that allows this action")
)

// SipSwpRepo backs real scheduled Spot execution for SIP (buy) and SWP
// (sell) plans — see db.ensureSipSwpTables for the schema and
// internal/sipswp's worker for the scheduling loop that calls DuePlans/
// RecordExecution/AdvancePlanAfterExecution each tick. A plan is pure
// metadata/audit, never a balance pool itself (unlike staking/prediction/
// p2p's own wallets) — the actual money movement happens via a real Spot
// MARKET order through engineclient.Client.SubmitOrder, which this repo
// never calls directly; that's the worker's job, using this repo purely
// for scheduling state.
type SipSwpRepo struct {
	pool *pgxpool.Pool
}

func NewSipSwpRepo(pool *pgxpool.Pool) *SipSwpRepo {
	return &SipSwpRepo{pool: pool}
}

func validSipSwpFrequency(f string) bool {
	switch f {
	case "DAILY", "WEEKLY", "MONTHLY", "YEARLY":
		return true
	}
	return false
}

// CreatePlan validates inputs and inserts a new plan, computing its first
// NextRunDate from startDate/frequency/dayOfPeriod (see nextRunDate's doc
// comment for the month-end/leap-year clamping rules). startDate is itself
// eligible to be the first run date — a plan created with startDate=today
// and a matching dayOfPeriod is due on its very first worker tick, not
// deferred to next cycle; this matches the "set it up and it starts
// running" expectation the frontend's "Start SIP Plan" button implies.
func (r *SipSwpRepo) CreatePlan(ctx context.Context, userID, kind, name, asset, quoteAsset, amountUsdRaw, frequency string, dayOfPeriod *int, startDate string, endDate *string) (*models.SipSwpPlan, error) {
	kind = strings.ToUpper(strings.TrimSpace(kind))
	if kind != "SIP" && kind != "SWP" {
		return nil, ErrSipSwpInvalidKind
	}
	frequency = strings.ToUpper(strings.TrimSpace(frequency))
	if !validSipSwpFrequency(frequency) {
		return nil, ErrSipSwpInvalidFrequency
	}
	asset = strings.ToUpper(strings.TrimSpace(asset))
	quoteAsset = strings.ToUpper(strings.TrimSpace(quoteAsset))
	if quoteAsset == "" {
		quoteAsset = "BI2XUSD"
	}
	amount, ok := new(big.Int).SetString(amountUsdRaw, 10)
	if !ok || amount.Sign() <= 0 {
		return nil, ErrSipSwpInvalidAmount
	}
	start, err := time.Parse("2006-01-02", strings.TrimSpace(startDate))
	if err != nil {
		return nil, fmt.Errorf("invalid startDate: %w", err)
	}
	var end *time.Time
	if endDate != nil && strings.TrimSpace(*endDate) != "" {
		parsed, err := time.Parse("2006-01-02", strings.TrimSpace(*endDate))
		if err != nil {
			return nil, fmt.Errorf("invalid endDate: %w", err)
		}
		if !parsed.After(start) {
			return nil, ErrSipSwpInvalidDates
		}
		end = &parsed
	}
	if (frequency == "MONTHLY" || frequency == "YEARLY") && (dayOfPeriod == nil || *dayOfPeriod < 1 || *dayOfPeriod > 31) {
		return nil, ErrSipSwpInvalidDayOfPeriod
	}

	firstRun := firstRunDate(start, frequency, dayOfPeriod)

	var id string
	var createdAt, updatedAt time.Time
	err = r.pool.QueryRow(ctx, `
		INSERT INTO sip_swp_plans (user_id, kind, name, asset, quote_asset, amount_usd_raw, frequency, day_of_period, start_date, end_date, next_run_date)
		VALUES ($1,$2,$3,$4,$5,$6::numeric,$7,$8,$9,$10,$11)
		RETURNING id, created_at, updated_at`,
		userID, kind, name, asset, quoteAsset, amount.String(), frequency, dayOfPeriod, start, end, firstRun,
	).Scan(&id, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	return r.GetPlan(ctx, userID, id)
}

func scanSipSwpPlan(row pgx.Row) (*models.SipSwpPlan, error) {
	var p models.SipSwpPlan
	var dayOfPeriod *int
	var startDate, nextRunDate time.Time
	var endDate *time.Time
	var amountRaw, totalRaw string
	if err := row.Scan(
		&p.ID, &p.Kind, &p.Name, &p.Asset, &p.QuoteAsset, &amountRaw, &p.Frequency, &dayOfPeriod,
		&startDate, &endDate, &p.Status, &nextRunDate, &p.ExecutionsCompleted, &totalRaw,
		&p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return nil, err
	}
	p.AmountUsdRaw = amountRaw
	p.TotalUsdRaw = totalRaw
	p.DayOfPeriod = dayOfPeriod
	p.StartDate = startDate.Format("2006-01-02")
	p.NextRunDate = nextRunDate.Format("2006-01-02")
	if endDate != nil {
		s := endDate.Format("2006-01-02")
		p.EndDate = &s
	}
	return &p, nil
}

const sipSwpPlanColumns = `id, kind, name, asset, quote_asset, amount_usd_raw::text, frequency, day_of_period,
	start_date, end_date, status, next_run_date, executions_completed, total_usd_raw::text, created_at, updated_at`

// GetPlan scopes by user_id in the same WHERE clause rather than fetching
// by id alone and checking ownership after — a plan belonging to another
// user is reported identically to a nonexistent one (ErrSipSwpNotFound),
// so this never leaks whether a given plan id exists for someone else's
// account.
func (r *SipSwpRepo) GetPlan(ctx context.Context, userID, planID string) (*models.SipSwpPlan, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+sipSwpPlanColumns+` FROM sip_swp_plans WHERE id=$1 AND user_id=$2`, planID, userID)
	p, err := scanSipSwpPlan(row)
	if err == pgx.ErrNoRows {
		return nil, ErrSipSwpNotFound
	}
	return p, err
}

func (r *SipSwpRepo) ListPlans(ctx context.Context, userID string) ([]models.SipSwpPlan, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+sipSwpPlanColumns+` FROM sip_swp_plans WHERE user_id=$1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.SipSwpPlan
	for rows.Next() {
		p, err := scanSipSwpPlan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

// setPlanStatus is PausePlan/ResumePlan/CancelPlan's shared implementation.
// fromStatuses is the set of statuses the plan must currently be in for
// this transition to apply (e.g. Resume only applies to a 'paused' plan) —
// a plan not in one of those states returns ErrSipSwpNotActive rather than
// silently no-opping, so e.g. double-cancelling or pausing an already-
// cancelled plan surfaces as a clear error instead of a false "success".
func (r *SipSwpRepo) setPlanStatus(ctx context.Context, userID, planID, newStatus string, fromStatuses []string, recomputeNextRun bool) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentStatus, frequency string
	var dayOfPeriod *int
	err = tx.QueryRow(ctx, `SELECT status, frequency, day_of_period FROM sip_swp_plans WHERE id=$1 AND user_id=$2 FOR UPDATE`, planID, userID).
		Scan(&currentStatus, &frequency, &dayOfPeriod)
	if err == pgx.ErrNoRows {
		return ErrSipSwpNotFound
	}
	if err != nil {
		return err
	}
	allowed := false
	for _, s := range fromStatuses {
		if currentStatus == s {
			allowed = true
			break
		}
	}
	if !allowed {
		return ErrSipSwpNotActive
	}

	if recomputeNextRun {
		// Resuming a paused plan recomputes next_run_date from TODAY rather
		// than leaving whatever date it was paused at — a plan paused for
		// three months and then resumed should pick up from its next real
		// cycle, not immediately try to "catch up" on every cycle it missed
		// while paused.
		today := time.Now().UTC().Truncate(24 * time.Hour)
		next := firstRunDate(today, frequency, dayOfPeriod)
		if _, err := tx.Exec(ctx, `UPDATE sip_swp_plans SET status=$3, next_run_date=$4, updated_at=now() WHERE id=$1 AND user_id=$2`, planID, userID, newStatus, next); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE sip_swp_plans SET status=$3, updated_at=now() WHERE id=$1 AND user_id=$2`, planID, userID, newStatus); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *SipSwpRepo) PausePlan(ctx context.Context, userID, planID string) error {
	return r.setPlanStatus(ctx, userID, planID, "paused", []string{"active"}, false)
}

func (r *SipSwpRepo) ResumePlan(ctx context.Context, userID, planID string) error {
	return r.setPlanStatus(ctx, userID, planID, "active", []string{"paused"}, true)
}

func (r *SipSwpRepo) CancelPlan(ctx context.Context, userID, planID string) error {
	return r.setPlanStatus(ctx, userID, planID, "cancelled", []string{"active", "paused"}, false)
}

// DuePlans is the worker's main query: every active plan whose
// next_run_date has arrived (today or, if the worker was down, any earlier
// date it missed — asOf lets a caught-up worker still run a backlog
// cycle-by-cycle rather than skip straight to today, since
// AdvancePlanAfterExecution only ever advances by one cycle at a time).
func (r *SipSwpRepo) DuePlans(ctx context.Context, asOf time.Time) ([]models.SipSwpPlan, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+sipSwpPlanColumns+` FROM sip_swp_plans WHERE status='active' AND next_run_date <= $1 ORDER BY next_run_date ASC`, asOf.UTC().Truncate(24*time.Hour))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.SipSwpPlan
	for rows.Next() {
		p, err := scanSipSwpPlan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

// SipSwpExecutionInput is what the worker has in hand right after
// attempting a cycle, before it's been persisted — RecordExecution takes
// this rather than a full models.SipSwpExecution since the caller never
// has an ID/CreatedAt yet.
type SipSwpExecutionInput struct {
	PlanID        string
	UserID        string
	ScheduledDate time.Time
	Status        string // completed | skipped | failed
	OrderID       *string
	AmountUsdRaw  string
	QtyRaw        *string
	Price         *string
	SkipReason    *string
}

// RecordExecution inserts one execution row. The UNIQUE(plan_id,
// scheduled_date) constraint is this function's idempotency guarantee — a
// duplicate call for a pair already recorded (a crash-and-retry mid-tick)
// returns ErrSipSwpExecutionAlreadyRecorded rather than inserting a second
// row or silently succeeding, so the worker can tell "this was already
// handled" apart from a genuine new failure and avoid placing a second
// real order for the same due date.
var ErrSipSwpExecutionAlreadyRecorded = errors.New("an execution for this plan/date was already recorded")

func (r *SipSwpRepo) RecordExecution(ctx context.Context, tx pgx.Tx, in SipSwpExecutionInput) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO sip_swp_executions (plan_id, user_id, scheduled_date, status, order_id, amount_usd_raw, qty_raw, price, skip_reason)
		VALUES ($1,$2,$3,$4,$5,$6::numeric,$7::numeric,$8::numeric,$9)`,
		in.PlanID, in.UserID, in.ScheduledDate, in.Status, in.OrderID, in.AmountUsdRaw, in.QtyRaw, in.Price, in.SkipReason,
	)
	if err != nil && strings.Contains(err.Error(), "sip_swp_executions_plan_id_scheduled_date_key") {
		return ErrSipSwpExecutionAlreadyRecorded
	}
	return err
}

// ListExecutions returns a plan's execution history, newest first —
// GetPlan's same ownership-scoping rationale applies: the caller passes
// userID, and a plan belonging to someone else returns ErrSipSwpNotFound
// rather than its executions.
func (r *SipSwpRepo) ListExecutions(ctx context.Context, userID, planID string, limit int) ([]models.SipSwpExecution, error) {
	if _, err := r.GetPlan(ctx, userID, planID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, plan_id, scheduled_date, status, order_id, amount_usd_raw::text, qty_raw::text, price::text, skip_reason, created_at
		FROM sip_swp_executions WHERE plan_id=$1 ORDER BY scheduled_date DESC LIMIT $2`, planID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.SipSwpExecution
	for rows.Next() {
		var e models.SipSwpExecution
		var scheduledDate time.Time
		var qtyRaw, price *string
		if err := rows.Scan(&e.ID, &e.PlanID, &scheduledDate, &e.Status, &e.OrderID, &e.AmountUsdRaw, &qtyRaw, &price, &e.SkipReason, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.ScheduledDate = scheduledDate.Format("2006-01-02")
		e.QtyRaw = qtyRaw
		e.Price = price
		out = append(out, e)
	}
	return out, rows.Err()
}

// AdvancePlanAfterExecution bumps a plan's scheduling state forward after
// EVERY execution attempt — completed, skipped, OR failed alike (the
// confirmed product decision: a failed/skipped cycle is never retried
// later the same day, it simply tries again next cycle). usdRawDelta is
// added to total_usd_raw and is zero for a skipped/failed cycle (nothing
// actually moved). If the plan's end_date has now passed, it's marked
// 'completed' instead of given another next_run_date, so DuePlans stops
// returning it.
func (r *SipSwpRepo) AdvancePlanAfterExecution(ctx context.Context, tx pgx.Tx, planID string, usdRawDelta string) error {
	var frequency string
	var dayOfPeriod *int
	var nextRunDate time.Time
	var endDate *time.Time
	err := tx.QueryRow(ctx, `SELECT frequency, day_of_period, next_run_date, end_date FROM sip_swp_plans WHERE id=$1 FOR UPDATE`, planID).
		Scan(&frequency, &dayOfPeriod, &nextRunDate, &endDate)
	if err != nil {
		return err
	}
	next := nextRunDateAfter(nextRunDate, frequency, dayOfPeriod)

	if endDate != nil && next.After(*endDate) {
		_, err = tx.Exec(ctx, `UPDATE sip_swp_plans SET status='completed', executions_completed=executions_completed+1, total_usd_raw=total_usd_raw+$2::numeric, updated_at=now() WHERE id=$1`, planID, usdRawDelta)
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE sip_swp_plans SET next_run_date=$2, executions_completed=executions_completed+1, total_usd_raw=total_usd_raw+$3::numeric, updated_at=now() WHERE id=$1`, planID, next, usdRawDelta)
	return err
}

// firstRunDate computes a brand-new plan's first NextRunDate from its
// startDate — startDate itself is eligible (see CreatePlan's doc comment),
// so this returns startDate unchanged for DAILY/WEEKLY, or the nearest
// occurrence of dayOfPeriod in startDate's own month/year for MONTHLY/
// YEARLY (which may be startDate itself, if startDate's day already equals
// dayOfPeriod).
func firstRunDate(start time.Time, frequency string, dayOfPeriod *int) time.Time {
	switch frequency {
	case "DAILY", "WEEKLY":
		return start
	case "MONTHLY":
		return clampToDayOfMonth(start.Year(), int(start.Month()), *dayOfPeriod)
	case "YEARLY":
		return clampToDayOfMonth(start.Year(), int(start.Month()), *dayOfPeriod)
	}
	return start
}

// nextRunDateAfter computes the next occurrence strictly after `from` (the
// cycle that was just due) — this is what AdvancePlanAfterExecution calls
// on every attempt. DAILY/WEEKLY are trivial day arithmetic; MONTHLY/
// YEARLY clamp to the target month's actual last day rather than skipping
// the month entirely, which is the "same date every month" behavior a real
// recurring-billing system uses: a plan set for the 31st runs on the 30th
// in a 30-day month, the 28th (or 29th in a leap year) in February — it
// does NOT wait for the next month that happens to have a 31st.
func nextRunDateAfter(from time.Time, frequency string, dayOfPeriod *int) time.Time {
	switch frequency {
	case "DAILY":
		return from.AddDate(0, 0, 1)
	case "WEEKLY":
		return from.AddDate(0, 0, 7)
	case "MONTHLY":
		day := 1
		if dayOfPeriod != nil {
			day = *dayOfPeriod
		}
		y, m := from.Year(), int(from.Month())+1
		if m > 12 {
			m = 1
			y++
		}
		return clampToDayOfMonth(y, m, day)
	case "YEARLY":
		day := 1
		if dayOfPeriod != nil {
			day = *dayOfPeriod
		}
		return clampToDayOfMonth(from.Year()+1, int(from.Month()), day)
	}
	return from.AddDate(0, 0, 1)
}

// clampToDayOfMonth builds a UTC date for (year, month, day), clamping day
// down to that month's actual last day if it overflows (e.g. day=31 in
// April becomes April 30; day=29 in a non-leap February becomes Feb 28).
func clampToDayOfMonth(year, month, day int) time.Time {
	// The last day of `month` is one day before the 1st of the next month.
	firstOfNextMonth := time.Date(year, time.Month(month)+1, 1, 0, 0, 0, 0, time.UTC)
	lastDayOfMonth := firstOfNextMonth.AddDate(0, 0, -1).Day()
	if day > lastDayOfMonth {
		day = lastDayOfMonth
	}
	if day < 1 {
		day = 1
	}
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}
