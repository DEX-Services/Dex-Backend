package repo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BI2XAllocationRepo backs the admin-facing BI2X token allocation page: a
// running remaining-quantity total per category, plus a permanent history
// of every burn/distribution recorded against one — see
// db.ensureBI2XAllocationTables for the schema and starting split.
type BI2XAllocationRepo struct {
	pool *pgxpool.Pool
}

func NewBI2XAllocationRepo(pool *pgxpool.Pool) *BI2XAllocationRepo {
	return &BI2XAllocationRepo{pool: pool}
}

// CurrentTotals returns every category's current remaining quantity,
// ordered by category name for a stable display order.
func (r *BI2XAllocationRepo) CurrentTotals(ctx context.Context) ([]models.BI2XAllocationBalance, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT category, remaining_qty::text, updated_at
		FROM bi2x_allocation_balances
		ORDER BY category`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.BI2XAllocationBalance
	for rows.Next() {
		var b models.BI2XAllocationBalance
		if err := rows.Scan(&b.Category, &b.RemainingQty, &b.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// History returns the most recent history entries across all categories,
// newest first, capped at limit.
func (r *BI2XAllocationRepo) History(ctx context.Context, limit int) ([]models.BI2XAllocationHistoryEntry, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, category, amount_qty::text, event_date, COALESCE(note, ''), COALESCE(created_by, ''), created_at
		FROM bi2x_allocation_history
		ORDER BY event_date DESC, id DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.BI2XAllocationHistoryEntry
	for rows.Next() {
		var h models.BI2XAllocationHistoryEntry
		if err := rows.Scan(&h.ID, &h.Category, &h.AmountQty, &h.EventDate, &h.Note, &h.CreatedBy, &h.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// AddHistoryEntry records a burn/distribution against category and
// decrements that category's remaining_qty by the same amount, atomically
// (one transaction: insert the history row, then debit the running total —
// a failure in either half leaves neither applied, so the history log and
// the current-total display can never drift apart). Rejects an amount that
// would take the category's remaining quantity negative — a distribution
// can never exceed what's actually left in that category.
func (r *BI2XAllocationRepo) AddHistoryEntry(ctx context.Context, category, amountQty, note, createdBy string, eventDate time.Time) (models.BI2XAllocationHistoryEntry, error) {
	category = strings.TrimSpace(category)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return models.BI2XAllocationHistoryEntry{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the category's row for the duration of this transaction so a
	// concurrent distribution against the same category can't both read the
	// same remaining_qty and both succeed past the CHECK below, together
	// overdrawing the category.
	var remaining string
	if err := tx.QueryRow(ctx, `
		SELECT remaining_qty::text FROM bi2x_allocation_balances
		WHERE category = $1 FOR UPDATE`, category).Scan(&remaining); err != nil {
		return models.BI2XAllocationHistoryEntry{}, fmt.Errorf("unknown category %q: %w", category, err)
	}

	var h models.BI2XAllocationHistoryEntry
	if err := tx.QueryRow(ctx, `
		INSERT INTO bi2x_allocation_history (category, amount_qty, event_date, note, created_by)
		VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''))
		RETURNING id, category, amount_qty::text, event_date, COALESCE(note, ''), COALESCE(created_by, ''), created_at`,
		category, amountQty, eventDate, strings.TrimSpace(note), strings.TrimSpace(createdBy)).
		Scan(&h.ID, &h.Category, &h.AmountQty, &h.EventDate, &h.Note, &h.CreatedBy, &h.CreatedAt); err != nil {
		return models.BI2XAllocationHistoryEntry{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE bi2x_allocation_balances
		SET remaining_qty = remaining_qty - $2::numeric, updated_at = now()
		WHERE category = $1 AND remaining_qty >= $2::numeric`,
		category, amountQty)
	if err != nil {
		return models.BI2XAllocationHistoryEntry{}, err
	}
	if tag.RowsAffected() == 0 {
		return models.BI2XAllocationHistoryEntry{}, fmt.Errorf("amount exceeds remaining quantity for category %q", category)
	}

	if err := tx.Commit(ctx); err != nil {
		return models.BI2XAllocationHistoryEntry{}, err
	}
	return h, nil
}
