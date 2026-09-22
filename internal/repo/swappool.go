package repo

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
)

// swapPoolAssets is the fixed set of assets that carry a swappable/reserve
// split — BI2XUSD itself is never split (it's the destination/source of
// every swap, not a pooled asset), and every other asset in user_balances
// (BTC, BI2X) never participates in a USDT/USDC<->BI2XUSD swap at all.
var swapPoolAssets = map[string]bool{"USDT": true, "USDC": true}

// ErrSwapPoolInsufficient is returned by DebitSwapPoolCapped when asset's
// swappable pool cannot cover amountRaw right now. Callers (WalletServer.Swap)
// surface this as a clear "insufficient swap liquidity" error rather than a
// generic failure — the frontend's own pre-check (GET /wallet/swap/max)
// should normally prevent a user from ever reaching this path, but the
// server-side check must still exist as the actual source of truth: the
// frontend figure can be stale by the time the swap actually executes.
var ErrSwapPoolInsufficient = errors.New("insufficient swap pool liquidity")

func normalizeSwapPoolAsset(asset string) (string, error) {
	if !swapPoolAssets[asset] {
		return "", fmt.Errorf("asset %q does not have a swap pool (only USDT/USDC do)", asset)
	}
	return asset, nil
}

// splitSixtyForty splits total into a 60% "swappable" leg and a 40%
// "reserve" leg, both raw integer amounts. The swappable leg uses
// splitRawByPercent (referral.go) — the same floor(total*pct/100) integer
// math already used for referral/affiliate revenue splits, so this follows
// an established, tested precedent rather than introducing new rounding
// behavior. Reserve is computed as total minus the swappable leg (NOT a
// second independent floor(total*40/100) call) so the two legs always sum
// to exactly total raw units with none lost or gained to floor-rounding on
// each side separately — e.g. splitting 101 raw units 60/40 must yield
// exactly 101 back out (60 + 41, not 60 + 40 with 1 unit silently vanishing).
func splitSixtyForty(total *big.Int) (swappable, reserve *big.Int) {
	swappable = splitRawByPercent(total, "60.00")
	reserve = new(big.Int).Sub(total, swappable)
	return swappable, reserve
}

// CreditSwapPoolSplit splits totalRaw 60/40 into asset's swappable/reserve
// pool and applies both legs inside the caller's already-open transaction —
// called from SwapBalance for the USDT/USDC -> BI2XUSD leg of a swap, so the
// user's own balance movement and this platform-wide bookkeeping commit or
// roll back together atomically. accountID is recorded on the audit entry
// (swap_pool_entries) for traceability, not used in the balance math.
func (r *LedgerRepo) CreditSwapPoolSplit(ctx context.Context, tx pgx.Tx, asset, totalRaw, accountID string) error {
	normalized, err := normalizeSwapPoolAsset(asset)
	if err != nil {
		return err
	}
	if err := validatePositiveAmount(totalRaw); err != nil {
		return err
	}
	total, ok := new(big.Int).SetString(totalRaw, 10)
	if !ok {
		return fmt.Errorf("invalid swap pool split amount %q", totalRaw)
	}
	swappable, reserve := splitSixtyForty(total)

	if _, err := tx.Exec(ctx, `
		UPDATE swap_pool_balances
		SET swappable_raw = swappable_raw + $2, reserve_raw = reserve_raw + $3, updated_at = now()
		WHERE asset = $1`,
		normalized, swappable.String(), reserve.String(),
	); err != nil {
		return fmt.Errorf("credit swap pool split: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO swap_pool_entries (kind, asset, swappable_delta_raw, reserve_delta_raw, account_id)
		VALUES ('split_credit', $1, $2, $3, $4)`,
		normalized, swappable.String(), reserve.String(), nullIfEmpty(accountID),
	); err != nil {
		return fmt.Errorf("log swap pool split entry: %w", err)
	}
	return nil
}

// DebitSwapPoolCapped atomically checks-and-debits asset's swappable pool by
// amountRaw, inside the caller's already-open transaction — called from
// SwapBalance for the BI2XUSD -> USDT/USDC leg of a swap. The single
// UPDATE ... WHERE swappable_raw >= $2 statement (not a separate SELECT then
// UPDATE) is what makes this safe under concurrent swaps: Postgres's own row
// lock for the UPDATE serializes concurrent debits against the same asset
// row, so two simultaneous swaps can never both observe "enough available"
// and both succeed when only one actually fits — the second one's UPDATE
// simply matches zero rows and returns ErrSwapPoolInsufficient. amountRaw
// must be the NET amount the user actually receives (i.e. destinationAmountRaw
// after any swap-out fee is deducted), not the gross BI2XUSD amount debited
// from them — that net figure is what the swap pool actually needs to cover.
func (r *LedgerRepo) DebitSwapPoolCapped(ctx context.Context, tx pgx.Tx, asset, amountRaw, accountID string) error {
	normalized, err := normalizeSwapPoolAsset(asset)
	if err != nil {
		return err
	}
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	commandTag, err := tx.Exec(ctx, `
		UPDATE swap_pool_balances
		SET swappable_raw = swappable_raw - $2, updated_at = now()
		WHERE asset = $1 AND swappable_raw >= $2`,
		normalized, amountRaw,
	)
	if err != nil {
		return fmt.Errorf("debit swap pool: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return ErrSwapPoolInsufficient
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO swap_pool_entries (kind, asset, swappable_delta_raw, account_id)
		VALUES ('user_debit', $1, $2, $3)`,
		normalized, "-"+amountRaw, nullIfEmpty(accountID),
	); err != nil {
		return fmt.Errorf("log swap pool debit entry: %w", err)
	}
	return nil
}

// SwapPoolBalance returns asset's current swappable/reserve raw balances —
// a plain read, used both by the user-facing "max swappable now" pre-check
// (GET /wallet/swap/max) and the admin status endpoint (GET /admin/swap-pool).
func (r *LedgerRepo) SwapPoolBalance(ctx context.Context, asset string) (swappableRaw, reserveRaw string, err error) {
	normalized, err := normalizeSwapPoolAsset(asset)
	if err != nil {
		return "", "", err
	}
	err = r.pool.QueryRow(ctx,
		`SELECT swappable_raw::text, reserve_raw::text FROM swap_pool_balances WHERE asset = $1`,
		normalized,
	).Scan(&swappableRaw, &reserveRaw)
	if err != nil {
		return "", "", fmt.Errorf("load swap pool balance: %w", err)
	}
	return swappableRaw, reserveRaw, nil
}

// AllSwapPoolBalances returns every asset's swappable/reserve raw balances,
// for the admin status endpoint's "list every pool" view.
func (r *LedgerRepo) AllSwapPoolBalances(ctx context.Context) (map[string]struct{ SwappableRaw, ReserveRaw string }, error) {
	rows, err := r.pool.Query(ctx, `SELECT asset, swappable_raw::text, reserve_raw::text FROM swap_pool_balances`)
	if err != nil {
		return nil, fmt.Errorf("load swap pool balances: %w", err)
	}
	defer rows.Close()
	out := make(map[string]struct{ SwappableRaw, ReserveRaw string })
	for rows.Next() {
		var asset, swappableRaw, reserveRaw string
		if err := rows.Scan(&asset, &swappableRaw, &reserveRaw); err != nil {
			return nil, err
		}
		out[asset] = struct{ SwappableRaw, ReserveRaw string }{swappableRaw, reserveRaw}
	}
	return out, rows.Err()
}

// AdminTopUpSwapPool credits amountRaw to asset's swappable pool only —
// there is deliberately no parameter or code path here that touches
// reserve_raw, and no "direction" flag like AdjustUserBalance has. This is
// intentional: admin can add fresh funds to swappable, but can never move
// money from reserve into swappable — reserve only ever grows via
// CreditSwapPoolSplit's 60/40 split, never spent back out through this
// system. Runs in its own transaction (not tied to a swap), since a top-up
// is an independent admin action.
func (r *LedgerRepo) AdminTopUpSwapPool(ctx context.Context, asset, amountRaw, adminAccountID string) error {
	normalized, err := normalizeSwapPoolAsset(asset)
	if err != nil {
		return err
	}
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		UPDATE swap_pool_balances SET swappable_raw = swappable_raw + $2, updated_at = now() WHERE asset = $1`,
		normalized, amountRaw,
	); err != nil {
		return fmt.Errorf("admin top up swap pool: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO swap_pool_entries (kind, asset, swappable_delta_raw, account_id)
		VALUES ('admin_topup', $1, $2, $3)`,
		normalized, amountRaw, nullIfEmpty(adminAccountID),
	); err != nil {
		return fmt.Errorf("log swap pool topup entry: %w", err)
	}
	return tx.Commit(ctx)
}
