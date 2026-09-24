package repo

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LedgerRepo struct {
	pool *pgxpool.Pool
}

func NewLedgerRepo(pool *pgxpool.Pool) *LedgerRepo {
	return &LedgerRepo{pool: pool}
}

var assetColumns = map[string]string{
	"BTC":  `"BTC"`,
	"USDC": `"USDC"`,
	"USDT": `"USDT"`,
	// BI2XUSD is the platform's internal stable quote currency, pegged 1:1 to
	// USDT — it has no on-chain contract of its own. Every market's quote
	// leg trades in BI2XUSD, not USDT; a real USDT/USDC deposit is credited as
	// BI2XUSD at 1:1 (see chain.Listener.handleDeposit). USDT/USDC columns are
	// kept only as the deposit-intake ledger, not as tradable balances.
	"BI2XUSD": `"BI2XUSD"`,
	// BI2X: base asset for the BI2X-BI2XUSD spot/futures pair (added
	// 2026-09-12) — same shape as BTC.
	//
	// ETH, SOL, and BNB previously had columns here backing the
	// ETH-BI2XUSD/SOL-BI2XUSD/BNB-BI2XUSD SPOT markets. Those SPOT markets were
	// removed in the 2026-09-12 restructure (ETH/SOL are now FUTURES-only,
	// settled entirely in BI2XUSD; BNB has no market at all) — see
	// matching-engine's cmd/engine/markets.go. Futures settlement never
	// touches a base-asset column (internal/settlement/futures.go debits/
	// credits quoteAsset only), so these columns became genuinely unused and
	// were dropped (see migrateDropETHSOLBNBColumns in internal/db/db.go).
	//
	// BI (the platform's own native token, distinct from BI2X/BI2XUSD) was
	// also removed (2026-09-13): it was never wired into any
	// matching-engine market, same as ETH/SOL/BNB were before their
	// removal — a wallet/ledger column with nothing behind it.
	"BI2X": `"BI2X"`,
}

var lockedColumns = map[string]string{
	"BTC":     `"BTC_locked"`,
	"USDC":    `"USDC_locked"`,
	"USDT":    `"USDT_locked"`,
	"BI2XUSD": `"BI2XUSD_locked"`,
	"BI2X":    `"BI2X_locked"`,
}

func normalizeAsset(asset string) (string, string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(asset))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	column, ok := assetColumns[normalized]
	if !ok {
		return "", "", fmt.Errorf("unsupported asset %q", asset)
	}
	return normalized, column, nil
}
func validatePositiveAmount(amountRaw string) error {
	amount, ok := new(big.Int).SetString(amountRaw, 10)
	if !ok || amount.Sign() <= 0 {
		return fmt.Errorf("amount must be a positive integer raw token amount")
	}
	return nil
}

func validateNonNegativeAmount(amountRaw string) error {
	amount, ok := new(big.Int).SetString(amountRaw, 10)
	if !ok || amount.Sign() < 0 {
		return fmt.Errorf("amount must be a non-negative integer raw token amount")
	}
	return nil
}

// ErrIdempotencyKeyReused means the same (endpoint, key) pair was seen
// before with different request parameters — the caller is either reusing a
// key across unrelated operations (a bug) or something is generating
// colliding keys; either way it's unsafe to guess which request was
// "right", so this fails loudly instead of silently applying one of them.
var ErrIdempotencyKeyReused = fmt.Errorf("idempotency key already used for a different request")

// checkIdempotency looks up (endpoint, key) inside tx. found=true means a
// prior attempt already completed and the caller should skip re-applying
// the mutation (fingerprint must match — see ErrIdempotencyKeyReused).
// recordIdempotency must be called inside the same transaction after the
// mutation succeeds, so the dedup row and the balance change commit or roll
// back together — a crash between them is impossible by construction.
func checkIdempotency(ctx context.Context, tx pgx.Tx, endpoint, key, fingerprint string) (found bool, err error) {
	if key == "" {
		return false, nil
	}
	var priorFingerprint string
	err = tx.QueryRow(ctx, `SELECT request FROM internal_idempotency_keys WHERE endpoint = $1 AND key = $2`, endpoint, key).Scan(&priorFingerprint)
	if err == nil {
		if priorFingerprint != fingerprint {
			return false, ErrIdempotencyKeyReused
		}
		return true, nil
	}
	if err == pgx.ErrNoRows {
		return false, nil
	}
	return false, err
}

func recordIdempotency(ctx context.Context, tx pgx.Tx, endpoint, key, fingerprint string) error {
	if key == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO internal_idempotency_keys (endpoint, key, request) VALUES ($1, $2, $3)`, endpoint, key, fingerprint)
	return err
}

func (r *LedgerRepo) lockUser(ctx context.Context, tx pgx.Tx, userID string) error {
	var exists int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM users WHERE id = $1 FOR UPDATE`, userID).Scan(&exists); err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("user %s not found", userID)
		}
		return err
	}
	return nil
}

// lockBalance ensures the account has a user_balances row and row-locks it for
// the caller's transaction. Every ledger mutation funnels through here, so this
// is the one place that has to guarantee the row exists.
//
// user_balances.user_id carries a foreign key to users(id) (currently named
// user_balances_reordered_user_id_fkey — the constraint kept its name from the
// table rebuild that reordered the asset columns). The balance INSERT below is
// an intentional create-on-demand, but it can only ever satisfy that FK when
// the users row already exists: otherwise Postgres raises SQLSTATE 23503 and
// the whole ledger call fails with an opaque
//
//	insert or update on table "user_balances" violates foreign key
//	constraint "user_balances_reordered_user_id_fkey"
//
// rather than anything actionable. That is exactly what happened to a
// market-maker desk: bots' mm.Service.Create/Deposit provision the desk wallet
// via /internal/user/ensure, but mm.Service.recreditDesk (called on every
// desk enable) reached release-locks first, so a desk whose users row was
// missing — after a DB restore/clear, or a cleanup pass — failed to start with
// the FK error above and no way to tell why.
//
// Provisioning the users row here, in the same transaction, makes the balance
// row always creatable and keeps the create-on-demand contract the rest of the
// ledger relies on. ON CONFLICT DO NOTHING makes it a no-op for the common
// case (a real, already-provisioned user), so this costs one extra statement
// on the first touch of an account and nothing thereafter. wallet_type is
// recorded as 'market-maker' only as an audit hint for an id the caller did
// not already know about; a pre-existing row is never modified.
func (r *LedgerRepo) lockBalance(ctx context.Context, tx pgx.Tx, userID string) error {
	if err := ensureUsersForTx(ctx, tx, []string{userID}); err != nil {
		return fmt.Errorf("ensure user %s: %w", userID, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_balances (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING`, userID); err != nil {
		return err
	}
	var exists int
	return tx.QueryRow(ctx,
		`SELECT 1 FROM user_balances WHERE user_id = $1 FOR UPDATE`,
		userID,
	).Scan(&exists)
}

// ensureUsersForTx provisions a users row for each id in a single statement, so
// a following user_balances INSERT cannot fail its foreign key to users(id).
// It is the batched counterpart to the provisioning inside lockBalance, for the
// multi-account paths (spot settlement, swaps) that lock both sides at once.
func ensureUsersForTx(ctx context.Context, tx pgx.Tx, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO users (id, wallet_address, wallet_type)
		SELECT id, id, 'market-maker' FROM unnest($1::text[]) AS id
		ON CONFLICT (id) DO NOTHING`, userIDs)
	return err
}

func (r *LedgerRepo) lockBalances(ctx context.Context, tx pgx.Tx, userIDs []string) error {
	// Same FK hazard as lockBalance: the user_balances rows below can only be
	// auto-created once each id has a users row, and buyer/seller/sender/
	// recipient desks are exactly the synthetic ids that may not.
	if err := ensureUsersForTx(ctx, tx, userIDs); err != nil {
		return err
	}
	// One statement for every user_id instead of a per-user round trip — see
	// PERFORMANCE-CODE-REVIEW-FINDINGS.md item #7. unnest($1) expands the
	// text[] param into one row per user_id, so this is exactly equivalent
	// to the old loop's N separate "INSERT ... VALUES ($1) ON CONFLICT DO
	// NOTHING" statements, just issued as a single round trip to Postgres.
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_balances (user_id)
		SELECT unnest($1::text[])
		ON CONFLICT (user_id) DO NOTHING`, userIDs); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		SELECT user_id FROM user_balances
		WHERE user_id = ANY($1)
		ORDER BY user_id
		FOR UPDATE`, userIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != len(userIDs) {
		return fmt.Errorf("could not lock all user balance records")
	}
	return nil
}

func (r *LedgerRepo) creditBalanceTx(ctx context.Context, tx pgx.Tx, userID, asset, amountRaw string) error {
	_, column, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE user_balances SET `+column+` = `+column+` + $2::numeric, updated_at = now() WHERE user_id = $1`, userID, amountRaw)
	return err
}

func (r *LedgerRepo) debitBalanceTx(ctx context.Context, tx pgx.Tx, userID, asset, amountRaw string) error {
	normalized, column, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	commandTag, err := tx.Exec(ctx, `UPDATE user_balances SET `+column+` = `+column+` - $2::numeric, updated_at = now() WHERE user_id = $1 AND `+column+` >= $2::numeric`, userID, amountRaw)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("insufficient %s balance", normalized)
	}
	return nil
}
func (r *LedgerRepo) pendingWithdrawalHoldTx(ctx context.Context, tx pgx.Tx, userID, normalizedToken string) (*big.Int, error) {
	var holdRaw string
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0)::text
		FROM ledger_entries
		WHERE user_id = $1
		  AND token = $2
		  AND kind = $3
		  AND status IN ($4, $5)`,
		userID,
		normalizedToken,
		models.LedgerKindWithdrawalRequest,
		models.LedgerStatusPending,
		models.LedgerStatusProcessing,
	).Scan(&holdRaw)
	if err != nil {
		return nil, err
	}
	hold, ok := new(big.Int).SetString(holdRaw, 10)
	if !ok {
		return nil, fmt.Errorf("invalid pending withdrawal amount %q", holdRaw)
	}
	return hold, nil
}
func (r *LedgerRepo) LockBalance(ctx context.Context, userID, asset, amountRaw string) error {
	normalized, column, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	// The pending-withdrawal hold used to be read in its own round trip
	// (pendingWithdrawalHoldTx) and then substituted into this UPDATE as a
	// literal parameter. Folded into a scalar subquery instead: same value,
	// computed by Postgres inline, one fewer statement in this transaction.
	// Semantics are unchanged -- COALESCE(SUM(...), 0) is exactly what
	// pendingWithdrawalHoldTx returned.
	commandTag, err := tx.Exec(ctx,
		`UPDATE user_balances SET `+lockedColumn+` = `+lockedColumn+` + $2::numeric, updated_at = now()
		 WHERE user_id = $1 AND `+column+` - `+lockedColumn+` - (
			SELECT COALESCE(SUM(amount), 0) FROM ledger_entries
			WHERE user_id = $1 AND token = $3 AND kind = $4 AND status IN ($5, $6)
		 ) >= $2::numeric`,
		userID, amountRaw, normalized, models.LedgerKindWithdrawalRequest,
		models.LedgerStatusPending, models.LedgerStatusProcessing)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("insufficient %s balance to lock", normalized)
	}
	return tx.Commit(ctx)
}

// LockBalanceIdempotent mirrors CreditBalanceIdempotent for LockBalance (see
// its doc comment for the guard semantics).
func (r *LedgerRepo) LockBalanceIdempotent(ctx context.Context, userID, asset, amountRaw, idempotencyKey string) error {
	normalized, column, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fingerprint := fmt.Sprintf("lock:%s:%s:%s", userID, asset, amountRaw)
	found, err := checkIdempotency(ctx, tx, "/internal/balance/lock", idempotencyKey, fingerprint)
	if err != nil {
		return err
	}
	if found {
		return tx.Commit(ctx)
	}
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	commandTag, err := tx.Exec(ctx,
		`UPDATE user_balances SET `+lockedColumn+` = `+lockedColumn+` + $2::numeric, updated_at = now()
		 WHERE user_id = $1 AND `+column+` - `+lockedColumn+` - (
			SELECT COALESCE(SUM(amount), 0) FROM ledger_entries
			WHERE user_id = $1 AND token = $3 AND kind = $4 AND status IN ($5, $6)
		 ) >= $2::numeric`,
		userID, amountRaw, normalized, models.LedgerKindWithdrawalRequest,
		models.LedgerStatusPending, models.LedgerStatusProcessing)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("insufficient %s balance to lock", normalized)
	}
	if err := recordIdempotency(ctx, tx, "/internal/balance/lock", idempotencyKey, fingerprint); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// UnlockBalance releases a previously locked amountRaw of asset for userID, e.g. on
// order cancel/rejection. Floors at zero locked, mirroring the matching-engine's
// in-memory Ledger.Release semantics.
func (r *LedgerRepo) UnlockBalance(ctx context.Context, userID, asset, amountRaw string) error {
	normalized, _, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE user_balances SET `+lockedColumn+` = GREATEST(0, `+lockedColumn+` - $2::numeric), updated_at = now()
		 WHERE user_id = $1`,
		userID, amountRaw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// UnlockBalanceIdempotent mirrors CreditBalanceIdempotent for UnlockBalance
// (see its doc comment for the guard semantics).
func (r *LedgerRepo) UnlockBalanceIdempotent(ctx context.Context, userID, asset, amountRaw, idempotencyKey string) error {
	normalized, _, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fingerprint := fmt.Sprintf("unlock:%s:%s:%s", userID, asset, amountRaw)
	found, err := checkIdempotency(ctx, tx, "/internal/balance/unlock", idempotencyKey, fingerprint)
	if err != nil {
		return err
	}
	if found {
		return tx.Commit(ctx)
	}
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE user_balances SET `+lockedColumn+` = GREATEST(0, `+lockedColumn+` - $2::numeric), updated_at = now()
		 WHERE user_id = $1`,
		userID, amountRaw); err != nil {
		return err
	}
	if err := recordIdempotency(ctx, tx, "/internal/balance/unlock", idempotencyKey, fingerprint); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReplaceLocksFor atomically replaces the trading holds for a dedicated
// market-maker wallet. targets are raw integer amounts keyed by asset. It is
// deliberately an absolute replacement rather than unlock-then-lock, so a
// quote refresh cannot temporarily expose an unfunded or over-locked state.
func (r *LedgerRepo) ReplaceLocksFor(ctx context.Context, userID string, targets map[string]string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	for asset, amountRaw := range targets {
		normalized, column, err := normalizeAsset(asset)
		if err != nil {
			return err
		}
		if err := validateNonNegativeAmount(amountRaw); err != nil {
			return err
		}
		pending, err := r.pendingWithdrawalHoldTx(ctx, tx, userID, normalized)
		if err != nil {
			return err
		}
		lockedColumn := lockedColumns[normalized]
		ct, err := tx.Exec(ctx, `UPDATE user_balances SET `+lockedColumn+` = $2::numeric, updated_at = now()
			WHERE user_id = $1 AND `+column+` - $2::numeric - $3::numeric >= 0`, userID, amountRaw, pending.String())
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return fmt.Errorf("insufficient %s balance to lock", normalized)
		}
	}
	return tx.Commit(ctx)
}

// ReleaseLocksFor clears all trading locks for one internal desk wallet and
// asset after the matching engine has restarted and discarded its in-memory
// orders. It preserves the wallet's balance.
func (r *LedgerRepo) ReleaseLocksFor(ctx context.Context, userID, asset string) error {
	normalized, _, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE user_balances SET `+lockedColumn+` = 0, updated_at = now() WHERE user_id = $1`, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ResetBalanceFor clears a desk wallet's balance and trading locks for one
// asset. It is used only when deleting/reclaiming an internal desk wallet.
func (r *LedgerRepo) ResetBalanceFor(ctx context.Context, userID, asset string) error {
	normalized, column, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE user_balances SET `+column+` = 0, `+lockedColumn+` = 0, updated_at = now() WHERE user_id = $1`, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SyncBalanceFor makes an internal desk wallet's durable balance match its
// authoritative desk allocation and clears trading locks. It is used only
// during matching-engine restart recovery, after the engine has discarded all
// in-memory orders and positions for that desk.
func (r *LedgerRepo) SyncBalanceFor(ctx context.Context, userID, asset, amountRaw string) error {
	normalized, column, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	if err := validateNonNegativeAmount(amountRaw); err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE user_balances SET `+column+` = $2::numeric, `+lockedColumn+` = 0, updated_at = now() WHERE user_id = $1`, userID, amountRaw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SettleLockedDebit converts a previously locked hold into a real debit, e.g. when an
// order fills: both balance and locked amount are reduced together in one transaction.
func (r *LedgerRepo) SettleLockedDebit(ctx context.Context, userID, asset, amountRaw string) error {
	normalized, column, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	commandTag, err := tx.Exec(ctx,
		`UPDATE user_balances
		 SET `+column+` = `+column+` - $2::numeric,
		     `+lockedColumn+` = GREATEST(0, `+lockedColumn+` - $2::numeric),
		     updated_at = now()
		 WHERE user_id = $1 AND `+column+` >= $2::numeric`,
		userID, amountRaw)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("insufficient %s balance to settle", normalized)
	}
	return tx.Commit(ctx)
}

// SettleLockedDebitIdempotent mirrors CreditBalanceIdempotent for
// SettleLockedDebit (see its doc comment for the guard semantics). Added
// alongside matching-engine's M3 durable-outbox fix: a Settle call that gets
// replayed by that outbox after an earlier attempt's response was lost must
// be safe to resend, same as Credit/SettleFee/Lock/Unlock already are.
func (r *LedgerRepo) SettleLockedDebitIdempotent(ctx context.Context, userID, asset, amountRaw, idempotencyKey string) error {
	normalized, column, err := normalizeAsset(asset)
	if err != nil {
		return err
	}
	lockedColumn := lockedColumns[normalized]
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fingerprint := fmt.Sprintf("settle:%s:%s:%s", userID, asset, amountRaw)
	found, err := checkIdempotency(ctx, tx, "/internal/balance/settle", idempotencyKey, fingerprint)
	if err != nil {
		return err
	}
	if found {
		return tx.Commit(ctx)
	}
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return err
	}
	commandTag, err := tx.Exec(ctx,
		`UPDATE user_balances
		 SET `+column+` = `+column+` - $2::numeric,
		     `+lockedColumn+` = GREATEST(0, `+lockedColumn+` - $2::numeric),
		     updated_at = now()
		 WHERE user_id = $1 AND `+column+` >= $2::numeric`,
		userID, amountRaw)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("insufficient %s balance to settle", normalized)
	}
	if err := recordIdempotency(ctx, tx, "/internal/balance/settle", idempotencyKey, fingerprint); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SettleSpotTrade completes both legs of a spot fill in one database
// transaction. The buyer's quote and seller's base were already reserved by
// the matching engine; this method consumes those reservations and credits the
// received assets atomically, so a timeout cannot leave a trade half-persisted.
func (r *LedgerRepo) SettleSpotTrade(ctx context.Context, buyerID, sellerID, base, quote, baseQtyRaw, buyerQuoteRaw, sellerQuoteRaw string) error {
	if buyerID == sellerID {
		return fmt.Errorf("spot trade buyer and seller must differ")
	}
	if err := validatePositiveAmount(baseQtyRaw); err != nil {
		return err
	}
	if err := validatePositiveAmount(buyerQuoteRaw); err != nil {
		return err
	}
	if err := validatePositiveAmount(sellerQuoteRaw); err != nil {
		return err
	}
	baseName, baseColumn, err := normalizeAsset(base)
	if err != nil {
		return err
	}
	quoteName, quoteColumn, err := normalizeAsset(quote)
	if err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalances(ctx, tx, []string{buyerID, sellerID}); err != nil {
		return err
	}
	baseLocked := lockedColumns[baseName]
	quoteLocked := lockedColumns[quoteName]
	// Each side used to take two separate UPDATEs (debit-locked, then a
	// later credit) — four statements total for one trade. Base and quote
	// are always different columns for a spot pair, so each side's debit and
	// credit can be folded into a single UPDATE with two independent SET
	// clauses instead: same rows touched, same values written, same
	// insufficient-funds gating (the debit side's >= checks still gate the
	// whole statement, exactly as before), just two round trips instead of
	// four inside this transaction.
	buyerTag, err := tx.Exec(ctx,
		`UPDATE user_balances SET `+quoteColumn+`=`+quoteColumn+`-$2::numeric, `+quoteLocked+`=`+quoteLocked+`-$2::numeric, `+baseColumn+`=`+baseColumn+`+$3::numeric, updated_at=now()
		 WHERE user_id=$1 AND `+quoteColumn+` >= $2::numeric AND `+quoteLocked+` >= $2::numeric`,
		buyerID, buyerQuoteRaw, baseQtyRaw)
	if err != nil {
		return err
	}
	if buyerTag.RowsAffected() != 1 {
		return fmt.Errorf("insufficient locked %s for buyer", quoteName)
	}
	sellerTag, err := tx.Exec(ctx,
		`UPDATE user_balances SET `+baseColumn+`=`+baseColumn+`-$2::numeric, `+baseLocked+`=`+baseLocked+`-$2::numeric, `+quoteColumn+`=`+quoteColumn+`+$3::numeric, updated_at=now()
		 WHERE user_id=$1 AND `+baseColumn+` >= $2::numeric AND `+baseLocked+` >= $2::numeric`,
		sellerID, baseQtyRaw, sellerQuoteRaw)
	if err != nil {
		return err
	}
	if sellerTag.RowsAffected() != 1 {
		return fmt.Errorf("insufficient locked %s for seller", baseName)
	}
	return tx.Commit(ctx)
}

func (r *LedgerRepo) CreditBalance(ctx context.Context, userID, asset, amountRaw string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.creditBalanceTx(ctx, tx, userID, asset, amountRaw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CreditBalanceIdempotent is CreditBalance guarded by idempotencyKey: a
// retry with the same key and same (userID, asset, amountRaw) is a no-op
// that reports success without crediting again; a retry with the same key
// but different parameters fails with ErrIdempotencyKeyReused instead of
// guessing which request to honor. Pass idempotencyKey="" to skip the guard
// entirely (identical to CreditBalance).
func (r *LedgerRepo) CreditBalanceIdempotent(ctx context.Context, userID, asset, amountRaw, idempotencyKey string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fingerprint := fmt.Sprintf("credit:%s:%s:%s", userID, asset, amountRaw)
	found, err := checkIdempotency(ctx, tx, "/internal/balance/credit", idempotencyKey, fingerprint)
	if err != nil {
		return err
	}
	if found {
		return tx.Commit(ctx)
	}
	if err := r.creditBalanceTx(ctx, tx, userID, asset, amountRaw); err != nil {
		return err
	}
	if err := recordIdempotency(ctx, tx, "/internal/balance/credit", idempotencyKey, fingerprint); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *LedgerRepo) DebitBalance(ctx context.Context, userID, asset, amountRaw string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.debitBalanceTx(ctx, tx, userID, asset, amountRaw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DebitBalanceIdempotent mirrors CreditBalanceIdempotent for debits (see its
// doc comment); used for InternalCreditBalance's negative-amount branch,
// which debits rather than credits.
func (r *LedgerRepo) DebitBalanceIdempotent(ctx context.Context, userID, asset, amountRaw, idempotencyKey string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fingerprint := fmt.Sprintf("debit:%s:%s:%s", userID, asset, amountRaw)
	found, err := checkIdempotency(ctx, tx, "/internal/balance/credit", idempotencyKey, fingerprint)
	if err != nil {
		return err
	}
	if found {
		return tx.Commit(ctx)
	}
	if err := r.debitBalanceTx(ctx, tx, userID, asset, amountRaw); err != nil {
		return err
	}
	if err := recordIdempotency(ctx, tx, "/internal/balance/credit", idempotencyKey, fingerprint); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *LedgerRepo) TransferBalance(ctx context.Context, senderID, recipientID, asset, amountRaw string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if senderID == recipientID {
		return fmt.Errorf("sender and recipient must be different users")
	}
	ids := []string{senderID, recipientID}
	if strings.Compare(ids[0], ids[1]) > 0 {
		ids[0], ids[1] = ids[1], ids[0]
	}
	for _, id := range ids {
		if err := r.lockUser(ctx, tx, id); err != nil {
			return err
		}
	}
	if err := r.lockBalances(ctx, tx, ids); err != nil {
		return err
	}
	if err := r.debitBalanceTx(ctx, tx, senderID, asset, amountRaw); err != nil {
		return err
	}
	if err := r.creditBalanceTx(ctx, tx, recipientID, asset, amountRaw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SwapBalance moves sourceAmountRaw of sourceAsset out of userID's balance
// and credits destinationAmountRaw of destinationAsset in, atomically. When
// one side of the swap is BI2XUSD and the other is USDT/USDC, this also
// applies the swappable/reserve pool bookkeeping (see swappool.go) inside
// the SAME transaction, so a swap's user-balance movement and its
// platform-wide pool effect always commit or roll back together:
//   - USDT/USDC -> BI2XUSD: splits sourceAmountRaw 60/40 into that asset's
//     swappable/reserve pool (CreditSwapPoolSplit).
//   - BI2XUSD -> USDT/USDC: caps the payout against that asset's swappable
//     pool (DebitSwapPoolCapped, using destinationAmountRaw — the NET
//     amount the user actually receives, matching what the pool needs to
//     cover). If the pool can't cover it, this returns ErrSwapPoolInsufficient
//     and the entire transaction rolls back, including the user-side
//     debit/credit above — the caller never sees a partially-applied swap.
func (r *LedgerRepo) SwapBalance(ctx context.Context, userID, sourceAsset, sourceAmountRaw, destinationAsset, destinationAmountRaw string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.debitBalanceTx(ctx, tx, userID, sourceAsset, sourceAmountRaw); err != nil {
		return err
	}
	if err := r.creditBalanceTx(ctx, tx, userID, destinationAsset, destinationAmountRaw); err != nil {
		return err
	}
	if destinationAsset == "BI2XUSD" && swapPoolAssets[sourceAsset] {
		if err := r.CreditSwapPoolSplit(ctx, tx, sourceAsset, sourceAmountRaw, userID); err != nil {
			return err
		}
	} else if sourceAsset == "BI2XUSD" && swapPoolAssets[destinationAsset] {
		if err := r.DebitSwapPoolCapped(ctx, tx, destinationAsset, destinationAmountRaw, userID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// InsertDeposit records a confirmed on-chain deposit and credits the user's
// tradable balance in creditToken at 1:1. token (what actually arrived
// on-chain, e.g. "USDC") and creditToken (what gets credited, e.g. "BI2XUSD")
// can differ: every market's quote leg trades in BI2XUSD, the platform's
// internal stable unit pegged 1:1 to the real deposited asset, so a real
// stablecoin deposit converts into BI2XUSD at credit time while the ledger row
// still records the true on-chain asset for an honest audit trail. Pass the
// same value for both to credit the deposited asset directly (no conversion).
func (r *LedgerRepo) InsertDeposit(ctx context.Context, userID, walletAddress, token, creditToken, amountRaw, txHash string) error {
	normalizedToken, _, err := normalizeAsset(token)
	if err != nil {
		return err
	}
	normalizedCreditToken, _, err := normalizeAsset(creditToken)
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

	commandTag, err := tx.Exec(ctx,
		`INSERT INTO ledger_entries (user_id, wallet_address, kind, token, amount, tx_hash, status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (tx_hash) WHERE tx_hash IS NOT NULL DO NOTHING`,
		userID, strings.ToLower(walletAddress), models.LedgerKindDeposit, normalizedToken, amountRaw, txHash, models.LedgerStatusConfirmed,
	)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() > 0 {
		if err := r.creditBalanceTx(ctx, tx, userID, normalizedCreditToken, amountRaw); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// InsertWithdrawalRequest records a withdrawal request and reserves the amount by
// counting pending/processing requests against available balance.
func (r *LedgerRepo) InsertWithdrawalRequest(ctx context.Context, userID, walletAddress, token, amountRaw string) (string, error) {
	normalizedToken, column, err := normalizeAsset(token)
	if err != nil {
		return "", err
	}
	lockedColumn := lockedColumns[normalizedToken]
	if err := validatePositiveAmount(amountRaw); err != nil {
		return "", err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return "", err
	}
	pendingHold, err := r.pendingWithdrawalHoldTx(ctx, tx, userID, normalizedToken)
	if err != nil {
		return "", err
	}

	var balanceRaw, lockedRaw string
	if err := tx.QueryRow(ctx, `SELECT `+column+`::text, `+lockedColumn+`::text FROM user_balances WHERE user_id = $1`, userID).Scan(&balanceRaw, &lockedRaw); err != nil {
		return "", err
	}
	balance, ok := new(big.Int).SetString(balanceRaw, 10)
	if !ok {
		return "", fmt.Errorf("invalid balance value %q", balanceRaw)
	}
	locked, ok := new(big.Int).SetString(lockedRaw, 10)
	if !ok {
		return "", fmt.Errorf("invalid locked value %q", lockedRaw)
	}
	amount, ok := new(big.Int).SetString(amountRaw, 10)
	if !ok {
		return "", fmt.Errorf("invalid amount value %q", amountRaw)
	}
	available := new(big.Int).Sub(balance, locked)
	available.Sub(available, pendingHold)
	if available.Cmp(amount) < 0 {
		return "", fmt.Errorf("insufficient %s available balance", normalizedToken)
	}

	var id string
	if err := tx.QueryRow(ctx,
		`INSERT INTO ledger_entries (user_id, wallet_address, kind, token, amount, status)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		userID, strings.ToLower(walletAddress), models.LedgerKindWithdrawalRequest, normalizedToken, amountRaw, models.LedgerStatusPending,
	).Scan(&id); err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// MarkWithdrawalProcessing atomically claims a withdrawal request for
// payout, from either "pending" (the normal first attempt) or "processing"
// (a retry — see AdminRecoverWithdrawal and the withdrawal watchdog). Retry
// was previously impossible: this only accepted "pending", so a request
// already stuck in "processing" (the exact state /admin/withdraw-recover's
// "retry" action and the watchdog exist to fix) always failed at this very
// first step with "withdrawal request is not pending" — the manual recovery
// endpoint's own documented purpose ("recovers withdrawals stuck in
// processing") could never actually succeed. A terminal state (confirmed/
// failed) is still correctly rejected — this only widens which non-terminal
// state counts as claimable.
func (r *LedgerRepo) MarkWithdrawalProcessing(ctx context.Context, requestID string) (*models.LedgerEntry, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var e models.LedgerEntry
	err = tx.QueryRow(ctx,
		`UPDATE ledger_entries
		 SET status = $2
		 WHERE id = $1 AND kind = $3 AND status IN ($4, $2)
		 RETURNING id, user_id, wallet_address, kind, token, amount::text, tx_hash, status, created_at`,
		requestID, models.LedgerStatusProcessing, models.LedgerKindWithdrawalRequest, models.LedgerStatusPending,
	).Scan(&e.ID, &e.UserID, &e.WalletAddress, &e.Kind, &e.Token, &e.Amount, &e.TxHash, &e.Status, &e.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("withdrawal request is not pending or processing")
	}
	if err != nil {
		return nil, err
	}
	if err := r.lockBalance(ctx, tx, e.UserID); err != nil {
		return nil, err
	}
	return &e, tx.Commit(ctx)
}

// StuckProcessingWithdrawals returns withdrawal_request rows that have sat in
// "processing" since before olderThan — candidates for the watchdog to
// retry. A normal withdrawal completes in seconds (submit + wait-for-receipt
// + a couple of fast DB writes), so a row still processing minutes later
// means the server crashed or restarted mid-flight, or a downstream call
// (the chain RPC, the engine ledger sync) hung — exactly the gap that used
// to require an admin to notice and call /admin/withdraw-recover by hand.
func (r *LedgerRepo) StuckProcessingWithdrawals(ctx context.Context, olderThan time.Time) ([]models.LedgerEntry, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, user_id, wallet_address, kind, token, amount::text, tx_hash, status, created_at
		 FROM ledger_entries
		 WHERE kind = $1 AND status = $2 AND created_at < $3
		 ORDER BY created_at ASC`,
		models.LedgerKindWithdrawalRequest, models.LedgerStatusProcessing, olderThan,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.LedgerEntry
	for rows.Next() {
		var e models.LedgerEntry
		if err := rows.Scan(&e.ID, &e.UserID, &e.WalletAddress, &e.Kind, &e.Token, &e.Amount, &e.TxHash, &e.Status, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkWithdrawalConfirmed stores the successful payout hash on the original request row
// and debits the user's balance. One completed withdrawal remains one ledger row.
func (r *LedgerRepo) MarkWithdrawalConfirmed(ctx context.Context, requestID, txHash string) (*models.LedgerEntry, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var e models.LedgerEntry
	err = tx.QueryRow(ctx,
		`SELECT id, user_id, wallet_address, kind, token, amount::text, tx_hash, status, created_at
		 FROM ledger_entries
		 WHERE id = $1 AND kind = $2 FOR UPDATE`,
		requestID, models.LedgerKindWithdrawalRequest,
	).Scan(&e.ID, &e.UserID, &e.WalletAddress, &e.Kind, &e.Token, &e.Amount, &e.TxHash, &e.Status, &e.CreatedAt)
	if err != nil {
		return nil, err
	}
	if e.Status != models.LedgerStatusProcessing {
		return nil, fmt.Errorf("withdrawal request is not processing")
	}

	if err := r.debitBalanceTx(ctx, tx, e.UserID, e.Token, e.Amount); err != nil {
		return nil, err
	}
	if err := tx.QueryRow(ctx,
		`UPDATE ledger_entries
		 SET status = $2, tx_hash = $3
		 WHERE id = $1
		 RETURNING id, user_id, wallet_address, kind, token, amount::text, tx_hash, status, created_at`,
		requestID, models.LedgerStatusConfirmed, txHash,
	).Scan(&e.ID, &e.UserID, &e.WalletAddress, &e.Kind, &e.Token, &e.Amount, &e.TxHash, &e.Status, &e.CreatedAt); err != nil {
		return nil, err
	}
	return &e, tx.Commit(ctx)
}
func (r *LedgerRepo) MarkWithdrawalFailed(ctx context.Context, requestID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE ledger_entries SET status = $2 WHERE id = $1 AND kind = $3 AND status = $4`,
		requestID, models.LedgerStatusFailed, models.LedgerKindWithdrawalRequest, models.LedgerStatusProcessing,
	)
	return err
}

// PendingNonce is a withdrawal request's last-submitted, not-yet-confirmed
// on-chain transaction parameters — nil fields mean no transaction has been
// submitted for this request yet (or it already confirmed/failed and was
// cleared). Used to replace a stuck transaction at the SAME nonce with a
// higher fee, instead of re-deriving a fresh nonce that would skip over it
// (H4).
type PendingNonce struct {
	Nonce     uint64
	FeeCapWei string
	TipCapWei string
}

// SavePendingNonce records the nonce/fee actually used for requestID's
// on-chain submission, before waiting for it to be mined — so a crash or
// timeout during that wait still leaves a durable record of exactly what
// was sent, for ReplaceStuckTx to build on.
func (r *LedgerRepo) SavePendingNonce(ctx context.Context, requestID string, nonce uint64, feeCapWei, tipCapWei string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE ledger_entries SET pending_nonce = $2, pending_fee_cap_wei = $3::numeric, pending_tip_cap_wei = $4::numeric WHERE id = $1`,
		requestID, nonce, feeCapWei, tipCapWei,
	)
	return err
}

// PendingNonceFor returns requestID's last-saved pending nonce/fee, or nil if
// none is recorded.
func (r *LedgerRepo) PendingNonceFor(ctx context.Context, requestID string) (*PendingNonce, error) {
	var nonce *int64
	var feeCap, tipCap *string
	err := r.pool.QueryRow(ctx,
		`SELECT pending_nonce, pending_fee_cap_wei::text, pending_tip_cap_wei::text FROM ledger_entries WHERE id = $1`,
		requestID,
	).Scan(&nonce, &feeCap, &tipCap)
	if err != nil {
		return nil, err
	}
	if nonce == nil || feeCap == nil || tipCap == nil {
		return nil, nil
	}
	return &PendingNonce{Nonce: uint64(*nonce), FeeCapWei: *feeCap, TipCapWei: *tipCap}, nil
}

// ClearPendingNonce removes requestID's saved nonce/fee once its transaction
// has reached a terminal state (confirmed or permanently failed).
func (r *LedgerRepo) ClearPendingNonce(ctx context.Context, requestID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE ledger_entries SET pending_nonce = NULL, pending_fee_cap_wei = NULL, pending_tip_cap_wei = NULL WHERE id = $1`,
		requestID,
	)
	return err
}

func (r *LedgerRepo) RejectWithdrawalRequest(ctx context.Context, requestID string) error {
	commandTag, err := r.pool.Exec(ctx,
		`UPDATE ledger_entries SET status = $2 WHERE id = $1 AND kind = $3 AND status = $4`,
		requestID, models.LedgerStatusRejected, models.LedgerKindWithdrawalRequest, models.LedgerStatusPending,
	)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("withdrawal request is not pending")
	}
	return nil
}

// BalanceFor returns the current balance for userID/token.
func (r *LedgerRepo) BalanceFor(ctx context.Context, userID, token string) (string, error) {
	_, column, err := normalizeAsset(token)
	if err != nil {
		return "0", err
	}
	var balance string
	err = r.pool.QueryRow(ctx, `SELECT `+column+`::text FROM user_balances WHERE user_id = $1`, userID).Scan(&balance)
	if err == pgx.ErrNoRows {
		return "0", nil
	}
	if err != nil {
		return "0", err
	}
	return balance, nil
}

// zeroBalanceMap is the default all-assets-zero map, shared by BalancesFor,
// LockedBalancesFor, and PendingWithdrawalHoldsFor so their zero-value
// results always list the same complete asset set as assetColumns.
func zeroBalanceMap() map[string]string {
	return map[string]string{"BTC": "0", "BI2X": "0", "BI2XUSD": "0", "USDC": "0", "USDT": "0"}
}

func (r *LedgerRepo) BalancesFor(ctx context.Context, userID string) (map[string]string, error) {
	balances := map[string]string{}
	var btc, bi2x, biusd, usdc, usdt string
	err := r.pool.QueryRow(ctx, `
		SELECT "BTC"::text, "BI2X"::text, "BI2XUSD"::text, "USDC"::text, "USDT"::text
		FROM user_balances
		WHERE user_id = $1`, userID).Scan(&btc, &bi2x, &biusd, &usdc, &usdt)
	if err == pgx.ErrNoRows {
		return zeroBalanceMap(), nil
	}
	if err != nil {
		return nil, err
	}
	balances["BTC"] = btc
	balances["BI2X"] = bi2x
	balances["BI2XUSD"] = biusd
	balances["USDC"] = usdc
	balances["USDT"] = usdt
	return balances, nil
}

// LockedBalancesFor returns the currently locked (held/frozen) amount per asset for userID.
func (r *LedgerRepo) LockedBalancesFor(ctx context.Context, userID string) (map[string]string, error) {
	locked := map[string]string{}
	var btc, bi2x, biusd, usdc, usdt string
	err := r.pool.QueryRow(ctx, `
		SELECT "BTC_locked"::text, "BI2X_locked"::text, "BI2XUSD_locked"::text, "USDC_locked"::text, "USDT_locked"::text
		FROM user_balances
		WHERE user_id = $1`, userID).Scan(&btc, &bi2x, &biusd, &usdc, &usdt)
	if err == pgx.ErrNoRows {
		return zeroBalanceMap(), nil
	}
	if err != nil {
		return nil, err
	}
	locked["BTC"] = btc
	locked["BI2X"] = bi2x
	locked["BI2XUSD"] = biusd
	locked["USDC"] = usdc
	locked["USDT"] = usdt
	return locked, nil
}

// PendingWithdrawalHoldsFor returns pending/processing withdrawal holds per asset for userID.
func (r *LedgerRepo) PendingWithdrawalHoldsFor(ctx context.Context, userID string) (map[string]string, error) {
	holds := zeroBalanceMap()
	rows, err := r.pool.Query(ctx, `
		SELECT token, COALESCE(SUM(amount), 0)::text
		FROM ledger_entries
		WHERE user_id = $1
		  AND kind = $2
		  AND status IN ($3, $4)
		GROUP BY token`,
		userID, models.LedgerKindWithdrawalRequest, models.LedgerStatusPending, models.LedgerStatusProcessing,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var token, amount string
		if err := rows.Scan(&token, &amount); err != nil {
			return nil, err
		}
		switch token {
		case "OUR_TOKEN", "BI":
			// BI (the platform's native token, formerly "OUR_TOKEN") was
			// removed 2026-09-13 — never wired into any market. Any
			// leftover withdrawal-request rows under either legacy name
			// are skipped rather than surfaced under a key holds no
			// longer tracks.
		default:
			holds[token] = amount
		}
	}
	return holds, rows.Err()
}

// AvailableBalanceFor returns balance minus trading locks and pending withdrawal holds.
func (r *LedgerRepo) AvailableBalanceFor(ctx context.Context, userID, token string) (string, error) {
	normalized, column, err := normalizeAsset(token)
	if err != nil {
		return "0", err
	}
	lockedColumn := lockedColumns[normalized]
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "0", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockBalance(ctx, tx, userID); err != nil {
		return "0", err
	}
	pendingHold, err := r.pendingWithdrawalHoldTx(ctx, tx, userID, normalized)
	if err != nil {
		return "0", err
	}
	var balanceRaw, lockedRaw string
	if err := tx.QueryRow(ctx, `SELECT `+column+`::text, `+lockedColumn+`::text FROM user_balances WHERE user_id = $1`, userID).Scan(&balanceRaw, &lockedRaw); err != nil {
		return "0", err
	}
	balance, ok := new(big.Int).SetString(balanceRaw, 10)
	if !ok {
		return "0", fmt.Errorf("invalid balance value %q", balanceRaw)
	}
	locked, ok := new(big.Int).SetString(lockedRaw, 10)
	if !ok {
		return "0", fmt.Errorf("invalid locked value %q", lockedRaw)
	}
	available := new(big.Int).Sub(balance, locked)
	available.Sub(available, pendingHold)
	if available.Sign() < 0 {
		available.SetInt64(0)
	}
	return available.String(), tx.Commit(ctx)
}

// NonzeroBalance is one user's nonzero balance for one asset.
type NonzeroBalance struct {
	UserID string
	Asset  string
	Amount string
}

// AllNonzeroBalances returns every (user, asset) pair with a positive
// *available* balance (total minus whatever is locked behind still-open
// orders), for one-time backfill of the matching-engine's in-memory ledger.
//
// The engine's ledger represents spendable capital — it's what Reserve/Lock
// draws down for new orders — so backfilling it with the raw total column
// double-counts any amount already locked behind an order that survived the
// restart (a resting limit order, an MM quote, an armed stop). When that
// order later fills, settlement's debit fails against the real (smaller)
// locked amount in Postgres with "insufficient locked <asset> for buyer/
// seller", which the engine treats as a serious integrity fault and halts
// the whole symbol. Subtracting *_locked here keeps the backfilled figure
// consistent with what LockBalance/reconcileOrderBalance already treat as
// "available" everywhere else.
func (r *LedgerRepo) AllNonzeroBalances(ctx context.Context) ([]NonzeroBalance, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT user_id, 'USDC', GREATEST("USDC" - "USDC_locked", 0)::text FROM user_balances WHERE "USDC" - "USDC_locked" > 0
		UNION ALL
		SELECT user_id, 'BTC', GREATEST("BTC" - "BTC_locked", 0)::text FROM user_balances WHERE "BTC" - "BTC_locked" > 0
		UNION ALL
		SELECT user_id, 'BI2X', GREATEST("BI2X" - "BI2X_locked", 0)::text FROM user_balances WHERE "BI2X" - "BI2X_locked" > 0
		UNION ALL
		SELECT user_id, 'USDT', GREATEST("USDT" - "USDT_locked", 0)::text FROM user_balances WHERE "USDT" - "USDT_locked" > 0
		UNION ALL
		SELECT user_id, 'BI2XUSD', GREATEST("BI2XUSD" - "BI2XUSD_locked", 0)::text FROM user_balances WHERE "BI2XUSD" - "BI2XUSD_locked" > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []NonzeroBalance
	for rows.Next() {
		var b NonzeroBalance
		if err := rows.Scan(&b.UserID, &b.Asset, &b.Amount); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// RecordBackfillFailure durably records that a backfill credit for
// (userID, asset) did not land in the engine even after runBackfill's own
// in-run retries, so a later backfill run can retry exactly this pair
// without re-crediting everything else that already succeeded. Upserts on
// (user_id, asset): a repeated failure for the same pair bumps attempts and
// refreshes amount/last_error/last_seen_at rather than accumulating rows.
func (r *LedgerRepo) RecordBackfillFailure(ctx context.Context, userID, asset, amount, lastErr string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO engine_backfill_failures (user_id, asset, amount, last_error, attempts, first_seen_at, last_seen_at)
		VALUES ($1, $2, $3, $4, 1, now(), now())
		ON CONFLICT (user_id, asset) DO UPDATE SET
			amount = EXCLUDED.amount,
			last_error = EXCLUDED.last_error,
			attempts = engine_backfill_failures.attempts + 1,
			last_seen_at = now()`,
		userID, asset, amount, lastErr)
	return err
}

// ClearBackfillFailure removes the durable failure record for (userID,
// asset) after a retry finally succeeds — called from runBackfill once a
// previously-failed pair credits successfully, so PendingBackfillFailures
// doesn't keep reporting a pair that has since been fixed.
func (r *LedgerRepo) ClearBackfillFailure(ctx context.Context, userID, asset string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM engine_backfill_failures WHERE user_id = $1 AND asset = $2`, userID, asset)
	return err
}

// PendingBackfillFailures returns every (user_id, asset) pair still marked
// as failed from a previous backfill run — the accounts a fresh backfill
// run should prioritize/retry, since AllNonzeroBalances alone can't
// distinguish "already synced" from "failed last time" (both look like a
// nonzero Postgres balance).
func (r *LedgerRepo) PendingBackfillFailures(ctx context.Context) ([]NonzeroBalance, error) {
	rows, err := r.pool.Query(ctx, `SELECT user_id, asset, amount FROM engine_backfill_failures`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NonzeroBalance
	for rows.Next() {
		var b NonzeroBalance
		if err := rows.Scan(&b.UserID, &b.Asset, &b.Amount); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// PendingWithdrawalRequest returns the most recent pending withdrawal request for userID, if any.
func (r *LedgerRepo) PendingWithdrawalRequest(ctx context.Context, userID string) (*models.LedgerEntry, error) {
	var e models.LedgerEntry
	err := r.pool.QueryRow(ctx,
		`SELECT id, user_id, wallet_address, kind, token, amount::text, tx_hash, status, created_at
		 FROM ledger_entries
		 WHERE user_id = $1 AND kind = $2 AND status = $3
		 ORDER BY created_at DESC LIMIT 1`,
		userID, models.LedgerKindWithdrawalRequest, models.LedgerStatusPending,
	).Scan(&e.ID, &e.UserID, &e.WalletAddress, &e.Kind, &e.Token, &e.Amount, &e.TxHash, &e.Status, &e.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}
