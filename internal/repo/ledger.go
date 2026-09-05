package repo

import (
	"context"
	"fmt"
	"math/big"
	"strings"

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
	"BTC":       `"BTC"`,
	"USDC":      `"USDC"`,
	"USDT":      `"USDT"`,
	"BUSD":      `"BUSD"`,
	"OUR_TOKEN": `"OUR_Token"`,
	// USDB is the platform's internal stable quote currency, pegged 1:1 to
	// USDT — it has no on-chain contract of its own. Every market's quote
	// leg trades in USDB, not USDT; a real USDT/USDC deposit is credited as
	// USDB at 1:1 (see chain.Listener.handleDeposit). USDT/USDC columns are
	// kept only as the deposit-intake ledger, not as tradable balances.
	"USDB": `"USDB"`,
	// ETH, SOL, and BNB back the ETH-USDB / SOL-USDB / BNB-USDB spot markets
	// (matching-engine's currentMarkets) — base-asset balance columns, same
	// shape as BTC.
	"ETH": `"ETH"`,
	"SOL": `"SOL"`,
	"BNB": `"BNB"`,
}

var lockedColumns = map[string]string{
	"BTC":       `"BTC_locked"`,
	"USDC":      `"USDC_locked"`,
	"USDT":      `"USDT_locked"`,
	"BUSD":      `"BUSD_locked"`,
	"OUR_TOKEN": `"OUR_Token_locked"`,
	"USDB":      `"USDB_locked"`,
	"ETH":       `"ETH_locked"`,
	"SOL":       `"SOL_locked"`,
	"BNB":       `"BNB_locked"`,
}

func normalizeAsset(asset string) (string, string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(asset))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	if normalized == "OURTOKEN" {
		normalized = "OUR_TOKEN"
	}
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

func (r *LedgerRepo) lockBalance(ctx context.Context, tx pgx.Tx, userID string) error {
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

func (r *LedgerRepo) lockBalances(ctx context.Context, tx pgx.Tx, userIDs []string) error {
	for _, userID := range userIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO user_balances (user_id)
			VALUES ($1)
			ON CONFLICT (user_id) DO NOTHING`, userID); err != nil {
			return err
		}
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
	pendingHold, err := r.pendingWithdrawalHoldTx(ctx, tx, userID, normalized)
	if err != nil {
		return err
	}
	commandTag, err := tx.Exec(ctx,
		`UPDATE user_balances SET `+lockedColumn+` = `+lockedColumn+` + $2::numeric, updated_at = now()
		 WHERE user_id = $1 AND `+column+` - `+lockedColumn+` - $3::numeric >= $2::numeric`,
		userID, amountRaw, pendingHold.String())
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("insufficient %s balance to lock", normalized)
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
	buyerTag, err := tx.Exec(ctx, `UPDATE user_balances SET `+quoteColumn+`=`+quoteColumn+`-$2::numeric, `+quoteLocked+`=`+quoteLocked+`-$2::numeric, updated_at=now() WHERE user_id=$1 AND `+quoteColumn+` >= $2::numeric AND `+quoteLocked+` >= $2::numeric`, buyerID, buyerQuoteRaw)
	if err != nil {
		return err
	}
	if buyerTag.RowsAffected() != 1 {
		return fmt.Errorf("insufficient locked %s for buyer", quoteName)
	}
	sellerTag, err := tx.Exec(ctx, `UPDATE user_balances SET `+baseColumn+`=`+baseColumn+`-$2::numeric, `+baseLocked+`=`+baseLocked+`-$2::numeric, updated_at=now() WHERE user_id=$1 AND `+baseColumn+` >= $2::numeric AND `+baseLocked+` >= $2::numeric`, sellerID, baseQtyRaw)
	if err != nil {
		return err
	}
	if sellerTag.RowsAffected() != 1 {
		return fmt.Errorf("insufficient locked %s for seller", baseName)
	}
	if _, err := tx.Exec(ctx, `UPDATE user_balances SET `+baseColumn+`=`+baseColumn+`+$2::numeric, updated_at=now() WHERE user_id=$1`, buyerID, baseQtyRaw); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE user_balances SET `+quoteColumn+`=`+quoteColumn+`+$2::numeric, updated_at=now() WHERE user_id=$1`, sellerID, sellerQuoteRaw); err != nil {
		return err
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
	return tx.Commit(ctx)
}

// InsertDeposit records a confirmed on-chain deposit and credits the user's
// tradable balance in creditToken at 1:1. token (what actually arrived
// on-chain, e.g. "USDC") and creditToken (what gets credited, e.g. "USDB")
// can differ: every market's quote leg trades in USDB, the platform's
// internal stable unit pegged 1:1 to the real deposited asset, so a real
// stablecoin deposit converts into USDB at credit time while the ledger row
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

// MarkWithdrawalProcessing atomically claims a pending withdrawal request for payout.
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
		 WHERE id = $1 AND kind = $3 AND status = $4
		 RETURNING id, user_id, wallet_address, kind, token, amount::text, tx_hash, status, created_at`,
		requestID, models.LedgerStatusProcessing, models.LedgerKindWithdrawalRequest, models.LedgerStatusPending,
	).Scan(&e.ID, &e.UserID, &e.WalletAddress, &e.Kind, &e.Token, &e.Amount, &e.TxHash, &e.Status, &e.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("withdrawal request is not pending")
	}
	if err != nil {
		return nil, err
	}
	if err := r.lockBalance(ctx, tx, e.UserID); err != nil {
		return nil, err
	}
	return &e, tx.Commit(ctx)
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
	return map[string]string{"BTC": "0", "ETH": "0", "SOL": "0", "BNB": "0", "USDB": "0", "USDC": "0", "USDT": "0", "BUSD": "0", "OUR_Token": "0"}
}

func (r *LedgerRepo) BalancesFor(ctx context.Context, userID string) (map[string]string, error) {
	balances := map[string]string{}
	var btc, eth, sol, bnb, usdb, usdc, usdt, busd, ourToken string
	err := r.pool.QueryRow(ctx, `
		SELECT "BTC"::text, "ETH"::text, "SOL"::text, "BNB"::text, "USDB"::text, "USDC"::text, "USDT"::text, "BUSD"::text, "OUR_Token"::text
		FROM user_balances
		WHERE user_id = $1`, userID).Scan(&btc, &eth, &sol, &bnb, &usdb, &usdc, &usdt, &busd, &ourToken)
	if err == pgx.ErrNoRows {
		return zeroBalanceMap(), nil
	}
	if err != nil {
		return nil, err
	}
	balances["BTC"] = btc
	balances["ETH"] = eth
	balances["SOL"] = sol
	balances["BNB"] = bnb
	balances["USDB"] = usdb
	balances["USDC"] = usdc
	balances["USDT"] = usdt
	balances["BUSD"] = busd
	balances["OUR_Token"] = ourToken
	return balances, nil
}

// LockedBalancesFor returns the currently locked (held/frozen) amount per asset for userID.
func (r *LedgerRepo) LockedBalancesFor(ctx context.Context, userID string) (map[string]string, error) {
	locked := map[string]string{}
	var btc, eth, sol, bnb, usdb, usdc, usdt, busd, ourToken string
	err := r.pool.QueryRow(ctx, `
		SELECT "BTC_locked"::text, "ETH_locked"::text, "SOL_locked"::text, "BNB_locked"::text, "USDB_locked"::text, "USDC_locked"::text, "USDT_locked"::text, "BUSD_locked"::text, "OUR_Token_locked"::text
		FROM user_balances
		WHERE user_id = $1`, userID).Scan(&btc, &eth, &sol, &bnb, &usdb, &usdc, &usdt, &busd, &ourToken)
	if err == pgx.ErrNoRows {
		return zeroBalanceMap(), nil
	}
	if err != nil {
		return nil, err
	}
	locked["BTC"] = btc
	locked["ETH"] = eth
	locked["SOL"] = sol
	locked["BNB"] = bnb
	locked["USDB"] = usdb
	locked["USDC"] = usdc
	locked["USDT"] = usdt
	locked["BUSD"] = busd
	locked["OUR_Token"] = ourToken
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
		case "OUR_TOKEN":
			holds["OUR_Token"] = amount
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
		SELECT user_id, 'ETH', GREATEST("ETH" - "ETH_locked", 0)::text FROM user_balances WHERE "ETH" - "ETH_locked" > 0
		UNION ALL
		SELECT user_id, 'SOL', GREATEST("SOL" - "SOL_locked", 0)::text FROM user_balances WHERE "SOL" - "SOL_locked" > 0
		UNION ALL
		SELECT user_id, 'BNB', GREATEST("BNB" - "BNB_locked", 0)::text FROM user_balances WHERE "BNB" - "BNB_locked" > 0
		UNION ALL
		SELECT user_id, 'USDT', GREATEST("USDT" - "USDT_locked", 0)::text FROM user_balances WHERE "USDT" - "USDT_locked" > 0
		UNION ALL
		SELECT user_id, 'BUSD', GREATEST("BUSD" - "BUSD_locked", 0)::text FROM user_balances WHERE "BUSD" - "BUSD_locked" > 0
		UNION ALL
		SELECT user_id, 'USDB', GREATEST("USDB" - "USDB_locked", 0)::text FROM user_balances WHERE "USDB" - "USDB_locked" > 0
		UNION ALL
		SELECT user_id, 'OUR_Token', GREATEST("OUR_Token" - "OUR_Token_locked", 0)::text FROM user_balances WHERE "OUR_Token" - "OUR_Token_locked" > 0`)
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
