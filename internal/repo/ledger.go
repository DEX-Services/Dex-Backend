// Package repo holds Postgres access for the deposit/withdrawal ledger.
package repo

import (
	"context"
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

// InsertDeposit records a confirmed on-chain deposit. Idempotent on txHash: replaying the
// same event (e.g. after a listener restart) is a no-op.
func (r *LedgerRepo) InsertDeposit(ctx context.Context, userID, walletAddress, token, amountRaw, txHash string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ledger_entries (user_id, wallet_address, kind, token, amount, tx_hash, status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (tx_hash) WHERE tx_hash IS NOT NULL DO NOTHING`,
		userID, strings.ToLower(walletAddress), models.LedgerKindDeposit, token, amountRaw, txHash, models.LedgerStatusConfirmed,
	)
	return err
}

// InsertWithdrawalRequest records an off-chain withdrawal request pending admin approval.
func (r *LedgerRepo) InsertWithdrawalRequest(ctx context.Context, userID, walletAddress, token, amountRaw string) (string, error) {
	var id string
	err := r.pool.QueryRow(ctx,
		`INSERT INTO ledger_entries (user_id, wallet_address, kind, token, amount, status)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		userID, strings.ToLower(walletAddress), models.LedgerKindWithdrawalRequest, token, amountRaw, models.LedgerStatusPending,
	).Scan(&id)
	return id, err
}

// MarkWithdrawalApproved records the on-chain WithdrawalApproved audit event for userID/amount.
func (r *LedgerRepo) MarkWithdrawalApproved(ctx context.Context, userID, walletAddress, token, amountRaw, txHash string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ledger_entries (user_id, wallet_address, kind, token, amount, tx_hash, status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		userID, strings.ToLower(walletAddress), models.LedgerKindWithdrawalApproved, token, amountRaw, txHash, models.LedgerStatusConfirmed,
	)
	return err
}

// BalanceFor returns confirmed deposits minus approved withdrawals for userID/token, in raw
// token units (as a decimal string, since amounts may exceed int64).
func (r *LedgerRepo) BalanceFor(ctx context.Context, userID, token string) (string, error) {
	var balance string
	err := r.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(
			CASE
				WHEN kind = $2 AND status = $3 THEN amount
				WHEN kind = $4 AND status = $3 THEN -amount
				ELSE 0
			END
		), 0)::text
		 FROM ledger_entries WHERE user_id = $1 AND token = $5`,
		userID, models.LedgerKindDeposit, models.LedgerStatusConfirmed, models.LedgerKindWithdrawalApproved, token,
	).Scan(&balance)
	if err != nil && err != pgx.ErrNoRows {
		return "0", err
	}
	return balance, nil
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
