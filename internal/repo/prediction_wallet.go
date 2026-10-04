package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrPredictionIdempotencyKey mirrors ErrP2PIdempotencyKey/
// ErrStakingIdempotencyKey for the prediction wallet's own idempotency
// guard — see FundPredictionWalletAsset's doc comment.
var ErrPredictionIdempotencyKey = errors.New("idempotency key was already used for another request")

// PredictionWalletRepo backs the prediction-market wallet (Phase 4 of
// ~/.claude/plans/wallet-separation.md) — a structural copy of
// StakingRepo's own wallet functions (Phase 3), widened to the full
// lock/unlock/debit/credit/fee vocabulary prediction-service's order flow
// actually needs (see ensurePredictionWalletTables' doc comment for why
// this differs from staking's narrower stake/redeem shape). BI2XUSD-only,
// single asset, same as staking.
type PredictionWalletRepo struct {
	pool   *pgxpool.Pool
	ledger *LedgerRepo
}

func NewPredictionWalletRepo(pool *pgxpool.Pool) *PredictionWalletRepo {
	return &PredictionWalletRepo{pool: pool, ledger: NewLedgerRepo(pool)}
}

// PredictionWalletBalance is userID's prediction wallet snapshot —
// structural copy of StakingWalletBalance.
type PredictionWalletBalance struct {
	AvailableRaw string `json:"availableRaw"`
	ReservedRaw  string `json:"reservedRaw"`
	TotalRaw     string `json:"totalRaw"`
}

func (r *PredictionWalletRepo) lockWallet(ctx context.Context, tx pgx.Tx, userID string) error {
	if _, err := tx.Exec(ctx, `INSERT INTO prediction_wallet_balances(user_id) VALUES($1) ON CONFLICT(user_id) DO NOTHING`, userID); err != nil {
		return err
	}
	var one int
	return tx.QueryRow(ctx, `SELECT 1 FROM prediction_wallet_balances WHERE user_id=$1 FOR UPDATE`, userID).Scan(&one)
}

func scanPredictionWallet(row pgx.Row) (*PredictionWalletBalance, error) {
	var b PredictionWalletBalance
	err := row.Scan(&b.AvailableRaw, &b.ReservedRaw, &b.TotalRaw)
	return &b, err
}

func (r *PredictionWalletRepo) walletTx(ctx context.Context, tx pgx.Tx, userID string) (*PredictionWalletBalance, error) {
	return scanPredictionWallet(tx.QueryRow(ctx, `SELECT available_raw::text,reserved_raw::text,(available_raw+reserved_raw)::text FROM prediction_wallet_balances WHERE user_id=$1`, userID))
}

// WalletBalance returns userID's prediction wallet snapshot, zero-valued if
// they have never funded it.
func (r *PredictionWalletRepo) WalletBalance(ctx context.Context, userID string) (*PredictionWalletBalance, error) {
	b, err := scanPredictionWallet(r.pool.QueryRow(ctx, `SELECT available_raw::text,reserved_raw::text,(available_raw+reserved_raw)::text FROM prediction_wallet_balances WHERE user_id=$1`, userID))
	if err == pgx.ErrNoRows {
		return &PredictionWalletBalance{AvailableRaw: "0", ReservedRaw: "0", TotalRaw: "0"}, nil
	}
	return b, err
}

// AvailableBalance returns just the available_raw figure, matching the
// shape of matching-engine/prediction-service's existing AvailableBalance
// callers (a fast pre-order check) without needing the full snapshot.
func (r *PredictionWalletRepo) AvailableBalance(ctx context.Context, userID string) (string, error) {
	b, err := r.WalletBalance(ctx, userID)
	if err != nil {
		return "", err
	}
	return b.AvailableRaw, nil
}

// FundPredictionWalletAsset moves available main-wallet BI2XUSD into the
// prediction wallet atomically — structural copy of
// StakingRepo.FundStakingWalletAsset / P2PRepo.FundWalletAsset. moved=false
// means an identical idempotent request was already applied.
func (r *PredictionWalletRepo) FundPredictionWalletAsset(ctx context.Context, userID, amountRaw, idempotencyKey string) (*PredictionWalletBalance, bool, error) {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return nil, false, err
	}
	key, err := validateIdempotencyKey(idempotencyKey, true)
	if err != nil {
		return nil, false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.ledger.lockBalance(ctx, tx, userID); err != nil {
		return nil, false, err
	}
	if err := r.lockWallet(ctx, tx, userID); err != nil {
		return nil, false, err
	}

	var prior string
	err = tx.QueryRow(ctx, `SELECT amount_raw::text FROM prediction_wallet_entries WHERE user_id=$1 AND kind='main_to_prediction' AND idempotency_key=$2`, userID, key).Scan(&prior)
	if err == nil {
		if prior != amountRaw {
			return nil, false, ErrPredictionIdempotencyKey
		}
		b, e := r.walletTx(ctx, tx, userID)
		if e != nil {
			return nil, false, e
		}
		return b, false, tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return nil, false, err
	}

	if err := r.ledger.debitBalanceTx(ctx, tx, userID, "BI2XUSD", amountRaw); err != nil {
		return nil, false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE prediction_wallet_balances SET available_raw=available_raw+$2::numeric,updated_at=now() WHERE user_id=$1`, userID, amountRaw); err != nil {
		return nil, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO prediction_wallet_entries(user_id,kind,amount_raw,idempotency_key) VALUES($1,'main_to_prediction',$2,$3)`, userID, amountRaw, key); err != nil {
		return nil, false, err
	}
	b, err := r.walletTx(ctx, tx, userID)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// UnfundPredictionWalletAsset moves available prediction-wallet BI2XUSD
// (funded but not locked into an order, or paid out from settlement) back
// to the main wallet — the logical reverse of FundPredictionWalletAsset.
func (r *PredictionWalletRepo) UnfundPredictionWalletAsset(ctx context.Context, userID, amountRaw, idempotencyKey string) (*PredictionWalletBalance, bool, error) {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return nil, false, err
	}
	key, err := validateIdempotencyKey(idempotencyKey, true)
	if err != nil {
		return nil, false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.ledger.lockBalance(ctx, tx, userID); err != nil {
		return nil, false, err
	}
	if err := r.lockWallet(ctx, tx, userID); err != nil {
		return nil, false, err
	}

	var prior string
	err = tx.QueryRow(ctx, `SELECT amount_raw::text FROM prediction_wallet_entries WHERE user_id=$1 AND kind='prediction_to_main' AND idempotency_key=$2`, userID, key).Scan(&prior)
	if err == nil {
		if prior != amountRaw {
			return nil, false, ErrPredictionIdempotencyKey
		}
		b, e := r.walletTx(ctx, tx, userID)
		if e != nil {
			return nil, false, e
		}
		return b, false, tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return nil, false, err
	}

	tag, err := tx.Exec(ctx, `UPDATE prediction_wallet_balances SET available_raw=available_raw-$2::numeric,updated_at=now() WHERE user_id=$1 AND available_raw>=$2::numeric`, userID, amountRaw)
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() != 1 {
		return nil, false, fmt.Errorf("insufficient available BI2XUSD in prediction wallet")
	}
	if err := r.ledger.creditBalanceTx(ctx, tx, userID, "BI2XUSD", amountRaw); err != nil {
		return nil, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO prediction_wallet_entries(user_id,kind,amount_raw,idempotency_key) VALUES($1,'prediction_to_main',$2,$3)`, userID, amountRaw, key); err != nil {
		return nil, false, err
	}
	b, err := r.walletTx(ctx, tx, userID)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// recordTradeEntry inserts one lock/unlock/debit/credit/fee audit row,
// idempotent on (kind, idempotency_key) per
// idx_prediction_wallet_trade_idempotency — ON CONFLICT DO NOTHING rather
// than erroring, since every trade-flow caller below already decides
// idempotency by the UPDATE's own RowsAffected (a retried Lock/Unlock/
// Debit/Credit is naturally a no-op once the balance row reflects it); this
// just keeps the audit trail from growing an extra row per retry.
func (r *PredictionWalletRepo) recordTradeEntry(ctx context.Context, tx pgx.Tx, userID, positionRef, kind, amountRaw, idempotencyKey string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO prediction_wallet_entries(user_id,position_ref,kind,amount_raw,idempotency_key)
		VALUES($1,$2,$3,$4,$5)
		ON CONFLICT (kind,idempotency_key) WHERE kind IN ('lock','unlock','debit','credit','fee') AND idempotency_key IS NOT NULL DO NOTHING`,
		userID, nullIfEmpty(positionRef), kind, amountRaw, nullIfEmpty(idempotencyKey))
	return err
}

// Lock holds amountRaw of BI2XUSD against userID's prediction wallet
// available balance for a resting order — the prediction-wallet
// counterpart to matching-engine's LockBalanceMarket, but against this
// dedicated wallet instead of user_balances. idempotencyKey may be empty
// (no dedup) for a caller that handles its own retries differently, but
// prediction-service's own order flow always supplies one.
func (r *PredictionWalletRepo) Lock(ctx context.Context, userID, positionRef, amountRaw, idempotencyKey string) error {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockWallet(ctx, tx, userID); err != nil {
		return err
	}
	if idempotencyKey != "" {
		if found, err := r.tradeEntryExists(ctx, tx, "lock", idempotencyKey); err != nil {
			return err
		} else if found {
			return tx.Commit(ctx)
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE prediction_wallet_balances SET available_raw=available_raw-$2::numeric,reserved_raw=reserved_raw+$2::numeric,updated_at=now() WHERE user_id=$1 AND available_raw>=$2::numeric`, userID, amountRaw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insufficient available BI2XUSD in prediction wallet; fund it first")
	}
	if err := r.recordTradeEntry(ctx, tx, userID, positionRef, "lock", amountRaw, idempotencyKey); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Unlock releases amountRaw previously held by Lock back to available —
// e.g. an order cancellation or an unfilled remainder's refund at window
// lock.
func (r *PredictionWalletRepo) Unlock(ctx context.Context, userID, positionRef, amountRaw, idempotencyKey string) error {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockWallet(ctx, tx, userID); err != nil {
		return err
	}
	if idempotencyKey != "" {
		if found, err := r.tradeEntryExists(ctx, tx, "unlock", idempotencyKey); err != nil {
			return err
		} else if found {
			return tx.Commit(ctx)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE prediction_wallet_balances SET available_raw=available_raw+$2::numeric,reserved_raw=GREATEST(0,reserved_raw-$2::numeric),updated_at=now() WHERE user_id=$1`, userID, amountRaw); err != nil {
		return err
	}
	if err := r.recordTradeEntry(ctx, tx, userID, positionRef, "unlock", amountRaw, idempotencyKey); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Debit consumes amountRaw from userID's locked (reserved) prediction
// wallet balance — a match consuming part of an order's lock at execution
// price plus fee, the prediction-wallet counterpart of a signed negative
// Credit in the old direct-main-wallet design.
func (r *PredictionWalletRepo) Debit(ctx context.Context, userID, positionRef, amountRaw, idempotencyKey string) error {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockWallet(ctx, tx, userID); err != nil {
		return err
	}
	if idempotencyKey != "" {
		if found, err := r.tradeEntryExists(ctx, tx, "debit", idempotencyKey); err != nil {
			return err
		} else if found {
			return tx.Commit(ctx)
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE prediction_wallet_balances SET reserved_raw=reserved_raw-$2::numeric,updated_at=now() WHERE user_id=$1 AND reserved_raw>=$2::numeric`, userID, amountRaw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insufficient reserved BI2XUSD in prediction wallet for user %s", userID)
	}
	if err := r.recordTradeEntry(ctx, tx, userID, positionRef, "debit", amountRaw, idempotencyKey); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Credit adds amountRaw to userID's available prediction wallet balance —
// a round settlement's payout to a winning position. Unlike Lock/Unlock/
// Debit, this never needs reserved_raw: a payout was never itself locked
// (the position's cost was locked and already consumed via Debit at match
// time), it's simply new money landing in the wallet.
func (r *PredictionWalletRepo) Credit(ctx context.Context, userID, positionRef, amountRaw, idempotencyKey string) error {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.lockWallet(ctx, tx, userID); err != nil {
		return err
	}
	if idempotencyKey != "" {
		if found, err := r.tradeEntryExists(ctx, tx, "credit", idempotencyKey); err != nil {
			return err
		} else if found {
			return tx.Commit(ctx)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE prediction_wallet_balances SET available_raw=available_raw+$2::numeric,updated_at=now() WHERE user_id=$1`, userID, amountRaw); err != nil {
		return err
	}
	if err := r.recordTradeEntry(ctx, tx, userID, positionRef, "credit", amountRaw, idempotencyKey); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *PredictionWalletRepo) tradeEntryExists(ctx context.Context, tx pgx.Tx, kind, idempotencyKey string) (bool, error) {
	var one int
	err := tx.QueryRow(ctx, `SELECT 1 FROM prediction_wallet_entries WHERE kind=$1 AND idempotency_key=$2`, kind, idempotencyKey).Scan(&one)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}
