// Package repo holds Postgres data access for users, sessions, the deposit/
// withdrawal ledger, P2P marketplace, and admin dashboard aggregates.
package repo

import (
	"context"
	"strings"

	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UserRepo struct {
	pool      *pgxpool.Pool
	referrals *ReferralRepo
}

func NewUserRepo(pool *pgxpool.Pool) *UserRepo {
	return &UserRepo{pool: pool}
}

// SetReferrals wires the referral repo used to resolve/link a signup code.
// Optional — a nil referrals means FindOrCreate simply never links new
// users to anything, e.g. in tests that don't care about this feature.
func (r *UserRepo) SetReferrals(referrals *ReferralRepo) {
	r.referrals = referrals
}

// FindOrCreate returns the user for walletAddress, creating one if it
// doesn't exist yet. signupCode (a referral or affiliate code, or empty) is
// only ever consulted when a NEW user is actually created — a returning
// user is never linked or re-linked, since a user's earning-source link is
// permanent and set exactly once, at signup (see
// REFERRAL-AFFILIATE-PLAN.md).
func (r *UserRepo) FindOrCreate(ctx context.Context, walletAddress, walletType string, signupCode string) (models.User, error) {
	address := strings.ToLower(walletAddress)

	var u models.User
	err := r.pool.QueryRow(ctx,
		`SELECT id, wallet_address, wallet_type, created_at, last_login_at FROM users WHERE wallet_address = $1`,
		address,
	).Scan(&u.ID, &u.WalletAddress, &u.WalletType, &u.CreatedAt, &u.LastLoginAt)
	if err == nil {
		return u, nil
	}
	if err != pgx.ErrNoRows {
		return models.User{}, err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return models.User{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var source *ReferralSource
	if r.referrals != nil && signupCode != "" {
		source, err = r.referrals.ResolveSignupCode(ctx, tx, signupCode)
		if err != nil {
			return models.User{}, err
		}
	}

	err = tx.QueryRow(ctx,
		`INSERT INTO users (wallet_address, wallet_type) VALUES ($1, $2)
		 RETURNING id, wallet_address, wallet_type, created_at, last_login_at`,
		address, walletType,
	).Scan(&u.ID, &u.WalletAddress, &u.WalletType, &u.CreatedAt, &u.LastLoginAt)
	if err != nil {
		return models.User{}, err
	}

	if r.referrals != nil && source != nil {
		if err := r.referrals.LinkNewUserTx(ctx, tx, u.ID, source); err != nil {
			return models.User{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return models.User{}, err
	}
	return u, nil
}

// EnsureByID upserts a users row with an explicit, caller-supplied id rather
// than letting Postgres assign one (unlike FindOrCreate, which is keyed by
// wallet_address and generates id via dex_user_seq). This is for synthetic,
// non-wallet identities — e.g. a market-maker desk's internal account id
// like "mm:BTC:spot" — that the matching engine and ledger reference
// directly as the literal user id, so the id must be exactly what the
// caller passed in, not database-generated. wallet_address is set to the
// same value since the column is NOT NULL UNIQUE and there is no real
// wallet address for a desk account; walletType records the caller's kind
// (e.g. "market-maker") for auditing.
func (r *UserRepo) EnsureByID(ctx context.Context, userID, walletType string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO users (id, wallet_address, wallet_type) VALUES ($1, $1, $2)
		 ON CONFLICT (id) DO NOTHING`,
		userID, walletType,
	)
	return err
}

func (r *UserRepo) TouchLogin(ctx context.Context, userID string) error {
	_, err := r.pool.Exec(ctx, `UPDATE users SET last_login_at = now() WHERE id = $1`, userID)
	return err
}

// CreateSession records a login event and returns the session id.
func (r *UserRepo) CreateSession(ctx context.Context, userID, walletAddress, ip, userAgent string) (string, error) {
	var sessionID string
	err := r.pool.QueryRow(ctx,
		`INSERT INTO user_sessions (user_id, wallet_address, ip_address, user_agent)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		userID, strings.ToLower(walletAddress), ip, userAgent,
	).Scan(&sessionID)
	return sessionID, err
}

// CloseSession marks the most recent open session for userID as logged out.
func (r *UserRepo) CloseSession(ctx context.Context, userID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE user_sessions SET logout_at = now()
		 WHERE id = (
			SELECT id FROM user_sessions
			WHERE user_id = $1 AND logout_at IS NULL
			ORDER BY login_at DESC LIMIT 1
		 )`,
		userID,
	)
	return err
}

func (r *UserRepo) FindByID(ctx context.Context, userID string) (models.User, error) {
	var u models.User
	err := r.pool.QueryRow(ctx,
		`SELECT id, wallet_address, wallet_type, created_at, last_login_at FROM users WHERE id = $1`,
		userID,
	).Scan(&u.ID, &u.WalletAddress, &u.WalletType, &u.CreatedAt, &u.LastLoginAt)
	return u, err
}

// Search finds users whose id or wallet_address contains query
// (case-insensitive), for the admin balance-adjustment picker. Empty query
// returns the most recently created users instead of everything, so an
// unfiltered picker load stays bounded.
func (r *UserRepo) Search(ctx context.Context, query string, limit int) ([]models.User, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	pattern := "%" + strings.ToLower(strings.TrimSpace(query)) + "%"
	rows, err := r.pool.Query(ctx,
		`SELECT id, wallet_address, wallet_type, created_at, last_login_at FROM users
		 WHERE lower(id) LIKE $1 OR lower(wallet_address) LIKE $1
		 ORDER BY created_at DESC LIMIT $2`,
		pattern, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.User{}
	for rows.Next() {
		var u models.User
		if err := rows.Scan(&u.ID, &u.WalletAddress, &u.WalletType, &u.CreatedAt, &u.LastLoginAt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
