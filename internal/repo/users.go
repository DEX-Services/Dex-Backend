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
	pool *pgxpool.Pool
}

func NewUserRepo(pool *pgxpool.Pool) *UserRepo {
	return &UserRepo{pool: pool}
}

// FindOrCreate returns the user for walletAddress, creating one if it doesn't exist yet.
func (r *UserRepo) FindOrCreate(ctx context.Context, walletAddress, walletType string) (models.User, error) {
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

	err = r.pool.QueryRow(ctx,
		`INSERT INTO users (wallet_address, wallet_type) VALUES ($1, $2)
		 RETURNING id, wallet_address, wallet_type, created_at, last_login_at`,
		address, walletType,
	).Scan(&u.ID, &u.WalletAddress, &u.WalletType, &u.CreatedAt, &u.LastLoginAt)
	return u, err
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
