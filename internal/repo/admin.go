package repo

import (
	"context"
	"strings"

	"github.com/dex/dex-backend/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AdminRepo struct {
	pool *pgxpool.Pool
}

func NewAdminRepo(pool *pgxpool.Pool) *AdminRepo {
	return &AdminRepo{pool: pool}
}

func (r *AdminRepo) Profile(ctx context.Context, loginID string) (models.AdminProfile, error) {
	var p models.AdminProfile
	err := r.pool.QueryRow(ctx, `
		SELECT login_id, name, email, phone, role, updated_at
		FROM admin_profiles
		WHERE login_id = $1`, loginID).
		Scan(&p.LoginID, &p.Name, &p.Email, &p.Phone, &p.Role, &p.UpdatedAt)
	return p, err
}

func (r *AdminRepo) UpdateProfile(ctx context.Context, p models.AdminProfile) (models.AdminProfile, error) {
	var out models.AdminProfile
	err := r.pool.QueryRow(ctx, `
		UPDATE admin_profiles
		SET name = $2, email = $3, phone = $4, updated_at = now()
		WHERE login_id = $1
		RETURNING login_id, name, email, phone, role, updated_at`,
		p.LoginID, strings.TrimSpace(p.Name), strings.TrimSpace(p.Email), strings.TrimSpace(p.Phone)).
		Scan(&out.LoginID, &out.Name, &out.Email, &out.Phone, &out.Role, &out.UpdatedAt)
	return out, err
}

func (r *AdminRepo) Summary(ctx context.Context) (models.AdminSummary, error) {
	var s models.AdminSummary
	if err := r.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM users),
			(SELECT COUNT(DISTINCT user_id) FROM user_sessions WHERE login_at >= now() - interval '24 hours'),
			(SELECT COUNT(*) FROM user_sessions WHERE logout_at IS NULL),
			(SELECT COUNT(*) FROM ledger_entries),
			(SELECT COALESCE(SUM(amount), 0)::text FROM ledger_entries WHERE status = 'confirmed' AND kind = 'deposit'),
			(SELECT COUNT(*) FROM ledger_entries WHERE kind = 'withdrawal_request' AND status IN ('pending', 'processing'))`).
		Scan(&s.TotalUsers, &s.ActiveUsers24h, &s.OpenSessions, &s.TotalLedgerEntries, &s.ConfirmedLedgerRaw, &s.PendingWithdrawals); err != nil {
		return s, err
	}

	tokenRows, err := r.pool.Query(ctx, `
		SELECT token, amount::text, locked::text
		FROM (
			SELECT 'USDC' AS token, COALESCE(SUM("USDC"), 0) AS amount, COALESCE(SUM("USDC_locked"), 0) AS locked FROM user_balances
			UNION ALL SELECT 'USDT', COALESCE(SUM("USDT"), 0), COALESCE(SUM("USDT_locked"), 0) FROM user_balances
			UNION ALL SELECT 'BUSD', COALESCE(SUM("BUSD"), 0), COALESCE(SUM("BUSD_locked"), 0) FROM user_balances
			UNION ALL SELECT 'OUR_Token', COALESCE(SUM("OUR_Token"), 0), COALESCE(SUM("OUR_Token_locked"), 0) FROM user_balances
		) totals`)
	if err != nil {
		return s, err
	}
	defer tokenRows.Close()
	for tokenRows.Next() {
		var t models.AdminTokenTotal
		if err := tokenRows.Scan(&t.Token, &t.Amount, &t.Locked); err != nil {
			return s, err
		}
		s.TotalBalances = append(s.TotalBalances, t)
	}
	if err := tokenRows.Err(); err != nil {
		return s, err
	}

	topRows, err := r.pool.Query(ctx, `
		SELECT u.id, u.wallet_address, u.wallet_type, COUNT(l.id) AS entry_count,
		       COALESCE(SUM(l.amount), 0)::text AS total_raw, u.last_login_at
		FROM users u
		LEFT JOIN ledger_entries l ON l.user_id = u.id AND l.status = 'confirmed'
		GROUP BY u.id, u.wallet_address, u.wallet_type, u.last_login_at
		ORDER BY COALESCE(SUM(l.amount), 0) DESC, COUNT(l.id) DESC
		LIMIT 5`)
	if err != nil {
		return s, err
	}
	defer topRows.Close()
	for topRows.Next() {
		var u models.AdminTopUser
		if err := topRows.Scan(&u.UserID, &u.WalletAddress, &u.WalletType, &u.EntryCount, &u.TotalRaw, &u.LastLoginAt); err != nil {
			return s, err
		}
		s.TopUsers = append(s.TopUsers, u)
	}
	if err := topRows.Err(); err != nil {
		return s, err
	}

	ledgerRows, err := r.pool.Query(ctx, `
		SELECT id, user_id, wallet_address, kind, token, amount::text, status, created_at
		FROM ledger_entries
		ORDER BY created_at DESC
		LIMIT 8`)
	if err != nil {
		return s, err
	}
	defer ledgerRows.Close()
	for ledgerRows.Next() {
		var e models.AdminLedgerEntry
		if err := ledgerRows.Scan(&e.ID, &e.UserID, &e.WalletAddress, &e.Kind, &e.Token, &e.Amount, &e.Status, &e.CreatedAt); err != nil {
			return s, err
		}
		s.RecentLedgerEntries = append(s.RecentLedgerEntries, e)
	}
	if err := ledgerRows.Err(); err != nil {
		return s, err
	}

	userRows, err := r.pool.Query(ctx, `
		SELECT id, wallet_address, wallet_type, created_at, last_login_at
		FROM users
		ORDER BY created_at DESC
		LIMIT 6`)
	if err != nil {
		return s, err
	}
	defer userRows.Close()
	for userRows.Next() {
		var u models.AdminRecentUser
		if err := userRows.Scan(&u.ID, &u.WalletAddress, &u.WalletType, &u.CreatedAt, &u.LastLoginAt); err != nil {
			return s, err
		}
		s.RecentUsers = append(s.RecentUsers, u)
	}
	return s, userRows.Err()
}
