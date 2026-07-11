// Package db sets up the Postgres pool and schema for the auth service.
package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SEQUENCE IF NOT EXISTS dex_user_seq;

CREATE TABLE IF NOT EXISTS users (
	id TEXT PRIMARY KEY DEFAULT ('DEXUSER_' || nextval('dex_user_seq')),
	wallet_address TEXT NOT NULL UNIQUE,
	wallet_type TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	last_login_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS user_sessions (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	wallet_address TEXT NOT NULL,
	login_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	logout_at TIMESTAMPTZ,
	ip_address TEXT,
	user_agent TEXT
);

CREATE INDEX IF NOT EXISTS idx_users_wallet_address ON users(wallet_address);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON user_sessions(user_id);

CREATE TABLE IF NOT EXISTS ledger_entries (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	wallet_address TEXT NOT NULL,
	kind TEXT NOT NULL,
	token TEXT NOT NULL,
	amount NUMERIC(38,0) NOT NULL,
	tx_hash TEXT,
	status TEXT NOT NULL DEFAULT 'confirmed',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_tx_hash ON ledger_entries(tx_hash) WHERE tx_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ledger_user_id ON ledger_entries(user_id);

CREATE TABLE IF NOT EXISTS chain_cursor (
	key TEXT PRIMARY KEY,
	block_number BIGINT NOT NULL
);
`

// New connects to Postgres and ensures the auth schema exists.
func New(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, migrateLegacyIDColumn); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, ensureIDDefault); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// ensureIDDefault (re)applies the DEXUSER_N default on users.id. Needed because
// the UUID->TEXT migration drops any prior default, and CREATE TABLE IF NOT EXISTS
// does not add a default to an already-existing column.
const ensureIDDefault = `
ALTER TABLE users ALTER COLUMN id SET DEFAULT ('DEXUSER_' || nextval('dex_user_seq'));
`

// migrateLegacyIDColumn converts users.id / user_sessions.user_id from UUID to TEXT
// in place (one-time, idempotent) so the new sequential DEXUSER_N id scheme fits.
// No-op once the columns are already TEXT.
const migrateLegacyIDColumn = `
DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'users' AND column_name = 'id' AND data_type = 'uuid'
	) THEN
		ALTER TABLE user_sessions DROP CONSTRAINT IF EXISTS user_sessions_user_id_fkey;
		ALTER TABLE users ALTER COLUMN id DROP DEFAULT;
		ALTER TABLE users ALTER COLUMN id TYPE TEXT USING id::text;
		ALTER TABLE user_sessions ALTER COLUMN user_id TYPE TEXT USING user_id::text;
		ALTER TABLE user_sessions ADD CONSTRAINT user_sessions_user_id_fkey
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
	END IF;
END $$;
`
