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

ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_idempotency
	ON ledger_entries(user_id, kind, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS chain_cursor (
	key TEXT PRIMARY KEY,
	block_number BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_profiles (
	login_id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	email TEXT NOT NULL,
	phone TEXT NOT NULL,
	role TEXT NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO admin_profiles (login_id, name, email, phone, role)
VALUES ('admin', 'DEX Admin', 'admin@dex.ai', '+91 00000 00000', 'Super Admin')
ON CONFLICT (login_id) DO NOTHING;
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
	if _, err := pool.Exec(ctx, ensureUserBalancesTable); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, ensureP2PTables); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

const ensureP2PTables = `
CREATE TABLE IF NOT EXISTS p2p_price_history (
	id BIGSERIAL PRIMARY KEY,
	asset TEXT NOT NULL,
	fiat_currency TEXT NOT NULL DEFAULT 'INR',
	price NUMERIC(38,8) NOT NULL CHECK (price > 0),
	price_date DATE NOT NULL DEFAULT CURRENT_DATE,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE (asset, fiat_currency, price_date)
);
INSERT INTO p2p_price_history (asset, fiat_currency, price, price_date)
VALUES ('USDC', 'INR', 100, CURRENT_DATE)
ON CONFLICT (asset, fiat_currency, price_date) DO NOTHING;
CREATE TABLE IF NOT EXISTS p2p_listings (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(), seller_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset = 'USDC'), amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0),
	remaining_raw NUMERIC(38,0) NOT NULL CHECK (remaining_raw >= 0), price NUMERIC(38,8) NOT NULL CHECK (price > 0),
	fiat_currency TEXT NOT NULL DEFAULT 'INR', payment_method TEXT NOT NULL CHECK (payment_method IN ('UPI', 'Bank Transfer', 'NEFT', 'IMPS')),
	status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FILLED', 'CANCELLED')),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_p2p_listings_active ON p2p_listings (created_at DESC) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_p2p_listings_seller ON p2p_listings (seller_id, created_at DESC);
CREATE TABLE IF NOT EXISTS p2p_orders (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(), listing_id UUID NOT NULL REFERENCES p2p_listings(id) ON DELETE RESTRICT,
	seller_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT, buyer_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
	asset TEXT NOT NULL, amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0), price NUMERIC(38,8) NOT NULL CHECK (price > 0),
	fiat_currency TEXT NOT NULL, gross_amount NUMERIC(38,8) NOT NULL, buyer_fee NUMERIC(38,8) NOT NULL,
	seller_fee NUMERIC(38,8) NOT NULL, buyer_payable NUMERIC(38,8) NOT NULL, seller_receivable NUMERIC(38,8) NOT NULL,
	payment_method TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'COMPLETED' CHECK (status = 'COMPLETED'),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(), CHECK (buyer_id <> seller_id)
);
CREATE INDEX IF NOT EXISTS idx_p2p_orders_buyer ON p2p_orders (buyer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_p2p_orders_seller ON p2p_orders (seller_id, created_at DESC);

-- Compatibility for the P2P order table that existed before listings were introduced.
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS listing_id UUID REFERENCES p2p_listings(id) ON DELETE RESTRICT;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS amount_raw NUMERIC(38,0);
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS fiat_currency TEXT DEFAULT 'INR';
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS buyer_payable NUMERIC(38,8);
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS seller_receivable NUMERIC(38,8);
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS buyer_credit NUMERIC(38,0) NOT NULL DEFAULT 1;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS seller_debit NUMERIC(38,0) NOT NULL DEFAULT 1;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS fiat_amount NUMERIC(38,8);
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS buyer_fee_fiat NUMERIC(38,8) NOT NULL DEFAULT 0;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS seller_fee_fiat NUMERIC(38,8) NOT NULL DEFAULT 0;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS buyer_pays_fiat NUMERIC(38,8) NOT NULL DEFAULT 0;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS seller_receives_fiat NUMERIC(38,8) NOT NULL DEFAULT 0;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE p2p_orders SET
	amount_raw = COALESCE(amount_raw, buyer_credit, seller_debit, gross_amount),
	fiat_currency = COALESCE(fiat_currency, 'INR'),
	fiat_amount = COALESCE(fiat_amount, round((COALESCE(amount_raw, buyer_credit, seller_debit, gross_amount) / 1000000) * price, 8)),
	buyer_payable = COALESCE(buyer_payable, NULLIF(buyer_pays_fiat, 0), round(((COALESCE(amount_raw, buyer_credit, seller_debit, gross_amount) / 1000000) * price) * 1.01, 8)),
	seller_receivable = COALESCE(seller_receivable, NULLIF(seller_receives_fiat, 0), round(((COALESCE(amount_raw, buyer_credit, seller_debit, gross_amount) / 1000000) * price) * .99, 8));

UPDATE p2p_orders SET
	gross_amount = fiat_amount,
	buyer_fee = COALESCE(NULLIF(buyer_fee_fiat, 0), round(fiat_amount * .01, 8)),
	seller_fee = COALESCE(NULLIF(seller_fee_fiat, 0), round(fiat_amount * .01, 8)),
	buyer_fee_fiat = COALESCE(NULLIF(buyer_fee_fiat, 0), round(fiat_amount * .01, 8)),
	seller_fee_fiat = COALESCE(NULLIF(seller_fee_fiat, 0), round(fiat_amount * .01, 8)),
	buyer_pays_fiat = buyer_payable,
	seller_receives_fiat = seller_receivable;

ALTER TABLE p2p_orders ALTER COLUMN amount_raw SET NOT NULL;
ALTER TABLE p2p_orders ALTER COLUMN fiat_currency SET NOT NULL;
ALTER TABLE p2p_orders ALTER COLUMN buyer_payable SET NOT NULL;
ALTER TABLE p2p_orders ALTER COLUMN seller_receivable SET NOT NULL;
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_payment_method_check;
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_payment_method_check CHECK (payment_method IN ('UPI','Bank Transfer','NEFT','IMPS','upi','bank_transfer','neft','imps','qr','test_payment'));
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_status_check;
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_status_check CHECK (status IN ('COMPLETED','completed','pending_payment','payment_made','cancelled','appeal'));
`

// ensureIDDefault (re)applies the DEXUSER_N default on users.id. Needed because
// the UUID->TEXT migration drops any prior default, and CREATE TABLE IF NOT EXISTS
// does not add a default to an already-existing column.
const ensureIDDefault = `
ALTER TABLE users ALTER COLUMN id SET DEFAULT ('DEXUSER_' || nextval('dex_user_seq'));
`

const ensureUserBalancesTable = `
ALTER TABLE users
	DROP COLUMN IF EXISTS "USDC",
	DROP COLUMN IF EXISTS "USDT",
	DROP COLUMN IF EXISTS "DUSD",
	DROP COLUMN IF EXISTS "BUSD",
	DROP COLUMN IF EXISTS "OUR_Token";

CREATE TABLE IF NOT EXISTS user_balances (
	balance_id BIGSERIAL PRIMARY KEY,
	user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	"USDC" NUMERIC(38,0) NOT NULL DEFAULT 0,
	"USDT" NUMERIC(38,0) NOT NULL DEFAULT 0,
	"BUSD" NUMERIC(38,0) NOT NULL DEFAULT 0,
	"OUR_Token" NUMERIC(38,0) NOT NULL DEFAULT 0,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE user_balances ALTER COLUMN user_id TYPE TEXT USING user_id::text;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDC" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDT" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BUSD" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "OUR_Token" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDC_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDT_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BUSD_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "OUR_Token_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

DO $wallet$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'user_balances' AND column_name = 'asset'
	) THEN
		DROP INDEX IF EXISTS user_balances_user_asset_uidx;

		EXECUTE $migration$
			WITH migrated AS (
				SELECT
					user_id,
					MIN(balance_id) AS keep_id,
					COALESCE(SUM(CASE WHEN UPPER(REPLACE(asset, '-', '_')) = 'USDC' THEN total ELSE 0 END), 0) AS usdc,
					COALESCE(SUM(CASE WHEN UPPER(REPLACE(asset, '-', '_')) = 'USDT' THEN total ELSE 0 END), 0) AS usdt,
					COALESCE(SUM(CASE WHEN UPPER(REPLACE(asset, '-', '_')) IN ('BUSD', 'DUSD') THEN total ELSE 0 END), 0) AS busd,
					COALESCE(SUM(CASE WHEN UPPER(REPLACE(asset, '-', '_')) IN ('OUR_TOKEN', 'OURTOKEN') THEN total ELSE 0 END), 0) AS our_token,
					MIN(updated_at) AS created_at,
					MAX(updated_at) AS updated_at
				FROM user_balances
				GROUP BY user_id
			)
			UPDATE user_balances ub
			SET "USDC" = migrated.usdc,
				"USDT" = migrated.usdt,
				"BUSD" = migrated.busd,
				"OUR_Token" = migrated.our_token,
				created_at = migrated.created_at,
				updated_at = migrated.updated_at
			FROM migrated
			WHERE ub.balance_id = migrated.keep_id
		$migration$;

		EXECUTE $deduplicate$
			DELETE FROM user_balances duplicate
			USING user_balances keeper
			WHERE duplicate.user_id = keeper.user_id
				AND duplicate.balance_id > keeper.balance_id
		$deduplicate$;

		ALTER TABLE user_balances
			DROP COLUMN asset,
			DROP COLUMN available,
			DROP COLUMN locked,
			DROP COLUMN total;
	END IF;
END $wallet$;

DO $asset_rename$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'user_balances' AND column_name = 'DUSD'
	) THEN
		UPDATE user_balances SET "BUSD" = "BUSD" + "DUSD";
		ALTER TABLE user_balances DROP COLUMN "DUSD";
	END IF;
END $asset_rename$;
DO $column_order$
DECLARE
	has_rows BOOLEAN;
BEGIN
	IF (
		SELECT busd.ordinal_position > own_token.ordinal_position
		FROM information_schema.columns busd
		JOIN information_schema.columns own_token
			ON own_token.table_schema = busd.table_schema
			AND own_token.table_name = busd.table_name
		WHERE busd.table_schema = 'public'
			AND busd.table_name = 'user_balances'
			AND busd.column_name = 'BUSD'
			AND own_token.column_name = 'OUR_Token'
	) THEN
		DROP TABLE IF EXISTS user_balances_reordered;
		CREATE TABLE user_balances_reordered (
			balance_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			user_id TEXT NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
			"USDC" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"USDT" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"BUSD" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"OUR_Token" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"USDC_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"USDT_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"BUSD_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"OUR_Token_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		INSERT INTO user_balances_reordered
			(balance_id, user_id, "USDC", "USDT", "BUSD", "OUR_Token", "USDC_locked", "USDT_locked", "BUSD_locked", "OUR_Token_locked", created_at, updated_at)
		SELECT balance_id, user_id, "USDC", "USDT", "BUSD", "OUR_Token", "USDC_locked", "USDT_locked", "BUSD_locked", "OUR_Token_locked", created_at, updated_at
		FROM user_balances;

		SELECT EXISTS (SELECT 1 FROM user_balances_reordered) INTO has_rows;
		IF has_rows THEN
			PERFORM setval(
				pg_get_serial_sequence('user_balances_reordered', 'balance_id'),
				(SELECT MAX(balance_id) FROM user_balances_reordered),
				true
			);
		END IF;

		DROP TABLE user_balances;
		ALTER TABLE user_balances_reordered RENAME TO user_balances;
	END IF;
END $column_order$;
CREATE UNIQUE INDEX IF NOT EXISTS user_balances_user_id_uidx ON user_balances (user_id);

DO $wallet$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM pg_constraint
		WHERE conrelid = 'user_balances'::regclass AND contype = 'f'
	) THEN
		ALTER TABLE user_balances ADD CONSTRAINT user_balances_user_id_fkey
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
	END IF;
END $wallet$;
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
