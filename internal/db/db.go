// Package db sets up the Postgres pool and schema for the auth service.
package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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
	// Startup DDL can wait behind transactions from another running instance.
	// Cancel that wait before the launcher times out and kills this process.
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	slog.Info("initializing postgres", "timeout", "60s")
	// Explicitly cap pool size: three services share one Aiven Postgres
	// instance with a hard 100-connection limit. Without an explicit
	// MaxConns, pgxpool defaults to max(4, NumCPU()) per process, which is
	// unbounded relative to the shared limit on larger deploy hosts.
	// matching-engine caps at 20, bots at 10; backend (heaviest HTTP/auth
	// traffic) gets 25, leaving headroom under the shared limit.
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 25
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres connection: %w", err)
	}
	for _, migration := range []struct{ name, sql string }{
		{"legacy user IDs", migrateLegacyIDColumn},
		{"base schema", schema},
		{"user ID defaults", ensureIDDefault},
		{"wallet balances", ensureUserBalancesTable},
		{"P2P tables", ensureP2PTables},
		{"USDT to USDB", migrateUSDTToUSDB},
	} {
		slog.Info("running database migration", "migration", migration.name)
		if _, err := pool.Exec(ctx, migration.sql); err != nil {
			pool.Close()
			return nil, fmt.Errorf("database migration %q failed (check for blocking transactions if timed out): %w", migration.name, err)
		}
	}
	slog.Info("postgres initialization complete")
	return pool, nil
}

const ensureP2PTables = `
ALTER TABLE users ADD COLUMN IF NOT EXISTS p2p_username TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_p2p_username_unique ON users (lower(p2p_username)) WHERE p2p_username IS NOT NULL;
CREATE OR REPLACE FUNCTION prevent_p2p_username_change() RETURNS trigger AS $$
BEGIN
	IF OLD.p2p_username IS NOT NULL AND NEW.p2p_username IS DISTINCT FROM OLD.p2p_username THEN
		RAISE EXCEPTION 'P2P username cannot be changed';
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_users_p2p_username_immutable ON users;
CREATE TRIGGER trg_users_p2p_username_immutable BEFORE UPDATE OF p2p_username ON users
	FOR EACH ROW EXECUTE FUNCTION prevent_p2p_username_change();

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
INSERT INTO p2p_price_history (asset, fiat_currency, price, price_date)
VALUES ('USDB', 'INR', 100, CURRENT_DATE)
ON CONFLICT (asset, fiat_currency, price_date) DO NOTHING;
CREATE TABLE IF NOT EXISTS p2p_listings (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(), seller_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','USDB')), amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0),
	remaining_raw NUMERIC(38,0) NOT NULL CHECK (remaining_raw >= 0), price NUMERIC(38,8) NOT NULL CHECK (price > 0),
	fiat_currency TEXT NOT NULL DEFAULT 'INR', payment_method TEXT NOT NULL CHECK (payment_method IN ('UPI', 'Bank Transfer', 'NEFT', 'IMPS')),
	status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FILLED', 'CANCELLED')),
	funding_source TEXT NOT NULL DEFAULT 'P2P_WALLET' CHECK (funding_source IN ('P2P_WALLET', 'MAIN_WALLET_LEGACY')),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS side TEXT NOT NULL DEFAULT 'SELL';
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_side_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_side_check CHECK (side IN ('BUY','SELL'));
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS payment_methods TEXT[];
UPDATE p2p_listings SET payment_methods=ARRAY[payment_method] WHERE payment_methods IS NULL OR cardinality(payment_methods)=0;
ALTER TABLE p2p_listings ALTER COLUMN payment_methods SET NOT NULL;
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_payment_method_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_payment_method_check CHECK (payment_method IN ('UPI','Bank Transfer','MPESN','NEFT','IMPS'));
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_payment_methods_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_payment_methods_check CHECK (
	cardinality(payment_methods) > 0
	AND payment_methods <@ ARRAY['UPI','Bank Transfer','MPESN','NEFT','IMPS']::TEXT[]
);
CREATE INDEX IF NOT EXISTS idx_p2p_listings_active ON p2p_listings (created_at DESC) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_p2p_listings_seller ON p2p_listings (seller_id, created_at DESC);
CREATE TABLE IF NOT EXISTS p2p_orders (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(), listing_id UUID NOT NULL REFERENCES p2p_listings(id) ON DELETE RESTRICT,
	seller_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT, buyer_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
	asset TEXT NOT NULL, amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0), price NUMERIC(38,8) NOT NULL CHECK (price > 0),
	fiat_currency TEXT NOT NULL, gross_amount NUMERIC(38,8) NOT NULL, buyer_fee NUMERIC(38,8) NOT NULL,
	seller_fee NUMERIC(38,8) NOT NULL, buyer_payable NUMERIC(38,8) NOT NULL, seller_receivable NUMERIC(38,8) NOT NULL,
	payment_method TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending_payment',
	escrow_raw NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (escrow_raw >= 0),
	idempotency_key TEXT, expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '15 minutes'),
	cancellation_reason TEXT, completed_at TIMESTAMPTZ,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), CHECK (buyer_id <> seller_id)
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
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS escrow_raw NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '15 minutes');
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS cancellation_reason TEXT;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS taker_id TEXT REFERENCES users(id) ON DELETE RESTRICT;
UPDATE p2p_orders o SET taker_id=CASE WHEN l.side='BUY' THEN o.seller_id ELSE o.buyer_id END
	FROM p2p_listings l WHERE o.listing_id=l.id AND o.taker_id IS NULL;
-- A short-lived P2P implementation used initiator_id for the same actor now
-- represented by taker_id. Preserve its values, then remove the stale NOT
-- NULL column so current order inserts are not rejected.
DO $legacy_initiator$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='p2p_orders' AND column_name='initiator_id'
	) THEN
		EXECUTE 'UPDATE p2p_orders SET taker_id=COALESCE(taker_id,initiator_id::text) WHERE taker_id IS NULL';
		EXECUTE 'ALTER TABLE p2p_orders DROP COLUMN initiator_id';
	END IF;
END $legacy_initiator$;

-- Listings created before the dedicated P2P wallet existed still reserve
-- user_balances.USDC_locked. Keep that funding source explicit so they can
-- finish or cancel safely without silently treating those funds as P2P funds.
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS funding_source TEXT;
UPDATE p2p_listings SET funding_source='MAIN_WALLET_LEGACY' WHERE funding_source IS NULL;
ALTER TABLE p2p_listings ALTER COLUMN funding_source SET DEFAULT 'P2P_WALLET';
ALTER TABLE p2p_listings ALTER COLUMN funding_source SET NOT NULL;
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_funding_source_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_funding_source_check
	CHECK (funding_source IN ('P2P_WALLET','MAIN_WALLET_LEGACY'));
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_asset_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_asset_check CHECK (asset IN ('USDC','USDB'));

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
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_asset_check;
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_asset_check CHECK (asset IN ('USDC','USDB'));
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_payment_method_check;
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_payment_method_check CHECK (payment_method IN ('UPI','Bank Transfer','MPESN','NEFT','IMPS','upi','bank_transfer','neft','imps','qr','test_payment'));
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_status_check;
UPDATE p2p_orders SET status='completed',completed_at=COALESCE(completed_at,created_at),escrow_raw=0
	WHERE status='COMPLETED';
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_status_check
	CHECK (status IN ('completed','pending_payment','payment_made','cancelled','appeal'));
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_escrow_raw_check;
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_escrow_raw_check CHECK (escrow_raw >= 0);
CREATE UNIQUE INDEX IF NOT EXISTS idx_p2p_orders_idempotency
	ON p2p_orders (buyer_id,idempotency_key) WHERE idempotency_key IS NOT NULL;
DROP INDEX IF EXISTS idx_p2p_orders_idempotency;
CREATE UNIQUE INDEX IF NOT EXISTS idx_p2p_orders_taker_idempotency
	ON p2p_orders (taker_id,idempotency_key) WHERE taker_id IS NOT NULL AND idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_p2p_orders_expiry
	ON p2p_orders (expires_at) WHERE status='pending_payment';

CREATE TABLE IF NOT EXISTS p2p_wallet_balances (
	user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','USDB')),
	available_raw NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (available_raw >= 0),
	reserved_raw NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (reserved_raw >= 0),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY (user_id,asset)
);

CREATE TABLE IF NOT EXISTS p2p_wallet_entries (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	listing_id UUID REFERENCES p2p_listings(id) ON DELETE SET NULL,
	order_id UUID REFERENCES p2p_orders(id) ON DELETE SET NULL,
	kind TEXT NOT NULL,
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','USDB')),
	amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0),
	idempotency_key TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_p2p_wallet_entries_user
	ON p2p_wallet_entries (user_id,created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_p2p_wallet_fund_idempotency
	ON p2p_wallet_entries (user_id,kind,idempotency_key)
	WHERE kind='main_to_p2p' AND idempotency_key IS NOT NULL;
ALTER TABLE p2p_wallet_balances DROP CONSTRAINT IF EXISTS p2p_wallet_balances_asset_check;
ALTER TABLE p2p_wallet_balances ADD CONSTRAINT p2p_wallet_balances_asset_check CHECK (asset IN ('USDC','USDB'));
ALTER TABLE p2p_wallet_entries DROP CONSTRAINT IF EXISTS p2p_wallet_entries_asset_check;
ALTER TABLE p2p_wallet_entries ADD CONSTRAINT p2p_wallet_entries_asset_check CHECK (asset IN ('USDC','USDB'));
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
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BTC" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BUSD" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "OUR_Token" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDC_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDT_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BTC_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BUSD_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "OUR_Token_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- USDB: the platform's internal stable quote currency, pegged 1:1 to USDT.
-- It has no on-chain contract; every market's quote leg trades in USDB. The
-- USDT/USDC columns above remain only as the deposit-intake ledger (a real
-- on-chain deposit lands there first via the chain listener), not as a
-- tradable balance any more.
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDB" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDB_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;

-- ETH, SOL, and BNB: base assets for the ETH-USDB / SOL-USDB / BNB-USDB spot
-- markets (matching-engine's currentMarkets). These were registered as
-- tradable markets before a real balance column backed them, so nobody could
-- ever actually hold or fund the base leg (deposits/MM desk funding failed
-- with "unsupported asset"). Added following the exact same pattern as BTC.
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "ETH" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "ETH_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "SOL" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "SOL_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BNB" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BNB_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;

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
			"BTC" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"USDC" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"USDT" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"BUSD" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"OUR_Token" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"USDC_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"BTC_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"USDT_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"BUSD_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			"OUR_Token_locked" NUMERIC(38,0) NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		INSERT INTO user_balances_reordered
			(balance_id, user_id, "BTC", "USDC", "USDT", "BUSD", "OUR_Token", "BTC_locked", "USDC_locked", "USDT_locked", "BUSD_locked", "OUR_Token_locked", created_at, updated_at)
		SELECT balance_id, user_id, "BTC", "USDC", "USDT", "BUSD", "OUR_Token", "BTC_locked", "USDC_locked", "USDT_locked", "BUSD_locked", "OUR_Token_locked", created_at, updated_at
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

// migrateUSDTToUSDB is the one-time conversion for the USDT→USDB currency
// switch: every market's quote leg now trades in USDB (pegged 1:1 to USDT,
// no on-chain contract of its own), so a balance sitting in the old
// tradable USDT column must move to USDB or it becomes permanently
// inaccessible to trading. Converts at 1:1 and zeroes the USDT columns.
//
// Idempotent by construction, not by a migration-log flag: it only touches
// rows where USDT (or USDT_locked) is still nonzero, which is false after
// the first successful run, so re-running on every boot is a no-op. Must
// run after `schema` has created the USDB columns.
const migrateUSDTToUSDB = `
UPDATE user_balances SET
	"USDB" = "USDB" + "USDT",
	"USDB_locked" = "USDB_locked" + "USDT_locked",
	"USDT" = 0,
	"USDT_locked" = 0,
	updated_at = now()
WHERE "USDT" > 0 OR "USDT_locked" > 0;
`
