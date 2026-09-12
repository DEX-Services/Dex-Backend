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
	cfg.MaxConns = 60
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
		{"USDT to BIUSDB", migrateUSDTToBIUSDB},
		{"fee config tables", ensureFeeConfigTables},
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
VALUES ('BIUSDB', 'INR', 100, CURRENT_DATE)
ON CONFLICT (asset, fiat_currency, price_date) DO NOTHING;
CREATE TABLE IF NOT EXISTS p2p_listings (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(), seller_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','BIUSDB')), amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0),
	remaining_raw NUMERIC(38,0) NOT NULL CHECK (remaining_raw >= 0), price NUMERIC(38,8) NOT NULL CHECK (price > 0),
	fiat_currency TEXT NOT NULL DEFAULT 'INR', payment_method TEXT NOT NULL CHECK (payment_method IN ('UPI', 'Bank Transfer', 'NEFT', 'IMPS')),
	status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FILLED', 'CANCELLED')),
	funding_source TEXT NOT NULL DEFAULT 'P2P_WALLET' CHECK (funding_source IN ('P2P_WALLET', 'MAIN_WALLET_LEGACY')),
	fee_model TEXT NOT NULL DEFAULT 'BIUSDB_1PCT_EACH' CHECK (fee_model IN ('LEGACY_FIAT','BIUSDB_1PCT_EACH')),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS side TEXT NOT NULL DEFAULT 'SELL';
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS fee_model TEXT;
UPDATE p2p_listings SET fee_model='LEGACY_FIAT' WHERE fee_model IS NULL;
ALTER TABLE p2p_listings ALTER COLUMN fee_model SET DEFAULT 'BIUSDB_1PCT_EACH';
ALTER TABLE p2p_listings ALTER COLUMN fee_model SET NOT NULL;
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_fee_model_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_fee_model_check CHECK (fee_model IN ('LEGACY_FIAT','BIUSDB_1PCT_EACH'));
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_side_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_side_check CHECK (side IN ('BUY','SELL'));
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS payment_methods TEXT[];
UPDATE p2p_listings SET payment_methods=ARRAY[payment_method] WHERE payment_methods IS NULL OR cardinality(payment_methods)=0;
ALTER TABLE p2p_listings ALTER COLUMN payment_methods SET NOT NULL;
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS min_order_fiat NUMERIC(38,8);
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS max_order_fiat NUMERIC(38,8);
UPDATE p2p_listings SET
	min_order_fiat=COALESCE(min_order_fiat,LEAST(0.01,round((amount_raw/1000000)*price,2))),
	max_order_fiat=COALESCE(max_order_fiat,round((amount_raw/1000000)*price,2));
ALTER TABLE p2p_listings ALTER COLUMN min_order_fiat SET NOT NULL;
ALTER TABLE p2p_listings ALTER COLUMN max_order_fiat SET NOT NULL;
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_order_limits_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_order_limits_check CHECK (
	min_order_fiat > 0 AND max_order_fiat >= min_order_fiat
	AND max_order_fiat <= round((amount_raw/1000000)*price,2)
);
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
	buyer_fee_raw NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (buyer_fee_raw >= 0),
	seller_fee_raw NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (seller_fee_raw >= 0),
	buyer_credit_raw NUMERIC(38,0) NOT NULL DEFAULT 1 CHECK (buyer_credit_raw > 0),
	seller_debit_raw NUMERIC(38,0) NOT NULL DEFAULT 1 CHECK (seller_debit_raw > 0),
	fee_model TEXT NOT NULL DEFAULT 'BIUSDB_1PCT_EACH' CHECK (fee_model IN ('LEGACY_FIAT','BIUSDB_1PCT_EACH')),
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
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS cancelled_by TEXT REFERENCES users(id) ON DELETE RESTRICT;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS payment_account_name TEXT NOT NULL DEFAULT '';
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS payment_account_identifier TEXT NOT NULL DEFAULT '';
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS payment_bank_name TEXT NOT NULL DEFAULT '';
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS payment_ifsc_code TEXT NOT NULL DEFAULT '';
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS payment_instructions TEXT NOT NULL DEFAULT '';
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS payment_marked_at TIMESTAMPTZ;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS buyer_own_account_attested BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS appeal_available_at TIMESTAMPTZ;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS appealed_by TEXT REFERENCES users(id) ON DELETE RESTRICT;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS appeal_reason TEXT;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS appealed_at TIMESTAMPTZ;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS buyer_fee_raw NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS seller_fee_raw NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS buyer_credit_raw NUMERIC(38,0);
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS seller_debit_raw NUMERIC(38,0);
ALTER TABLE p2p_orders ADD COLUMN IF NOT EXISTS fee_model TEXT;
UPDATE p2p_orders SET fee_model='LEGACY_FIAT' WHERE fee_model IS NULL;
UPDATE p2p_orders SET buyer_credit_raw=COALESCE(buyer_credit_raw,amount_raw,buyer_credit),seller_debit_raw=COALESCE(seller_debit_raw,escrow_raw,amount_raw,seller_debit);
ALTER TABLE p2p_orders ALTER COLUMN buyer_credit_raw SET NOT NULL;
ALTER TABLE p2p_orders ALTER COLUMN seller_debit_raw SET NOT NULL;
ALTER TABLE p2p_orders ALTER COLUMN fee_model SET DEFAULT 'BIUSDB_1PCT_EACH';
ALTER TABLE p2p_orders ALTER COLUMN fee_model SET NOT NULL;
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_fee_model_check;
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_fee_model_check CHECK (fee_model IN ('LEGACY_FIAT','BIUSDB_1PCT_EACH'));
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
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_asset_check CHECK (asset IN ('USDC','BIUSDB'));

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
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_asset_check CHECK (asset IN ('USDC','BIUSDB'));
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
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','BIUSDB')),
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
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','BIUSDB')),
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
ALTER TABLE p2p_wallet_balances ADD CONSTRAINT p2p_wallet_balances_asset_check CHECK (asset IN ('USDC','BIUSDB'));
ALTER TABLE p2p_wallet_entries DROP CONSTRAINT IF EXISTS p2p_wallet_entries_asset_check;
ALTER TABLE p2p_wallet_entries ADD CONSTRAINT p2p_wallet_entries_asset_check CHECK (asset IN ('USDC','BIUSDB'));

-- System-owned fee wallet. It deliberately has no user_id: customers cannot
-- authenticate as or spend from this account through the P2P wallet APIs.
CREATE TABLE IF NOT EXISTS p2p_admin_wallet_balances (
	asset TEXT PRIMARY KEY CHECK (asset = 'BIUSDB'),
	available_raw NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (available_raw >= 0),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO p2p_admin_wallet_balances(asset) VALUES('BIUSDB') ON CONFLICT(asset) DO NOTHING;
CREATE TABLE IF NOT EXISTS p2p_admin_wallet_entries (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	order_id UUID NOT NULL REFERENCES p2p_orders(id) ON DELETE RESTRICT,
	asset TEXT NOT NULL CHECK (asset = 'BIUSDB'),
	buyer_fee_raw NUMERIC(38,0) NOT NULL CHECK (buyer_fee_raw >= 0),
	seller_fee_raw NUMERIC(38,0) NOT NULL CHECK (seller_fee_raw >= 0),
	amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE(order_id)
);

CREATE TABLE IF NOT EXISTS p2p_payment_accounts (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	method TEXT NOT NULL CHECK (method IN ('UPI','Bank Transfer','MPESN','NEFT','IMPS')),
	account_name TEXT NOT NULL CHECK (char_length(account_name) BETWEEN 2 AND 100),
	account_identifier TEXT NOT NULL CHECK (char_length(account_identifier) BETWEEN 2 AND 200),
	bank_name TEXT NOT NULL DEFAULT '',
	ifsc_code TEXT NOT NULL DEFAULT '',
	instructions TEXT NOT NULL DEFAULT '' CHECK (char_length(instructions) <= 500),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE(user_id,method)
);
ALTER TABLE p2p_payment_accounts ADD COLUMN IF NOT EXISTS bank_name TEXT NOT NULL DEFAULT '';
ALTER TABLE p2p_payment_accounts ADD COLUMN IF NOT EXISTS ifsc_code TEXT NOT NULL DEFAULT '';
ALTER TABLE p2p_payment_accounts DROP CONSTRAINT IF EXISTS p2p_payment_accounts_bank_details_check;
ALTER TABLE p2p_payment_accounts ADD CONSTRAINT p2p_payment_accounts_bank_details_check CHECK (
	method NOT IN ('Bank Transfer','NEFT','IMPS') OR (
		char_length(bank_name) BETWEEN 2 AND 100
		AND ifsc_code ~ '^[A-Z]{4}0[A-Z0-9]{6}$'
	)
);

CREATE TABLE IF NOT EXISTS p2p_order_proofs (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	order_id UUID NOT NULL REFERENCES p2p_orders(id) ON DELETE RESTRICT,
	uploader_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
	file_name TEXT NOT NULL,
	mime_type TEXT NOT NULL CHECK (mime_type IN ('image/jpeg','image/png','image/webp','application/pdf')),
	file_data BYTEA NOT NULL,
	size_bytes BIGINT NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 5242880),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_p2p_order_proofs_order ON p2p_order_proofs(order_id,created_at);

CREATE TABLE IF NOT EXISTS p2p_order_messages (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	order_id UUID NOT NULL REFERENCES p2p_orders(id) ON DELETE RESTRICT,
	sender_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
	body TEXT NOT NULL CHECK (char_length(body) BETWEEN 1 AND 1000),
	is_system BOOLEAN NOT NULL DEFAULT false,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_p2p_order_messages_order ON p2p_order_messages(order_id,created_at);

CREATE TABLE IF NOT EXISTS p2p_order_events (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	order_id UUID NOT NULL REFERENCES p2p_orders(id) ON DELETE RESTRICT,
	actor_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
	kind TEXT NOT NULL,
	metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_p2p_order_events_order ON p2p_order_events(order_id,created_at);
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
	DROP COLUMN IF EXISTS "BI";

CREATE TABLE IF NOT EXISTS user_balances (
	balance_id BIGSERIAL PRIMARY KEY,
	user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	"USDC" NUMERIC(38,0) NOT NULL DEFAULT 0,
	"USDT" NUMERIC(38,0) NOT NULL DEFAULT 0,
	"BI" NUMERIC(38,0) NOT NULL DEFAULT 0,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE user_balances ALTER COLUMN user_id TYPE TEXT USING user_id::text;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDC" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDT" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BTC" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BI" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDC_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "USDT_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BTC_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BI_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- BIUSDB: the platform's internal stable quote currency, pegged 1:1 to USDT.
-- It has no on-chain contract; every market's quote leg trades in BIUSDB. The
-- USDT/USDC columns above remain only as the deposit-intake ledger (a real
-- on-chain deposit lands there first via the chain listener), not as a
-- tradable balance any more.
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BIUSDB" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BIUSDB_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;

-- BUSD removed: it was a dormant deposit-intake balance column never wired
-- into any market, deposit path, or swap conversion — fully retired. Any
-- lingering "BUSD"/"BUSD_locked" columns and their balances are folded into
-- BIUSDB (the platform's real stable-quote balance) before being dropped.
DO $drop_busd$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'user_balances' AND column_name = 'BUSD'
	) THEN
		UPDATE user_balances SET "BIUSDB" = "BIUSDB" + "BUSD", "BIUSDB_locked" = "BIUSDB_locked" + "BUSD_locked";
		ALTER TABLE user_balances DROP COLUMN "BUSD";
		ALTER TABLE user_balances DROP COLUMN IF EXISTS "BUSD_locked";
	END IF;
END $drop_busd$;

-- ETH, SOL, and BNB: base assets for the ETH-BIUSDB / SOL-BIUSDB / BNB-BIUSDB spot
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

-- BI2X: base asset for the BI2X-BIUSDB spot/futures pair (added 2026-09-12,
-- matching-engine's currentMarkets). Same pattern as ETH/SOL/BNB above —
-- added alongside the market's own registration this time, not after, so
-- deposits/MM funding never hit "unsupported asset" the way ETH/SOL/BNB did.
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BI2X" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BI2X_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;

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
					COALESCE(SUM(CASE WHEN UPPER(REPLACE(asset, '-', '_')) = 'BIUSDB' THEN total ELSE 0 END), 0) AS biusd,
					COALESCE(SUM(CASE WHEN UPPER(REPLACE(asset, '-', '_')) IN ('OUR_TOKEN', 'OURTOKEN') THEN total ELSE 0 END), 0) AS our_token,
					MIN(updated_at) AS created_at,
					MAX(updated_at) AS updated_at
				FROM user_balances
				GROUP BY user_id
			)
			UPDATE user_balances ub
			SET "USDC" = migrated.usdc,
				"USDT" = migrated.usdt,
				"BIUSDB" = migrated.biusd,
				"BI" = migrated.our_token,
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

// migrateUSDTToBIUSDB is the one-time conversion for the USDT→BIUSDB currency
// switch: every market's quote leg now trades in BIUSDB (pegged 1:1 to USDT,
// no on-chain contract of its own), so a balance sitting in the old
// tradable USDT column must move to BIUSDB or it becomes permanently
// inaccessible to trading. Converts at 1:1 and zeroes the USDT columns.
//
// Idempotent by construction, not by a migration-log flag: it only touches
// rows where USDT (or USDT_locked) is still nonzero, which is false after
// the first successful run, so re-running on every boot is a no-op. Must
// run after `schema` has created the BIUSDB columns.
const migrateUSDTToBIUSDB = `
UPDATE user_balances SET
	"BIUSDB" = "BIUSDB" + "USDT",
	"BIUSDB_locked" = "BIUSDB_locked" + "USDT_locked",
	"USDT" = 0,
	"USDT_locked" = 0,
	updated_at = now()
WHERE "USDT" > 0 OR "USDT_locked" > 0;
`

// ensureFeeConfigTables creates and seeds fee_config, fee_tiers, and
// user_fee_subscriptions — the same three tables matching-engine's
// internal/feeconfig and internal/discounts packages own (see
// FEE-TIER-SYSTEM-PLAN.md). Both services share one Postgres instance and
// either may boot first, so this is written identically idempotent
// (CREATE TABLE IF NOT EXISTS, ON CONFLICT DO NOTHING) — whichever service
// starts first creates the tables/seeds the defaults, and the other just
// finds them already there.
const ensureFeeConfigTables = `
CREATE TABLE IF NOT EXISTS fee_config (
    key        TEXT PRIMARY KEY,
    rate       NUMERIC(10,6) NOT NULL,
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO fee_config (key, rate) VALUES
    ('spot.maker', '0.0015'),
    ('spot.taker', '0.0045'),
    ('futures.maker', '0.00015'),
    ('futures.taker', '0.00045'),
    ('p2p.buyer', '0.0025'),
    ('p2p.seller', '0.0025'),
    ('swap.in', '0'),
    ('swap.out', '0.01'),
    ('liquidation', '0.02')
ON CONFLICT (key) DO NOTHING;

CREATE TABLE IF NOT EXISTS fee_tiers (
    tier         INT PRIMARY KEY,
    biusdb_value NUMERIC(20,2) NOT NULL,
    discount_pct NUMERIC(5,2) NOT NULL,
    active       BOOLEAN NOT NULL DEFAULT true
);

INSERT INTO fee_tiers (tier, biusdb_value, discount_pct) VALUES
    (1, '500', '5'),
    (2, '1000', '10'),
    (3, '5000', '15'),
    (4, '10000', '20'),
    (5, '20000', '25'),
    (6, '40000', '30'),
    (7, '60000', '35'),
    (8, '80000', '40'),
    (9, '100000', '45'),
    (10, '200000', '50')
ON CONFLICT (tier) DO NOTHING;

CREATE TABLE IF NOT EXISTS user_fee_subscriptions (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             TEXT NOT NULL,
    tier                INT NOT NULL REFERENCES fee_tiers(tier),
    bi2x_price_snapshot NUMERIC(20,8) NOT NULL,
    bi2x_amount_paid    NUMERIC(38,0) NOT NULL,
    purchased_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ NOT NULL,
    status              TEXT NOT NULL DEFAULT 'active',
    created_by_admin    BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_user_fee_subscriptions_lookup
    ON user_fee_subscriptions (user_id, status, expires_at);
`
