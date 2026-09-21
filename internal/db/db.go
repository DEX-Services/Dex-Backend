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

-- H4: nonce-replacement for stuck withdrawal transactions. A withdrawal that
-- gets stuck waiting to be mined (network congestion, a restart mid-wait)
-- used to be retried by asking the chain for a fresh nonce — which SKIPS the
-- still-pending nonce rather than freeing it, permanently jamming every
-- withdrawal after it. Persisting the exact nonce/fee actually submitted
-- lets a retry resubmit at that SAME nonce with a higher fee (the standard
-- "speed up/replace" pattern), which is the only way to actually unstick it.
ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS pending_nonce BIGINT;
ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS pending_fee_cap_wei NUMERIC(78,0);
ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS pending_tip_cap_wei NUMERIC(78,0);

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
		{"USDT to BI2XUSD", migrateUSDTToBI2XUSD},
		{"fee config tables", ensureFeeConfigTables},
		{"fee_model BIUSDB to BI2XUSD", migrateFeeModelBI2XUSD},
		{"fee_tiers biusdb_value to bi2xusd_value", migrateFeeTiersBI2XUSDValueColumn},
		{"user_balances BIUSDB to BI2XUSD", migrateUserBalancesBI2XUSDColumn},
		{"referral and affiliate tables", ensureReferralTables},
		{"platform_treasury_entries category column", ensureTreasuryEntryCategory},
		{"BI2X allocation tables", ensureBI2XAllocationTables},
		{"staking tables", ensureStakingTables},
		{"platform_treasury_entries prediction category", ensureTreasuryEntryPredictionCategory},
		{"internal balance idempotency keys table", ensureInternalIdempotencyKeysTable},
		{"prop-firm purchases table", ensurePropFirmPurchasesTable},
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
VALUES ('BI2XUSD', 'INR', 100, CURRENT_DATE)
ON CONFLICT (asset, fiat_currency, price_date) DO NOTHING;
-- Historical price rows recorded under the pre-rename asset name. No CHECK
-- constraint here to violate (this table has none on asset), so this is a
-- data-consistency cleanup rather than a required fix — old rows would
-- otherwise just sit under the old name forever.
UPDATE p2p_price_history SET asset = 'BI2XUSD' WHERE asset = 'BIUSDB';
CREATE TABLE IF NOT EXISTS p2p_listings (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(), seller_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','BI2XUSD')), amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0),
	remaining_raw NUMERIC(38,0) NOT NULL CHECK (remaining_raw >= 0), price NUMERIC(38,8) NOT NULL CHECK (price > 0),
	fiat_currency TEXT NOT NULL DEFAULT 'INR', payment_method TEXT NOT NULL CHECK (payment_method IN ('UPI', 'Bank Transfer', 'NEFT', 'IMPS')),
	status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FILLED', 'CANCELLED')),
	funding_source TEXT NOT NULL DEFAULT 'P2P_WALLET' CHECK (funding_source IN ('P2P_WALLET', 'MAIN_WALLET_LEGACY')),
	fee_model TEXT NOT NULL DEFAULT 'BI2XUSD_1PCT_EACH' CHECK (fee_model IN ('LEGACY_FIAT','BI2XUSD_1PCT_EACH')),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS side TEXT NOT NULL DEFAULT 'SELL';
ALTER TABLE p2p_listings ADD COLUMN IF NOT EXISTS fee_model TEXT;
UPDATE p2p_listings SET fee_model='LEGACY_FIAT' WHERE fee_model IS NULL;
ALTER TABLE p2p_listings ALTER COLUMN fee_model SET DEFAULT 'BI2XUSD_1PCT_EACH';
ALTER TABLE p2p_listings ALTER COLUMN fee_model SET NOT NULL;
-- Must run before the CHECK constraint below is (re)added: any row still
-- holding the pre-rename 'BIUSDB_1PCT_EACH' value would fail validation
-- against a constraint that only allows the new value. This makes the data
-- fix a prerequisite of the constraint, not a follow-up migration.
UPDATE p2p_listings SET fee_model = 'BI2XUSD_1PCT_EACH' WHERE fee_model = 'BIUSDB_1PCT_EACH';
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_fee_model_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_fee_model_check CHECK (fee_model IN ('LEGACY_FIAT','BI2XUSD_1PCT_EACH'));
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
	fee_model TEXT NOT NULL DEFAULT 'BI2XUSD_1PCT_EACH' CHECK (fee_model IN ('LEGACY_FIAT','BI2XUSD_1PCT_EACH')),
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
ALTER TABLE p2p_orders ALTER COLUMN fee_model SET DEFAULT 'BI2XUSD_1PCT_EACH';
ALTER TABLE p2p_orders ALTER COLUMN fee_model SET NOT NULL;
-- Must run before the CHECK constraint below is (re)added — see the
-- identical comment on p2p_listings above.
UPDATE p2p_orders SET fee_model = 'BI2XUSD_1PCT_EACH' WHERE fee_model = 'BIUSDB_1PCT_EACH';
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_fee_model_check;
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_fee_model_check CHECK (fee_model IN ('LEGACY_FIAT','BI2XUSD_1PCT_EACH'));
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
-- Must run before the CHECK constraint below: any row still holding the raw
-- pre-rename literal 'BIUSDB' in its asset column would fail validation
-- against a constraint that only allows 'BI2XUSD'.
UPDATE p2p_listings SET asset = 'BI2XUSD' WHERE asset = 'BIUSDB';
ALTER TABLE p2p_listings DROP CONSTRAINT IF EXISTS p2p_listings_asset_check;
ALTER TABLE p2p_listings ADD CONSTRAINT p2p_listings_asset_check CHECK (asset IN ('USDC','BI2XUSD'));

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
-- Must run before the CHECK constraint below — see the identical comment on
-- p2p_listings above.
UPDATE p2p_orders SET asset = 'BI2XUSD' WHERE asset = 'BIUSDB';
ALTER TABLE p2p_orders DROP CONSTRAINT IF EXISTS p2p_orders_asset_check;
ALTER TABLE p2p_orders ADD CONSTRAINT p2p_orders_asset_check CHECK (asset IN ('USDC','BI2XUSD'));
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
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','BI2XUSD')),
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
	asset TEXT NOT NULL DEFAULT 'USDC' CHECK (asset IN ('USDC','BI2XUSD')),
	amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0),
	idempotency_key TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_p2p_wallet_entries_user
	ON p2p_wallet_entries (user_id,created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_p2p_wallet_fund_idempotency
	ON p2p_wallet_entries (user_id,kind,idempotency_key)
	WHERE kind='main_to_p2p' AND idempotency_key IS NOT NULL;
-- Must run before the CHECK constraints below — see the identical comment
-- on p2p_listings above.
UPDATE p2p_wallet_balances SET asset = 'BI2XUSD' WHERE asset = 'BIUSDB';
UPDATE p2p_wallet_entries SET asset = 'BI2XUSD' WHERE asset = 'BIUSDB';
ALTER TABLE p2p_wallet_balances DROP CONSTRAINT IF EXISTS p2p_wallet_balances_asset_check;
ALTER TABLE p2p_wallet_balances ADD CONSTRAINT p2p_wallet_balances_asset_check CHECK (asset IN ('USDC','BI2XUSD'));
ALTER TABLE p2p_wallet_entries DROP CONSTRAINT IF EXISTS p2p_wallet_entries_asset_check;
ALTER TABLE p2p_wallet_entries ADD CONSTRAINT p2p_wallet_entries_asset_check CHECK (asset IN ('USDC','BI2XUSD'));

-- System-owned fee wallet. It deliberately has no user_id: customers cannot
-- authenticate as or spend from this account through the P2P wallet APIs.
CREATE TABLE IF NOT EXISTS p2p_admin_wallet_balances (
	asset TEXT PRIMARY KEY CHECK (asset = 'BI2XUSD'),
	available_raw NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (available_raw >= 0),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS p2p_admin_wallet_entries (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	order_id UUID NOT NULL REFERENCES p2p_orders(id) ON DELETE RESTRICT,
	asset TEXT NOT NULL CHECK (asset = 'BI2XUSD'),
	buyer_fee_raw NUMERIC(38,0) NOT NULL CHECK (buyer_fee_raw >= 0),
	seller_fee_raw NUMERIC(38,0) NOT NULL CHECK (seller_fee_raw >= 0),
	amount_raw NUMERIC(38,0) NOT NULL CHECK (amount_raw > 0),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE(order_id)
);
-- The two CREATE TABLE IF NOT EXISTS above are no-ops on a database where
-- these tables already exist from before the rename — their CHECK
-- constraints (baked into the column definition, not added separately)
-- still require the literal 'BIUSDB' on such a database, and the existing
-- admin-wallet row's primary key is still literally 'BIUSDB'. Both must be
-- fixed explicitly: rename the row's data first, then swap the constraint,
-- exactly the same ordering requirement as every other asset-check rename
-- above (data before constraint, or the ADD CONSTRAINT fails validation).
ALTER TABLE p2p_admin_wallet_balances DROP CONSTRAINT IF EXISTS p2p_admin_wallet_balances_asset_check;
UPDATE p2p_admin_wallet_balances SET asset = 'BI2XUSD' WHERE asset = 'BIUSDB';
ALTER TABLE p2p_admin_wallet_balances ADD CONSTRAINT p2p_admin_wallet_balances_asset_check CHECK (asset = 'BI2XUSD');
ALTER TABLE p2p_admin_wallet_entries DROP CONSTRAINT IF EXISTS p2p_admin_wallet_entries_asset_check;
UPDATE p2p_admin_wallet_entries SET asset = 'BI2XUSD' WHERE asset = 'BIUSDB';
ALTER TABLE p2p_admin_wallet_entries ADD CONSTRAINT p2p_admin_wallet_entries_asset_check CHECK (asset = 'BI2XUSD');
INSERT INTO p2p_admin_wallet_balances(asset) VALUES('BI2XUSD') ON CONFLICT(asset) DO NOTHING;

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
	file_data BYTEA,
	storage_key TEXT,
	size_bytes BIGINT NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 5242880),
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_p2p_order_proofs_order ON p2p_order_proofs(order_id,created_at);
-- P2P-M3: proofs now upload to object storage (Backblaze B2); file_data stays
-- nullable so existing rows keep working while storage_key is used for new ones.
ALTER TABLE p2p_order_proofs ALTER COLUMN file_data DROP NOT NULL;
ALTER TABLE p2p_order_proofs ADD COLUMN IF NOT EXISTS storage_key TEXT;

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

-- Every "BI"/"BI_locked" reference below (CREATE TABLE, ADD COLUMN, and the
-- legacy $wallet$ migration further down that still writes into it) must
-- stay intact: the platform's native "BI" token column is finally dropped
-- for good at the END of this migration (see the DROP COLUMN there), after
-- that legacy migration has had the chance to run on any database still
-- upgrading from the old wide-table schema. Removing it earlier here would
-- break that migration on such a database.
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

-- BI2XUSD: the platform's internal stable quote currency, pegged 1:1 to USDT.
-- It has no on-chain contract; every market's quote leg trades in BI2XUSD. The
-- USDT/USDC columns above remain only as the deposit-intake ledger (a real
-- on-chain deposit lands there first via the chain listener), not as a
-- tradable balance any more.
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BI2XUSD" NUMERIC(38,0) NOT NULL DEFAULT 0;
ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS "BI2XUSD_locked" NUMERIC(38,0) NOT NULL DEFAULT 0;

-- BUSD removed: it was a dormant deposit-intake balance column never wired
-- into any market, deposit path, or swap conversion — fully retired. Any
-- lingering "BUSD"/"BUSD_locked" columns and their balances are folded into
-- BI2XUSD (the platform's real stable-quote balance) before being dropped.
DO $drop_busd$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'user_balances' AND column_name = 'BUSD'
	) THEN
		UPDATE user_balances SET "BI2XUSD" = "BI2XUSD" + "BUSD", "BI2XUSD_locked" = "BI2XUSD_locked" + "BUSD_locked";
		ALTER TABLE user_balances DROP COLUMN "BUSD";
		ALTER TABLE user_balances DROP COLUMN IF EXISTS "BUSD_locked";
	END IF;
END $drop_busd$;

-- ETH, SOL, and BNB columns removed: the SPOT markets they backed
-- (ETH-BI2XUSD/SOL-BI2XUSD/BNB-BI2XUSD) were removed in the 2026-09-12
-- restructure — ETH and SOL now trade FUTURES-only (settled entirely in
-- BI2XUSD, never touching a base-asset column; see
-- matching-engine/internal/settlement/futures.go), and BNB has no market at
-- all any more. Unlike BUSD above, there is no equivalent asset to fold
-- these into (ETH/SOL/BNB are not interchangeable with BI2XUSD), so this
-- drops them outright rather than attempting a conversion.
ALTER TABLE user_balances DROP COLUMN IF EXISTS "ETH";
ALTER TABLE user_balances DROP COLUMN IF EXISTS "ETH_locked";
ALTER TABLE user_balances DROP COLUMN IF EXISTS "SOL";
ALTER TABLE user_balances DROP COLUMN IF EXISTS "SOL_locked";
ALTER TABLE user_balances DROP COLUMN IF EXISTS "BNB";
ALTER TABLE user_balances DROP COLUMN IF EXISTS "BNB_locked";

-- BI2X: base asset for the BI2X-BI2XUSD spot/futures pair (added 2026-09-12,
-- matching-engine's currentMarkets). Added alongside the market's own
-- registration, so deposits/MM funding never hit "unsupported asset" the way
-- ETH/SOL/BNB briefly did before their columns existed (see the ETH/SOL/BNB
-- ADD-then-DROP history below — those columns backed SPOT markets that were
-- later removed in the 2026-09-12 restructure, so the ADD is gone from here
-- and only the DROP remains).
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
					COALESCE(SUM(CASE WHEN UPPER(REPLACE(asset, '-', '_')) = 'BI2XUSD' THEN total ELSE 0 END), 0) AS biusd,
					COALESCE(SUM(CASE WHEN UPPER(REPLACE(asset, '-', '_')) IN ('OUR_TOKEN', 'OURTOKEN') THEN total ELSE 0 END), 0) AS our_token,
					MIN(updated_at) AS created_at,
					MAX(updated_at) AS updated_at
				FROM user_balances
				GROUP BY user_id
			)
			UPDATE user_balances ub
			SET "USDC" = migrated.usdc,
				"USDT" = migrated.usdt,
				"BI2XUSD" = migrated.biusd,
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

-- BI (the platform's own native token, formerly migrated from an older
-- "OUR_TOKEN"/"OURTOKEN" naming — see the $wallet$ migration block above,
-- which still needs this column to exist when it runs) removed 2026-09-13:
-- never wired into any matching-engine market, a wallet/ledger column with
-- nothing behind it, same as ETH/SOL/BNB before their removal. This drop
-- must stay AFTER the migration block above, since that block still writes
-- into "BI" on databases upgrading from the old wide-table schema.
ALTER TABLE user_balances DROP COLUMN IF EXISTS "BI";
ALTER TABLE user_balances DROP COLUMN IF EXISTS "BI_locked";
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

// migrateUSDTToBI2XUSD is the one-time conversion for the USDT→BI2XUSD currency
// switch: every market's quote leg now trades in BI2XUSD (pegged 1:1 to USDT,
// no on-chain contract of its own), so a balance sitting in the old
// tradable USDT column must move to BI2XUSD or it becomes permanently
// inaccessible to trading. Converts at 1:1 and zeroes the USDT columns.
//
// Idempotent by construction, not by a migration-log flag: it only touches
// rows where USDT (or USDT_locked) is still nonzero, which is false after
// the first successful run, so re-running on every boot is a no-op. Must
// run after `schema` has created the BI2XUSD columns.
const migrateUSDTToBI2XUSD = `
UPDATE user_balances SET
	"BI2XUSD" = "BI2XUSD" + "USDT",
	"BI2XUSD_locked" = "BI2XUSD_locked" + "USDT_locked",
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
    tier          INT PRIMARY KEY,
    bi2xusd_value NUMERIC(20,2) NOT NULL,
    discount_pct  NUMERIC(5,2) NOT NULL,
    active        BOOLEAN NOT NULL DEFAULT true
);

INSERT INTO fee_tiers (tier, bi2xusd_value, discount_pct) VALUES
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

// migrateFeeModelBI2XUSD is the one-time rename of the fee_model enum value
// BIUSDB_1PCT_EACH -> BI2XUSD_1PCT_EACH (part of the BIUSDB -> BI2XUSD
// identifier rename). The Go-level constant/string literal was renamed
// alongside this migration; existing rows still holding the old enum string
// must be updated too, since the CHECK constraints on both tables now only
// allow the new value.
//
// Idempotent by construction: the UPDATE only matches rows still holding the
// old value, which is none after the first successful run.
const migrateFeeModelBI2XUSD = `
UPDATE p2p_listings SET fee_model = 'BI2XUSD_1PCT_EACH' WHERE fee_model = 'BIUSDB_1PCT_EACH';
UPDATE p2p_orders SET fee_model = 'BI2XUSD_1PCT_EACH' WHERE fee_model = 'BIUSDB_1PCT_EACH';
`

// migrateFeeTiersBI2XUSDValueColumn renames fee_tiers.biusdb_value to
// bi2xusd_value (part of the BIUSDB -> BI2XUSD identifier rename). Guarded so
// it only runs if the old column still exists, so it is safe against a fresh
// database whose CREATE TABLE already used the new column name, and safe to
// re-run.
const migrateFeeTiersBI2XUSDValueColumn = `
DO $rename_fee_tiers_bi2xusd_value$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'fee_tiers' AND column_name = 'biusdb_value'
	) THEN
		ALTER TABLE public.fee_tiers RENAME COLUMN biusdb_value TO bi2xusd_value;
	END IF;
END $rename_fee_tiers_bi2xusd_value$;
`

// migrateUserBalancesBI2XUSDColumn renames the user_balances "BIUSDB" and
// "BIUSDB_locked" columns (holding real user balance data) to "BI2XUSD" and
// "BI2XUSD_locked" (part of the BIUSDB -> BI2XUSD identifier rename). Guarded
// so it only runs if the old columns still exist, so it is safe against a
// fresh database whose CREATE TABLE already used the new column names, and
// safe to re-run.
//
// Also handles a split state where BOTH "BIUSDB" (still holding the real
// data) and an empty "BI2XUSD" already coexist — this can happen if another
// service sharing this table (e.g. matching-engine, which also runs its own
// ADD COLUMN IF NOT EXISTS "BI2XUSD" as part of its own schema-ensure step)
// creates the new empty column before this migration gets a chance to
// rename the old one over it. A plain RENAME COLUMN would fail with
// "column already exists" in that case, so the empty new-name column (and
// its _locked twin) is dropped first if it holds no data, making room for
// the real rename.
const migrateUserBalancesBI2XUSDColumn = `
DO $rename_user_balances_bi2xusd$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'user_balances' AND column_name = 'BIUSDB'
	) THEN
		IF EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'user_balances' AND column_name = 'BI2XUSD'
		) THEN
			IF (SELECT COALESCE(SUM("BI2XUSD"), 0) + COALESCE(SUM("BI2XUSD_locked"), 0) FROM public.user_balances) = 0 THEN
				ALTER TABLE public.user_balances DROP COLUMN "BI2XUSD";
				ALTER TABLE public.user_balances DROP COLUMN IF EXISTS "BI2XUSD_locked";
			ELSE
				RAISE EXCEPTION 'user_balances has both "BIUSDB" and a non-empty "BI2XUSD" column; manual reconciliation required before this migration can proceed safely';
			END IF;
		END IF;
		ALTER TABLE public.user_balances RENAME COLUMN "BIUSDB" TO "BI2XUSD";
		ALTER TABLE public.user_balances RENAME COLUMN "BIUSDB_locked" TO "BI2XUSD_locked";
	END IF;
END $rename_user_balances_bi2xusd$;
`

// ensureReferralTables creates the schema for the referral/affiliate
// revenue-share feature (see REFERRAL-AFFILIATE-PLAN.md) and the platform
// treasury tables that make it possible: spot/futures trading fees
// previously had nowhere to go once debited from the paying user (a pure
// burn) — platform_treasury_balances/entries is the first real destination
// for that revenue, and referral_codes/affiliate_links/user_referral_links/
// referral_config layer the revenue-split on top of it.
const ensureReferralTables = `
-- The platform's own fee-revenue account. All spot/futures trading fees
-- land here (minus any referral/affiliate share carved out at the same
-- moment) instead of vanishing. Mirrors p2p_admin_wallet_balances'
-- existing shape/role for P2P fees.
CREATE TABLE IF NOT EXISTS platform_treasury_balances (
    asset         TEXT PRIMARY KEY,
    available_raw NUMERIC(38,0) NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO platform_treasury_balances (asset) VALUES ('BI2XUSD') ON CONFLICT (asset) DO NOTHING;

-- Audit trail: every fee collected and every referral/affiliate payout, so
-- "why does the treasury balance say X" and "why did this user's balance
-- just go up" both have a real record, not just an opaque balance change.
CREATE TABLE IF NOT EXISTS platform_treasury_entries (
    id         BIGSERIAL PRIMARY KEY,
    kind       TEXT NOT NULL CHECK (kind IN ('trading_fee', 'referral_payout', 'affiliate_payout')),
    asset      TEXT NOT NULL,
    amount_raw NUMERIC(38,0) NOT NULL,
    account_id TEXT REFERENCES users(id),
    trade_ref  TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_platform_treasury_entries_account
    ON platform_treasury_entries (account_id, created_at DESC);

-- The one global referral percentage (fixed 20% by default, admin-editable
-- for FUTURE referrals only -- see user_referral_links.share_pct, which
-- snapshots this value at signup so a later change never rewrites an
-- existing referral's terms).
CREATE TABLE IF NOT EXISTS referral_config (
    key        TEXT PRIMARY KEY,
    value      NUMERIC(5,2) NOT NULL,
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO referral_config (key, value) VALUES ('referral_share_pct', 20) ON CONFLICT (key) DO NOTHING;

-- One personal referral code per user, generated lazily on first request
-- (most users never share their link) rather than for every signup.
CREATE TABLE IF NOT EXISTS referral_codes (
    user_id    TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    code       TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Admin-created affiliate links, each with its own admin-chosen percentage.
-- One owner may hold multiple links (nothing in the requirements makes it
-- one-per-user, and admin-created is naturally many-to-one).
CREATE TABLE IF NOT EXISTS affiliate_links (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code          TEXT NOT NULL UNIQUE,
    owner_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    share_pct     NUMERIC(5,2) NOT NULL CHECK (share_pct >= 0 AND share_pct <= 100),
    active        BOOLEAN NOT NULL DEFAULT true,
    created_by    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The permanent, exclusive signup-time link: at most one earning source per
-- user, set once (during FindOrCreate's user-creation branch), never
-- updated afterward. share_pct is snapshotted at signup for the same reason
-- user_fee_subscriptions.bi2x_price_snapshot freezes a value at purchase --
-- a later admin change to referral_config or an affiliate_links.share_pct
-- must not retroactively rewrite an existing user's terms.
CREATE TABLE IF NOT EXISTS user_referral_links (
    user_id           TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    source_type       TEXT NOT NULL CHECK (source_type IN ('referral', 'affiliate')),
    referrer_id       TEXT REFERENCES users(id),
    affiliate_link_id UUID REFERENCES affiliate_links(id),
    share_pct         NUMERIC(5,2) NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_user_referral_links_referrer ON user_referral_links (referrer_id);
CREATE INDEX IF NOT EXISTS idx_user_referral_links_affiliate ON user_referral_links (affiliate_link_id);
`

// ensureTreasuryEntryCategory adds the category column the admin Fee Revenue
// page breaks totals down by (spot/futures/liquidation/swap) — separate from
// `kind`, which only distinguishes a trading fee from a referral/affiliate
// payout, not which trading surface the fee came from. NULL for rows written
// before this column existed (none in practice: this ships in the same
// release as the referral tables, so platform_treasury_entries is always
// empty the first time this runs) and for referral_payout/affiliate_payout
// rows, which aren't a fee-revenue category of their own.
const ensureTreasuryEntryCategory = `
DO $add_treasury_entry_category$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'platform_treasury_entries' AND column_name = 'category'
    ) THEN
        ALTER TABLE public.platform_treasury_entries ADD COLUMN category TEXT
            CHECK (category IS NULL OR category IN ('spot', 'futures', 'liquidation', 'swap'));
        CREATE INDEX IF NOT EXISTS idx_platform_treasury_entries_category
            ON public.platform_treasury_entries (category, created_at DESC);
    END IF;
END $add_treasury_entry_category$;
`

// ensureBI2XAllocationTables creates the schema for the admin-facing BI2X
// token allocation feature: a running "remaining quantity" per allocation
// category (Initial Burn, Team Reserve, Community, Airdrop, Marketing,
// Treasury Reserve, Initial Liquidity, Staking Reward), seeded from the
// fixed 500,000,000 total supply at the original 6/5/2/3/4/4/2/74 percent
// split, plus a permanent history log of every burn/distribution recorded
// against a category (each of which also decrements that category's
// remaining total — see repo.BI2XAllocationRepo.AddHistoryEntry).
//
// Plain whole-token NUMERIC, not the raw-integer-scaled NUMERIC(38,0) used
// for on-chain-mirrored wallet balances elsewhere (e.g.
// platform_treasury_balances.available_raw, user_balances) — this table is
// an admin bookkeeping/display feature over a fixed, already-minted supply,
// not a real on-chain or ledger balance subject to RawToHumanUnits-style
// unit conversion, so introducing that scale here would be a false
// consistency with a system this table isn't actually part of.
const ensureBI2XAllocationTables = `
CREATE TABLE IF NOT EXISTS bi2x_allocation_balances (
    category      TEXT PRIMARY KEY,
    remaining_qty NUMERIC(20,0) NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 500,000,000 total supply at 6/5/2/3/4/4/2/74 percent -- sums to exactly
-- 500,000,000. Seeded once; ON CONFLICT DO NOTHING means a later admin
-- distribution recorded against a category (which decrements this row) is
-- never overwritten back to its starting value by a subsequent restart.
INSERT INTO bi2x_allocation_balances (category, remaining_qty) VALUES
    ('Initial Burn', 30000000),
    ('Team Reserve', 25000000),
    ('Community', 10000000),
    ('Airdrop', 15000000),
    ('Marketing', 20000000),
    ('Treasury Reserve', 20000000),
    ('Initial Liquidity', 10000000),
    ('Staking Reward', 370000000)
ON CONFLICT (category) DO NOTHING;

-- Permanent audit trail: every burn/distribution an admin records against a
-- category, so "why is Staking Reward's remaining total lower than 370M"
-- always has a real, dated answer instead of just an opaque running number.
CREATE TABLE IF NOT EXISTS bi2x_allocation_history (
    id         BIGSERIAL PRIMARY KEY,
    category   TEXT NOT NULL REFERENCES bi2x_allocation_balances(category),
    amount_qty NUMERIC(20,0) NOT NULL CHECK (amount_qty > 0),
    event_date TIMESTAMPTZ NOT NULL DEFAULT now(),
    note       TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bi2x_allocation_history_category
    ON bi2x_allocation_history (category, event_date DESC);
`

// ensureStakingTables creates the schema for BI2X staking: 5% APR simple
// interest, no lock-up period, BI2X-only. A user's stake is debited straight
// out of their tradable BI2X wallet balance (see LedgerRepo.StakeBI2X) into
// a separate staking_positions row — NOT the same "locked" column used by
// order reservations elsewhere, since staked BI2X is meant to be entirely
// out of the tradable wallet, not merely held against an open order.
//
// Interest is deliberately NOT stored/accrued as a running column: simple
// interest at a fixed 5% APR is fully determined by principal_raw and
// started_at alone (interest = principal * 0.05 * hours_elapsed / 8760), so
// it's computed on demand — by the frontend for live display (see
// stakingApi.ts), and authoritatively by the backend at redeem time
// (LedgerRepo.RedeemBI2X) — rather than needing a background job to credit
// it hourly into a stored column. "Credited every hour" is satisfied by the
// number being live-recalculated continuously, not by a scheduled write.
//
// A partial redemption reduces principal_raw and pays out that portion's
// accrued interest, but leaves started_at UNCHANGED for the remainder — the
// remaining principal keeps accruing from its original start time,
// uninterrupted, per product decision (2026-09-16).
const ensureStakingTables = `
CREATE TABLE IF NOT EXISTS staking_positions (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       TEXT NOT NULL REFERENCES users(id),
    asset         TEXT NOT NULL DEFAULT 'BI2X' CHECK (asset = 'BI2X'),
    principal_raw NUMERIC(38,0) NOT NULL CHECK (principal_raw > 0),
    apr_bps       INTEGER NOT NULL DEFAULT 500, -- 500 basis points = 5.00% APR, fixed at stake time so a later admin rate change never rewrites an existing stake's terms
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    status        TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'redeemed')),
    closed_at     TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_staking_positions_user
    ON staking_positions (user_id, status, started_at DESC);

-- Permanent audit trail: every stake and every (partial or full) redemption,
-- so "why did my staking position's principal change" and "how much
-- interest have I actually been paid" both have a real, dated record.
CREATE TABLE IF NOT EXISTS staking_events (
    id              BIGSERIAL PRIMARY KEY,
    position_id     UUID NOT NULL REFERENCES staking_positions(id),
    user_id         TEXT NOT NULL REFERENCES users(id),
    kind            TEXT NOT NULL CHECK (kind IN ('stake', 'redeem')),
    principal_raw   NUMERIC(38,0) NOT NULL, -- amount staked, or amount of principal redeemed
    interest_raw    NUMERIC(38,0) NOT NULL DEFAULT 0, -- 0 for 'stake' events; the interest paid out for 'redeem' events
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_staking_events_user
    ON staking_events (user_id, created_at DESC);
`

// ensureTreasuryEntryPredictionCategory widens platform_treasury_entries'
// category CHECK constraint (added by ensureTreasuryEntryCategory above,
// originally 'spot'|'futures'|'liquidation'|'swap') to also accept
// 'prediction', so the new prediction-service can route its maker/taker fee
// revenue into this same shared treasury ledger with its own honest
// category label instead of being silently miscounted as a different
// market's fee revenue.
//
// The original constraint was added inline via ALTER TABLE ... ADD COLUMN
// ... CHECK (...) with no explicit name, so Postgres auto-generated one.
// This migration looks that name up from the system catalog rather than
// guessing/hardcoding it, drops it, and adds the widened one — idempotent
// and safe to re-run (checks the constraint's current definition first, so
// it never re-runs the drop+add after the first successful application).
// ensureInternalIdempotencyKeysTable backs dedup on the internal balance
// endpoints (/internal/balance/{credit,fee,lock,unlock}) that matching-engine,
// the futures engine, and prediction-service all call. A caller may retry
// one of these calls after losing the response to a timeout/network error
// without knowing whether the original attempt actually landed; without this
// table a retry just re-applies the balance change a second time. Callers
// that opt in send an Idempotency-Key header; the key (scoped by endpoint,
// since the same key string must not collide across different call types)
// is checked and inserted inside the SAME transaction as the balance
// mutation, so a retry either finds the already-committed row and skips the
// mutation, or the whole thing rolls back together on failure.
const ensureInternalIdempotencyKeysTable = `
CREATE TABLE IF NOT EXISTS internal_idempotency_keys (
    endpoint    TEXT NOT NULL,
    key         TEXT NOT NULL,
    request     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (endpoint, key)
);
`

const ensureTreasuryEntryPredictionCategory = `
DO $widen_treasury_entry_category$
DECLARE
    constraint_name TEXT;
BEGIN
    SELECT con.conname INTO constraint_name
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    WHERE rel.relname = 'platform_treasury_entries'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) LIKE '%category%'
      AND pg_get_constraintdef(con.oid) NOT LIKE '%prediction%';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE public.platform_treasury_entries DROP CONSTRAINT %I', constraint_name);
        ALTER TABLE public.platform_treasury_entries ADD CONSTRAINT platform_treasury_entries_category_check
            CHECK (category IS NULL OR category IN ('spot', 'futures', 'liquidation', 'swap', 'prediction'));
    END IF;
END $widen_treasury_entry_category$;
`

// ensurePropFirmPurchasesTable backs POST /prop-firm/purchase
// (PROP_FIRM_PLAN.md section 3, the exchange side of the one integration
// point with the standalone BitDX Prop Firm backend). This is deliberately
// a SEPARATE table from that backend's own pf_purchases — this row is the
// exchange's own record that "user X paid Y BI2XUSD for package Z", created
// and updated entirely within this database/service, independent of
// whatever the prop-firm backend does with the externalRef afterwards.
//
// id is what's sent as externalRef to POST /internal/provision: generating
// it BEFORE calling that endpoint (status starts 'pending') means a crash
// or timeout between the debit and the provision call leaves a durable,
// idempotency-key-able record to retry against, rather than a debited
// wallet with no trace of what it paid for.
const ensurePropFirmPurchasesTable = `
CREATE TABLE IF NOT EXISTS prop_firm_purchases (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         TEXT NOT NULL REFERENCES users(id),
    package_id      TEXT NOT NULL,
    price_bi2xusd   NUMERIC(38,0) NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'fulfilled', 'failed', 'refund_needed')),
    prop_firm_account_id TEXT,
    fail_reason     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_prop_firm_purchases_user ON prop_firm_purchases (user_id, created_at DESC);
`
