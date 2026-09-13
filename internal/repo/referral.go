package repo

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReferralRepo handles the referral/affiliate revenue-share feature and the
// platform treasury it's built on top of — see REFERRAL-AFFILIATE-PLAN.md.
type ReferralRepo struct {
	pool   *pgxpool.Pool
	ledger *LedgerRepo
}

func NewReferralRepo(pool *pgxpool.Pool, ledger *LedgerRepo) *ReferralRepo {
	return &ReferralRepo{pool: pool, ledger: ledger}
}

// ReferralSource describes what a code passed at signup resolved to, if
// anything — used by UserRepo.FindOrCreate to write the permanent
// user_referral_links row at the moment a new user is created.
type ReferralSource struct {
	SourceType      string // "referral" or "affiliate"
	ReferrerID      string // set when SourceType == "referral"
	AffiliateLinkID string // set when SourceType == "affiliate"
	SharePct        string // snapshotted at the moment of lookup
}

// ResolveSignupCode looks up code against referral_codes first, then
// affiliate_links (active only). Returns (nil, nil) if the code matches
// neither — a new user with an unrecognized or absent code simply gets no
// earning source, ever (the link can only be set at creation).
func (r *ReferralRepo) ResolveSignupCode(ctx context.Context, tx pgx.Tx, code string) (*ReferralSource, error) {
	code = strings.TrimSpace(code)
	if code == "" {
		return nil, nil
	}

	var referrerID string
	err := tx.QueryRow(ctx, `SELECT user_id FROM referral_codes WHERE code = $1`, code).Scan(&referrerID)
	if err == nil {
		pct, err := r.currentReferralSharePctTx(ctx, tx)
		if err != nil {
			return nil, err
		}
		return &ReferralSource{SourceType: "referral", ReferrerID: referrerID, SharePct: pct}, nil
	}
	if err != pgx.ErrNoRows {
		return nil, fmt.Errorf("lookup referral code: %w", err)
	}

	var linkID, sharePct string
	err = tx.QueryRow(ctx,
		`SELECT id, share_pct::text FROM affiliate_links WHERE code = $1 AND active = true`, code,
	).Scan(&linkID, &sharePct)
	if err == nil {
		return &ReferralSource{SourceType: "affiliate", AffiliateLinkID: linkID, SharePct: sharePct}, nil
	}
	if err != pgx.ErrNoRows {
		return nil, fmt.Errorf("lookup affiliate link: %w", err)
	}
	return nil, nil
}

// LinkNewUserTx records userID's permanent earning-source link. Must be
// called inside the SAME transaction as the user's own INSERT, and only
// from the create branch — a returning user is never (re-)linked, since
// user_referral_links.user_id is a primary key: a second insert for the
// same user fails, which is the correct behavior (defense in depth beyond
// the caller only invoking this once).
func (r *ReferralRepo) LinkNewUserTx(ctx context.Context, tx pgx.Tx, userID string, source *ReferralSource) error {
	if source == nil {
		return nil
	}
	var referrerID, affiliateLinkID any
	if source.SourceType == "referral" {
		referrerID = source.ReferrerID
	} else {
		affiliateLinkID = source.AffiliateLinkID
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO user_referral_links (user_id, source_type, referrer_id, affiliate_link_id, share_pct)
		 VALUES ($1, $2, $3, $4, $5)`,
		userID, source.SourceType, referrerID, affiliateLinkID, source.SharePct,
	)
	return err
}

func (r *ReferralRepo) currentReferralSharePctTx(ctx context.Context, tx pgx.Tx) (string, error) {
	var pct string
	err := tx.QueryRow(ctx, `SELECT value::text FROM referral_config WHERE key = 'referral_share_pct'`).Scan(&pct)
	if err != nil {
		return "", fmt.Errorf("load referral_config: %w", err)
	}
	return pct, nil
}

// earningSourceTx returns the beneficiary account id and share fraction (as
// a string suitable for big.Int-free decimal math elsewhere — callers doing
// raw-integer fee math should use SplitFee, not this directly) for userID's
// permanent link, or (nil, false) if userID has none.
type earningSource struct {
	beneficiaryID string
	sharePct      string // e.g. "20.00"
	kind          string // "referral_payout" or "affiliate_payout"
}

func (r *ReferralRepo) earningSourceTx(ctx context.Context, tx pgx.Tx, userID string) (*earningSource, error) {
	var sourceType, sharePct string
	var referrerID, ownerUserID *string
	err := tx.QueryRow(ctx, `
		SELECT l.source_type, l.share_pct::text, l.referrer_id, al.owner_user_id
		FROM user_referral_links l
		LEFT JOIN affiliate_links al ON al.id = l.affiliate_link_id
		WHERE l.user_id = $1`, userID,
	).Scan(&sourceType, &sharePct, &referrerID, &ownerUserID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load earning source: %w", err)
	}
	if sourceType == "referral" {
		if referrerID == nil {
			return nil, nil
		}
		return &earningSource{beneficiaryID: *referrerID, sharePct: sharePct, kind: "referral_payout"}, nil
	}
	if ownerUserID == nil {
		return nil, nil
	}
	return &earningSource{beneficiaryID: *ownerUserID, sharePct: sharePct, kind: "affiliate_payout"}, nil
}

// SettleFee is the single entry point for routing one account's trading
// fee: split into a beneficiary payout (if the fee-paying account has a
// referral/affiliate link) and a treasury share, both applied in one
// transaction, with an audit-trail entry for each leg. asset is always
// "BI2XUSD" today (the only fee-denominated asset — see
// REFERRAL-AFFILIATE-PLAN.md §10.2), tradeRef is an optional order/trade id
// for the audit trail. category tags which trading surface this fee came
// from ("spot", "futures", "liquidation", or "swap") for the admin Fee
// Revenue breakdown page — it does not affect the split math at all.
//
// This does NOT touch the fee-paying account's own balance — the caller
// (matching-engine, via the engine bridge) already debited that separately;
// this only decides where the collected fee goes.
func (r *ReferralRepo) SettleFee(ctx context.Context, payerUserID, asset, amountRaw, tradeRef, category string) error {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return nil // zero/negative fee: nothing to route, not an error
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	source, err := r.earningSourceTx(ctx, tx, payerUserID)
	if err != nil {
		return err
	}

	total, ok := new(big.Int).SetString(amountRaw, 10)
	if !ok {
		return fmt.Errorf("invalid fee amount %q", amountRaw)
	}

	payout := new(big.Int)
	if source != nil {
		payout = splitRawByPercent(total, source.sharePct)
	}
	treasuryCut := new(big.Int).Sub(total, payout)

	if source != nil && payout.Sign() > 0 {
		if err := r.ledger.creditBalanceTx(ctx, tx, source.beneficiaryID, asset, payout.String()); err != nil {
			return fmt.Errorf("credit %s beneficiary: %w", source.kind, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO platform_treasury_entries (kind, asset, amount_raw, account_id, trade_ref, category) VALUES ($1, $2, $3, $4, $5, $6)`,
			source.kind, asset, payout.String(), source.beneficiaryID, nullIfEmpty(tradeRef), nullIfEmpty(category),
		); err != nil {
			return fmt.Errorf("log %s entry: %w", source.kind, err)
		}
	}

	if treasuryCut.Sign() > 0 {
		if _, err := tx.Exec(ctx,
			`INSERT INTO platform_treasury_balances (asset, available_raw) VALUES ($1, $2)
			 ON CONFLICT (asset) DO UPDATE SET available_raw = platform_treasury_balances.available_raw + $2, updated_at = now()`,
			asset, treasuryCut.String(),
		); err != nil {
			return fmt.Errorf("credit treasury: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO platform_treasury_entries (kind, asset, amount_raw, account_id, trade_ref, category) VALUES ('trading_fee', $1, $2, $3, $4, $5)`,
			asset, treasuryCut.String(), payerUserID, nullIfEmpty(tradeRef), nullIfEmpty(category),
		); err != nil {
			return fmt.Errorf("log trading_fee entry: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// CreditTreasuryFee records a fee that goes to the platform treasury in
// full, with no referral/affiliate split — used for swap fees, which the
// product spec explicitly excludes from revenue-sharing (only spot/futures
// trading fees split; see REFERRAL-AFFILIATE-PLAN.md). category tags it for
// the admin Fee Revenue breakdown (e.g. "swap").
func (r *ReferralRepo) CreditTreasuryFee(ctx context.Context, asset, amountRaw, accountID, tradeRef, category string) error {
	if err := validatePositiveAmount(amountRaw); err != nil {
		return nil
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO platform_treasury_balances (asset, available_raw) VALUES ($1, $2)
		 ON CONFLICT (asset) DO UPDATE SET available_raw = platform_treasury_balances.available_raw + $2, updated_at = now()`,
		asset, amountRaw,
	); err != nil {
		return fmt.Errorf("credit treasury: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO platform_treasury_entries (kind, asset, amount_raw, account_id, trade_ref, category) VALUES ('trading_fee', $1, $2, $3, $4, $5)`,
		asset, amountRaw, nullIfEmpty(accountID), nullIfEmpty(tradeRef), nullIfEmpty(category),
	); err != nil {
		return fmt.Errorf("log treasury fee entry: %w", err)
	}
	return tx.Commit(ctx)
}

// splitRawByPercent returns floor(total * pct / 100) as a raw integer,
// parsing pct (e.g. "20.00") as a decimal string without pulling in a
// decimal library for one multiply — pct always has at most 2 fractional
// digits (NUMERIC(5,2)), so scaling by 100 and using integer math is exact.
func splitRawByPercent(total *big.Int, pct string) *big.Int {
	whole, frac, _ := strings.Cut(strings.TrimSpace(pct), ".")
	if whole == "" {
		whole = "0"
	}
	for len(frac) < 2 {
		frac += "0"
	}
	frac = frac[:2]
	scaled, ok := new(big.Int).SetString(whole+frac, 10) // pct * 100, e.g. "20.00" -> 2000
	if !ok {
		return new(big.Int)
	}
	out := new(big.Int).Mul(total, scaled)
	out.Div(out, big.NewInt(10000)) // undo the *100 (pct) * 100 (frac digits) scaling
	return out
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// referralCodeAlphabet excludes visually ambiguous characters (0/O, 1/I/L).
const referralCodeAlphabet = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"

// MyReferralCode returns userID's personal referral code, generating and
// persisting one on first request (most users never share their link, so
// this isn't done eagerly at signup).
func (r *ReferralRepo) MyReferralCode(ctx context.Context, userID string) (string, error) {
	var code string
	err := r.pool.QueryRow(ctx, `SELECT code FROM referral_codes WHERE user_id = $1`, userID).Scan(&code)
	if err == nil {
		return code, nil
	}
	if err != pgx.ErrNoRows {
		return "", err
	}
	for attempt := 0; attempt < 5; attempt++ {
		candidate, genErr := generateReferralCode()
		if genErr != nil {
			return "", genErr
		}
		err = r.pool.QueryRow(ctx,
			`INSERT INTO referral_codes (user_id, code) VALUES ($1, $2)
			 ON CONFLICT (user_id) DO NOTHING
			 RETURNING code`,
			userID, candidate,
		).Scan(&code)
		if err == nil {
			return code, nil
		}
		if err != pgx.ErrNoRows {
			return "", err
		}
		// ON CONFLICT DO NOTHING + no RETURNING row means either the code
		// collided (rare, retry with a fresh one) or another concurrent
		// request already created this user's row — check which.
		if existing, lookupErr := r.lookupCode(ctx, userID); lookupErr == nil && existing != "" {
			return existing, nil
		}
	}
	return "", fmt.Errorf("could not generate a unique referral code after several attempts")
}

func (r *ReferralRepo) lookupCode(ctx context.Context, userID string) (string, error) {
	var code string
	err := r.pool.QueryRow(ctx, `SELECT code FROM referral_codes WHERE user_id = $1`, userID).Scan(&code)
	return code, err
}

func generateReferralCode() (string, error) {
	buf := make([]byte, 7)
	for i := range buf {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(referralCodeAlphabet))))
		if err != nil {
			return "", err
		}
		buf[i] = referralCodeAlphabet[n.Int64()]
	}
	return "DEX-" + string(buf), nil
}

// ReferredCount returns how many users signed up through userID's own
// referral link, ever.
func (r *ReferralRepo) ReferredCount(ctx context.Context, userID string) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM user_referral_links WHERE source_type = 'referral' AND referrer_id = $1`,
		userID,
	).Scan(&count)
	return count, err
}

// ReferralEarnings returns the all-time total (raw units) userID has earned
// as a referrer.
func (r *ReferralRepo) ReferralEarnings(ctx context.Context, userID string) (string, error) {
	var total string
	err := r.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount_raw), 0)::text FROM platform_treasury_entries WHERE kind = 'referral_payout' AND account_id = $1`,
		userID,
	).Scan(&total)
	return total, err
}

// AffiliateLinkSummary is one admin-created link plus its lifetime stats,
// for both the owner's own "my affiliate links" view and the admin listing.
type AffiliateLinkSummary struct {
	ID           string
	Code         string
	OwnerUserID  string
	SharePct     string
	Active       bool
	JoinedCount  int
	EarningsRaw  string
	CreatedAt    string
}

// AffiliateLinksForOwner returns every link owned by userID.
func (r *ReferralRepo) AffiliateLinksForOwner(ctx context.Context, userID string) ([]AffiliateLinkSummary, error) {
	return r.queryAffiliateLinks(ctx, `WHERE al.owner_user_id = $1`, userID)
}

// AllAffiliateLinks returns every affiliate link on the platform, for the
// admin listing page.
func (r *ReferralRepo) AllAffiliateLinks(ctx context.Context) ([]AffiliateLinkSummary, error) {
	return r.queryAffiliateLinks(ctx, ``)
}

func (r *ReferralRepo) queryAffiliateLinks(ctx context.Context, whereClause string, args ...any) ([]AffiliateLinkSummary, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT al.id::text, al.code, al.owner_user_id, al.share_pct::text, al.active, al.created_at::text,
		       COALESCE((SELECT count(*) FROM user_referral_links l WHERE l.affiliate_link_id = al.id), 0),
		       COALESCE((SELECT SUM(amount_raw) FROM platform_treasury_entries WHERE kind = 'affiliate_payout' AND account_id = al.owner_user_id), 0)::text
		FROM affiliate_links al
		`+whereClause+`
		ORDER BY al.created_at DESC`, args...)
	if err != nil {
		return nil, fmt.Errorf("query affiliate_links: %w", err)
	}
	defer rows.Close()

	var out []AffiliateLinkSummary
	for rows.Next() {
		var s AffiliateLinkSummary
		if err := rows.Scan(&s.ID, &s.Code, &s.OwnerUserID, &s.SharePct, &s.Active, &s.CreatedAt, &s.JoinedCount, &s.EarningsRaw); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// CreateAffiliateLink is the admin action: pick an owner, set a percentage.
func (r *ReferralRepo) CreateAffiliateLink(ctx context.Context, ownerUserID, sharePct, createdBy string) (AffiliateLinkSummary, error) {
	code, err := generateAffiliateCode()
	if err != nil {
		return AffiliateLinkSummary{}, err
	}
	var s AffiliateLinkSummary
	err = r.pool.QueryRow(ctx,
		`INSERT INTO affiliate_links (code, owner_user_id, share_pct, created_by)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id::text, code, owner_user_id, share_pct::text, active, created_at::text`,
		code, ownerUserID, sharePct, nullIfEmpty(createdBy),
	).Scan(&s.ID, &s.Code, &s.OwnerUserID, &s.SharePct, &s.Active, &s.CreatedAt)
	return s, err
}

// SetAffiliateLinkActive deactivates (or reactivates) a link. Deactivating
// only stops NEW signups from using the code — already-linked users keep
// their permanent share_pct forever, per the "lifetime, never changed"
// requirement.
func (r *ReferralRepo) SetAffiliateLinkActive(ctx context.Context, linkID string, active bool) error {
	tag, err := r.pool.Exec(ctx, `UPDATE affiliate_links SET active = $2 WHERE id = $1`, linkID, active)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// ReferralSharePct returns the current global referral percentage.
func (r *ReferralRepo) ReferralSharePct(ctx context.Context) (string, error) {
	var pct string
	err := r.pool.QueryRow(ctx, `SELECT value::text FROM referral_config WHERE key = 'referral_share_pct'`).Scan(&pct)
	return pct, err
}

// SetReferralSharePct updates the global referral percentage. Only affects
// referrals created AFTER this call — existing user_referral_links rows
// keep their own snapshotted share_pct forever.
func (r *ReferralRepo) SetReferralSharePct(ctx context.Context, pct, updatedBy string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE referral_config SET value = $1, updated_by = $2, updated_at = now() WHERE key = 'referral_share_pct'`,
		pct, updatedBy,
	)
	return err
}

// FeeRevenueTotals is the admin Fee Revenue page's breakdown: gross fees
// collected per trading surface, all-time, in raw BI2XUSD units. "Gross"
// here means the full fee collected from the paying user, before any
// referral/affiliate share was carved out of it — i.e. spot+futures totals
// already include whatever was paid out to a referrer/affiliate owner, not
// just what the treasury kept. P2P is tracked in a separate table
// (p2p_admin_wallet_entries) since P2P fees never touch platform_treasury_*
// at all — see FeeRevenueTotals in the caller for how it's combined.
type FeeRevenueTotals struct {
	SpotRaw        string
	FuturesRaw     string
	LiquidationRaw string
	SwapRaw        string
	P2PRaw         string
	TotalRaw       string
}

// FeeRevenueTotals sums platform_treasury_entries by category (the gross
// fee collected, i.e. treasury cut + any referral/affiliate payout carved
// out of it, added back together) plus p2p_admin_wallet_entries separately,
// since P2P fees never flow through the treasury tables at all. since, if
// non-nil, restricts both sums to entries created at or after that instant
// (the admin page's 1h/1d/1w/1m windows); nil means all-time.
func (r *ReferralRepo) FeeRevenueTotals(ctx context.Context, since *time.Time) (FeeRevenueTotals, error) {
	var out FeeRevenueTotals
	rows, err := r.pool.Query(ctx, `
		SELECT category, COALESCE(SUM(amount_raw), 0)::text
		FROM platform_treasury_entries
		WHERE category IS NOT NULL AND ($1::timestamptz IS NULL OR created_at >= $1)
		GROUP BY category`, since)
	if err != nil {
		return out, fmt.Errorf("query treasury fee totals: %w", err)
	}
	totals := map[string]string{}
	for rows.Next() {
		var category, total string
		if err := rows.Scan(&category, &total); err != nil {
			rows.Close()
			return out, err
		}
		totals[category] = total
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	rows.Close()

	zero := func(s string) string {
		if s == "" {
			return "0"
		}
		return s
	}
	out.SpotRaw = zero(totals["spot"])
	out.FuturesRaw = zero(totals["futures"])
	out.LiquidationRaw = zero(totals["liquidation"])
	out.SwapRaw = zero(totals["swap"])

	if err := r.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(buyer_fee_raw + seller_fee_raw), 0)::text FROM p2p_admin_wallet_entries
		 WHERE $1::timestamptz IS NULL OR created_at >= $1`, since,
	).Scan(&out.P2PRaw); err != nil {
		return out, fmt.Errorf("query p2p fee totals: %w", err)
	}

	sum := new(big.Int)
	for _, v := range []string{out.SpotRaw, out.FuturesRaw, out.LiquidationRaw, out.SwapRaw, out.P2PRaw} {
		n, ok := new(big.Int).SetString(v, 10)
		if !ok {
			return out, fmt.Errorf("invalid fee total %q", v)
		}
		sum.Add(sum, n)
	}
	out.TotalRaw = sum.String()
	return out, nil
}

func generateAffiliateCode() (string, error) {
	buf := make([]byte, 6)
	for i := range buf {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(referralCodeAlphabet))))
		if err != nil {
			return "", err
		}
		buf[i] = referralCodeAlphabet[n.Int64()]
	}
	return "AFF-" + string(buf), nil
}
