// Package api: referral/affiliate revenue-share endpoints (user-facing) and
// admin endpoints for managing affiliate links and the global referral
// percentage. See REFERRAL-AFFILIATE-PLAN.md for the full design.
package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/dex/dex-backend/internal/repo"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// ReferralServer exposes the referral/affiliate flow (any signed-in user for
// their own data) and admin endpoints (admin only, same gate as FeeServer's).
type ReferralServer struct {
	*Server
	Referrals *repo.ReferralRepo
}

// MyReferral: GET /referral/me — the caller's own referral code, link, and
// referred-user count. Creates the code on first request.
func (s *ReferralServer) MyReferral(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	code, err := s.Referrals.MyReferralCode(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load referral code")
		return
	}
	count, err := s.Referrals.ReferredCount(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load referral count")
		return
	}
	earnings, err := s.Referrals.ReferralEarnings(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load referral earnings")
		return
	}
	sharePct, err := s.Referrals.ReferralSharePct(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load referral share")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"code":          code,
		"referredCount": count,
		"earningsRaw":   earnings,
		"sharePct":      sharePct,
	})
}

// MyAffiliateLinks: GET /affiliate/me — every affiliate link the caller
// owns, if any (affiliate links are admin-created, not self-service, so an
// empty result is the normal case for most users).
func (s *ReferralServer) MyAffiliateLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	links, err := s.Referrals.AffiliateLinksForOwner(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load affiliate links")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"links": links})
}

// AdminReferralConfig: GET /admin/referral-config — the current global
// referral percentage.
func (s *ReferralServer) AdminReferralConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireReferralAdmin(w, r) {
		return
	}
	pct, err := s.Referrals.ReferralSharePct(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load referral config")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"sharePct": pct})
}

type adminSetReferralConfigBody struct {
	SharePct string `json:"sharePct"`
}

// AdminSetReferralConfig: POST /admin/referral-config {sharePct} — updates
// the global referral percentage. Only affects referrals linked AFTER this
// call; existing user_referral_links rows keep their own snapshotted
// share_pct forever.
func (s *ReferralServer) AdminSetReferralConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	claims, ok := s.requireReferralAdminClaims(w, r)
	if !ok {
		return
	}
	var req adminSetReferralConfigBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	pct, err := decimal.NewFromString(strings.TrimSpace(req.SharePct))
	if err != nil || pct.LessThan(decimal.Zero) || pct.GreaterThan(decimal.NewFromInt(100)) {
		writeError(w, http.StatusBadRequest, "sharePct must be a number between 0 and 100")
		return
	}
	if err := s.Referrals.SetReferralSharePct(r.Context(), pct.String(), claims.UserID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update referral config")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated", "sharePct": pct.String()})
}

// AdminAffiliateLinks: GET /admin/affiliate-links — every affiliate link on
// the platform, for the admin listing page.
func (s *ReferralServer) AdminAffiliateLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireReferralAdmin(w, r) {
		return
	}
	links, err := s.Referrals.AllAffiliateLinks(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load affiliate links")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"links": links})
}

type adminCreateAffiliateLinkBody struct {
	OwnerUserID string `json:"ownerUserId"`
	SharePct    string `json:"sharePct"`
}

// AdminCreateAffiliateLink: POST /admin/affiliate-links {ownerUserId,
// sharePct} — creates a new affiliate link. The percentage is fixed for
// this link's lifetime once set (only a future admin edit to a DIFFERENT
// link's percentage, or a new link, is possible — see
// REFERRAL-AFFILIATE-PLAN.md; there's no PATCH for sharePct itself,
// only for active).
func (s *ReferralServer) AdminCreateAffiliateLink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	claims, ok := s.requireReferralAdminClaims(w, r)
	if !ok {
		return
	}
	var req adminCreateAffiliateLinkBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.OwnerUserID = strings.TrimSpace(req.OwnerUserID)
	if req.OwnerUserID == "" {
		writeError(w, http.StatusBadRequest, "ownerUserId is required")
		return
	}
	pct, err := decimal.NewFromString(strings.TrimSpace(req.SharePct))
	if err != nil || pct.LessThan(decimal.Zero) || pct.GreaterThan(decimal.NewFromInt(100)) {
		writeError(w, http.StatusBadRequest, "sharePct must be a number between 0 and 100")
		return
	}
	link, err := s.Referrals.CreateAffiliateLink(r.Context(), req.OwnerUserID, pct.String(), claims.UserID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "foreign key") {
			writeError(w, http.StatusNotFound, "owner user not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create affiliate link")
		return
	}
	writeJSON(w, http.StatusOK, link)
}

type adminSetAffiliateLinkActiveBody struct {
	LinkID string `json:"linkId"`
	Active bool   `json:"active"`
}

// AdminSetAffiliateLinkActive: POST /admin/affiliate-links/active {linkId,
// active} — deactivates or reactivates a link. Only stops NEW signups from
// using the code; already-linked users keep their permanent share forever.
func (s *ReferralServer) AdminSetAffiliateLinkActive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireReferralAdmin(w, r) {
		return
	}
	var req adminSetAffiliateLinkActiveBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.LinkID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Referrals.SetAffiliateLinkActive(r.Context(), req.LinkID, req.Active); err != nil {
		if err == pgx.ErrNoRows {
			writeError(w, http.StatusNotFound, "affiliate link not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to update affiliate link")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "updated", "linkId": req.LinkID, "active": req.Active})
}

// AdminFeeRevenue: GET /admin/fee-revenue — all-time gross fee revenue
// collected per trading surface (spot, futures, liquidation, swap, P2P), for
// the admin Fee Revenue page. Prop firm and any other fee type not yet
// backed by a real charge is simply absent from this response; the frontend
// shows it as zero/not-yet-implemented rather than this endpoint fabricating
// a number for it.
func (s *ReferralServer) AdminFeeRevenue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireReferralAdmin(w, r) {
		return
	}
	totals, err := s.Referrals.FeeRevenueTotals(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load fee revenue")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"spotRaw":        totals.SpotRaw,
		"futuresRaw":     totals.FuturesRaw,
		"liquidationRaw": totals.LiquidationRaw,
		"swapRaw":        totals.SwapRaw,
		"p2pRaw":         totals.P2PRaw,
		"totalRaw":       totals.TotalRaw,
	})
}

// requireReferralAdmin/requireReferralAdminClaims mirror
// FeeServer.requireFeeAdmin (same admin-login-id check) — duplicated rather
// than shared across server types, same precedent FeeServer itself set.
func (s *ReferralServer) requireReferralAdmin(w http.ResponseWriter, r *http.Request) bool {
	_, ok := s.requireReferralAdminClaims(w, r)
	return ok
}

func (s *ReferralServer) requireReferralAdminClaims(w http.ResponseWriter, r *http.Request) (claims struct{ UserID string }, ok bool) {
	c, authOK := s.authenticate(r)
	if !authOK {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return claims, false
	}
	if c.UserID != adminLoginID {
		writeError(w, http.StatusForbidden, "not authorized")
		return claims, false
	}
	claims.UserID = c.UserID
	return claims, true
}
