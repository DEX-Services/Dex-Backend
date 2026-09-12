// Package api: fee-tier discount subscription endpoints (user-facing) and
// fee-config admin endpoints. See FEE-TIER-SYSTEM-PLAN.md for the full
// design — this is the Dex-Backend half; matching-engine applies the
// resulting discount on the settlement hot path via its own in-memory
// discounts.Registry (never a per-request read like this file's handlers,
// which are fine to hit Postgres directly since they're one-off HTTP calls,
// not per-fill matching-loop code).
package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/dex/dex-backend/internal/bi2xprice"
	"github.com/dex/dex-backend/internal/feeconfig"
	"github.com/dex/dex-backend/internal/repo"
	"github.com/shopspring/decimal"
)

// FeeServer exposes the fee-tier subscription flow (any signed-in user) and
// the fee-config admin endpoints (admin only, gated the same way AdminServer
// gates its own routes).
type FeeServer struct {
	*Server
	Fees      *feeconfig.Client
	FeeTiers  *repo.FeeTierRepo
	BI2XPrice bi2xprice.Reader
}

// Tiers: GET /fees/tiers — public tier listing (no auth required: a
// prospective subscriber should be able to see the ladder before logging
// in), each with its live BI2X cost computed from the current price.
func (s *FeeServer) Tiers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tiers, err := s.Fees.AllTiers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load fee tiers")
		return
	}
	price, priceErr := s.BI2XPrice.CurrentPrice(r.Context())

	type tierResp struct {
		Tier          int    `json:"tier"`
		BIUSDBValue   string `json:"biusdbValue"`
		DiscountPct   string `json:"discountPct"`
		Active        bool   `json:"active"`
		BI2XCost      string `json:"bi2xCost,omitempty"`
		BI2XCostError string `json:"bi2xCostError,omitempty"`
	}
	out := make([]tierResp, 0, len(tiers))
	for _, t := range tiers {
		tr := tierResp{
			Tier:        t.Tier,
			BIUSDBValue: t.BIUSDBValue.String(),
			DiscountPct: t.DiscountPct.String(),
			Active:      t.Active,
		}
		if priceErr == nil {
			tr.BI2XCost = t.BIUSDBValue.Div(price).StringFixed(6)
		} else {
			// Show the ladder even if the live price feed is briefly down —
			// the BIUSDB value alone is still useful information; only the
			// live BI2X-quantity preview is unavailable. The /fees/subscribe
			// endpoint independently re-checks the price and refuses the
			// purchase itself if it's still unavailable at that point.
			tr.BI2XCostError = "live BI2X price unavailable"
		}
		out = append(out, tr)
	}
	writeJSON(w, http.StatusOK, map[string]any{"tiers": out})
}

// MySubscription: GET /fees/my-subscription — the caller's own active
// fee-tier discount, if any.
func (s *FeeServer) MySubscription(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	sub, err := s.FeeTiers.ActiveSubscription(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load subscription")
		return
	}
	if sub == nil {
		writeJSON(w, http.StatusOK, map[string]any{"active": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active":      true,
		"tier":        sub.Tier,
		"discountPct": sub.DiscountPct.String(),
		"purchasedAt": sub.PurchasedAt,
		"expiresAt":   sub.ExpiresAt,
	})
}

type subscribeRequestBody struct {
	Tier int `json:"tier"`
}

// Subscribe: POST /fees/subscribe {tier} — purchases a fee-tier discount
// subscription. Reads BI2X's live price fresh (never cached across this
// request), computes the exact BI2X quantity for the tier's fixed BIUSDB
// value, and atomically debits it and records the subscription (see
// repo.FeeTierRepo.Subscribe). Any existing active subscription is
// superseded — a repurchase always resets the 1-year clock, per the
// confirmed product decision.
func (s *FeeServer) Subscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	var req subscribeRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Tier < 1 || req.Tier > 10 {
		writeError(w, http.StatusBadRequest, "tier must be between 1 and 10")
		return
	}
	price, err := s.BI2XPrice.CurrentPrice(r.Context())
	if err != nil {
		// Refuse rather than compute a purchase against a stale/missing
		// price — see FEE-TIER-SYSTEM-PLAN.md.
		writeError(w, http.StatusServiceUnavailable, "BI2X price is temporarily unavailable, try again shortly")
		return
	}
	sub, err := s.FeeTiers.Subscribe(r.Context(), claims.UserID, req.Tier, price)
	if err != nil {
		status := http.StatusBadRequest
		if err == repo.ErrTierNotFound {
			status = http.StatusNotFound
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tier":           sub.Tier,
		"discountPct":    sub.DiscountPct.String(),
		"bi2xPriceUsed":  sub.BI2XPriceSnapshot.String(),
		"bi2xAmountPaid": sub.BI2XAmountPaid,
		"purchasedAt":    sub.PurchasedAt,
		"expiresAt":      sub.ExpiresAt,
	})
}

// AdminFees: GET /admin/fees — lists every fee_config rate. Admin only.
func (s *FeeServer) AdminFees(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireFeeAdmin(w, r) {
		return
	}
	rates, err := s.Fees.AllRates(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load fee config")
		return
	}
	out := make(map[string]string, len(rates))
	for k, v := range rates {
		out[k] = v.String()
	}
	writeJSON(w, http.StatusOK, map[string]any{"rates": out})
}

type adminSetFeeRequestBody struct {
	Key  string `json:"key"`
	Rate string `json:"rate"`
}

// AdminSetFee: POST /admin/fees {key, rate} — updates one fee_config rate.
// Admin only. Clamped to [0, 5%] for ordinary fees and [0, 10%] for the
// liquidation penalty (see feeconfig.RateBounds) — a fat-fingered "45"
// instead of "0.45" is rejected outright rather than silently applied to
// every trade on the platform.
func (s *FeeServer) AdminSetFee(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	claims, ok := s.requireFeeAdminClaims(w, r)
	if !ok {
		return
	}
	var req adminSetFeeRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Key = strings.TrimSpace(req.Key)
	valid := false
	for _, k := range feeconfig.ValidKeys() {
		if k == req.Key {
			valid = true
			break
		}
	}
	if !valid {
		writeError(w, http.StatusBadRequest, "unknown fee key")
		return
	}
	rate, err := decimal.NewFromString(strings.TrimSpace(req.Rate))
	if err != nil {
		writeError(w, http.StatusBadRequest, "rate must be a decimal number, e.g. 0.0025 for 0.25%")
		return
	}
	min, max := feeconfig.RateBounds(req.Key)
	if rate.LessThan(min) || rate.GreaterThan(max) {
		writeError(w, http.StatusBadRequest, "rate must be between "+min.String()+" and "+max.String())
		return
	}
	if err := s.Fees.SetRate(r.Context(), req.Key, rate, claims.UserID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update fee rate")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated", "key": req.Key, "rate": rate.String()})
}

// AdminFeeSubscriptions: GET /admin/fees/subscriptions?user=... — a user's
// fee-tier subscription history, for support/audit use. Admin only.
func (s *FeeServer) AdminFeeSubscriptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireFeeAdmin(w, r) {
		return
	}
	userID := strings.TrimSpace(r.URL.Query().Get("user"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user is required")
		return
	}
	history, err := s.FeeTiers.SubscriptionHistory(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load subscription history")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": history})
}

// requireFeeAdmin/requireFeeAdminClaims mirror AdminServer.requireAdmin
// (same admin-login-id check) — duplicated rather than shared across
// packages since FeeServer intentionally has no dependency on AdminServer.
func (s *FeeServer) requireFeeAdmin(w http.ResponseWriter, r *http.Request) bool {
	_, ok := s.requireFeeAdminClaims(w, r)
	return ok
}

func (s *FeeServer) requireFeeAdminClaims(w http.ResponseWriter, r *http.Request) (claims struct{ UserID string }, ok bool) {
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
