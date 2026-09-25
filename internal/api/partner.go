package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/repo"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

// partnerRole is stored in the JWT's WalletAddress claim field, which is
// unused for admin/partner tokens otherwise (it's only meaningful for real
// user wallet sessions) — this is how requirePartner tells a partner token
// apart from an owner-admin token or a user token without adding a new
// claims field to auth.Claims.
const partnerRole = "partner"

// PartnerServer authenticates the 3 fixed partner accounts (see
// partner_accounts, seeded by seedPartnerAccounts in internal/db/db.go) and
// serves their own view-only daily profit-share history. Deliberately
// separate from AdminServer: the owner admin's single env-var login and a
// partner's per-row DB login are different enough (and partners must never
// be able to reach an owner-only route) that keeping them as distinct
// server types with distinct route prefixes is safer than overloading
// requireAdmin with a third identity case.
type PartnerServer struct {
	*Server
	Partner *repo.PartnerRepo
}

type partnerLoginRequest struct {
	LoginID  string `json:"loginId"`
	Password string `json:"password"`
}

func (s *PartnerServer) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req partnerLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	loginID := strings.TrimSpace(req.LoginID)
	account, err := s.Partner.Account(r.Context(), loginID)
	if err != nil {
		if err != pgx.ErrNoRows {
			s.Log.Error("partner account lookup failed", "err", err)
		}
		writeError(w, http.StatusUnauthorized, "invalid partner credentials")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(account.PasswordHash), []byte(req.Password)); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid partner credentials")
		return
	}
	token, _, err := s.JWT.Issue(account.LoginID, partnerRole, "")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not issue partner session")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"token": token,
		"user":  map[string]string{"loginId": account.LoginID, "name": account.Name},
	})
}

// requirePartner mirrors AdminServer.requireAdmin's shape, but checks the
// role marker instead of a single hardcoded login ID, and re-validates the
// login still exists in partner_accounts on every request (a removed
// partner's already-issued tokens stop working within one request round
// trip, not just at next login).
func (s *PartnerServer) requirePartner(w http.ResponseWriter, r *http.Request) (string, bool) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return "", false
	}
	if claims.WalletAddress != partnerRole {
		writeError(w, http.StatusForbidden, "not authorized")
		return "", false
	}
	if _, err := s.Partner.Account(r.Context(), claims.UserID); err != nil {
		writeError(w, http.StatusForbidden, "not authorized")
		return "", false
	}
	return claims.UserID, true
}

var partnerProfitRanges = map[string]time.Duration{
	"1h": time.Hour,
	"1d": 24 * time.Hour,
	"1w": 7 * 24 * time.Hour,
	"1m": 30 * 24 * time.Hour,
}

// Profit serves GET /partner/profit?range=1h|1d|1w|1m|all — a partner's own
// daily share history plus their all-time cumulative total. range follows
// the same convention as AdminFeeRevenue (a single rolling lower-bound
// cutoff on when the split was RECORDED, not the profit_date itself — so
// "1d" means "splits recorded in the last day," which in practice is either
// 0 or 1 row given the job runs once daily).
func (s *PartnerServer) Profit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	partnerID, ok := s.requirePartner(w, r)
	if !ok {
		return
	}

	var since *time.Time
	if d, found := partnerProfitRanges[r.URL.Query().Get("range")]; found {
		t := time.Now().Add(-d)
		since = &t
	}

	history, err := s.Partner.History(r.Context(), partnerID, since)
	if err != nil {
		s.Log.Error("partner profit history failed", "err", err, "partnerId", partnerID)
		writeError(w, http.StatusInternalServerError, "could not load profit history")
		return
	}
	cumulative, err := s.Partner.CumulativeShare(r.Context(), partnerID)
	if err != nil {
		s.Log.Error("partner cumulative share failed", "err", err, "partnerId", partnerID)
		writeError(w, http.StatusInternalServerError, "could not load profit history")
		return
	}

	type entry struct {
		ProfitDate     string `json:"profitDate"`
		ShareRaw       string `json:"shareRaw"`
		SourceTotalRaw string `json:"sourceTotalRaw"`
		PartnerCount   int    `json:"partnerCount"`
	}
	entries := make([]entry, 0, len(history))
	for _, h := range history {
		entries = append(entries, entry{
			ProfitDate:     h.ProfitDate.Format("2006-01-02"),
			ShareRaw:       h.ShareRaw,
			SourceTotalRaw: h.SourceTotalRaw,
			PartnerCount:   h.PartnerCount,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entries":       entries,
		"cumulativeRaw": cumulative,
	})
}
