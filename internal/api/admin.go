package api

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/models"
	"github.com/dex/dex-backend/internal/repo"
	"golang.org/x/crypto/bcrypt"
)

// balanceRawScale is the fixed-point scale user_balances stores every asset
// column in (NUMERIC(38,0) — raw integer units, amount × 10^balanceRawScale).
// Matches rawUnitScale in the bots service's backend client and the
// matching-engine's backendclient package; all three must agree since they
// write into the same columns.
const balanceRawScale = 6

// toRawUnits converts a human decimal amount (e.g. "0.5") into the raw
// integer string user_balances expects, truncating any precision beyond
// balanceRawScale. Returns an error for a non-numeric or non-positive input.
func toRawUnits(amount string) (string, error) {
	// Split on '.' by hand rather than pull in a decimal library for one
	// conversion: parse whole and fractional parts separately, then combine.
	neg := strings.HasPrefix(amount, "-")
	if neg {
		amount = amount[1:]
	}
	whole, frac, _ := strings.Cut(amount, ".")
	if whole == "" {
		whole = "0"
	}
	if len(frac) > balanceRawScale {
		frac = frac[:balanceRawScale]
	}
	for len(frac) < balanceRawScale {
		frac += "0"
	}
	combined := whole + frac
	n, ok := new(big.Int).SetString(combined, 10)
	if !ok {
		return "", fmt.Errorf("invalid amount %q", amount)
	}
	if n.Sign() <= 0 {
		return "", fmt.Errorf("amount must be positive")
	}
	if neg {
		n = n.Neg(n)
	}
	return n.String(), nil
}

// AdminServer authenticates administrators. Credentials are env-driven:
//   - ADMIN_LOGIN_ID  (default: disabled if unset)
//   - ADMIN_PASSWORD  (bcrypt hash of the password; must be set)
//
// If ADMIN_PASSWORD is unset, admin login is refused entirely so the old
// hardcoded "admin"/"admin" backdoor can never be reached.
type AdminServer struct {
	*Server
	Admin         *repo.AdminRepo
	Users         *repo.UserRepo
	Ledger        *repo.LedgerRepo
	EngineClient  *engineclient.Client
	AdminLoginID  string
	AdminPassword string
}

const adminLoginID = "admin"

type adminLoginRequest struct {
	LoginID  string `json:"loginId"`
	Password string `json:"password"`
}

func (s *AdminServer) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.AdminLoginID == "" || s.AdminPassword == "" {
		writeError(w, http.StatusServiceUnavailable, "admin login not configured")
		return
	}
	var req adminLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(req.LoginID) != s.AdminLoginID {
		writeError(w, http.StatusUnauthorized, "invalid admin credentials")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(s.AdminPassword), []byte(req.Password)); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid admin credentials")
		return
	}
	profile, err := s.Admin.Profile(r.Context(), adminLoginID)
	if err != nil {
		s.Log.Error("admin profile lookup failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load admin profile")
		return
	}
	token, _, err := s.JWT.Issue(adminLoginID, "admin")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not issue admin session")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": token, "user": profile})
}

func (s *AdminServer) Dashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	summary, err := s.Admin.Summary(r.Context())
	if err != nil {
		s.Log.Error("admin summary failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load admin dashboard")
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (s *AdminServer) Profile(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		profile, err := s.Admin.Profile(r.Context(), adminLoginID)
		if err != nil {
			s.Log.Error("admin profile lookup failed", "err", err)
			writeError(w, http.StatusInternalServerError, "could not load admin profile")
			return
		}
		writeJSON(w, http.StatusOK, profile)
	case http.MethodPut:
		var req models.AdminProfile
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.Email) == "" || strings.TrimSpace(req.Phone) == "" {
			writeError(w, http.StatusBadRequest, "name, email and phone are required")
			return
		}
		req.LoginID = adminLoginID
		updated, err := s.Admin.UpdateProfile(r.Context(), req)
		if err != nil {
			s.Log.Error("admin profile update failed", "err", err)
			writeError(w, http.StatusInternalServerError, "could not update admin profile")
			return
		}
		writeJSON(w, http.StatusOK, updated)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// SearchUsers: GET /admin/users/search?q=...&limit=20
// Backs the admin balance-adjustment picker. q matches against user id or
// wallet address (substring, case-insensitive); empty q returns the most
// recently created users.
func (s *AdminServer) SearchUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	users, err := s.Users.Search(r.Context(), r.URL.Query().Get("q"), limit)
	if err != nil {
		s.Log.Error("admin user search failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not search users")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

type adjustBalanceRequest struct {
	UserID    string `json:"userId"`
	Asset     string `json:"asset"`
	Amount    string `json:"amount"`    // human decimal, e.g. "0.5"
	Direction string `json:"direction"` // "credit" | "debit"
}

// AdjustUserBalance: POST /admin/users/balance {userId, asset, amount, direction}
//
// DEV/TESTING ONLY: manually credits or debits any user's balance for one
// asset, entirely outside the normal on-chain deposit / trade-settlement
// paths. There is no real asset movement backing this — it exists so an
// admin can set up test balances without needing an actual chain deposit or
// a counterparty to trade against. Writes both the durable Postgres balance
// and the engine's in-memory mirror, matching every other credit path here
// (see InternalCreditBalance, chain.Listener.handleDeposit).
func (s *AdminServer) AdjustUserBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	var req adjustBalanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.UserID = strings.TrimSpace(req.UserID)
	if req.UserID == "" {
		writeError(w, http.StatusBadRequest, "userId is required")
		return
	}
	if req.Direction != "credit" && req.Direction != "debit" {
		writeError(w, http.StatusBadRequest, `direction must be "credit" or "debit"`)
		return
	}
	if _, err := s.Users.FindByID(r.Context(), req.UserID); err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	raw, err := toRawUnits(req.Amount)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Direction == "credit" {
		err = s.Ledger.CreditBalance(r.Context(), req.UserID, req.Asset, raw)
	} else {
		err = s.Ledger.DebitBalance(r.Context(), req.UserID, req.Asset, raw)
	}
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	// Best-effort mirror into the engine's in-memory ledger, matching every
	// other balance-adjusting path. Not fatal if the engine bridge is down —
	// the durable Postgres balance (checked by anything reading via the
	// normal wallet API) is already correct; the engine will pick it up on
	// its next restart backfill if this fails.
	if s.EngineClient != nil && s.EngineClient.Enabled() {
		engErr := s.EngineClient.Credit(r.Context(), req.UserID, req.Asset, raw)
		if req.Direction == "debit" {
			engErr = s.EngineClient.Debit(r.Context(), req.UserID, req.Asset, raw)
		}
		if engErr != nil {
			s.Log.Warn("admin balance adjust: engine ledger sync failed", "userId", req.UserID, "asset", req.Asset, "err", engErr)
		}
	}

	balances, err := s.Ledger.BalancesFor(r.Context(), req.UserID)
	if err != nil {
		s.Log.Error("balance lookup after admin adjust failed", "err", err)
		writeError(w, http.StatusInternalServerError, "adjusted but could not reload balance")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"userId": req.UserID, "balances": balances})
}

func (s *AdminServer) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return false
	}
	if claims.UserID != adminLoginID {
		writeError(w, http.StatusForbidden, "not authorized")
		return false
	}
	return true
}
