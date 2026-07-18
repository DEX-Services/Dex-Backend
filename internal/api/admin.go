package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/dex/dex-backend/internal/models"
	"github.com/dex/dex-backend/internal/repo"
	"golang.org/x/crypto/bcrypt"
)

// AdminServer authenticates administrators. Credentials are env-driven:
//   - ADMIN_LOGIN_ID  (default: disabled if unset)
//   - ADMIN_PASSWORD  (bcrypt hash of the password; must be set)
//
// If ADMIN_PASSWORD is unset, admin login is refused entirely so the old
// hardcoded "admin"/"admin" backdoor can never be reached.
type AdminServer struct {
	*Server
	Admin         *repo.AdminRepo
	P2P           *repo.P2PRepo
	AdminLoginID  string
	AdminPassword string
}

type adminP2PPriceRequest struct {
	Price string `json:"price"`
}

func (s *AdminServer) P2PPrice(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		price, err := s.P2P.TodayPrice(r.Context())
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"price": price})
	case http.MethodPost, http.MethodPut:
		var req adminP2PPriceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		price, err := s.P2P.SetTodayPrice(r.Context(), req.Price)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"price": price})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

type resolveP2PAppealRequest struct {
	OrderID string `json:"orderId"`
	Action  string `json:"action"`
}

func (s *AdminServer) ResolveP2PAppeal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	var req resolveP2PAppealRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	order, err := s.P2P.ResolveAppeal(r.Context(), req.OrderID, req.Action)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"order": order})
}

func (s *AdminServer) P2PAppeals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	orders, err := s.P2P.AppealOrders(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load P2P appeals")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": orders})
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
