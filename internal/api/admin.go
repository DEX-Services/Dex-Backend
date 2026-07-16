package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/dex/dex-backend/internal/models"
	"github.com/dex/dex-backend/internal/repo"
)

const adminLoginID = "admin"
const adminPassword = "admin"

type AdminServer struct {
	*Server
	Admin *repo.AdminRepo
}

type adminLoginRequest struct {
	LoginID  string `json:"loginId"`
	Password string `json:"password"`
}

func (s *AdminServer) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req adminLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(req.LoginID) != adminLoginID || req.Password != adminPassword {
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
