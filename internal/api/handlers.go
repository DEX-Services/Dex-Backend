// Package api implements the HTTP handlers for wallet login/logout.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/auth"
	"github.com/dex/dex-backend/internal/repo"
)

const sessionCookie = "dex_session"

var validWalletTypes = map[string]bool{"metamask": true, "coinbase": true, "bitget": true}

type Server struct {
	Nonces       *auth.NonceStore
	JWT          *auth.JWTIssuer
	Users        *repo.UserRepo
	Log          *slog.Logger
	SecureCookie bool
	TrustedProxy string
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// NonceRequest / handler: GET /auth/nonce?address=0x...
func (s *Server) Nonce(w http.ResponseWriter, r *http.Request) {
	address := r.URL.Query().Get("address")
	if address == "" || !strings.HasPrefix(address, "0x") {
		writeError(w, http.StatusBadRequest, "valid address query param required")
		return
	}
	nonce, err := s.Nonces.Create(address)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create nonce")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"nonce":   nonce,
		"message": auth.SignMessage(address, nonce),
	})
}

type loginRequest struct {
	Address    string `json:"address"`
	Signature  string `json:"signature"`
	WalletType string `json:"walletType"`
}

// Login: POST /auth/login {address, signature, walletType}
func (s *Server) Login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Address == "" || req.Signature == "" {
		writeError(w, http.StatusBadRequest, "address and signature required")
		return
	}
	if !validWalletTypes[req.WalletType] {
		req.WalletType = "metamask"
	}

	nonce, err := s.Nonces.Consume(req.Address)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "nonce missing or expired, request a new one")
		return
	}

	message := auth.SignMessage(req.Address, nonce)
	if err := auth.VerifySignature(req.Address, message, req.Signature); err != nil {
		writeError(w, http.StatusUnauthorized, "signature verification failed")
		return
	}

	ctx := r.Context()
	user, err := s.Users.FindOrCreate(ctx, req.Address, req.WalletType)
	if err != nil {
		s.Log.Error("find or create user failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not create user")
		return
	}
	if err := s.Users.TouchLogin(ctx, user.ID); err != nil {
		s.Log.Warn("touch login failed", "err", err)
	}
	if _, err := s.Users.CreateSession(ctx, user.ID, user.WalletAddress, s.clientIP(r), r.UserAgent()); err != nil {
		s.Log.Warn("create session failed", "err", err)
	}

	token, expiresAt, err := s.JWT.Issue(user.ID, user.WalletAddress)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not issue session")
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.SecureCookie,
		SameSite: http.SameSiteLaxMode,
		Expires:  expiresAt,
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"user":  user,
		"token": token,
	})
}

// Logout: POST /auth/logout
func (s *Server) Logout(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if ok {
		if err := s.Users.CloseSession(r.Context(), claims.UserID); err != nil {
			s.Log.Warn("close session failed", "err", err)
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   s.SecureCookie,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "logged out"})
}

// Me: GET /auth/me - returns current user from session cookie/bearer token.
func (s *Server) Me(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	user, err := s.Users.FindByID(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "session user not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": user})
}

func (s *Server) authenticate(r *http.Request) (*auth.Claims, bool) {
	token := ""
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		token = cookie.Value
	} else if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		token = strings.TrimPrefix(h, "Bearer ")
	}
	if token == "" {
		return nil, false
	}
	claims, err := s.JWT.Verify(token)
	if err != nil {
		return nil, false
	}
	return claims, true
}

func (s *Server) clientIP(r *http.Request) string {
	if s.TrustedProxy != "" {
		if r.RemoteAddr == s.TrustedProxy || strings.HasPrefix(r.RemoteAddr, s.TrustedProxy) {
			if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
				return strings.TrimSpace(strings.Split(fwd, ",")[0])
			}
		}
	}
	return r.RemoteAddr
}

func clientIP(r *http.Request) string {
	return r.RemoteAddr
}
