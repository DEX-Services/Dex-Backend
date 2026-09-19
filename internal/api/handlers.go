// Package api implements the HTTP handlers for wallet login/logout.
package api

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/auth"
	"github.com/dex/dex-backend/internal/p2psse"
	"github.com/dex/dex-backend/internal/repo"
	"github.com/dex/dex-backend/internal/sessions"
)

const sessionCookie = "dex_session"

var validWalletTypes = map[string]bool{"metamask": true, "trust": true, "binance": true, "coinbase": true, "bitget": true}

type Server struct {
	Nonces       *auth.NonceStore
	JWT          *auth.JWTIssuer
	Users        *repo.UserRepo
	Log          *slog.Logger
	SecureCookie bool
	TrustedProxy string
	// P2PEvents pushes order status updates over SSE (P2P-L2), replacing the
	// frontend's previous poll of GET /p2p/order. Held on the shared Server
	// so every handler that can change an order's status (P2PServer and
	// AdminServer both embed *Server) can publish to it without separate
	// wiring per server type.
	P2PEvents *p2psse.Hub
	// Sessions backs JWT revocation (M8): nil disables the check, so a JWT
	// stays valid until natural expiry, matching pre-M8 behavior.
	Sessions *sessions.Store
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
	// ReferralCode is an optional referral or affiliate code, captured by
	// the frontend from a "?ref=CODE" URL param on first visit. Only ever
	// consulted the first time this wallet address logs in (i.e. when the
	// user is actually created) — see UserRepo.FindOrCreate.
	ReferralCode string `json:"referralCode"`
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
	user, err := s.Users.FindOrCreate(ctx, req.Address, req.WalletType, req.ReferralCode)
	if err != nil {
		s.Log.Error("find or create user failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not create user")
		return
	}
	if err := s.Users.TouchLogin(ctx, user.ID); err != nil {
		s.Log.Warn("touch login failed", "err", err)
	}
	sessionID, err := s.Users.CreateSession(ctx, user.ID, user.WalletAddress, s.ClientIP(r), r.UserAgent())
	if err != nil {
		s.Log.Warn("create session failed", "err", err)
	}

	token, expiresAt, err := s.JWT.Issue(user.ID, user.WalletAddress, sessionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not issue session")
		return
	}
	if s.Sessions != nil && sessionID != "" {
		if err := s.Sessions.Activate(ctx, sessionID, s.JWT.TTL()); err != nil {
			s.Log.Warn("activate session failed", "err", err)
		}
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
		if s.Sessions != nil && claims.ID != "" {
			if err := s.Sessions.Revoke(r.Context(), claims.ID); err != nil {
				s.Log.Warn("revoke session failed", "err", err)
			}
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
	// Tokens issued before M8 (or when Sessions is nil) carry no jti; skip the
	// revocation check for those rather than locking every existing session
	// out the moment this ships.
	if s.Sessions != nil && claims.ID != "" {
		active, err := s.Sessions.IsActive(r.Context(), claims.ID)
		if err != nil || !active {
			return nil, false
		}
	}
	return claims, true
}

// ClientIP resolves the caller's real IP, honoring X-Forwarded-For only when
// the immediate peer is the configured trusted proxy. Previously compared
// with strings.HasPrefix(r.RemoteAddr, s.TrustedProxy) — RemoteAddr includes
// the port ("10.0.0.1:54321"), so TRUSTED_PROXY=10.0.0.1 also matched
// 10.0.0.100:port, 10.0.0.12:port, etc., letting any host on a /24-ish range
// spoof its origin IP via X-Forwarded-For. Now splits the host out first and
// compares it exactly.
func (s *Server) ClientIP(r *http.Request) string {
	if s.TrustedProxy != "" {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr // RemoteAddr had no port (e.g. a unix socket or test harness)
		}
		if host == s.TrustedProxy {
			if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
				return strings.TrimSpace(strings.Split(fwd, ",")[0])
			}
		}
	}
	return r.RemoteAddr
}
