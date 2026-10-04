package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/dex/dex-backend/internal/repo"
)

// predictionWalletErrorStatus mirrors p2pErrorStatus/stakingWalletErrorStatus
// for the prediction wallet's own idempotency guard.
func predictionWalletErrorStatus(err error) int {
	if errors.Is(err, repo.ErrPredictionIdempotencyKey) {
		return http.StatusConflict
	}
	return http.StatusBadRequest
}

// fundPredictionWalletRequest mirrors fundStakingWalletRequest — the
// prediction wallet is BI2XUSD-only, same as staking's BI2X-only shape.
type fundPredictionWalletRequest struct {
	AmountRaw      string `json:"amountRaw"`
	IdempotencyKey string `json:"idempotencyKey"`
}

// PredictionWalletBalance handles GET /wallet/prediction: the caller's
// prediction wallet snapshot (available/reserved/total BI2XUSD) — Phase 4
// of ~/.claude/plans/wallet-separation.md.
func (s *WalletServer) PredictionWalletBalance(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	balance, err := s.Prediction.WalletBalance(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load prediction wallet")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"balance": balance})
}

// FundPredictionWallet handles POST /wallet/prediction/fund: moves
// available main-wallet BI2XUSD into the prediction wallet — the only way
// to get funds into it; prediction-service's Lock/Debit now draw from this
// wallet, never from the main wallet directly.
func (s *WalletServer) FundPredictionWallet(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	var req fundPredictionWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	balance, _, err := s.Prediction.FundPredictionWalletAsset(r.Context(), claims.UserID, req.AmountRaw, req.IdempotencyKey)
	if err != nil {
		writeError(w, predictionWalletErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"balance": balance})
}

// UnfundPredictionWallet handles POST /wallet/prediction/unfund: moves
// available prediction-wallet BI2XUSD back to the main wallet.
func (s *WalletServer) UnfundPredictionWallet(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	var req fundPredictionWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	balance, _, err := s.Prediction.UnfundPredictionWalletAsset(r.Context(), claims.UserID, req.AmountRaw, req.IdempotencyKey)
	if err != nil {
		writeError(w, predictionWalletErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"balance": balance})
}

// internalPredictionWalletBody is the shared wire shape for the internal
// lock/unlock/debit/credit endpoints prediction-service's backendclient
// calls — mirrors matching-engine's internalLockBody shape.
type internalPredictionWalletBody struct {
	UserID      string `json:"userId"`
	PositionRef string `json:"positionRef"`
	Amount      string `json:"amount"`
}

// InternalPredictionLock: POST /internal/prediction-wallet/lock
// {userId, positionRef, amount} — called by prediction-service when an
// order's notional cost needs holding against the user's prediction wallet.
func (s *WalletServer) InternalPredictionLock(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalPredictionWalletBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Prediction.Lock(r.Context(), req.UserID, req.PositionRef, req.Amount, r.Header.Get("Idempotency-Key")); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "locked"})
}

// InternalPredictionUnlock: POST /internal/prediction-wallet/unlock
// {userId, positionRef, amount} — called when an order is cancelled or a
// round refunds an unfilled remainder.
func (s *WalletServer) InternalPredictionUnlock(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalPredictionWalletBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Prediction.Unlock(r.Context(), req.UserID, req.PositionRef, req.Amount, r.Header.Get("Idempotency-Key")); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "unlocked"})
}

// InternalPredictionDebit: POST /internal/prediction-wallet/debit
// {userId, positionRef, amount} — called when a match consumes part of a
// locked order (cost + fee) at execution price.
func (s *WalletServer) InternalPredictionDebit(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalPredictionWalletBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Prediction.Debit(r.Context(), req.UserID, req.PositionRef, req.Amount, r.Header.Get("Idempotency-Key")); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "debited"})
}

// InternalPredictionCredit: POST /internal/prediction-wallet/credit
// {userId, positionRef, amount} — called when a round settles and pays out
// a winning position.
func (s *WalletServer) InternalPredictionCredit(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalPredictionWalletBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Prediction.Credit(r.Context(), req.UserID, req.PositionRef, req.Amount, r.Header.Get("Idempotency-Key")); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "credited"})
}

// InternalPredictionAvailable: GET /internal/prediction-wallet/available?userId=
// — mirrors InternalAvailableBalance's shape for prediction-service's
// pre-order available-balance check.
func (s *WalletServer) InternalPredictionAvailable(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	userID := strings.TrimSpace(r.URL.Query().Get("userId"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "userId is required")
		return
	}
	available, err := s.Prediction.AvailableBalance(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load available balance")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"available": available})
}
