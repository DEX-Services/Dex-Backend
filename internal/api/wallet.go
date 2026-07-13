package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"math/big"
	"net/http"
	"strings"

	"github.com/dex/dex-backend/internal/chain"
	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/repo"
)

const usdcToken = "USDC"

// WalletServer extends Server with deposit-ledger and withdrawal-approval endpoints. It is a
// separate type from Server so the base auth service keeps compiling standalone if chain wiring
// (RPC/treasury key) isn't configured for a given deployment.
type WalletServer struct {
	*Server
	Ledger       *repo.LedgerRepo
	Signer       *chain.Signer
	Admins       map[string]bool
	EngineSecret string
	EngineClient *engineclient.Client
}

// Balance: GET /wallet/balance
func (s *WalletServer) Balance(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	balances, err := s.Ledger.BalancesFor(r.Context(), claims.UserID)
	if err != nil {
		s.Log.Error("balance lookup failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load balance")
		return
	}
	locked, err := s.Ledger.LockedBalancesFor(r.Context(), claims.UserID)
	if err != nil {
		s.Log.Error("locked balance lookup failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load balance")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"balances": balances,
		"locked":   locked,
		"token":    usdcToken,
		"amount":   balances[usdcToken],
	})
}

type withdrawRequestBody struct {
	Amount string `json:"amount"`
	Asset  string `json:"asset"`
}

func requestAsset(asset string) string {
	if strings.TrimSpace(asset) == "" {
		return usdcToken
	}
	return asset
}

// WithdrawRequest: POST /wallet/withdraw-request {amount, asset?}
// Records an off-chain withdrawal request against the user's current balance. Actual approval
// and payout happen separately (see AdminApproveWithdrawal) - the contract never moves funds.
func (s *WalletServer) WithdrawRequest(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	var req withdrawRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	asset := requestAsset(req.Asset)

	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be a positive integer (raw token units)")
		return
	}

	balanceStr, err := s.Ledger.BalanceFor(r.Context(), claims.UserID, asset)
	if err != nil {
		s.Log.Error("balance lookup failed", "err", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	balance, _ := new(big.Int).SetString(balanceStr, 10)
	if balance == nil || amount.Cmp(balance) > 0 {
		writeError(w, http.StatusBadRequest, "amount exceeds balance")
		return
	}

	id, err := s.Ledger.InsertWithdrawalRequest(r.Context(), claims.UserID, claims.WalletAddress, asset, amount.String())
	if err != nil {
		s.Log.Error("insert withdrawal request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not record withdrawal request")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"id": id, "asset": asset, "status": "pending"})
}

type adminApproveBody struct {
	UserID string `json:"userId"`
	Amount string `json:"amount"`
	Asset  string `json:"asset"`
}

// AdminApproveWithdrawal: POST /admin/withdraw-approve {userId, amount, asset?}
// Restricted to wallet addresses in the ADMIN_WALLET_ADDRESSES allowlist. Submits
// recordWithdrawalApproval on-chain for auditability; treasury still pays the user directly,
// off-chain, outside this flow (manual today, automatable later).
func (s *WalletServer) AdminApproveWithdrawal(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if !s.Admins[strings.ToLower(claims.WalletAddress)] {
		writeError(w, http.StatusForbidden, "not authorized")
		return
	}
	if s.Signer == nil {
		writeError(w, http.StatusServiceUnavailable, "treasury signer not configured")
		return
	}

	var req adminApproveBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.UserID == "" {
		writeError(w, http.StatusBadRequest, "userId required")
		return
	}
	asset := requestAsset(req.Asset)

	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be a positive integer (raw token units)")
		return
	}

	user, err := s.Users.FindByID(r.Context(), req.UserID)
	if err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	txHash, err := s.Signer.SubmitWithdrawalApproval(r.Context(), user.WalletAddress, amount)
	if err != nil {
		s.Log.Error("submit withdrawal approval failed", "err", err)
		writeError(w, http.StatusBadGateway, "failed to submit approval transaction")
		return
	}

	if err := s.Ledger.MarkWithdrawalApproved(r.Context(), user.ID, user.WalletAddress, asset, amount.String(), txHash); err != nil {
		s.Log.Error("mark withdrawal approved failed", "err", err)
		writeError(w, http.StatusInternalServerError, "approval submitted on-chain but ledger update failed")
		return
	}

	engineclient.Async("debit", func(ctx context.Context) error {
		return s.EngineClient.Debit(ctx, user.ID, asset, amount.String())
	})

	writeJSON(w, http.StatusOK, map[string]string{"txHash": txHash, "asset": asset, "status": "approved"})
}

// AdminEngineBackfill: POST /admin/engine-backfill
// One-time (admin-triggered) push of every existing nonzero Postgres balance into
// the matching-engine's in-memory ledger, for balances that predate the
// deposit/withdrawal sync hooks.
func (s *WalletServer) AdminEngineBackfill(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if !s.Admins[strings.ToLower(claims.WalletAddress)] {
		writeError(w, http.StatusForbidden, "not authorized")
		return
	}
	if !s.EngineClient.Enabled() {
		writeError(w, http.StatusServiceUnavailable, "engine ledger-sync bridge not configured")
		return
	}

	synced, failed, total, err := s.runBackfill(r.Context())
	if err != nil {
		s.Log.Error("backfill: load balances failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load balances")
		return
	}

	writeJSON(w, http.StatusOK, map[string]int{"synced": synced, "failed": failed, "total": total})
}

// runBackfill pushes every nonzero Postgres balance into the engine ledger,
// shared by AdminEngineBackfill (human-triggered) and InternalEngineBackfill
// (engine self-triggered on startup).
func (s *WalletServer) runBackfill(ctx context.Context) (synced, failed, total int, err error) {
	balances, err := s.Ledger.AllNonzeroBalances(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	for _, b := range balances {
		if cerr := s.EngineClient.Credit(ctx, b.UserID, b.Asset, b.Amount); cerr != nil {
			s.Log.Error("backfill: credit failed", "err", cerr, "userId", b.UserID, "asset", b.Asset)
			failed++
			continue
		}
		synced++
	}
	return synced, failed, len(balances), nil
}

// InternalEngineBackfill: POST /internal/engine-backfill
// Same as AdminEngineBackfill but authorized via the engine shared secret
// instead of an admin session, so the matching-engine can self-trigger this
// on its own startup without a human in the loop.
func (s *WalletServer) InternalEngineBackfill(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	if !s.EngineClient.Enabled() {
		writeError(w, http.StatusServiceUnavailable, "engine ledger-sync bridge not configured")
		return
	}
	synced, failed, total, err := s.runBackfill(r.Context())
	if err != nil {
		s.Log.Error("backfill: load balances failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load balances")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"synced": synced, "failed": failed, "total": total})
}

type internalLockBody struct {
	UserID string `json:"userId"`
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
}

// checkEngineSecret authorizes the matching-engine, which is not a logged-in wallet
// user and so can't present a JWT. Returns false (and writes the error response)
// if the shared secret is missing/misconfigured or doesn't match.
func (s *WalletServer) checkEngineSecret(w http.ResponseWriter, r *http.Request) bool {
	if s.EngineSecret == "" {
		writeError(w, http.StatusServiceUnavailable, "engine balance bridge not configured")
		return false
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Engine-Secret")), []byte(s.EngineSecret)) != 1 {
		writeError(w, http.StatusForbidden, "not authorized")
		return false
	}
	return true
}

// InternalLockBalance: POST /internal/balance/lock {userId, asset, amount}
// Called by the matching-engine when it reserves margin/notional for a new order,
// to mirror the hold against the user's real Postgres balance.
func (s *WalletServer) InternalLockBalance(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalLockBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Ledger.LockBalance(r.Context(), req.UserID, req.Asset, req.Amount); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "locked"})
}

// InternalUnlockBalance: POST /internal/balance/unlock {userId, asset, amount}
// Called by the matching-engine when a reservation is released (order cancelled,
// rejected, or partially filled).
func (s *WalletServer) InternalUnlockBalance(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalLockBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Ledger.UnlockBalance(r.Context(), req.UserID, req.Asset, req.Amount); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "unlocked"})
}

// InternalSettleBalance: POST /internal/balance/settle {userId, asset, amount}
// Called by the matching-engine when a reserved order fills, converting the
// Postgres-side hold into a real debit.
func (s *WalletServer) InternalSettleBalance(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalLockBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Ledger.SettleLockedDebit(r.Context(), req.UserID, req.Asset, req.Amount); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "settled"})
}
