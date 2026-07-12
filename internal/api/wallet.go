package api

import (
	"encoding/json"
	"math/big"
	"net/http"
	"strings"

	"github.com/dex/dex-backend/internal/chain"
	"github.com/dex/dex-backend/internal/repo"
)

const usdcToken = "USDC"

// WalletServer extends Server with deposit-ledger and withdrawal-approval endpoints. It is a
// separate type from Server so the base auth service keeps compiling standalone if chain wiring
// (RPC/treasury key) isn't configured for a given deployment.
type WalletServer struct {
	*Server
	Ledger *repo.LedgerRepo
	Signer *chain.Signer
	Admins map[string]bool
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
	writeJSON(w, http.StatusOK, map[string]any{
		"balances": balances,
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

	writeJSON(w, http.StatusOK, map[string]string{"txHash": txHash, "asset": asset, "status": "approved"})
}
