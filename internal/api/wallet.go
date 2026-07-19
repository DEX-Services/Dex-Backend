package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
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
	// EngineOutbox durably queues engine balance syncs; nil falls back to the
	// non-durable async path.
	EngineOutbox *engineclient.Outbox
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
	withdrawalLocked, err := s.Ledger.PendingWithdrawalHoldsFor(r.Context(), claims.UserID)
	if err != nil {
		s.Log.Error("withdrawal hold lookup failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load balance")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"balances":         balances,
		"locked":           locked,
		"withdrawalLocked": withdrawalLocked,
		"token":            usdcToken,
		"amount":           balances[usdcToken],
	})
}

type withdrawRequestBody struct {
	Amount string `json:"amount"`
	Asset  string `json:"asset"`
	// IdempotencyKey makes client retries safe: the same key never creates a
	// second withdrawal. Can also be supplied via the Idempotency-Key header.
	IdempotencyKey string `json:"idempotencyKey"`
}

func requestAsset(asset string) string {
	if strings.TrimSpace(asset) == "" {
		return usdcToken
	}
	return asset
}

// WithdrawRequest: POST /wallet/withdraw-request {amount, asset?}
// Reserves the user's withdrawable balance, pays USDC from the treasury signer wallet immediately,
// and confirms the ledger debit after the chain receipt succeeds.
func (s *WalletServer) WithdrawRequest(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if s.Signer == nil {
		writeError(w, http.StatusServiceUnavailable, "treasury signer not configured")
		return
	}

	var req withdrawRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	asset := requestAsset(req.Asset)
	if asset != usdcToken {
		writeError(w, http.StatusBadRequest, "only USDC withdrawals are supported right now")
		return
	}

	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be a positive integer (raw token units)")
		return
	}

	idemKey := strings.TrimSpace(req.IdempotencyKey)
	if idemKey == "" {
		idemKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}

	id, alreadyExists, err := s.Ledger.InsertWithdrawalRequest(r.Context(), claims.UserID, claims.WalletAddress, asset, amount.String(), idemKey)
	if err != nil {
		s.Log.Error("insert withdrawal request failed", "err", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if alreadyExists {
		// Retry of a request we already accepted: report current state, do NOT
		// run the payout again (the original flow or admin recovery owns it).
		entry, err := s.Ledger.WithdrawalByID(r.Context(), id)
		if err != nil {
			s.Log.Error("idempotent withdrawal lookup failed", "err", err, "requestId", id)
			writeError(w, http.StatusInternalServerError, "could not load withdrawal state")
			return
		}
		txHash := ""
		if entry.TxHash != nil {
			txHash = *entry.TxHash
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": entry.ID, "txHash": txHash, "asset": entry.Token, "status": entry.Status})
		return
	}

	response, status, err := s.processWithdrawalRequest(r.Context(), id)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, status, response)
}

func (s *WalletServer) processWithdrawalRequest(ctx context.Context, requestID string) (map[string]string, int, error) {
	entry, err := s.Ledger.MarkWithdrawalProcessing(ctx, requestID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	if entry.Token != usdcToken {
		_ = s.Ledger.MarkWithdrawalFailed(ctx, requestID)
		return nil, http.StatusBadRequest, errors.New("only USDC withdrawals are supported right now")
	}
	amount, ok := new(big.Int).SetString(entry.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		_ = s.Ledger.MarkWithdrawalFailed(ctx, requestID)
		return nil, http.StatusInternalServerError, errors.New("invalid withdrawal amount")
	}

	txHash, err := s.Signer.SubmitWithdrawal(ctx, entry.WalletAddress, amount)
	if err != nil {
		if txHash == "" || errors.Is(err, chain.ErrTxReverted) {
			_ = s.Ledger.MarkWithdrawalFailed(ctx, requestID)
		}
		s.Log.Error("submit withdrawal failed", "err", err, "requestId", requestID, "txHash", txHash)
		if txHash != "" {
			return map[string]string{"id": requestID, "txHash": txHash, "asset": entry.Token, "status": "processing"}, http.StatusBadGateway, errors.New("withdrawal transaction was submitted but not confirmed; request remains processing")
		}
		return nil, http.StatusBadGateway, errors.New("failed to submit withdrawal transaction")
	}

	confirmed, err := s.Ledger.MarkWithdrawalConfirmed(ctx, requestID, txHash)
	if err != nil {
		s.Log.Error("mark withdrawal confirmed failed", "err", err, "requestId", requestID, "txHash", txHash)
		return nil, http.StatusInternalServerError, errors.New("withdrawal submitted on-chain but ledger update failed")
	}

	if s.EngineOutbox != nil {
		s.EngineOutbox.EnqueueDebit(ctx, confirmed.UserID, confirmed.Token, confirmed.Amount)
	} else {
		engineclient.Async("debit", func(ctx context.Context) error {
			return s.EngineClient.Debit(ctx, confirmed.UserID, confirmed.Token, confirmed.Amount)
		})
	}

	return map[string]string{"id": requestID, "txHash": txHash, "asset": confirmed.Token, "status": "confirmed"}, http.StatusOK, nil
}

type adminApproveBody struct {
	RequestID string `json:"requestId"`
	Action    string `json:"action"`
}

// AdminApproveWithdrawal: POST /admin/withdraw-approve {requestId, action?}
// Restricted to wallet addresses in the ADMIN_WALLET_ADDRESSES allowlist. For
// action=approve it pays USDC from the treasury signer wallet to the request wallet, waits for a
// successful receipt, then debits the user's ledger balance.
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

	var req adminApproveBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(req.RequestID) == "" {
		writeError(w, http.StatusBadRequest, "requestId required")
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action == "" {
		action = "approve"
	}
	if action == "reject" {
		if err := s.Ledger.RejectWithdrawalRequest(r.Context(), req.RequestID); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": req.RequestID, "status": "rejected"})
		return
	}
	if action != "approve" {
		writeError(w, http.StatusBadRequest, "action must be approve or reject")
		return
	}
	if s.Signer == nil {
		writeError(w, http.StatusServiceUnavailable, "treasury signer not configured")
		return
	}

	response, status, err := s.processWithdrawalRequest(r.Context(), req.RequestID)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, status, response)
}

// AdminRecoverWithdrawal: POST /admin/withdraw-recover {requestId, action}
// Recovers withdrawals stuck in "processing" (e.g. after a crash between marking
// processing and the on-chain tx). action=retry re-attempts the payout;
// action=fail marks it failed without payout. Restricted to admin wallets.
func (s *WalletServer) AdminRecoverWithdrawal(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if !s.Admins[strings.ToLower(claims.WalletAddress)] {
		writeError(w, http.StatusForbidden, "not authorized")
		return
	}
	var req adminApproveBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(req.RequestID) == "" {
		writeError(w, http.StatusBadRequest, "requestId required")
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action == "" {
		action = "retry"
	}
	switch action {
	case "fail":
		if err := s.Ledger.MarkWithdrawalFailed(r.Context(), req.RequestID); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": req.RequestID, "status": "failed"})
	case "retry":
		if s.Signer == nil {
			writeError(w, http.StatusServiceUnavailable, "treasury signer not configured")
			return
		}
		response, status, err := s.processWithdrawalRequest(r.Context(), req.RequestID)
		if err != nil {
			writeError(w, status, err.Error())
			return
		}
		writeJSON(w, status, response)
	default:
		writeError(w, http.StatusBadRequest, "action must be retry or fail")
	}
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

// InternalCreditBalance: POST /internal/balance/credit {userId, asset, amount}
// Called by the matching-engine when a futures position is closed, to realize
// released margin plus PnL into the user's real Postgres balance. Amount may
// be negative (a net loss beyond the released margin); a negative amount is
// applied as a debit instead of a credit.
func (s *WalletServer) InternalCreditBalance(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalLockBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	amount := new(big.Int)
	if _, ok := amount.SetString(req.Amount, 10); !ok {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}
	if amount.Sign() < 0 {
		if err := s.Ledger.DebitBalance(r.Context(), req.UserID, req.Asset, amount.Neg(amount).String()); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
	} else if amount.Sign() > 0 {
		if err := s.Ledger.CreditBalance(r.Context(), req.UserID, req.Asset, amount.String()); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "credited"})
}
