package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/chain"
	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/feeconfig"
	"github.com/dex/dex-backend/internal/repo"
	"github.com/shopspring/decimal"
)

const usdcToken = "USDC"

// withdrawalTxTimeout bounds the on-chain submit+confirm wait when it's
// deliberately run on a background context (see processWithdrawalRequest) —
// generous enough for real confirmation delays under network congestion,
// but still finite so a genuinely stuck chain doesn't leak the goroutine
// forever.
const withdrawalTxTimeout = 2 * time.Minute

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
	Fees         *feeconfig.Client
	Referrals    *repo.ReferralRepo
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

	id, err := s.Ledger.InsertWithdrawalRequest(r.Context(), claims.UserID, claims.WalletAddress, asset, amount.String())
	if err != nil {
		s.Log.Error("insert withdrawal request failed", "err", err)
		writeError(w, http.StatusBadRequest, err.Error())
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

	// H4: if a previous attempt at this same request already broadcast a
	// transaction (crash/timeout during MarkWithdrawalConfirmed or the
	// confirmation wait, or a prior watchdog retry that itself got stuck),
	// reuse that exact nonce with a bumped fee instead of asking the chain
	// for a fresh nonce — which would count the still-pending original as
	// occupying its slot and skip past it, jamming every withdrawal behind
	// it indefinitely.
	var prior *chain.PendingTx
	if pn, err := s.Ledger.PendingNonceFor(ctx, requestID); err != nil {
		s.Log.Warn("load pending nonce failed, submitting with a fresh nonce", "requestId", requestID, "err", err)
	} else if pn != nil {
		feeCap, feeOK := new(big.Int).SetString(pn.FeeCapWei, 10)
		tipCap, tipOK := new(big.Int).SetString(pn.TipCapWei, 10)
		if feeOK && tipOK {
			prior = &chain.PendingTx{Nonce: pn.Nonce, FeeCapWei: feeCap, TipCapWei: tipCap}
		} else {
			s.Log.Warn("stored pending nonce fee could not be parsed, submitting with a fresh nonce", "requestId", requestID)
		}
	}
	onSubmit := func(nonce uint64, feeCapWei, tipCapWei string) {
		if err := s.Ledger.SavePendingNonce(ctx, requestID, nonce, feeCapWei, tipCapWei); err != nil {
			s.Log.Error("save pending nonce failed", "requestId", requestID, "err", err)
		}
	}

	// Deliberately NOT ctx (the HTTP request's context): SubmitWithdrawal's
	// on-chain wait (bind.WaitMined inside submitTx) previously ran on the
	// request's own context, so a client disconnecting mid-request — closing
	// their browser tab, a mobile network drop — canceled the wait for a
	// transaction that had ALREADY been broadcast to the chain. The withdrawal
	// then sat stuck in "processing" with no automatic recovery, fixable only
	// by an admin manually running /admin/withdraw-recover. A background
	// context with its own generous bound survives the request regardless of
	// what the client does.
	txCtx, cancel := context.WithTimeout(context.Background(), withdrawalTxTimeout)
	defer cancel()
	txHash, err := s.Signer.SubmitWithdrawal(txCtx, entry.WalletAddress, amount, prior, onSubmit)
	if err != nil {
		if txHash == "" || errors.Is(err, chain.ErrTxReverted) {
			_ = s.Ledger.MarkWithdrawalFailed(ctx, requestID)
			_ = s.Ledger.ClearPendingNonce(ctx, requestID)
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
	_ = s.Ledger.ClearPendingNonce(ctx, requestID)

	s.EngineClient.DebitAsync("debit", confirmed.UserID, confirmed.Token, confirmed.Amount)

	return map[string]string{"id": requestID, "txHash": txHash, "asset": confirmed.Token, "status": "confirmed"}, http.StatusOK, nil
}

type swapRequestBody struct {
	Amount           string `json:"amount"`
	SourceAsset      string `json:"sourceAsset"`
	DestinationAsset string `json:"destinationAsset"`
}

// swapDestinations maps each allowed source asset to the destinations it may
// be swapped into. The exchange is deliberately one-directional per asset:
// USDT/USDC convert only into BI2XUSD, and BI2XUSD converts only back into
// USDT/USDC. A direct USDT↔USDC conversion is not offered (route through
// BI2XUSD instead), and no other assets participate in swaps.
var swapDestinations = map[string]map[string]bool{
	"USDT":    {"BI2XUSD": true},
	"USDC":    {"BI2XUSD": true},
	"BI2XUSD": {"USDT": true, "USDC": true},
}

// swapFeeRate returns the (discount-adjusted) fee in basis points charged
// when userID converts source into destination, and whether any fee applies
// at all. Into BI2XUSD is free; out of BI2XUSD carries the conversion charge —
// read from fee_config (feeconfig.KeySwapIn/KeySwapOut), not a hardcoded
// constant, so an admin can retune it without a redeploy, and adjusted by
// userID's active fee-tier discount the same way P2P/spot/futures fees are.
func (s *WalletServer) swapFeeRate(ctx context.Context, destination, userID string) (feeBps int64, charged bool) {
	if s.Fees == nil {
		// No fee-config client wired (e.g. a test harness or a deployment that
		// hasn't set one up) — behave as "no fee configured" rather than panic.
		return 0, false
	}
	key := feeconfig.KeySwapIn
	if destination != "BI2XUSD" {
		key = feeconfig.KeySwapOut
	}
	rate := s.Fees.EffectiveRate(ctx, key, userID)
	if rate.IsZero() {
		return 0, false
	}
	bps := rate.Mul(decimal.NewFromInt(10000)).Round(0).IntPart()
	return bps, true
}

// Swap: POST /wallet/swap {amount, sourceAsset, destinationAsset}
// Converts deposit-intake stables and the platform's internal stable:
//   - USDT → BI2XUSD and USDC → BI2XUSD: 1:1, no fee.
//   - BI2XUSD → USDT and BI2XUSD → USDC: 1:1 with a 1% conversion charge,
//     deducted from the credited destination amount.
//
// All three assets are raw integer token balances at the same 6-decimal
// scale (see assetColumns in repo/ledger.go), so the 1:1 base rate needs no
// decimal conversion. Deliberately restricted to swapDestinations; NOT a
// general-purpose swap endpoint and should not be reused for real
// market-priced conversions.
func (s *WalletServer) Swap(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	var req swapRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	source := strings.ToUpper(strings.TrimSpace(req.SourceAsset))
	destination := strings.ToUpper(strings.TrimSpace(req.DestinationAsset))
	if source == destination {
		writeError(w, http.StatusBadRequest, "source and destination asset must differ")
		return
	}
	if !swapDestinations[source][destination] {
		writeError(w, http.StatusBadRequest, "swap is only available from USDT or USDC into BI2XUSD (no fee), or from BI2XUSD into USDT or USDC (1% fee)")
		return
	}

	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be a positive integer (raw token units)")
		return
	}

	// 1:1 base rate; the fee is computed in raw units on the source amount
	// and deducted from what is credited. Integer arithmetic (no floats) so
	// raw token amounts stay exact.
	credited := new(big.Int).Set(amount)
	feeBps, feeCharged := s.swapFeeRate(r.Context(), destination, claims.UserID)
	var feeAmount *big.Int
	if feeCharged {
		feeAmount = new(big.Int).Mul(amount, big.NewInt(feeBps))
		feeAmount.Div(feeAmount, big.NewInt(10000))
		if feeAmount.Sign() < 0 {
			feeAmount.SetInt64(0)
		}
		credited.Sub(credited, feeAmount)
	}
	if credited.Sign() <= 0 {
		writeError(w, http.StatusBadRequest, "amount too small: fee exceeds the swapped amount")
		return
	}

	// Debit the full source amount (fee included) from the source balance and
	// credit only the net amount to the destination balance.
	if err := s.Ledger.SwapBalance(r.Context(), claims.UserID, source, amount.String(), destination, credited.String()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if feeAmount != nil && feeAmount.Sign() > 0 && s.Referrals != nil {
		// Swap fees go to the treasury in full — no referral/affiliate split
		// (only spot/futures trading fees split; see
		// REFERRAL-AFFILIATE-PLAN.md). Logged, not failed, on error: the
		// swap itself already completed correctly above.
		if err := s.Referrals.CreditTreasuryFee(r.Context(), destination, feeAmount.String(), claims.UserID, "", "swap"); err != nil {
			s.Log.Error("credit swap fee to treasury failed", "userId", claims.UserID, "err", err)
		}
	}
	resp := map[string]string{
		"status":           "swapped",
		"sourceAsset":      source,
		"destinationAsset": destination,
		"amount":           amount.String(),
		"creditedAmount":   credited.String(),
	}
	if feeAmount != nil {
		resp["feeAmount"] = feeAmount.String()
	}
	writeJSON(w, http.StatusOK, resp)
}

// SwapPoolMax: GET /wallet/swap/max?asset=USDT|USDC
// Returns the amount of asset currently available in that asset's swappable
// pool — the exact figure a BI2XUSD -> USDT/USDC swap will be capped at
// right now (see repo.LedgerRepo.DebitSwapPoolCapped, which is the actual
// source of truth this mirrors). Any authenticated user can call this (same
// auth level as Swap itself) — it's a plain read of platform-wide state, not
// account-specific, so there's no reason to restrict it further. The
// frontend calls this before a user confirms a BI2XUSD -> stablecoin swap so
// the cap is visible upfront rather than only surfacing as a submit-time
// rejection; the server-side cap in SwapBalance is still the real
// enforcement point since this figure can go stale between the read and the
// actual swap (a concurrent swap-out or a fresh admin top-up can both move
// it in between).
func (s *WalletServer) SwapPoolMax(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(r); !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	asset := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("asset")))
	if asset != "USDT" && asset != "USDC" {
		writeError(w, http.StatusBadRequest, "asset must be USDT or USDC")
		return
	}
	swappableRaw, _, err := s.Ledger.SwapPoolBalance(r.Context(), asset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "load swap pool balance: "+err.Error())
		return
	}
	maxSwappable, err := rawToHumanUnits(swappableRaw)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "convert swap pool balance: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"asset": asset, "maxSwappable": maxSwappable})
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

// stuckWithdrawalAge is how long a withdrawal may sit in "processing" before
// the watchdog treats it as stuck rather than merely in flight. A normal
// withdrawal completes in well under a minute (chain confirmation is the
// slowest step, still seconds on Fuji); several minutes past that means the
// server crashed or restarted mid-flight, or a downstream call hung — the
// exact gap that used to require an admin to notice and call
// /admin/withdraw-recover by hand.
const stuckWithdrawalAge = 10 * time.Minute

// RunWithdrawalWatchdog polls for withdrawals stuck in "processing" and
// retries them automatically via the same path AdminRecoverWithdrawal's
// action=retry already uses, so a stuck withdrawal self-heals instead of
// silently waiting for a human to run the manual recovery endpoint. Call in
// its own goroutine; blocks until ctx is done. Safe to run even if s.Signer
// is nil (retry will fail fast per-item and get picked up again next tick,
// same failure mode as any other Signer-unavailable path in this file).
func (s *WalletServer) RunWithdrawalWatchdog(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recoverStuckWithdrawals(ctx)
		}
	}
}

func (s *WalletServer) recoverStuckWithdrawals(ctx context.Context) {
	stuck, err := s.Ledger.StuckProcessingWithdrawals(ctx, time.Now().Add(-stuckWithdrawalAge))
	if err != nil {
		s.Log.Error("withdrawal watchdog: list stuck withdrawals failed", "err", err)
		return
	}
	for _, entry := range stuck {
		s.Log.Warn("withdrawal watchdog: retrying withdrawal stuck in processing",
			"requestId", entry.ID, "userId", entry.UserID, "age", time.Since(entry.CreatedAt))
		if _, _, err := s.processWithdrawalRequest(ctx, entry.ID); err != nil {
			s.Log.Error("withdrawal watchdog: retry failed, will retry again next tick",
				"requestId", entry.ID, "err", err)
		}
	}
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
		// Deliberately does NOT clear any saved pending nonce (see
		// PendingNonceFor): if this request's earlier transaction is still
		// sitting in the mempool, it can still land on-chain after this call
		// marks the request "failed" in Postgres. That's an inherent risk of
		// force-failing a withdrawal whose broadcast transaction can't be
		// un-broadcast — an admin using this action should first confirm on a
		// block explorer that the transaction actually dropped/reverted, not
		// just that it looked stuck here.
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

// backfillPacingDelay spaces consecutive Credit calls during a backfill run.
// The engine's per-IP rate limiter (cmd/engine/ratelimit.go) allows 40
// req/sec sustained with a burst of 80 — a live incident showed a backfill
// with hundreds of nonzero balances firing Credit in a tight loop blew
// through the burst allowance almost immediately and then had every single
// remaining call rejected with 429, so backfill "succeeded" for zero users
// after a restart. 30ms keeps this comfortably under the sustained rate
// (~33 req/sec) even with Dex-Backend as the only caller of this endpoint,
// while still finishing a few hundred accounts in well under a minute.
const backfillPacingDelay = 30 * time.Millisecond

// runBackfill pushes every nonzero Postgres balance into the engine ledger,
// shared by AdminEngineBackfill (human-triggered) and InternalEngineBackfill
// (engine self-triggered on startup).
//
// Two things changed after the incident referenced above (both entries
// still logged as before, so nothing here reduces visibility, only fixes
// the actual sync):
//   - Calls are paced (backfillPacingDelay between each) instead of firing
//     as fast as the loop can go, and EngineClient.Credit itself now
//     retries a 429/5xx a few times internally (see engineclient.Client's
//     StatusError/callWithRetry) — between the two, a burst that used to
//     blow through the rate limiter now mostly doesn't, and any credit that
//     still hits a transient 429 gets a few more chances before this loop
//     gives up on it.
//   - A credit that still fails after those retries is persisted via
//     RecordBackfillFailure instead of only logged, so it isn't lost the
//     moment this process's log rotates — engine_backfill_failures. On
//     every run (not just when zero-value old failures exist), pending
//     failures from a PREVIOUS run are retried first: without this, a
//     later backfill would only ever see AllNonzeroBalances, which cannot
//     distinguish "this account already synced last time" from "this
//     account failed last time" — both just look like a nonzero Postgres
//     balance — and re-crediting an already-synced account would double
//     it (engineclient.Credit's per-call requestId dedupes a RETRY of the
//     same call, not two separate backfill runs, which each generate a
//     fresh id).
func (s *WalletServer) runBackfill(ctx context.Context) (synced, failed, total int, err error) {
	pending, err := s.Ledger.PendingBackfillFailures(ctx)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("load pending backfill failures: %w", err)
	}
	balances, err := s.Ledger.AllNonzeroBalances(ctx)
	if err != nil {
		return 0, 0, 0, err
	}

	// Pending failures first (already human-unit amounts as recorded at
	// failure time — see RecordBackfillFailure's call site below), then
	// every current nonzero balance. A pair present in both is retried
	// once: AllNonzeroBalances is the authoritative current amount, so it
	// wins if a legitimate credit already changed the balance since the
	// failure was recorded.
	seen := make(map[[2]string]bool, len(pending)+len(balances))
	items := make([]backfillItem, 0, len(pending)+len(balances))
	for _, p := range pending {
		key := [2]string{p.UserID, p.Asset}
		seen[key] = true
		items = append(items, backfillItem{userID: p.UserID, asset: p.Asset, amount: p.Amount, alreadyHuman: true})
	}
	for _, b := range balances {
		key := [2]string{b.UserID, b.Asset}
		if seen[key] {
			continue
		}
		items = append(items, backfillItem{userID: b.UserID, asset: b.Asset, amount: b.Amount})
	}

	return s.processBackfillItems(ctx, items)
}

// backfillItem is one (account, asset) pair runBackfill needs to credit.
// amount is either a raw Postgres balance (needs rawToHumanUnits) or an
// already-human-unit amount previously recorded by RecordBackfillFailure —
// alreadyHuman distinguishes the two, since re-converting an
// already-converted amount would silently shrink it by 10^6.
type backfillItem struct {
	userID, asset, amount string
	alreadyHuman          bool
}

// processBackfillItems is runBackfill's actual pacing/retry/durable-failure
// loop, factored out so it can be exercised directly against a small
// synthetic item list in tests — calling it only through runBackfill would
// mean every test run processes the ENTIRE real AllNonzeroBalances table
// (104+ rows in this deployment's Postgres as of the incident this was
// added for), which is both slow (each item pays the real pacing delay
// plus, for a failing item, real network round trips for its retries) and
// touches production tables as a side effect of testing.
func (s *WalletServer) processBackfillItems(ctx context.Context, items []backfillItem) (synced, failed, total int, err error) {
	for i, it := range items {
		if i > 0 {
			select {
			case <-time.After(backfillPacingDelay):
			case <-ctx.Done():
				return synced, failed, len(items), ctx.Err()
			}
		}
		amount := it.amount
		if !it.alreadyHuman {
			// Postgres stores token amounts in 10^-6 raw units, while the
			// matching engine ledger uses human units. Passing the raw
			// integer through here inflated every restored balance by one
			// million after a restart. A pending-failure entry was already
			// converted to human units before being recorded, so it's used
			// as-is (re-converting it here would be a second, wrong
			// conversion).
			converted, convErr := rawToHumanUnits(it.amount)
			if convErr != nil {
				s.Log.Error("backfill: invalid raw balance", "err", convErr, "userId", it.userID, "asset", it.asset)
				failed++
				continue
			}
			amount = converted
		}
		if cerr := s.EngineClient.Credit(ctx, it.userID, it.asset, amount); cerr != nil {
			s.Log.Error("backfill: credit failed", "err", cerr, "userId", it.userID, "asset", it.asset)
			if rerr := s.Ledger.RecordBackfillFailure(ctx, it.userID, it.asset, amount, cerr.Error()); rerr != nil {
				s.Log.Error("backfill: could not durably record failure", "err", rerr, "userId", it.userID, "asset", it.asset)
			}
			failed++
			continue
		}
		if it.alreadyHuman {
			// This was a previously-failed pair that just succeeded — clear
			// its durable record so it stops being retried on every future
			// run once it's actually fixed.
			if cerr := s.Ledger.ClearBackfillFailure(ctx, it.userID, it.asset); cerr != nil {
				s.Log.Error("backfill: could not clear resolved failure record", "err", cerr, "userId", it.userID, "asset", it.asset)
			}
		}
		synced++
	}
	return synced, failed, len(items), nil
}

// rawToHumanUnits converts this platform's six-decimal database representation
// to the decimal representation used by the matching engine.
func rawToHumanUnits(raw string) (string, error) {
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return "", fmt.Errorf("invalid raw token amount %q", raw)
	}
	r := new(big.Rat).SetFrac(n, big.NewInt(1_000_000))
	return strings.TrimRight(strings.TrimRight(r.FloatString(6), "0"), "."), nil
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

type internalSpotSettleBody struct {
	BuyerID           string `json:"buyerId"`
	SellerID          string `json:"sellerId"`
	Base              string `json:"base"`
	Quote             string `json:"quote"`
	BaseQuantity      string `json:"baseQuantity"`
	BuyerQuoteDebit   string `json:"buyerQuoteDebit"`
	SellerQuoteCredit string `json:"sellerQuoteCredit"`
	BuyerFee          string `json:"buyerFee"`
	SellerFee         string `json:"sellerFee"`
}

type internalReplaceLocksBody struct {
	UserID string            `json:"userId"`
	Locks  map[string]string `json:"locks"`
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

type internalEnsureUserBody struct {
	UserID string `json:"userId"`
}

// InternalEnsureUser: POST /internal/user/ensure {userId}
// Called by the bots service before it funds a market-maker desk's synthetic
// account, so the desk's user_balances credits/locks satisfy the foreign key
// to users. The bots service risk-locks the desk by this literal id against
// Dex-Backend's ledger, so the id in the users row must be exactly userId,
// not a database-generated one - see repo.UserRepo.EnsureByID. Idempotent.
func (s *WalletServer) InternalEnsureUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalEnsureUserBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
		writeError(w, http.StatusBadRequest, "userId is required")
		return
	}
	if err := s.Users.EnsureByID(r.Context(), req.UserID, "market-maker"); err != nil {
		s.Log.Error("ensure backend user failed", "userId", req.UserID, "err", err)
		writeError(w, http.StatusInternalServerError, "could not ensure user")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "userId": req.UserID})
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
	// Idempotency-Key is optional: a caller that doesn't send one (e.g.
	// matching-engine today) gets the exact old behavior — see
	// LedgerRepo.LockBalanceIdempotent, which no-ops the guard when the key
	// is empty.
	if err := s.Ledger.LockBalanceIdempotent(r.Context(), req.UserID, req.Asset, req.Amount, r.Header.Get("Idempotency-Key")); err != nil {
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
	if err := s.Ledger.UnlockBalanceIdempotent(r.Context(), req.UserID, req.Asset, req.Amount, r.Header.Get("Idempotency-Key")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "unlocked"})
}

// InternalReplaceLocks atomically sets all active reservation totals for a
// dedicated market-maker wallet. It avoids the unlock/lock race that used to
// leave a refresh with no liquidity or with stale durable locks.
func (s *WalletServer) InternalReplaceLocks(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalReplaceLocksBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.Locks) == 0 {
		writeError(w, http.StatusBadRequest, "userId and locks are required")
		return
	}
	if err := s.Ledger.ReplaceLocksFor(r.Context(), req.UserID, req.Locks); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "replaced"})
}

// InternalReleaseLocks clears locks orphaned by a matching-engine restart.
func (s *WalletServer) InternalReleaseLocks(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalLockBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Ledger.ReleaseLocksFor(r.Context(), req.UserID, req.Asset); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "locks released"})
}

// InternalAvailableBalance: GET /internal/balance/available?userId=&asset=
// returns one account's true Postgres available balance (total minus
// locked) for one asset, in human units. Used by the bots service's
// recreditDesk to resync the matching engine's in-memory ledger to what the
// account can actually spend right now — after ReleaseLocks, that's the
// account's real total, which is not always the same as a desk's tracked
// quote_amount/base_amount config (those reflect admin deposits/withdrawals
// only; they never move when trading P&L consumes or adds to the wallet's
// real balance, so trusting them instead of asking Postgres directly can
// resync the engine to a stale/wrong figure — see recreditDesk's comment).
func (s *WalletServer) InternalAvailableBalance(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	userID := strings.TrimSpace(r.URL.Query().Get("userId"))
	asset := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("asset")))
	if userID == "" || asset == "" {
		writeError(w, http.StatusBadRequest, "userId and asset are required")
		return
	}
	balances, err := s.Ledger.BalancesFor(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "load balance: "+err.Error())
		return
	}
	locked, err := s.Ledger.LockedBalancesFor(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "load locked balance: "+err.Error())
		return
	}
	rawTotal, ok := balances[asset]
	if !ok {
		writeError(w, http.StatusBadRequest, "unsupported asset "+asset)
		return
	}
	total, ok := new(big.Int).SetString(rawTotal, 10)
	if !ok {
		writeError(w, http.StatusInternalServerError, "invalid balance amount")
		return
	}
	lockedAmt, ok := new(big.Int).SetString(locked[asset], 10)
	if !ok {
		lockedAmt = big.NewInt(0)
	}
	available := new(big.Int).Sub(total, lockedAmt)
	if available.Sign() < 0 {
		available = big.NewInt(0)
	}
	humanAvailable, err := rawToHumanUnits(available.String())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "convert balance: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"userId": userID, "asset": asset, "available": humanAvailable})
}

// InternalResetBalance reclaims an internal desk wallet during desk deletion.
func (s *WalletServer) InternalResetBalance(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalLockBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Ledger.ResetBalanceFor(r.Context(), req.UserID, req.Asset); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "balance reset"})
}

// InternalSyncBalance restores an internal desk wallet to its authoritative
// allocated capital after an engine restart discarded its in-memory state.
func (s *WalletServer) InternalSyncBalance(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalLockBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.Ledger.SyncBalanceFor(r.Context(), req.UserID, req.Asset, req.Amount); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "balance synchronized"})
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
	if err := s.Ledger.SettleLockedDebitIdempotent(r.Context(), req.UserID, req.Asset, req.Amount, r.Header.Get("Idempotency-Key")); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "settled"})
}

// InternalSettleSpot atomically persists both sides of one completed spot
// trade. It replaces the previous sequence of independent settle/credit calls.
func (s *WalletServer) InternalSettleSpot(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalSpotSettleBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.BuyerID == "" || req.SellerID == "" || req.Base == "" || req.Quote == "" {
		writeError(w, http.StatusBadRequest, "invalid spot settlement request")
		return
	}
	if err := s.Ledger.SettleSpotTrade(r.Context(), req.BuyerID, req.SellerID, req.Base, req.Quote, req.BaseQuantity, req.BuyerQuoteDebit, req.SellerQuoteCredit); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	// Route each side's fee (referral/affiliate beneficiary share, remainder
	// to the platform treasury) — a separate step from the balance transfer
	// above since it's a revenue-accounting concern, not a settlement
	// correctness one: if this fails, the trade itself has already settled
	// correctly, so this is logged rather than surfaced as a settlement
	// error (same best-effort spirit as the async credit calls elsewhere in
	// this bridge). See REFERRAL-AFFILIATE-PLAN.md.
	if s.Referrals != nil {
		if req.BuyerFee != "" && req.BuyerFee != "0" {
			if err := s.Referrals.SettleFee(r.Context(), req.BuyerID, req.Quote, req.BuyerFee, "", "spot"); err != nil {
				s.Log.Error("route buyer spot fee failed", "err", err, "userId", req.BuyerID)
			}
		}
		if req.SellerFee != "" && req.SellerFee != "0" {
			if err := s.Referrals.SettleFee(r.Context(), req.SellerID, req.Quote, req.SellerFee, "", "spot"); err != nil {
				s.Log.Error("route seller spot fee failed", "err", err, "userId", req.SellerID)
			}
		}
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
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if amount.Sign() < 0 {
		if err := s.Ledger.DebitBalanceIdempotent(r.Context(), req.UserID, req.Asset, amount.Neg(amount).String(), idempotencyKey); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
	} else if amount.Sign() > 0 {
		if err := s.Ledger.CreditBalanceIdempotent(r.Context(), req.UserID, req.Asset, amount.String(), idempotencyKey); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "credited"})
}

// InternalCreditTreasury: POST /internal/treasury/credit
// {asset, amount, accountId, tradeRef, category} — records real platform
// revenue directly into platform_treasury_entries/platform_treasury_balances
// via the existing ReferralRepo.CreditTreasuryFee (100% to treasury, no
// referral/affiliate split — same as a swap fee). Added for BitDX Prop
// Firm's 20% share of a funded trader's realized live-trading profit
// (PROP_FIRM_PLAN.md §11/§13): that revenue is computed and split entirely
// inside the separate PropFirm Backend service, which has no direct access
// to this database, so it calls this endpoint to record its share honestly
// instead of the money silently vanishing from any ledger. category is
// validated against ValidTreasuryCategories rather than trusted verbatim,
// since it becomes part of a SQL CHECK-constrained column.
func (s *WalletServer) InternalCreditTreasury(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req struct {
		Asset     string `json:"asset"`
		Amount    string `json:"amount"`
		AccountID string `json:"accountId"`
		TradeRef  string `json:"tradeRef"`
		Category  string `json:"category"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Asset == "" || req.Amount == "" {
		writeError(w, http.StatusBadRequest, "asset and amount are required")
		return
	}
	if !ValidTreasuryCategories[req.Category] {
		writeError(w, http.StatusBadRequest, "unknown category")
		return
	}
	if err := s.Referrals.CreditTreasuryFee(r.Context(), req.Asset, req.Amount, req.AccountID, req.TradeRef, req.Category); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "credited"})
}

// ValidTreasuryCategories mirrors platform_treasury_entries' category CHECK
// constraint (db.ensureTreasuryEntryCategory and its widening migrations) —
// kept as a Go-side allowlist so InternalCreditTreasury rejects an unknown
// category with a clear 400 instead of a raw Postgres constraint-violation
// error surfacing to a caller.
var ValidTreasuryCategories = map[string]bool{
	"spot": true, "futures": true, "liquidation": true, "swap": true, "prediction": true, "propfirm": true,
}

// InternalSettleFee: POST /internal/balance/fee {userId, asset, amount}
// Called by the matching-engine after a futures maker/taker fee has already
// been debited from userId's balance (see FuturesSettlement.applyFill).
// Unlike InternalCreditBalance/InternalSettleBalance (plain debit/credit),
// this routes the collected fee: a referral/affiliate beneficiary's share
// (if userId has a permanent earning-source link) to that beneficiary's own
// balance, and the remainder to the platform treasury — see
// REFERRAL-AFFILIATE-PLAN.md. A no-op (200 OK) if Referrals isn't wired or
// amount is zero.
type internalSettleFeeBody struct {
	UserID   string `json:"userId"`
	Asset    string `json:"asset"`
	Amount   string `json:"amount"`
	Category string `json:"category"` // "futures" or "liquidation" — see FuturesSettlement.applyFill
}

func (s *WalletServer) InternalSettleFee(w http.ResponseWriter, r *http.Request) {
	if !s.checkEngineSecret(w, r) {
		return
	}
	var req internalSettleFeeBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	// Added 2026-09-16 alongside the prediction-service's fee routing: this
	// used to silently coerce ANY unrecognized category to "futures" (only
	// "futures"/"liquidation" ever called this endpoint before), which would
	// have misattributed prediction-market fee revenue as futures fee
	// revenue in reporting. "spot" and "swap" are accepted here too for the
	// same reason, even though those callers currently route fees through
	// SettleSpot instead — this endpoint's own accepted set should match the
	// database's CHECK constraint (ensureTreasuryEntryPredictionCategory),
	// not silently diverge from it.
	category := req.Category
	switch category {
	case "futures", "liquidation", "spot", "swap", "prediction":
		// already a recognized category
	default:
		category = "futures"
	}
	if s.Referrals != nil && req.Amount != "" && req.Amount != "0" {
		if err := s.Referrals.SettleFeeIdempotent(r.Context(), req.UserID, req.Asset, req.Amount, "", category, r.Header.Get("Idempotency-Key")); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "fee settled"})
}
