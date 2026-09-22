package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/repo"
)

// TradeServer is the authenticated user-facing gateway to the matching
// engine. It derives the account from the wallet session and never accepts an
// account identifier from the browser.
type TradeServer struct {
	*Server
	Engine *engineclient.Client
	Ledger *repo.LedgerRepo

	// acctLocks serializes reconcileOrderBalance + order submission per
	// account. Without this, two orders from the same account placed close
	// together race: order 1 reserves in the engine's in-memory ledger
	// (synchronous) and only afterward locks the same amount in Postgres
	// (a separate HTTP round-trip). If order 2's reconcileOrderBalance reads
	// Postgres in that gap — after order 1's in-memory reservation but
	// before its Postgres lock lands — order 1's lock is invisible to
	// order 2's "total minus locked" read while already reflected in the
	// engine's mirror, so the delta comes out positive and reconcile
	// CREDITS the engine with a phantom amount equal to order 1's in-flight
	// reservation, silently erasing it. Both orders then believe there's
	// more available than Postgres can actually back, and the second one to
	// settle fails "insufficient locked ... " — which halts the whole
	// symbol (see marketMakerReplaceHandler and the engine's settlement
	// path). Serializing per account closes exactly this window; it does
	// not serialize across different accounts, so it costs nothing under
	// normal multi-user load.
	//
	// Each entry is a 1-buffered channel used as a try-lock: acquiring means
	// sending into it, releasing means receiving. A deep same-account burst
	// (e.g. a client double-click storm, or a broken retry loop) would
	// otherwise queue silently behind a plain mutex until each request's own
	// HTTP client timeout fires anyway — acquireAccountSlot instead waits
	// only up to a short bound and fails fast with a clear, cheap 429 so the
	// caller can retry deliberately instead of the request hanging for the
	// full engine-call timeout for no useful reason.
	acctLocks   map[string]chan struct{}
	acctLocksMu sync.Mutex

	// reconcileVerified caches, per accountID+asset, the last time
	// reconcileOrderBalance ran its full three-way read (Postgres balance,
	// Postgres locked, engine mirror) and found the two ledgers already in
	// agreement (delta == 0) — see PERFORMANCE-CODE-REVIEW-FINDINGS.md item
	// #8. Every order pays reconcileOrderBalance's full round trip (two
	// Postgres queries + one engine call, fired concurrently but still
	// ~4-6s of real network latency against the live Aiven instance per
	// engineclient.New's doc comment) before it even reaches the engine's
	// own submit path. In the steady state — no deposit, no restart, no
	// drift — that whole check reliably finds nothing to correct, so a
	// short TTL lets back-to-back orders from the same account+asset skip
	// straight past it. Entries are invalidated (deleted, not just left to
	// expire) the moment ANY correction is applied for that account+asset,
	// so a real drift is never masked by a stale "recently verified" entry
	// — see reconcileOrderBalance's use of this map for exactly where.
	reconcileVerified   map[string]time.Time
	reconcileVerifiedMu sync.Mutex
}

// reconcileVerifiedTTL bounds how long a "no drift found" result from
// reconcileOrderBalance may be trusted without re-checking. Short enough
// that a real deposit or engine restart is caught within a few seconds of
// the next order (reconcileOrderBalance's whole purpose), long enough to
// skip the full three-way round trip for every order in a fast burst from
// the same account.
const reconcileVerifiedTTL = 5 * time.Second

// reconcileCacheKey identifies one account+asset pair for reconcileVerified.
// asset alone (not symbol/market/side) matches what reconcileOrderBalance
// actually reconciles — settlementAssetForOrder resolves symbol/market/side
// down to a single asset before any of the reads happen, so two orders on
// different symbols that happen to settle in the same asset (e.g. two
// different USDC-quoted pairs) correctly share one cache entry.
func reconcileCacheKey(accountID, asset string) string {
	return accountID + ":" + strings.ToUpper(asset)
}

// reconcileRecentlyVerified reports whether accountID+asset was found fully
// in sync within the last reconcileVerifiedTTL.
func (s *TradeServer) reconcileRecentlyVerified(accountID, asset string) bool {
	s.reconcileVerifiedMu.Lock()
	defer s.reconcileVerifiedMu.Unlock()
	t, ok := s.reconcileVerified[reconcileCacheKey(accountID, asset)]
	return ok && time.Since(t) < reconcileVerifiedTTL
}

// markReconcileVerified records that accountID+asset was just found fully in
// sync (delta == 0), starting a fresh TTL window.
func (s *TradeServer) markReconcileVerified(accountID, asset string) {
	s.reconcileVerifiedMu.Lock()
	defer s.reconcileVerifiedMu.Unlock()
	if s.reconcileVerified == nil {
		s.reconcileVerified = make(map[string]time.Time)
	}
	s.reconcileVerified[reconcileCacheKey(accountID, asset)] = time.Now()
}

// invalidateReconcileVerified drops any cached "recently verified" entry for
// accountID+asset — called whenever a real correction is applied, so the
// next order re-checks from scratch instead of trusting a window that
// started before the correction (and therefore before whatever caused the
// drift in the first place).
func (s *TradeServer) invalidateReconcileVerified(accountID, asset string) {
	s.reconcileVerifiedMu.Lock()
	defer s.reconcileVerifiedMu.Unlock()
	delete(s.reconcileVerified, reconcileCacheKey(accountID, asset))
}

// acctQueueWait bounds how long a request waits for its own account's slot
// before giving up. Deliberately shorter than the engine client's own 10s
// call timeout: a request that's still queued this deep in has no realistic
// chance of also completing the engine round-trip before that timeout, so
// failing fast here gives a clearer error than a generic gateway timeout.
// 8s: half of the engine client's 20s call ceiling (see engineclient.New's
// doc comment on real-world Postgres latency) — a request already waiting
// this long for its own account's turn has no realistic chance of also
// completing a full engine round-trip inside that ceiling.
const acctQueueWait = 8 * time.Second

// acquireAccountSlot waits for accountID's turn (creating its slot on first
// use) and returns a release func, or ok=false if acctQueueWait elapsed
// first — the account already has too many orders in flight.
func (s *TradeServer) acquireAccountSlot(ctx context.Context, accountID string) (release func(), ok bool) {
	s.acctLocksMu.Lock()
	ch, exists := s.acctLocks[accountID]
	if !exists {
		if s.acctLocks == nil {
			s.acctLocks = make(map[string]chan struct{})
		}
		ch = make(chan struct{}, 1)
		s.acctLocks[accountID] = ch
	}
	s.acctLocksMu.Unlock()

	timer := time.NewTimer(acctQueueWait)
	defer timer.Stop()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true
	case <-timer.C:
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}

type tradeOrderRequest struct {
	Symbol      string `json:"symbol"`
	Market      string `json:"market"`
	Side        string `json:"side"`
	Type        string `json:"type"`
	Price       string `json:"price,omitempty"`
	Qty         string `json:"qty"`
	StopPrice   string `json:"stopPrice,omitempty"`
	ReduceOnly  bool   `json:"reduceOnly,omitempty"`
	SlippageBps *int   `json:"slippageBps,omitempty"`
	Leverage    *int   `json:"leverage,omitempty"`
	MarginMode  string `json:"marginMode,omitempty"`
	OptionType  string `json:"optionType,omitempty"`
	Strike      string `json:"strike,omitempty"`
	Expiry      string `json:"expiry,omitempty"`
}

type tradeCancelRequest struct {
	Symbol  string `json:"symbol"`
	Market  string `json:"market"`
	OrderID string `json:"orderId"`
}

type attachedOrderRequest struct {
	Parent     tradeOrderRequest  `json:"parent"`
	TakeProfit *tradeOrderRequest `json:"takeProfit,omitempty"`
	StopLoss   *tradeOrderRequest `json:"stopLoss,omitempty"`
}

func (s *TradeServer) claims(w http.ResponseWriter, r *http.Request) (string, bool) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return "", false
	}
	return claims.UserID, true
}

func (s *TradeServer) Order(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req tradeOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Symbol = strings.TrimSpace(req.Symbol)
	req.Market = strings.ToUpper(strings.TrimSpace(req.Market))
	req.Side = strings.ToUpper(strings.TrimSpace(req.Side))
	req.Type = strings.ToUpper(strings.TrimSpace(req.Type))
	if req.Symbol == "" || req.Market == "" || req.Side == "" || req.Qty == "" {
		writeError(w, http.StatusBadRequest, "symbol, market, side, and qty are required")
		return
	}
	if req.Type == "" {
		req.Type = "LIMIT"
	}
	// Hold this account's slot across reconcile-then-submit so a second
	// order from the same account can't read Postgres mid-window and
	// corrupt the engine mirror — see acctLocks' doc comment on TradeServer.
	release, ok := s.acquireAccountSlot(r.Context(), accountID)
	if !ok {
		writeError(w, http.StatusTooManyRequests, "too many concurrent orders for this account; retry shortly")
		return
	}
	defer release()
	if err := s.reconcileOrderBalance(r.Context(), accountID, req.Symbol, req.Market, req.Side); err != nil {
		s.tradeError(w, err)
		return
	}
	response, err := s.Engine.SubmitOrder(r.Context(), engineclient.TradeOrder{
		AccountID: accountID, Symbol: req.Symbol, Market: req.Market, Side: req.Side,
		Type: req.Type, Price: req.Price, Qty: req.Qty, StopPrice: req.StopPrice,
		ReduceOnly: req.ReduceOnly, SlippageBps: req.SlippageBps, Leverage: req.Leverage,
		MarginMode: req.MarginMode, OptionType: req.OptionType, Strike: req.Strike, Expiry: req.Expiry,
	})
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// reconcileOrderBalance repairs the engine mirror immediately before risk
// checks. This covers balances deposited before the engine started and engine
// restarts that occurred after the normal startup backfill.
//
// Which asset to reconcile depends on market, not just side: a SPOT order
// locks its base asset on SELL and its quote asset on BUY (the two legs the
// trader actually holds), but a FUTURES order always margins in the quote
// asset (BI2XUSD) regardless of side — shorting a non-crypto-backed future like
// EURUSD/GOLD/AAPL.us has no base-asset ledger column at all (there is no
// spot book, so no such balance exists), and even for crypto-backed futures
// (BTC/ETH/SOL/BNB) a SELL is a margined short, not a spend of held BTC/ETH/
// SOL/BNB. Reconciling the base leg for a futures SELL either fails outright
// (non-crypto symbols) or silently checks the wrong balance (crypto symbols).
// settlementAssetForOrder derives which asset reconcileOrderBalance should
// check/repair for a given order, mirroring the matching engine's own
// risk.assetFor logic (matching-engine/internal/risk/checker.go) so the two
// never disagree about which balance an order actually draws from.
//
// Spot/futures symbols are the simple 2-part BASE-QUOTE form: a futures
// order (either side) and a spot BUY draw quote currency; a spot SELL draws
// base currency.
//
// Option instrument symbols are the 5-part BASE-QUOTE-STRIKE-EXPIRY-TYPE
// form (e.g. "BTC-BI2XUSD-55000-20260917-CALL") — naively reusing the 2-part
// SplitN(symbol, "-", 2) split used for spot/futures took "BI2XUSD-55000-
// 20260917-CALL" as the "asset", which is never a real balance column, so
// EVERY options BUY order (whose parts[1] became that garbage string) was
// rejected "unsupported asset" and never even reached the engine. A SELL
// (writer) order happened to still work by accident: SplitN's parts[0]
// ("BTC") IS a real asset, purely coincidentally, not because the logic was
// options-aware. Both option sides actually settle in the quote currency
// (see the engine's assetFor: "Buyer pays premium in quote currency;
// seller... posts cash-secured collateral in quote currency too") — the
// buy/sell branch below that spot/futures need does not apply to options at
// all, which is why this needs its own branch rather than reusing theirs.
func settlementAssetForOrder(symbol, market, side string) (asset string, ok bool) {
	if strings.EqualFold(market, "OPTIONS") {
		parts := strings.Split(symbol, "-")
		if len(parts) < 5 {
			return "", false
		}
		return parts[1], true
	}
	parts := strings.SplitN(symbol, "-", 2)
	if len(parts) != 2 {
		return "", false
	}
	asset = parts[1]
	if strings.EqualFold(side, "SELL") && !strings.EqualFold(market, "FUTURES") {
		asset = parts[0]
	}
	return asset, true
}

// hasLiveReservation reports whether an engine BalanceResponse.Reserved
// string represents a positive reservation — i.e. whether
// reconcileOrderBalance must skip its drift-correcting Debit/Credit call for
// this account+asset. Extracted as a pure function so this specific guard
// (the fix for the cross-process settlement race described in
// reconcileOrderBalance's own comment) is directly unit-testable without
// standing up a real engine or Postgres.
func hasLiveReservation(reservedStr string) bool {
	reserved, ok := new(big.Rat).SetString(reservedStr)
	return ok && reserved.Sign() > 0
}

// reconcileDustFloor: a drift correction of this size or smaller is always
// allowed regardless of the account's balance — plausible rounding-scale
// drift on any asset, any balance size (including a near-zero one, where a
// fraction-of-balance cap alone would block even a fair correction).
const reconcileDustFloor = "0.01"

// reconcileMaxFraction: above the dust floor, a correction may still only
// be at most this fraction of whichever side (Postgres or engine) reports
// the larger balance for the asset.
const reconcileMaxFraction = "0.02"

// reconcileDeltaExceedsSafetyCap reports whether reconcileOrderBalance's
// computed delta (dbAmount - engineAmount) is too large to auto-correct
// safely. Extracted as a pure function so this guard is directly
// unit-testable without a real engine or Postgres — same shape as
// hasLiveReservation above, which exists for the same reason.
//
// Added 2026-09-14 after a live incident: reconcileOrderBalance silently
// debited a user's entire real BI2X balance to zero as a "correction",
// destroying it with no order ever created to explain where it went. This
// function's surrounding doc comments already record TWO earlier incidents
// where a timing/read race made the drift comparison see a bogus delta and
// auto-correct it for real; this was the third. Rather than trust any
// single fix to have closed every possible race in the comparison, this
// caps what an automatic correction is allowed to do: small drift (at most
// reconcileDustFloor of the asset, or at most reconcileMaxFraction of the
// larger side's balance) still auto-corrects exactly as before; anything
// bigger is presumed to be a bug in the comparison itself rather than real
// drift, and is skipped (the caller logs full detail and returns without
// touching the balance) rather than silently applied. This can never make
// a real, legitimate drift worse — the case this function exists for
// (a stale mirror after a deposit or an engine restart) is caught on a
// later call once conditions change — but it can no longer wipe an
// account's real balance in one silent shot.
func reconcileDeltaExceedsSafetyCap(dbAmount, engineAmount, delta *big.Rat) bool {
	absDelta := new(big.Rat).Abs(delta)
	dustFloor, _ := new(big.Rat).SetString(reconcileDustFloor)
	if absDelta.Cmp(dustFloor) <= 0 {
		return false
	}
	reference := new(big.Rat).Abs(dbAmount)
	if engAbs := new(big.Rat).Abs(engineAmount); engAbs.Cmp(reference) > 0 {
		reference = engAbs
	}
	maxFraction, _ := new(big.Rat).SetString(reconcileMaxFraction)
	maxAllowed := new(big.Rat).Mul(reference, maxFraction)
	return absDelta.Cmp(maxAllowed) > 0
}

func (s *TradeServer) reconcileOrderBalance(ctx context.Context, accountID, symbol, market, side string) error {
	if s.Ledger == nil || s.Engine == nil {
		return nil
	}
	asset, ok := settlementAssetForOrder(symbol, market, side)
	if !ok {
		return nil
	}
	// Short-circuit when this account+asset was already found fully in sync
	// within the last reconcileVerifiedTTL — see reconcileVerified's doc
	// comment on TradeServer for why this is safe (nothing skipped here can
	// mask a NEW drift: a deposit or engine restart during the TTL window
	// just means the next order after the window re-checks and catches it,
	// same as today's behavior always finding it on the very next order).
	if s.reconcileRecentlyVerified(accountID, asset) {
		return nil
	}
	// The three reads below (Postgres balances, Postgres locked balances,
	// engine's mirror balance) are independent of each other — none needs
	// another's result, they're only compared once all three are in. Firing
	// them concurrently instead of one-after-another turns 3 sequential
	// network/DB round trips (~4-6s each against the live Aiven Postgres
	// instance and the engine, per engineclient.New's doc comment) into the
	// cost of whichever one is slowest, since every order pays this before
	// even reaching the engine's own lock/settle round trips. The
	// comparison/safety-cap logic after this point is untouched.
	var (
		bals, lockedBals              map[string]string
		engineBal                     engineclient.BalanceResponse
		balsErr, lockedErr, engineErr error
	)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		bals, balsErr = s.Ledger.BalancesFor(ctx, accountID)
	}()
	go func() {
		defer wg.Done()
		lockedBals, lockedErr = s.Ledger.LockedBalancesFor(ctx, accountID)
	}()
	go func() {
		defer wg.Done()
		engineBal, engineErr = s.Engine.Balance(ctx, accountID, asset)
	}()
	wg.Wait()
	if balsErr != nil {
		return fmt.Errorf("load balance: %w", balsErr)
	}
	raw, ok := bals[strings.ToUpper(asset)]
	if !ok {
		return fmt.Errorf("unsupported asset %s", asset)
	}
	// Compare like with like: the engine's own /admin/balance reports THREE
	// separate figures — Balance (total, including reserved), Reserved, and
	// Available (Balance - Reserved) — see BalanceResponse and the engine's
	// ledger.Available/Balance/Reserved. This function's job is to repair
	// drift between Postgres's available capital and the engine's mirror of
	// it, so it must read the engine's Available field, not Balance.
	//
	// The previous version compared Postgres's available (total - locked)
	// against the engine's TOTAL (engineBal.Balance, which does NOT subtract
	// the engine's own in-memory reservations for still-open orders). Any
	// account with even one resting order made that comparison see a bogus
	// "deficit" equal to that order's reservation and call Engine.Debit for
	// it — which does not just adjust balance, it also releases that same
	// amount from the engine's reserved-for-orders tracking (see
	// risk.Ledger.Debit: "Release reservation up to the debited amount").
	// That silently zeroed out a live resting order's reservation on EVERY
	// subsequent order from the same account, with no concurrency or race
	// required to trigger it — reproduced with two fully sequential orders
	// against a fresh account. The account's real Postgres lock was
	// untouched, so the next trade against that resting order failed
	// "insufficient locked ..." and halted the whole symbol.
	if lockedErr != nil {
		return fmt.Errorf("load locked balance: %w", lockedErr)
	}
	lockedRaw := lockedBals[strings.ToUpper(asset)]
	dbTotalStr, err := rawToHumanUnits(raw)
	if err != nil {
		return fmt.Errorf("convert balance: %w", err)
	}
	dbLockedStr, err := rawToHumanUnits(lockedRaw)
	if err != nil {
		return fmt.Errorf("convert locked balance: %w", err)
	}
	dbAmount, ok := new(big.Rat).SetString(dbTotalStr)
	if !ok {
		return fmt.Errorf("invalid balance amount %s", dbTotalStr)
	}
	dbLocked, ok := new(big.Rat).SetString(dbLockedStr)
	if !ok {
		return fmt.Errorf("invalid locked balance amount %s", dbLockedStr)
	}
	dbAmount.Sub(dbAmount, dbLocked)
	if engineErr != nil {
		return fmt.Errorf("check engine balance: %w", engineErr)
	}

	// Never correct drift while the engine has ANY live reservation for this
	// asset — a second, deeper bug behind the same symptom the comment above
	// already found once (the stale Balance-vs-Available comparison). Even
	// comparing Available correctly, Engine.Debit/Credit are still unsafe to
	// call on an account with open reservations: risk.Ledger.Debit both
	// reduces balance AND releases reservation "up to the debited amount",
	// with no notion of WHICH order that reservation belongs to. A resting
	// order can be settling (via the engine's own independent, concurrent
	// call to /internal/balance/spot-settle, triggered by a DIFFERENT
	// account's incoming order matching against it) at the exact moment this
	// request's drift-correction Debit call runs — releasing the engine's
	// reservation tracking for that in-flight settlement out from under it.
	// The account's real Postgres lock is untouched by that Debit (it only
	// mutates the engine's in-memory mirror), so the settlement's own
	// locked-balance check then sees less than it needs and fails
	// "insufficient locked ...", which halts the ENTIRE symbol for every
	// account trading it — reproduced under concurrent multi-account load
	// with no restart or outage involved, a genuine live race, not merely
	// stale state.
	//
	// Reconciliation's actual job (per this function's own doc comment) is
	// to catch drift from deposits made before the engine started or
	// restarts after the startup backfill — both are properties of an
	// account with NO open orders yet (a fresh deposit, or an account whose
	// resting orders all predate the engine's last restart and so were
	// already re-backfilled at boot). An account with a live reservation
	// right now has already proven the two ledgers agreed on that
	// reservation at some point; skipping correction here costs nothing
	// real (any genuine remaining drift for OTHER, unreserved capital on
	// this asset is still caught in full once the account is fully flat).
	if hasLiveReservation(engineBal.Reserved) {
		return nil
	}

	engineAmount, ok := new(big.Rat).SetString(engineBal.Available)
	if !ok {
		return fmt.Errorf("invalid engine balance %s", engineBal.Available)
	}
	delta := new(big.Rat).Sub(dbAmount, engineAmount)
	if delta.Sign() == 0 {
		s.markReconcileVerified(accountID, asset)
		return nil
	}

	// Safety cap (added 2026-09-14, after a live incident where this
	// function debited a user's entire real BI2X balance to zero with no
	// order ever created to account for it — see
	// SEQUENCE-RESET-HISTORY-LOSS-BUG.md's sibling incident writeup, or ask
	// about "reconcileOrderBalance wiped BI2X balance" from this date).
	// This function's own comments above already document TWO prior
	// incidents where a timing/read race made this comparison see a bogus
	// delta and auto-correct it for real — this is the third. Rather than
	// trust any single fix to have closed every possible race in this
	// comparison, cap what an automatic correction is allowed to do — see
	// reconcileDeltaExceedsSafetyCap's doc comment.
	if reconcileDeltaExceedsSafetyCap(dbAmount, engineAmount, delta) {
		s.Log.Error("reconcileOrderBalance: delta exceeds safety cap, skipping auto-correction",
			"accountId", accountID, "asset", asset, "dbAvailable", dbAmount.FloatString(18),
			"engineAvailable", engineAmount.FloatString(18), "delta", delta.FloatString(18))
		return nil
	}

	// A real correction is about to be applied — drop any cached "recently
	// verified" entry so the next order re-checks from scratch rather than
	// trusting a TTL window that predates whatever caused this drift.
	s.invalidateReconcileVerified(accountID, asset)
	if delta.Sign() > 0 {
		return s.Engine.Credit(ctx, accountID, asset, delta.FloatString(18))
	}
	return s.Engine.Debit(ctx, accountID, asset, new(big.Rat).Abs(delta).FloatString(18))
}

func (s *TradeServer) AttachedOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req attachedOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	toOrder := func(in *tradeOrderRequest) *engineclient.TradeOrder {
		if in == nil {
			return nil
		}
		return &engineclient.TradeOrder{AccountID: accountID, Symbol: strings.TrimSpace(in.Symbol), Market: strings.ToUpper(in.Market), Side: strings.ToUpper(in.Side), Type: strings.ToUpper(in.Type), Price: in.Price, Qty: in.Qty, StopPrice: in.StopPrice, ReduceOnly: in.ReduceOnly, SlippageBps: in.SlippageBps, Leverage: in.Leverage, MarginMode: in.MarginMode}
	}
	parent := toOrder(&req.Parent)
	if parent == nil || parent.Symbol == "" || parent.Qty == "" {
		writeError(w, http.StatusBadRequest, "parent order is required")
		return
	}
	// Same stale-mirror repair Order() does before every plain order — an
	// attached order's entry leg is exactly as exposed to the engine-mirror
	// drift reconcileOrderBalance exists to fix, and skipping it here left
	// every TP/SL trade able to trigger the same "insufficient locked ..."
	// settlement halt that plain orders were already protected against.
	// Same per-account serialization as Order() — see acctLocks' doc comment.
	release, ok := s.acquireAccountSlot(r.Context(), accountID)
	if !ok {
		writeError(w, http.StatusTooManyRequests, "too many concurrent orders for this account; retry shortly")
		return
	}
	defer release()
	if err := s.reconcileOrderBalance(r.Context(), accountID, parent.Symbol, parent.Market, parent.Side); err != nil {
		s.tradeError(w, err)
		return
	}
	response, err := s.Engine.SubmitAttachedOrder(r.Context(), engineclient.AttachedOrder{Parent: *parent, TakeProfit: toOrder(req.TakeProfit), StopLoss: toOrder(req.StopLoss)})
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *TradeServer) Cancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req tradeCancelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(req.Symbol) == "" || strings.TrimSpace(req.Market) == "" || strings.TrimSpace(req.OrderID) == "" {
		writeError(w, http.StatusBadRequest, "symbol, market, and orderId are required")
		return
	}
	response, err := s.Engine.CancelOrder(r.Context(), accountID, req.Symbol, strings.ToUpper(req.Market), req.OrderID)
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *TradeServer) Orders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	response, err := s.Engine.Orders(r.Context(), accountID)
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *TradeServer) OrderHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	response, err := s.Engine.OrderHistory(r.Context(), accountID, parseHistoryFilter(r))
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// Fills returns individual trade executions for the caller's account — the
// fill-level complement to OrderHistory's per-order aggregates.
func (s *TradeServer) Fills(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	response, err := s.Engine.Fills(r.Context(), accountID, parseHistoryFilter(r))
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// FundingHistory returns the caller's persisted funding payments.
func (s *TradeServer) FundingHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	response, err := s.Engine.FundingHistory(r.Context(), accountID, parseHistoryFilter(r))
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// PnlHistory returns the caller's authoritative realized-PnL events.
func (s *TradeServer) PnlHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	response, err := s.Engine.PnlHistory(r.Context(), accountID, parseHistoryFilter(r))
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// parseHistoryFilter reads the symbol/market/after/before/limit query params
// shared by OrderHistory and Fills.
func parseHistoryFilter(r *http.Request) engineclient.HistoryFilter {
	q := r.URL.Query()
	limit := 50
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	return engineclient.HistoryFilter{
		Symbol: strings.TrimSpace(q.Get("symbol")),
		Market: strings.ToUpper(strings.TrimSpace(q.Get("market"))),
		After:  q.Get("after"),
		Before: q.Get("before"),
		Limit:  limit,
	}
}

func (s *TradeServer) Positions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	response, err := s.Engine.Positions(r.Context(), accountID)
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *TradeServer) Balance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	asset := strings.TrimSpace(r.URL.Query().Get("asset"))
	if asset == "" {
		writeError(w, http.StatusBadRequest, "asset is required")
		return
	}
	response, err := s.Engine.Balance(r.Context(), accountID, asset)
	if err != nil {
		s.tradeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *TradeServer) tradeError(w http.ResponseWriter, err error) {
	var engineErr *engineclient.Error
	if errors.As(err, &engineErr) {
		status := engineErr.Status
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		writeError(w, status, strings.TrimSpace(engineErr.Message))
		return
	}
	s.Log.Error("matching engine gateway failed", "error", err)
	writeError(w, http.StatusBadGateway, "trading service unavailable")
}
