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
// asset (USDB) regardless of side — shorting a non-crypto-backed future like
// EURUSD/GOLD/AAPL.us has no base-asset ledger column at all (there is no
// spot book, so no such balance exists), and even for crypto-backed futures
// (BTC/ETH/SOL/BNB) a SELL is a margined short, not a spend of held BTC/ETH/
// SOL/BNB. Reconciling the base leg for a futures SELL either fails outright
// (non-crypto symbols) or silently checks the wrong balance (crypto symbols).
func (s *TradeServer) reconcileOrderBalance(ctx context.Context, accountID, symbol, market, side string) error {
	if s.Ledger == nil || s.Engine == nil {
		return nil
	}
	parts := strings.SplitN(symbol, "-", 2)
	if len(parts) != 2 {
		return nil
	}
	asset := parts[1]
	if strings.EqualFold(side, "SELL") && !strings.EqualFold(market, "FUTURES") {
		asset = parts[0]
	}
	bals, err := s.Ledger.BalancesFor(ctx, accountID)
	if err != nil {
		return fmt.Errorf("load balance: %w", err)
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
	lockedBals, err := s.Ledger.LockedBalancesFor(ctx, accountID)
	if err != nil {
		return fmt.Errorf("load locked balance: %w", err)
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
	engineBal, err := s.Engine.Balance(ctx, accountID, asset)
	if err != nil {
		return fmt.Errorf("check engine balance: %w", err)
	}
	engineAmount, ok := new(big.Rat).SetString(engineBal.Available)
	if !ok {
		return fmt.Errorf("invalid engine balance %s", engineBal.Available)
	}
	delta := new(big.Rat).Sub(dbAmount, engineAmount)
	if delta.Sign() > 0 {
		return s.Engine.Credit(ctx, accountID, asset, delta.FloatString(18))
	}
	if delta.Sign() < 0 {
		return s.Engine.Debit(ctx, accountID, asset, new(big.Rat).Abs(delta).FloatString(18))
	}
	return nil
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
