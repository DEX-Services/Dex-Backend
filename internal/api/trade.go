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
	// The engine's mirrored balance represents *available* (spendable)
	// capital — it's what Reserve/Lock draws down as orders are placed. Bals
	// above is the raw total column (unlocked + locked). Comparing total
	// against the engine's available figure and crediting the difference
	// double-counts any amount already locked behind another open order: the
	// engine gets re-credited funds it correctly excluded, letting a second
	// concurrent order over-reserve against capital that isn't actually free.
	// That mismatch surfaces downstream as a spot-settlement failure
	// ("insufficient locked USDB for buyer") which halts the whole symbol.
	// Subtract the locked amount so this reconciles against the same
	// available balance the engine is supposed to track.
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
	engineAmount, ok := new(big.Rat).SetString(engineBal.Balance)
	if !ok {
		return fmt.Errorf("invalid engine balance %s", engineBal.Balance)
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
