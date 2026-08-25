package engineclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// TradeOrder is the gateway's validated representation of an engine order.
// AccountID is deliberately supplied by Dex-Backend from verified JWT claims,
// never decoded from a browser request body.
type TradeOrder struct {
	AccountID   string
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

type OrderResponse struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Filled  string `json:"filled"`
	Trades  int    `json:"trades"`
}

type AttachedOrder struct {
	Parent     TradeOrder
	TakeProfit *TradeOrder
	StopLoss   *TradeOrder
}

// AttachedOrderResponse is the payload from the engine's atomic
// POST /attached-order.
type AttachedOrderResponse struct {
	OrderID      string `json:"orderId"`
	Status       string `json:"status"`
	Filled       string `json:"filled"`
	Trades       int    `json:"trades"`
	GroupID      string `json:"groupId,omitempty"`
	TakeProfitID string `json:"takeProfitId,omitempty"`
	StopLossID   string `json:"stopLossId,omitempty"`
}

// SubmitAttachedOrder places the entry and its TP/SL legs as one atomic
// engine request. The engine itself only activates and places the legs
// once the entry actually fills, sized to the real fill (not the requested
// quantity), and links them with a shared GroupID so the engine's OCO
// listener can cancel the sibling leg the instant one triggers and resize
// both on partial close/liquidation/reversal. This replaced a prior
// implementation that submitted the entry and each leg as independent,
// unlinked /order calls with no OCO or resize guarantees.
func (c *Client) SubmitAttachedOrder(ctx context.Context, attached AttachedOrder) (AttachedOrderResponse, error) {
	q := url.Values{}
	q.Set("account", attached.Parent.AccountID)
	q.Set("symbol", attached.Parent.Symbol)
	q.Set("market", attached.Parent.Market)
	q.Set("side", attached.Parent.Side)
	q.Set("type", attached.Parent.Type)
	q.Set("price", attached.Parent.Price)
	q.Set("qty", attached.Parent.Qty)
	setOptional(q, "stopPrice", attached.Parent.StopPrice)
	if attached.Parent.ReduceOnly {
		q.Set("reduceOnly", "true")
	}
	if attached.Parent.SlippageBps != nil {
		q.Set("slippageBps", fmt.Sprintf("%d", *attached.Parent.SlippageBps))
	}
	if attached.Parent.Leverage != nil {
		q.Set("leverage", fmt.Sprintf("%d", *attached.Parent.Leverage))
	}
	setOptional(q, "marginMode", attached.Parent.MarginMode)
	if attached.TakeProfit != nil {
		setOptional(q, "tpPrice", attached.TakeProfit.Price)
	}
	if attached.StopLoss != nil {
		setOptional(q, "slStopPrice", attached.StopLoss.StopPrice)
	}

	var out AttachedOrderResponse
	err := c.tradeCall(ctx, http.MethodPost, "/attached-order", q, &out)
	return out, err
}

type OrdersResponse struct {
	Orders json.RawMessage `json:"orders"`
}
type OrderHistoryResponse struct {
	Orders     json.RawMessage `json:"orders"`
	NextCursor string          `json:"nextCursor,omitempty"`
}

func (c *Client) OrderHistory(ctx context.Context, accountID, before string, limit int) (OrderHistoryResponse, error) {
	q := url.Values{"account": {accountID}, "before": {before}, "limit": {fmt.Sprintf("%d", limit)}}
	var out OrderHistoryResponse
	err := c.tradeCall(ctx, http.MethodGet, "/order-history", q, &out)
	return out, err
}

type PositionsResponse struct {
	Futures json.RawMessage `json:"futures"`
	Options json.RawMessage `json:"options"`
}

type BalanceResponse struct {
	Account   string `json:"account"`
	Asset     string `json:"asset"`
	Balance   string `json:"balance"`
	Reserved  string `json:"reserved"`
	Available string `json:"available"`
}

// SubmitOrder forwards an account-scoped order using the internal service
// credential. The account is supplied by the calling gateway, not the user.
func (c *Client) SubmitOrder(ctx context.Context, order TradeOrder) (OrderResponse, error) {
	q := url.Values{}
	q.Set("account", order.AccountID)
	q.Set("symbol", order.Symbol)
	q.Set("market", order.Market)
	q.Set("side", order.Side)
	q.Set("type", order.Type)
	q.Set("price", order.Price)
	q.Set("qty", order.Qty)
	setOptional(q, "stopPrice", order.StopPrice)
	if order.ReduceOnly {
		q.Set("reduceOnly", "true")
	}
	if order.SlippageBps != nil {
		q.Set("slippageBps", fmt.Sprintf("%d", *order.SlippageBps))
	}
	if order.Leverage != nil {
		q.Set("leverage", fmt.Sprintf("%d", *order.Leverage))
	}
	setOptional(q, "marginMode", order.MarginMode)
	setOptional(q, "optionType", order.OptionType)
	setOptional(q, "strike", order.Strike)
	setOptional(q, "expiry", order.Expiry)

	var out OrderResponse
	err := c.tradeCall(ctx, http.MethodPost, "/order", q, &out)
	return out, err
}

func (c *Client) CancelOrder(ctx context.Context, accountID, symbol, market, orderID string) (OrderResponse, error) {
	q := url.Values{"account": {accountID}, "symbol": {symbol}, "market": {market}, "order_id": {orderID}}
	var out OrderResponse
	err := c.tradeCall(ctx, http.MethodPost, "/cancel", q, &out)
	return out, err
}

func (c *Client) Orders(ctx context.Context, accountID string) (OrdersResponse, error) {
	var out OrdersResponse
	err := c.tradeCall(ctx, http.MethodGet, "/orders", url.Values{"account": {accountID}}, &out)
	return out, err
}

func (c *Client) Positions(ctx context.Context, accountID string) (PositionsResponse, error) {
	var out PositionsResponse
	err := c.tradeCall(ctx, http.MethodGet, "/positions", url.Values{"account": {accountID}}, &out)
	return out, err
}

func (c *Client) Balance(ctx context.Context, accountID, asset string) (BalanceResponse, error) {
	var out BalanceResponse
	err := c.tradeCall(ctx, http.MethodGet, "/admin/balance", url.Values{"account": {accountID}, "asset": {asset}}, &out)
	return out, err
}

func (c *Client) tradeCall(ctx context.Context, method, path string, q url.Values, out any) error {
	if !c.Enabled() || c.secret == "" {
		return fmt.Errorf("matching engine gateway is not configured")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Engine-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("engine trade request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &Error{Status: resp.StatusCode, Message: string(body)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode engine response: %w", err)
	}
	return nil
}

func setOptional(q url.Values, key, value string) {
	if value != "" {
		q.Set(key, value)
	}
}

// Error preserves the engine HTTP status so gateway handlers can return a
// meaningful client error without exposing a generic 500 for an order reject.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return e.Message }
