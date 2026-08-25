package engineclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
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

func (c *Client) SubmitAttachedOrder(ctx context.Context, attached AttachedOrder) (OrderResponse, error) {
	parent, err := c.SubmitOrder(ctx, attached.Parent)
	if err != nil {
		return parent, err
	}
	filled, err := strconv.ParseFloat(parent.Filled, 64)
	if err != nil || filled <= 0 {
		return parent, nil
	}
	for _, leg := range []*TradeOrder{attached.TakeProfit, attached.StopLoss} {
		if leg == nil {
			continue
		}
		copy := *leg
		copy.Qty = strconv.FormatFloat(filled, 'f', -1, 64)
		copy.ReduceOnly = true
		if _, err := c.SubmitOrder(ctx, copy); err != nil {
			return parent, err
		}
	}
	return parent, nil
}

type OrdersResponse struct {
	Orders json.RawMessage `json:"orders"`
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
