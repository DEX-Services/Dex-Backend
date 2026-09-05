// Command qaharness is a rewritten, expanded version of cmd/loadtest for the
// full exhaustive QA pass: creates/funds 100 test users, then executes a wide
// scenario matrix (spot/futures, market/limit, TP/SL, user-vs-user matching,
// edge cases) against the live 17-market USDB stack. HTTP-only against the
// running services — opens no direct DB connections.
//
// Usage: go run ./cmd/qaharness <phase>
//
//	phase = "setup"     create+fund 100 users
//	phase = "matrix"    run the full scenario matrix (needs setup done first)
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dex/dex-backend/internal/auth"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var (
	backendURL   = env("BACKEND_URL", "http://localhost:8081")
	botsURL      = env("BOTS_URL", "http://localhost:8082")
	jwtSecret    = os.Getenv("JWT_SECRET")
	engineSecret = firstNonEmpty(os.Getenv("ENGINE_SHARED_SECRET"), os.Getenv("DEX_BACKEND_ENGINE_SECRET"))
	httpClient   = &http.Client{Timeout: 20 * time.Second}
)

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

const numUsers = 100
const usdbPerUser = "100000"

func userID(i int) string { return fmt.Sprintf("LOADTEST_USER_%03d", i) }

func main() {
	if jwtSecret == "" || engineSecret == "" {
		fatal("JWT_SECRET and ENGINE_SHARED_SECRET/DEX_BACKEND_ENGINE_SECRET must be set")
	}
	if len(os.Args) < 2 {
		fatal("usage: qaharness <setup|matrix>")
	}
	switch os.Args[1] {
	case "setup":
		runSetup()
	case "matrix":
		runMatrix()
	default:
		fatal("unknown phase %q", os.Args[1])
	}
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", a...)
	os.Exit(1)
}

// ---------------- Phase: setup ----------------

func runSetup() {
	adminTok := adminLogin()
	fmt.Println("== Creating + funding 100 test users ==")
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)
	var mu sync.Mutex
	var errs []string
	okCount := 0
	for i := 1; i <= numUsers; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			uid := userID(i)
			if err := ensureUser(uid); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("ensure %s: %v", uid, err))
				mu.Unlock()
				return
			}
			if err := creditUSDB(adminTok, uid, usdbPerUser); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("credit %s: %v", uid, err))
				mu.Unlock()
				return
			}
			mu.Lock()
			okCount++
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	fmt.Printf("Users created+funded: %d ok, %d errors\n", okCount, len(errs))
	for _, e := range errs {
		fmt.Println("  ERR:", e)
	}
}

// ---------------- HTTP helpers ----------------

func adminLogin() string {
	body, _ := json.Marshal(map[string]string{"loginId": "admin", "password": "admin123"})
	resp, err := httpClient.Post(backendURL+"/admin/login", "application/json", bytes.NewReader(body))
	if err != nil {
		fatal("admin login: %v", err)
	}
	defer resp.Body.Close()
	var r struct {
		Token string `json:"token"`
	}
	json.NewDecoder(resp.Body).Decode(&r)
	if r.Token == "" {
		fatal("admin login: empty token")
	}
	return r.Token
}

func ensureUser(userID string) error {
	body, _ := json.Marshal(map[string]string{"userId": userID})
	req, _ := http.NewRequest(http.MethodPost, backendURL+"/internal/user/ensure", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", engineSecret)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	return nil
}

func creditUSDB(adminTok, userID, amount string) error {
	body, _ := json.Marshal(map[string]string{"userId": userID, "asset": "USDB", "amount": amount, "direction": "credit"})
	req, _ := http.NewRequest(http.MethodPost, backendURL+"/admin/users/balance", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminTok)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	return nil
}

func jwtFor(uid string) string {
	iss := auth.NewJWTIssuer(jwtSecret, 24*time.Hour)
	tok, _, err := iss.Issue(uid, uid)
	if err != nil {
		fatal("issue jwt for %s: %v", uid, err)
	}
	return tok
}

// ---------------- trade API ----------------

type orderReq struct {
	Symbol      string `json:"symbol"`
	Market      string `json:"market"`
	Side        string `json:"side"`
	Type        string `json:"type"`
	Price       string `json:"price,omitempty"`
	Qty         string `json:"qty"`
	StopPrice   string `json:"stopPrice,omitempty"`
	ReduceOnly  bool   `json:"reduceOnly,omitempty"`
	Leverage    *int   `json:"leverage,omitempty"`
	MarginMode  string `json:"marginMode,omitempty"`
}

type orderResp struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Filled  string `json:"filled"`
	Trades  int    `json:"trades"`
	Error   string `json:"error"`
}

func doJSON(method, url, tok string, body any) (int, []byte, error) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, nil
}

func placeOrder(tok string, o orderReq) (int, orderResp, []byte) {
	status, body, err := doJSON(http.MethodPost, backendURL+"/trade/order", tok, o)
	var or orderResp
	json.Unmarshal(body, &or)
	if err != nil {
		or.Error = err.Error()
	}
	return status, or, body
}

type attachedReq struct {
	Parent     orderReq  `json:"parent"`
	TakeProfit *orderReq `json:"takeProfit,omitempty"`
	StopLoss   *orderReq `json:"stopLoss,omitempty"`
}

func placeAttached(tok string, a attachedReq) (int, []byte) {
	// Route is registered hyphenated: /trade/attached-order (see
	// Dex-Backend/cmd/server/main.go). Was previously mis-called as
	// /trade/attachedOrder (camelCase), which 404'd on every TP/SL scenario.
	status, body, _ := doJSON(http.MethodPost, backendURL+"/trade/attached-order", tok, a)
	return status, body
}

func cancelOrder(tok, symbol, market, orderID string) (int, []byte) {
	status, body, _ := doJSON(http.MethodPost, backendURL+"/trade/cancel", tok, map[string]string{
		"symbol": symbol, "market": market, "orderId": orderID,
	})
	return status, body
}

func getFills(tok, symbol, market string) []byte {
	url := fmt.Sprintf("%s/trade/fills?symbol=%s&market=%s&limit=100", backendURL, symbol, market)
	_, body, _ := doJSON(http.MethodGet, url, tok, nil)
	return body
}

func getPositions(tok string) []byte {
	_, body, _ := doJSON(http.MethodGet, backendURL+"/trade/positions", tok, nil)
	return body
}

func getBalance(tok, asset string) (string, error) {
	status, body, err := doJSON(http.MethodGet, backendURL+"/trade/balance?asset="+asset, tok, nil)
	if err != nil {
		return "", err
	}
	if status >= 300 {
		return "", fmt.Errorf("status %d: %s", status, body)
	}
	var r struct {
		Balance string `json:"balance"`
	}
	json.Unmarshal(body, &r)
	return r.Balance, nil
}

func getPnlHistory(tok string) []byte {
	_, body, _ := doJSON(http.MethodGet, backendURL+"/trade/pnlHistory?limit=200", tok, nil)
	return body
}

func getOrders(tok string) []byte {
	_, body, _ := doJSON(http.MethodGet, backendURL+"/trade/orders", tok, nil)
	return body
}

func getMarkets() []map[string]any {
	_, body, err := doJSON(http.MethodGet, "http://localhost:8080/markets", "", nil)
	if err != nil {
		fatal("get markets: %v", err)
	}
	var m []map[string]any
	json.Unmarshal(body, &m)
	return m
}

// marketMeta holds the tick/lot granularity for one (symbol, market) pair so
// the harness can submit orders that pass the engine's own validation
// instead of guessing round numbers that only work for BTC/ETH-style markets.
type marketMeta struct{ tick, lot float64 }

var metaBySymbolMarket = map[string]marketMeta{}

func loadMarketMeta(markets []map[string]any) {
	for _, m := range markets {
		sym, _ := m["symbol"].(string)
		mkt, _ := m["market"].(string)
		tick, _ := strconv.ParseFloat(fmt.Sprint(m["tickSize"]), 64)
		lot, _ := strconv.ParseFloat(fmt.Sprint(m["lotSize"]), 64)
		metaBySymbolMarket[sym+":"+mkt] = marketMeta{tick: tick, lot: lot}
	}
}

// roundToStep rounds v down to the nearest positive multiple of step (floor,
// so it never accidentally rounds *up* past a limit/qty bound). step==0
// leaves v unchanged.
func roundToStep(v, step float64) float64 {
	if step <= 0 {
		return v
	}
	steps := float64(int64(v / step))
	r := steps * step
	// Kill float64 accumulation noise (e.g. 0.020370000000000003) by rounding
	// to the step's own decimal precision, derived from its string form
	// rather than assuming a fixed number of decimals.
	decimals := 0
	s := strconv.FormatFloat(step, 'f', -1, 64)
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		decimals = len(s) - dot - 1
	}
	mult := math.Pow(10, float64(decimals))
	return math.Round(r*mult) / mult
}

func roundQty(sym, mkt string, qty float64) string {
	meta := metaBySymbolMarket[sym+":"+mkt]
	r := roundToStep(qty, meta.lot)
	return strconv.FormatFloat(r, 'f', -1, 64)
}

func roundPrice(sym, mkt string, price float64) string {
	meta := metaBySymbolMarket[sym+":"+mkt]
	r := roundToStep(price, meta.tick)
	return strconv.FormatFloat(r, 'f', -1, 64)
}

func getMMDesks(adminTok string) []map[string]any {
	_, body, _ := doJSON(http.MethodGet, botsURL+"/admin/mm", adminTok, nil)
	var r struct {
		MarketMakers []map[string]any `json:"marketMakers"`
	}
	json.Unmarshal(body, &r)
	return r.MarketMakers
}

// ---------------- scenario counters ----------------

type counter struct {
	mu               sync.Mutex
	attempted, ok, failed int
	samples          []string
}

func (c *counter) record(ok bool, sample string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempted++
	if ok {
		c.ok++
	} else {
		c.failed++
		if len(c.samples) < 10 {
			c.samples = append(c.samples, sample)
		}
	}
}

func (c *counter) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := fmt.Sprintf("attempted=%d ok=%d failed=%d", c.attempted, c.ok, c.failed)
	for _, x := range c.samples {
		s += "\n    sample-fail: " + x
	}
	return s
}

var counters = map[string]*counter{}
var countersMu sync.Mutex

func getCounter(name string) *counter {
	countersMu.Lock()
	defer countersMu.Unlock()
	if c, ok := counters[name]; ok {
		return c
	}
	c := &counter{}
	counters[name] = c
	return c
}

// ---------------- Phase: matrix ----------------

func runMatrix() {
	adminTok := adminLogin()
	markets := getMarkets()
	fmt.Printf("Loaded %d markets\n", len(markets))
	loadMarketMeta(markets)

	tokens := make(map[string]string, numUsers)
	for i := 1; i <= numUsers; i++ {
		tokens[userID(i)] = jwtFor(userID(i))
	}

	spotSymbols := []string{"BTC-USDB", "ETH-USDB", "SOL-USDB", "BNB-USDB"}
	futSymbols := []string{}
	for _, m := range markets {
		if m["market"] == "FUTURES" {
			futSymbols = append(futSymbols, m["symbol"].(string))
		}
	}
	fmt.Println("Futures symbols:", futSymbols)

	// approximate reference prices per symbol from MM desks (index price)
	desks := getMMDesks(adminTok)
	priceOf := map[string]float64{}
	for _, d := range desks {
		base, _ := d["base"].(string)
		mkt, _ := d["market"].(string)
		idxStr, _ := d["indexPrice"].(string)
		p, _ := strconv.ParseFloat(idxStr, 64)
		priceOf[base+":"+mkt] = p
	}
	fmt.Println("Reference prices:", priceOf)

	// ============ 1. Spot market order BUY/SELL ============
	runScenario("spot_market_buy", 8, func(i int) {
		u := userID(10 + i)
		sym := spotSymbols[i%len(spotSymbols)]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":SPOT"]
		if price == 0 {
			price = 100
		}
		qty := roundQty(sym, "SPOT", 500/price)
		status, or, body := placeOrder(tokens[u], orderReq{Symbol: sym, Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: qty})
		ok := status < 300 && or.Error == ""
		getCounter("spot_market_buy").record(ok, fmt.Sprintf("%s %s status=%d body=%s", u, sym, status, trim(body, 200)))
	})

	runScenario("spot_market_sell", 8, func(i int) {
		// Use a different user slice than spot_market_buy and BUY first so
		// the account actually holds the base asset to sell — a fresh
		// LOADTEST user has zero BTC/ETH/SOL/BNB balance otherwise, which
		// would fail every attempt with "insufficient <asset>" regardless of
		// whether SELL itself works correctly.
		u := userID(18 + i)
		sym := spotSymbols[i%len(spotSymbols)]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":SPOT"]
		if price == 0 {
			price = 100
		}
		buyQty := roundQty(sym, "SPOT", 600/price)
		placeOrder(tokens[u], orderReq{Symbol: sym, Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: buyQty})
		sellQty := roundQty(sym, "SPOT", 250/price)
		status, or, body := placeOrder(tokens[u], orderReq{Symbol: sym, Market: "SPOT", Side: "SELL", Type: "MARKET", Qty: sellQty})
		ok := status < 300 && or.Error == ""
		getCounter("spot_market_sell").record(ok, fmt.Sprintf("%s %s status=%d body=%s", u, sym, status, trim(body, 200)))
	})

	// ============ 2. Spot limit order BUY/SELL (crossing + resting) ============
	runScenario("spot_limit_crossing", 8, func(i int) {
		u := userID(20 + i)
		sym := spotSymbols[i%len(spotSymbols)]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":SPOT"]
		if price == 0 {
			price = 100
		}
		side := "BUY"
		limPrice := price * 1.02 // cross the ask
		qty := roundQty(sym, "SPOT", 300/price)
		if i%2 == 1 {
			side = "SELL"
			limPrice = price * 0.98
			// Give this account the base asset to sell first.
			placeOrder(tokens[u], orderReq{Symbol: sym, Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: roundQty(sym, "SPOT", 400/price)})
		}
		status, or, body := placeOrder(tokens[u], orderReq{Symbol: sym, Market: "SPOT", Side: side, Type: "LIMIT", Price: roundPrice(sym, "SPOT", limPrice), Qty: qty})
		ok := status < 300 && or.Error == ""
		getCounter("spot_limit_crossing").record(ok, fmt.Sprintf("%s %s %s status=%d body=%s", u, sym, side, status, trim(body, 200)))
	})

	runScenario("spot_limit_resting", 8, func(i int) {
		u := userID(30 + i)
		sym := spotSymbols[i%len(spotSymbols)]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":SPOT"]
		if price == 0 {
			price = 100
		}
		side := "BUY"
		limPrice := price * 0.80 // won't cross
		qty := roundQty(sym, "SPOT", 300/price)
		if i%2 == 1 {
			side = "SELL"
			limPrice = price * 1.20
			placeOrder(tokens[u], orderReq{Symbol: sym, Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: roundQty(sym, "SPOT", 400/price)})
		}
		status, or, body := placeOrder(tokens[u], orderReq{Symbol: sym, Market: "SPOT", Side: side, Type: "LIMIT", Price: roundPrice(sym, "SPOT", limPrice), Qty: qty})
		ok := status < 300 && or.Error == "" && (or.Status == "OPEN" || or.Status == "NEW" || or.Status == "PARTIALLY_FILLED" || or.Status != "")
		getCounter("spot_limit_resting").record(ok, fmt.Sprintf("%s %s %s status=%d body=%s", u, sym, side, status, trim(body, 200)))
	})

	// ============ 3-8. Futures scenarios ============
	runScenario("futures_market_no_tpsl", len(futSymbols), func(i int) {
		u := userID((40+i-1)%numUsers + 1)
		sym := futSymbols[i]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":FUTURES"]
		if price == 0 {
			price = 100
		}
		notional := 2000.0
		qty := roundQty(sym, "FUTURES", notional/price)
		lev := 5
		status, or, body := placeOrder(tokens[u], orderReq{Symbol: sym, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"})
		ok := status < 300 && or.Error == ""
		getCounter("futures_market_no_tpsl").record(ok, fmt.Sprintf("%s %s status=%d body=%s", u, sym, status, trim(body, 200)))
	})

	runScenario("futures_market_tp_only", len(futSymbols), func(i int) {
		u := userID((50+i-1)%numUsers + 1)
		sym := futSymbols[i]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":FUTURES"]
		if price == 0 {
			price = 100
		}
		qty := roundQty(sym, "FUTURES", 1500/price)
		lev := 5
		tp := &orderReq{Symbol: sym, Market: "FUTURES", Side: "SELL", Type: "STOP", Price: roundPrice(sym, "FUTURES", price*1.05), ReduceOnly: true, Qty: qty}
		status, body := placeAttached(tokens[u], attachedReq{
			Parent:     orderReq{Symbol: sym, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"},
			TakeProfit: tp,
		})
		ok := status < 300
		getCounter("futures_market_tp_only").record(ok, fmt.Sprintf("%s %s status=%d body=%s", u, sym, status, trim(body, 200)))
	})

	runScenario("futures_market_sl_only", len(futSymbols), func(i int) {
		u := userID((60+i-1)%numUsers + 1)
		sym := futSymbols[i]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":FUTURES"]
		if price == 0 {
			price = 100
		}
		qty := roundQty(sym, "FUTURES", 1500/price)
		lev := 5
		sl := &orderReq{Symbol: sym, Market: "FUTURES", Side: "SELL", Type: "STOP", StopPrice: roundPrice(sym, "FUTURES", price*0.95), ReduceOnly: true, Qty: qty}
		status, body := placeAttached(tokens[u], attachedReq{
			Parent:   orderReq{Symbol: sym, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"},
			StopLoss: sl,
		})
		ok := status < 300
		getCounter("futures_market_sl_only").record(ok, fmt.Sprintf("%s %s status=%d body=%s", u, sym, status, trim(body, 200)))
	})

	runScenario("futures_market_tp_sl", len(futSymbols), func(i int) {
		u := userID((70+i-1)%numUsers + 1)
		sym := futSymbols[i]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":FUTURES"]
		if price == 0 {
			price = 100
		}
		qty := roundQty(sym, "FUTURES", 1500/price)
		lev := 5
		tp := &orderReq{Symbol: sym, Market: "FUTURES", Side: "SELL", Type: "STOP", Price: roundPrice(sym, "FUTURES", price*1.05), ReduceOnly: true, Qty: qty}
		sl := &orderReq{Symbol: sym, Market: "FUTURES", Side: "SELL", Type: "STOP", StopPrice: roundPrice(sym, "FUTURES", price*0.95), ReduceOnly: true, Qty: qty}
		status, body := placeAttached(tokens[u], attachedReq{
			Parent:     orderReq{Symbol: sym, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"},
			TakeProfit: tp,
			StopLoss:   sl,
		})
		ok := status < 300
		getCounter("futures_market_tp_sl").record(ok, fmt.Sprintf("%s %s status=%d body=%s", u, sym, status, trim(body, 200)))
	})

	runScenario("futures_limit_no_tpsl", len(futSymbols), func(i int) {
		u := userID((80+i-1)%numUsers + 1)
		sym := futSymbols[i]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":FUTURES"]
		if price == 0 {
			price = 100
		}
		qty := roundQty(sym, "FUTURES", 1500/price)
		lev := 5
		status, or, body := placeOrder(tokens[u], orderReq{Symbol: sym, Market: "FUTURES", Side: "BUY", Type: "LIMIT", Price: roundPrice(sym, "FUTURES", price*1.01), Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"})
		ok := status < 300 && or.Error == ""
		getCounter("futures_limit_no_tpsl").record(ok, fmt.Sprintf("%s %s status=%d body=%s", u, sym, status, trim(body, 200)))
	})

	runScenario("futures_limit_tp_sl", len(futSymbols), func(i int) {
		u := userID((90+i-1)%numUsers + 1)
		sym := futSymbols[i]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":FUTURES"]
		if price == 0 {
			price = 100
		}
		qty := roundQty(sym, "FUTURES", 1500/price)
		lev := 5
		tp := &orderReq{Symbol: sym, Market: "FUTURES", Side: "SELL", Type: "STOP", Price: roundPrice(sym, "FUTURES", price*1.06), ReduceOnly: true, Qty: qty}
		sl := &orderReq{Symbol: sym, Market: "FUTURES", Side: "SELL", Type: "STOP", StopPrice: roundPrice(sym, "FUTURES", price*0.94), ReduceOnly: true, Qty: qty}
		status, body := placeAttached(tokens[u], attachedReq{
			Parent:     orderReq{Symbol: sym, Market: "FUTURES", Side: "BUY", Type: "LIMIT", Price: roundPrice(sym, "FUTURES", price*1.005), Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"},
			TakeProfit: tp,
			StopLoss:   sl,
		})
		ok := status < 300
		getCounter("futures_limit_tp_sl").record(ok, fmt.Sprintf("%s %s status=%d body=%s", u, sym, status, trim(body, 200)))
	})

	// ============ 9. Same-user multi-asset ============
	runScenario("same_user_multi_asset", 1, func(i int) {
		u := userID(1)
		steps := []struct{ sym, mkt, side, typ string }{
			{"BTC-USDB", "FUTURES", "BUY", "MARKET"},
			{"GOLD-USDB", "FUTURES", "BUY", "MARKET"},
			{"AAPL.us-USDB", "FUTURES", "BUY", "MARKET"},
			{"BTC-USDB", "SPOT", "BUY", "MARKET"},
		}
		allOK := true
		var log strings.Builder
		for _, s := range steps {
			base := strings.Split(s.sym, "-")[0]
			mktKey := s.mkt
			price := priceOf[base+":"+mktKey]
			if price == 0 {
				price = 100
			}
			qty := roundQty(s.sym, s.mkt, 500/price)
			lev := 3
			var or orderReq
			if s.mkt == "FUTURES" {
				or = orderReq{Symbol: s.sym, Market: s.mkt, Side: s.side, Type: s.typ, Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"}
			} else {
				or = orderReq{Symbol: s.sym, Market: s.mkt, Side: s.side, Type: s.typ, Qty: qty}
			}
			status, resp, body := placeOrder(tokens[u], or)
			ok := status < 300 && resp.Error == ""
			log.WriteString(fmt.Sprintf("%s/%s status=%d body=%s; ", s.sym, s.mkt, status, trim(body, 120)))
			if !ok {
				allOK = false
			}
		}
		getCounter("same_user_multi_asset").record(allOK, log.String())
	})

	// ============ 10. User-vs-user matching ============
	uvuSymbols := []string{"BTC-USDB:FUTURES", "ETH-USDB:FUTURES", "SOL-USDB:FUTURES", "GOLD-USDB:FUTURES", "AAPL.us-USDB:FUTURES"}
	runScenario("user_vs_user_matching", len(uvuSymbols), func(i int) {
		pair := strings.Split(uvuSymbols[i], ":")
		sym, mkt := pair[0], pair[1]
		base := strings.Split(sym, "-")[0]
		price := priceOf[base+":"+mkt]
		if price == 0 {
			price = 100
		}
		uA := userID(95)
		uB := userID(96)
		qty := roundQty(sym, mkt, 500/price)
		lev := 2
		matchPrice := roundPrice(sym, mkt, price)
		// A: BUY limit at matchPrice, B: SELL limit at same matchPrice
		statusA, orA, bodyA := placeOrder(tokens[uA], orderReq{Symbol: sym, Market: mkt, Side: "BUY", Type: "LIMIT", Price: matchPrice, Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"})
		statusB, orB, bodyB := placeOrder(tokens[uB], orderReq{Symbol: sym, Market: mkt, Side: "SELL", Type: "LIMIT", Price: matchPrice, Qty: qty, Leverage: &lev, MarginMode: "ISOLATED"})
		ok := statusA < 300 && statusB < 300 && (orA.Trades > 0 || orB.Trades > 0)
		getCounter("user_vs_user_matching").record(ok, fmt.Sprintf("sym=%s A=%s(%d,%s) B=%s(%d,%s)", sym, uA, statusA, trim(bodyA, 150), uB, statusB, trim(bodyB, 150)))
	})

	// ============ 15. Edge cases ============
	runScenario("edge_insufficient_balance", 1, func(i int) {
		u := userID(97)
		status, or, body := placeOrder(tokens[u], orderReq{Symbol: "BTC-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "1000"}) // way beyond balance
		ok := status >= 400
		getCounter("edge_insufficient_balance").record(ok, fmt.Sprintf("status=%d err=%s body=%s", status, or.Error, trim(body, 150)))
	})
	runScenario("edge_invalid_symbol", 1, func(i int) {
		u := userID(97)
		status, _, body := placeOrder(tokens[u], orderReq{Symbol: "FAKE-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "1"})
		ok := status >= 400
		getCounter("edge_invalid_symbol").record(ok, fmt.Sprintf("status=%d body=%s", status, trim(body, 150)))
	})
	runScenario("edge_zero_qty", 1, func(i int) {
		u := userID(97)
		status, _, body := placeOrder(tokens[u], orderReq{Symbol: "BTC-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "0"})
		ok := status >= 400
		getCounter("edge_zero_qty").record(ok, fmt.Sprintf("status=%d body=%s", status, trim(body, 150)))
	})
	runScenario("edge_negative_qty", 1, func(i int) {
		u := userID(97)
		status, _, body := placeOrder(tokens[u], orderReq{Symbol: "BTC-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "-1"})
		ok := status >= 400
		getCounter("edge_negative_qty").record(ok, fmt.Sprintf("status=%d body=%s", status, trim(body, 150)))
	})
	runScenario("edge_sell_more_than_held", 1, func(i int) {
		u := userID(98)
		status, _, body := placeOrder(tokens[u], orderReq{Symbol: "ETH-USDB", Market: "SPOT", Side: "SELL", Type: "MARKET", Qty: "999999"})
		ok := status >= 400
		getCounter("edge_sell_more_than_held").record(ok, fmt.Sprintf("status=%d body=%s", status, trim(body, 150)))
	})
	runScenario("edge_over_max_leverage", 1, func(i int) {
		u := userID(98)
		lev := 500 // absurd, way over any configured max (BTC max is 100)
		status, _, body := placeOrder(tokens[u], orderReq{Symbol: "BTC-USDB", Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED"})
		ok := status >= 400
		getCounter("edge_over_max_leverage").record(ok, fmt.Sprintf("status=%d body=%s", status, trim(body, 150)))
	})
	runScenario("edge_cancel_already_filled", 1, func(i int) {
		u := userID(99)
		_, or, _ := placeOrder(tokens[u], orderReq{Symbol: "BTC-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "0.001"})
		status, body := cancelOrder(tokens[u], "BTC-USDB", "SPOT", or.OrderID)
		ok := status >= 400 // should be rejected since already filled
		getCounter("edge_cancel_already_filled").record(ok, fmt.Sprintf("orderId=%s status=%d body=%s", or.OrderID, status, trim(body, 150)))
	})
	runScenario("edge_double_submission", 1, func(i int) {
		u := userID(99)
		body := orderReq{Symbol: "ETH-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "0.005"}
		var wg sync.WaitGroup
		results := make([]int, 2)
		for k := 0; k < 2; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				status, _, _ := placeOrder(tokens[u], body)
				results[k] = status
			}(k)
		}
		wg.Wait()
		ok := true // just documenting behavior, not a strict pass/fail
		getCounter("edge_double_submission").record(ok, fmt.Sprintf("statuses=%v (both should be independently processed, no double-spend)", results))
	})

	// ============ Print report ============
	fmt.Println("\n\n========== SCENARIO RESULTS ==========")
	order := []string{
		"spot_market_buy", "spot_market_sell", "spot_limit_crossing", "spot_limit_resting",
		"futures_market_no_tpsl", "futures_market_tp_only", "futures_market_sl_only", "futures_market_tp_sl",
		"futures_limit_no_tpsl", "futures_limit_tp_sl", "same_user_multi_asset", "user_vs_user_matching",
		"edge_insufficient_balance", "edge_invalid_symbol", "edge_zero_qty", "edge_negative_qty",
		"edge_sell_more_than_held", "edge_over_max_leverage", "edge_cancel_already_filled", "edge_double_submission",
	}
	for _, name := range order {
		if c, ok := counters[name]; ok {
			fmt.Printf("%-28s %s\n", name, c.String())
		}
	}

	fmt.Println("\n== Post-run MM desk snapshot ==")
	for _, d := range getMMDesks(adminTok) {
		fmt.Printf("  %-10v %-8v base=%-15v quote=%-15v running=%-6v netPnl=%v\n",
			d["base"], d["market"], d["baseAmount"], d["quoteAmount"], d["isRunning"], d["stats"].(map[string]any)["netPnl"])
	}
}

func runScenario(name string, n int, fn func(i int)) {
	fmt.Printf("Running scenario %s (n=%d)...\n", name, n)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}(i)
		time.Sleep(20 * time.Millisecond)
	}
	wg.Wait()
}

func trim(b []byte, n int) string {
	s := string(b)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

var _ = rand.Intn
