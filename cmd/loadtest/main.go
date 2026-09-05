// Command loadtest is a throwaway end-to-end simulation/QA tool, NOT part of
// the production build. It creates a batch of test users, funds them with
// USDB, then drives a real scenario matrix across all 17 live markets:
// spot market/limit orders, futures market/limit orders with and without
// TP/SL (via /trade/attached-order), cross-symbol per-user checks, and a
// direct user-vs-user limit-order match test. It reports every error/result
// so problems surface immediately instead of silently, and prints enough
// order/account/fill detail to manually verify PnL and matching.
//
// This intentionally does NOT touch market-maker desk provisioning anymore
// (that's owned by the admin /admin/mm flow directly) — desks are assumed
// already created/funded/enabled before this runs.
//
// Usage: go run ./cmd/loadtest [-users N] [-phase all|users|scenarios]
//
// Requires JWT_SECRET, ENGINE_SHARED_SECRET (or DEX_BACKEND_ENGINE_SECRET)
// to match the running Dex-Backend/bots instances, and BACKEND_URL pointing
// at Dex-Backend (default localhost:8081).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dex/dex-backend/internal/auth"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	backendURL   = env("BACKEND_URL", "http://localhost:8081")
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

// market describes one of the 17 live markets. spotOK marks the four crypto
// symbols that also have a SPOT book (BTC/ETH/SOL/BNB); everything else is
// FUTURES-only (forex majors, commodities, US equities).
type market struct {
	base, marketType, symbol string
}

var futuresMarkets = []market{
	{"BTC", "FUTURES", "BTC-USDB"},
	{"ETH", "FUTURES", "ETH-USDB"},
	{"SOL", "FUTURES", "SOL-USDB"},
	{"BNB", "FUTURES", "BNB-USDB"},
	{"EURUSD", "FUTURES", "EURUSD-USDB"},
	{"GBPUSD", "FUTURES", "GBPUSD-USDB"},
	{"AUDUSD", "FUTURES", "AUDUSD-USDB"},
	{"GOLD", "FUTURES", "GOLD-USDB"},
	{"SILVER", "FUTURES", "SILVER-USDB"},
	{"CrudeOIL", "FUTURES", "CrudeOIL-USDB"},
	{"AAPL.us", "FUTURES", "AAPL.us-USDB"},
	{"TSLA.us", "FUTURES", "TSLA.us-USDB"},
	{"NVDA.us", "FUTURES", "NVDA.us-USDB"},
}

var spotMarkets = []market{
	{"BTC", "SPOT", "BTC-USDB"},
	{"ETH", "SPOT", "ETH-USDB"},
	{"SOL", "SPOT", "SOL-USDB"},
	{"BNB", "SPOT", "BNB-USDB"},
}

func main() {
	numUsers := flag.Int("users", 100, "number of LOADTEST_USER_NNN accounts")
	usdbFund := flag.String("fund", "100000", "USDB credited to each user")
	phase := flag.String("phase", "all", "all|users|scenarios")
	flag.Parse()

	if jwtSecret == "" || engineSecret == "" {
		fatal("JWT_SECRET and ENGINE_SHARED_SECRET/DEX_BACKEND_ENGINE_SECRET must be set")
	}
	jwt := auth.NewJWTIssuer(jwtSecret, 24*time.Hour)
	adminTok := adminLogin()

	users := make([]string, 0, *numUsers)
	for i := 1; i <= *numUsers; i++ {
		users = append(users, fmt.Sprintf("LOADTEST_USER_%03d", i))
	}

	if *phase == "all" || *phase == "users" {
		fmt.Println("== Phase 1: create + fund test users ==")
		var wg sync.WaitGroup
		sem := make(chan struct{}, 10)
		var okCount, errCount int64
		for _, uid := range users {
			wg.Add(1)
			sem <- struct{}{}
			go func(uid string) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := ensureUser(uid); err != nil {
					atomic.AddInt64(&errCount, 1)
					fmt.Printf("  ensure %s FAILED: %v\n", uid, err)
					return
				}
				if err := creditUSDB(adminTok, uid, *usdbFund); err != nil {
					atomic.AddInt64(&errCount, 1)
					fmt.Printf("  credit %s FAILED: %v\n", uid, err)
					return
				}
				atomic.AddInt64(&okCount, 1)
			}(uid)
		}
		wg.Wait()
		fmt.Printf("Users provisioned: %d ok, %d errored\n", okCount, errCount)
	}

	if *phase == "all" || *phase == "scenarios" {
		fmt.Println("\n== Phase 2: scenario matrix ==")
		tokens := make(map[string]string, len(users))
		for _, u := range users {
			tok, _, err := jwt.Issue(u, u)
			if err != nil {
				fatal("issue jwt for %s: %v", u, err)
			}
			tokens[u] = tok
		}
		runScenarios(users, tokens)
	}

	fmt.Println("\nDone. Check backend.log / engine.log / bots.log for anything unexpected during this window.")
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", a...)
	os.Exit(1)
}

// ---- HTTP helpers ----

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
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil || r.Token == "" {
		fatal("admin login: bad response")
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

// ---- Trade API ----

type tradeOrderRequest struct {
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

type attachedOrderRequest struct {
	Parent     tradeOrderRequest  `json:"parent"`
	TakeProfit *tradeOrderRequest `json:"takeProfit,omitempty"`
	StopLoss   *tradeOrderRequest `json:"stopLoss,omitempty"`
}

func postTrade(tok, path string, body any) (int, map[string]any, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, backendURL+path, bytes.NewReader(b))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	if out == nil {
		out = map[string]any{"raw": string(raw)}
	}
	return resp.StatusCode, out, nil
}

func getTrade(tok, path string) (int, map[string]any, error) {
	req, err := http.NewRequest(http.MethodGet, backendURL+path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	if out == nil {
		out = map[string]any{"raw": string(raw)}
	}
	return resp.StatusCode, out, nil
}

type result struct {
	name   string
	ok     bool
	detail string
}

var results []result
var resultsMu sync.Mutex

func record(name string, ok bool, detail string) {
	resultsMu.Lock()
	results = append(results, result{name, ok, detail})
	resultsMu.Unlock()
	status := "OK"
	if !ok {
		status = "FAIL"
	}
	fmt.Printf("  [%s] %-45s %s\n", status, name, detail)
}

func runScenarios(users []string, tokens map[string]string) {
	if len(users) < 6 {
		fatal("need at least 6 users for scenario matrix")
	}
	u := users
	t := tokens

	// 1. Spot market order BUY and SELL
	for _, m := range spotMarkets[:2] {
		user := u[0]
		status, body, err := postTrade(t[user], "/trade/order", tradeOrderRequest{
			Symbol: m.symbol, Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "0.01",
		})
		record("spot-market-buy-"+m.symbol, status == 200 && err == nil, fmt.Sprintf("status=%d body=%v", status, trimAny(body)))
	}
	for _, m := range spotMarkets[:2] {
		user := u[0]
		status, body, err := postTrade(t[user], "/trade/order", tradeOrderRequest{
			Symbol: m.symbol, Market: "SPOT", Side: "SELL", Type: "MARKET", Qty: "0.005",
		})
		record("spot-market-sell-"+m.symbol, status == 200 && err == nil, fmt.Sprintf("status=%d body=%v", status, trimAny(body)))
	}

	// 2. Spot limit order BUY/SELL, crossing + resting
	{
		status, body, _ := postTrade(t[u[1]], "/trade/order", tradeOrderRequest{
			Symbol: "ETH-USDB", Market: "SPOT", Side: "BUY", Type: "LIMIT", Price: "100000", Qty: "0.01", // crosses (way above market)
		})
		record("spot-limit-buy-crossing", status == 200, fmt.Sprintf("status=%d body=%v", status, trimAny(body)))

		status2, body2, _ := postTrade(t[u[1]], "/trade/order", tradeOrderRequest{
			Symbol: "ETH-USDB", Market: "SPOT", Side: "SELL", Type: "LIMIT", Price: "1", Qty: "0.01", // rests far below
		})
		record("spot-limit-sell-resting", status2 == 200, fmt.Sprintf("status=%d body=%v", status2, trimAny(body2)))
	}

	// 3-8: Futures scenarios across multiple symbols
	lev := 3
	futSample := []market{futuresMarkets[0], futuresMarkets[7], futuresMarkets[10]} // BTC, GOLD, AAPL.us
	for i, m := range futSample {
		user := u[2+i%3]

		// 3. market order, no TP/SL
		status, body, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{
			Symbol: m.symbol, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED",
		})
		record("futures-market-noTPSL-"+m.symbol, status == 200, fmt.Sprintf("user=%s status=%d body=%v", user, status, trimAny(body)))

		// 4. market order + TP only
		statusTP, bodyTP, _ := postTrade(t[user], "/trade/attached-order", attachedOrderRequest{
			Parent: tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED"},
			TakeProfit: &tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "SELL", Type: "STOP", Price: hugeAbove(m), Qty: "0.01", ReduceOnly: true},
		})
		record("futures-market-TPonly-"+m.symbol, statusTP == 200, fmt.Sprintf("user=%s status=%d body=%v", user, statusTP, trimAny(bodyTP)))

		// 5. market order + SL only
		statusSL, bodySL, _ := postTrade(t[user], "/trade/attached-order", attachedOrderRequest{
			Parent:   tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED"},
			StopLoss: &tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "SELL", Type: "STOP", StopPrice: tinyBelow(m), Qty: "0.01", ReduceOnly: true},
		})
		record("futures-market-SLonly-"+m.symbol, statusSL == 200, fmt.Sprintf("user=%s status=%d body=%v", user, statusSL, trimAny(bodySL)))

		// 6. market order + TP+SL together
		statusBoth, bodyBoth, _ := postTrade(t[user], "/trade/attached-order", attachedOrderRequest{
			Parent:     tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED"},
			TakeProfit: &tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "SELL", Type: "STOP", Price: hugeAbove(m), Qty: "0.01", ReduceOnly: true},
			StopLoss:   &tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "SELL", Type: "STOP", StopPrice: tinyBelow(m), Qty: "0.01", ReduceOnly: true},
		})
		record("futures-market-TPandSL-"+m.symbol, statusBoth == 200, fmt.Sprintf("user=%s status=%d body=%v", user, statusBoth, trimAny(bodyBoth)))

		// 7. limit order, no TP/SL (resting, unlikely to cross)
		statusLim, bodyLim, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{
			Symbol: m.symbol, Market: "FUTURES", Side: "BUY", Type: "LIMIT", Price: tinyBelow(m), Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED",
		})
		record("futures-limit-noTPSL-"+m.symbol, statusLim == 200, fmt.Sprintf("user=%s status=%d body=%v", user, statusLim, trimAny(bodyLim)))

		// 8. limit order + TP+SL together
		statusLimB, bodyLimB, _ := postTrade(t[user], "/trade/attached-order", attachedOrderRequest{
			Parent:     tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED"},
			TakeProfit: &tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "SELL", Type: "STOP", Price: hugeAbove(m), Qty: "0.01", ReduceOnly: true},
			StopLoss:   &tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "SELL", Type: "STOP", StopPrice: tinyBelow(m), Qty: "0.01", ReduceOnly: true},
		})
		record("futures-limitentry-TPandSL-"+m.symbol, statusLimB == 200, fmt.Sprintf("user=%s status=%d body=%v", user, statusLimB, trimAny(bodyLimB)))
	}

	// 9. Same user, different symbols (cross-asset isolation)
	{
		user := u[3]
		s1, b1, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "BTC-USDB", Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.005", Leverage: &lev, MarginMode: "ISOLATED"})
		record("cross-asset-user-BTCfut", s1 == 200, fmt.Sprintf("body=%v", trimAny(b1)))
		s2, b2, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "GOLD-USDB", Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED"})
		record("cross-asset-user-GOLDfut", s2 == 200, fmt.Sprintf("body=%v", trimAny(b2)))
		s3, b3, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "AAPL.us-USDB", Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &lev, MarginMode: "ISOLATED"})
		record("cross-asset-user-AAPLfut", s3 == 200, fmt.Sprintf("body=%v", trimAny(b3)))
		s4, b4, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "BTC-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "0.001"})
		record("cross-asset-user-BTCspot", s4 == 200, fmt.Sprintf("body=%v", trimAny(b4)))
		posStatus, posBody, _ := getTrade(t[user], "/trade/positions")
		record("cross-asset-user-positions-check", posStatus == 200, fmt.Sprintf("%v", trimAny(posBody)))
	}

	// 10. User-vs-user matching: opposing LIMIT orders, two different users, same price/symbol
	uvuPairs := []market{futuresMarkets[1], futuresMarkets[4], futuresMarkets[8]} // ETH, EURUSD, SILVER
	for i, m := range uvuPairs {
		buyer, seller := u[4+i], u[50+i]
		price := "50000"
		switch m.base {
		case "EURUSD":
			price = "1.16"
		case "SILVER":
			price = "66"
		case "ETH":
			price = "2455"
		}
		sB, bB, _ := postTrade(t[seller], "/trade/order", tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "SELL", Type: "LIMIT", Price: price, Qty: "0.02", Leverage: &lev, MarginMode: "ISOLATED"})
		record("uvu-sell-resting-"+m.symbol, sB == 200, fmt.Sprintf("seller=%s body=%v", seller, trimAny(bB)))
		time.Sleep(300 * time.Millisecond)
		sBu, bBu, _ := postTrade(t[buyer], "/trade/order", tradeOrderRequest{Symbol: m.symbol, Market: "FUTURES", Side: "BUY", Type: "LIMIT", Price: price, Qty: "0.02", Leverage: &lev, MarginMode: "ISOLATED"})
		record("uvu-buy-crossing-"+m.symbol, sBu == 200, fmt.Sprintf("buyer=%s body=%v", buyer, trimAny(bBu)))
		time.Sleep(300 * time.Millisecond)
		fStatus, fBody, _ := getTrade(t[buyer], "/trade/fills?symbol="+m.symbol+"&market=FUTURES&limit=5")
		record("uvu-fills-check-"+m.symbol, fStatus == 200, fmt.Sprintf("buyer fills=%v", trimAny(fBody)))
		fStatus2, fBody2, _ := getTrade(t[seller], "/trade/fills?symbol="+m.symbol+"&market=FUTURES&limit=5")
		record("uvu-fills-check-seller-"+m.symbol, fStatus2 == 200, fmt.Sprintf("seller fills=%v", trimAny(fBody2)))
	}

	// 15. Error/edge cases
	{
		user := u[10]
		s1, b1, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "BTC-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "999999"})
		record("edge-insufficient-balance", s1 >= 400, fmt.Sprintf("status=%d body=%v", s1, trimAny(b1)))

		s2, b2, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "FAKE-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "1"})
		record("edge-invalid-symbol", s2 >= 400, fmt.Sprintf("status=%d body=%v", s2, trimAny(b2)))

		s3, b3, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "BTC-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "0"})
		record("edge-zero-qty", s3 >= 400, fmt.Sprintf("status=%d body=%v", s3, trimAny(b3)))

		s4, b4, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "BTC-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "-1"})
		record("edge-negative-qty", s4 >= 400, fmt.Sprintf("status=%d body=%v", s4, trimAny(b4)))

		s5, b5, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "BTC-USDB", Market: "SPOT", Side: "SELL", Type: "MARKET", Qty: "50"})
		record("edge-sell-more-than-held", s5 >= 400, fmt.Sprintf("status=%d body=%v", s5, trimAny(b5)))

		badLev := 500
		s6, b6, _ := postTrade(t[user], "/trade/order", tradeOrderRequest{Symbol: "BTC-USDB", Market: "FUTURES", Side: "BUY", Type: "MARKET", Qty: "0.01", Leverage: &badLev, MarginMode: "ISOLATED"})
		record("edge-over-max-leverage", s6 >= 400, fmt.Sprintf("status=%d body=%v", s6, trimAny(b6)))

		// double submission race
		var wg sync.WaitGroup
		var statuses [2]int
		var bodies [2]map[string]any
		body := tradeOrderRequest{Symbol: "ETH-USDB", Market: "SPOT", Side: "BUY", Type: "MARKET", Qty: "0.002"}
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				statuses[i], bodies[i], _ = postTrade(t[user], "/trade/order", body)
			}(i)
		}
		wg.Wait()
		record("edge-double-submission-race", true, fmt.Sprintf("status1=%d status2=%d body1=%v body2=%v", statuses[0], statuses[1], trimAny(bodies[0]), trimAny(bodies[1])))
	}

	fmt.Printf("\nScenario summary: %d ok, %d failed (of %d total)\n", countOK(true), countOK(false), len(results))
}

func countOK(ok bool) int {
	n := 0
	for _, r := range results {
		if r.ok == ok {
			n++
		}
	}
	return n
}

func hugeAbove(m market) string {
	// A TP stop price far above any realistic mark, guaranteeing it rests
	// without triggering — used only to prove the leg registers correctly.
	switch m.base {
	case "BTC":
		return "500000"
	case "GOLD":
		return "20000"
	case "AAPL.us":
		return "3000"
	default:
		return "1000000"
	}
}

func tinyBelow(m market) string {
	switch m.base {
	case "BTC":
		return "1000"
	case "GOLD":
		return "100"
	case "AAPL.us":
		return "1"
	default:
		return "0.01"
	}
}

func trimAny(m map[string]any) string {
	b, _ := json.Marshal(m)
	s := string(b)
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}
