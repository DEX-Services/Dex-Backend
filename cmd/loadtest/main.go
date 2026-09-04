// Command loadtest is a throwaway end-to-end simulation tool, NOT part of the
// production build. It creates a batch of test users, funds them with USDB,
// funds and starts every market-maker desk, then drives randomized buy/sell
// market orders from every user across every registered market concurrently
// — a rough approximation of "real" multi-user trading activity — while
// reporting every error it hits so problems surface immediately instead of
// silently.
//
// Usage: go run ./cmd/loadtest [-skip-setup]
//
//	-skip-setup skips user creation/funding and MM desk funding/enabling,
//	going straight to the order flood — use when a previous run already did
//	that part (e.g. it was interrupted after Phase 2).
//
// Requires JWT_SECRET, ENGINE_SHARED_SECRET (or DEX_BACKEND_ENGINE_SECRET)
// to match the running Dex-Backend/bots instances, and BACKEND_URL/BOTS_URL
// pointing at them (defaults: localhost:8081 / localhost:8082).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
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
	botsURL      = env("BOTS_URL", "http://localhost:8082")
	jwtSecret    = os.Getenv("JWT_SECRET")
	engineSecret = firstNonEmpty(os.Getenv("ENGINE_SHARED_SECRET"), os.Getenv("DEX_BACKEND_ENGINE_SECRET"))
	httpClient   = &http.Client{Timeout: 15 * time.Second}
)

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

type market struct {
	base, marketType, symbol string
}

var markets = []market{
	{"BTC", "SPOT", "BTC-USDB"},
	{"ETH", "SPOT", "ETH-USDB"},
	{"SOL", "SPOT", "SOL-USDB"},
	{"BNB", "SPOT", "BNB-USDB"},
	{"BTC", "FUTURES", "BTC-USDC"},
	{"ETH", "FUTURES", "ETH-USDC"},
}

const (
	numUsers         = 10
	usdbFundPerUser  = "50000"
	mmBaseFund       = "20"    // per-desk base amount for spot; futures desks use quote only
	mmQuoteFund      = "50000" // per-desk quote amount
	ordersPerUserMkt = 4       // orders each user places per market
	orderConcurrency = 8
)

func main() {
	skipSetup := flag.Bool("skip-setup", false, "skip user/desk provisioning, go straight to the order flood")
	flag.Parse()

	if jwtSecret == "" || engineSecret == "" {
		fatal("JWT_SECRET and ENGINE_SHARED_SECRET/DEX_BACKEND_ENGINE_SECRET must be set")
	}
	jwt := auth.NewJWTIssuer(jwtSecret, 24*time.Hour)

	adminTok := adminLogin()

	users := make([]string, 0, numUsers)
	for i := 1; i <= numUsers; i++ {
		users = append(users, fmt.Sprintf("LOADTEST_USER_%02d", i))
	}

	if !*skipSetup {
		fmt.Println("== Phase 1: create + fund test users ==")
		for _, uid := range users {
			ensureUser(uid)
			creditUSDB(adminTok, uid, usdbFundPerUser)
			fmt.Printf("  %s funded with %s USDB\n", uid, usdbFundPerUser)
		}

		fmt.Println("\n== Phase 2: fund + start market-maker desks ==")
		desks := listDesks(adminTok)
		for _, d := range desks {
			fundAndStartDesk(adminTok, d)
		}
	} else {
		fmt.Println("== Skipping Phase 1/2 (--skip-setup) ==")
	}

	fmt.Println("\n== Phase 3: randomized order flood ==")
	tokens := make(map[string]string, len(users))
	for _, u := range users {
		tok, _, err := jwt.Issue(u, u)
		if err != nil {
			fatal("issue jwt for %s: %v", u, err)
		}
		tokens[u] = tok
	}

	var okCount, errCount int64
	var errSamples []string
	var errMu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, orderConcurrency)

	type job struct {
		user string
		m    market
	}
	var jobs []job
	for _, u := range users {
		for _, m := range markets {
			jobs = append(jobs, job{u, m})
		}
	}
	rand.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })

	for _, j := range jobs {
		for k := 0; k < ordersPerUserMkt; k++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(j job) {
				defer wg.Done()
				defer func() { <-sem }()
				side := "BUY"
				if rand.Intn(2) == 0 {
					side = "SELL"
				}
				qty := randQty(j.m)
				status, body, err := placeOrder(tokens[j.user], j.m, side, qty)
				if err != nil || status >= 300 {
					atomic.AddInt64(&errCount, 1)
					errMu.Lock()
					if len(errSamples) < 40 {
						errSamples = append(errSamples, fmt.Sprintf("%s %s %s qty=%s -> status=%d body=%s err=%v", j.user, j.m.symbol, side, qty, status, trim(body, 200), err))
					}
					errMu.Unlock()
				} else {
					atomic.AddInt64(&okCount, 1)
				}
			}(j)
			time.Sleep(5 * time.Millisecond) // gentle pacing, not a real DoS
		}
	}
	wg.Wait()

	fmt.Printf("\nOrders placed: %d ok, %d errored\n", okCount, errCount)
	if len(errSamples) > 0 {
		fmt.Println("\nSample errors:")
		for _, s := range errSamples {
			fmt.Println("  " + s)
		}
	}

	fmt.Println("\n== Phase 4: post-run desk snapshot ==")
	for _, d := range listDesks(adminTok) {
		fmt.Printf("  %-4s %-8s base=%s quote=%s running=%v netPnl=%s\n",
			d.Base, d.Market, d.BaseAmount, d.QuoteAmount, d.IsRunning, d.Stats.NetPnl)
	}

	fmt.Println("\nDone. Check backend.log / engine.log / bots.log for anything unexpected during this window.")
}

func randQty(m market) string {
	// Small qty, deliberately not lot-perfect — exercises the engine's own
	// snapping/validation rather than pre-rounding on the client.
	base := 0.001 + rand.Float64()*0.05
	return fmt.Sprintf("%.5f", base)
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
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

func ensureUser(userID string) {
	body, _ := json.Marshal(map[string]string{"userId": userID})
	req, _ := http.NewRequest(http.MethodPost, backendURL+"/internal/user/ensure", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", engineSecret)
	resp, err := httpClient.Do(req)
	if err != nil {
		fatal("ensure user %s: %v", userID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		fatal("ensure user %s: status %d: %s", userID, resp.StatusCode, b)
	}
}

func creditUSDB(adminTok, userID, amount string) {
	body, _ := json.Marshal(map[string]string{"userId": userID, "asset": "USDB", "amount": amount, "direction": "credit"})
	req, _ := http.NewRequest(http.MethodPost, backendURL+"/admin/users/balance", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminTok)
	resp, err := httpClient.Do(req)
	if err != nil {
		fatal("credit %s: %v", userID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		fatal("credit %s: status %d: %s", userID, resp.StatusCode, b)
	}
}

type deskStats struct {
	NetPnl string `json:"netPnl"`
}
type desk struct {
	ID          string    `json:"id"`
	Base        string    `json:"base"`
	Market      string    `json:"market"`
	Symbol      string    `json:"symbol"`
	BaseAmount  string    `json:"baseAmount"`
	QuoteAmount string    `json:"quoteAmount"`
	Enabled     bool      `json:"enabled"`
	IsRunning   bool      `json:"isRunning"`
	Stats       deskStats `json:"stats"`
}

func listDesks(adminTok string) []desk {
	req, _ := http.NewRequest(http.MethodGet, botsURL+"/admin/mm", nil)
	req.Header.Set("Authorization", "Bearer "+adminTok)
	resp, err := httpClient.Do(req)
	if err != nil {
		fatal("list desks: %v", err)
	}
	defer resp.Body.Close()
	var r struct {
		MarketMakers []desk `json:"marketMakers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		fatal("list desks: decode: %v", err)
	}
	return r.MarketMakers
}

func fundAndStartDesk(adminTok string, d desk) {
	if d.Enabled && d.IsRunning {
		fmt.Printf("  %-4s %-8s %-10s already running, skip\n", d.Base, d.Market, d.Symbol)
		return
	}
	// Futures desks (this codebase's collateralAsset) fund only the quote
	// leg; spot desks fund both legs independently (see mm.Service.Deposit).
	if d.Market == "SPOT" && d.BaseAmount == "0" {
		depositDesk(adminTok, d.ID, "base", mmBaseFund)
	}
	if d.QuoteAmount == "0" {
		depositDesk(adminTok, d.ID, "quote", mmQuoteFund)
	}
	enableDesk(adminTok, d.ID)
	fmt.Printf("  %-4s %-8s %-10s funded + started\n", d.Base, d.Market, d.Symbol)
}

func depositDesk(adminTok, deskID, asset, amount string) {
	body, _ := json.Marshal(map[string]string{"asset": asset, "amount": amount, "note": "loadtest"})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/admin/mm/%s/deposit", botsURL, deskID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminTok)
	resp, err := httpClient.Do(req)
	if err != nil {
		fatal("deposit desk %s: %v", deskID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		fatal("deposit desk %s (%s): status %d: %s", deskID, asset, resp.StatusCode, b)
	}
}

func enableDesk(adminTok, deskID string) {
	body, _ := json.Marshal(map[string]bool{"enabled": true})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/admin/mm/%s/enable", botsURL, deskID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminTok)
	resp, err := httpClient.Do(req)
	if err != nil {
		fatal("enable desk %s: %v", deskID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		fatal("enable desk %s: status %d: %s", deskID, resp.StatusCode, b)
	}
}

func placeOrder(userTok string, m market, side, qty string) (int, string, error) {
	body, _ := json.Marshal(map[string]string{
		"symbol": m.symbol, "market": m.marketType, "side": side, "type": "MARKET", "qty": qty,
	})
	req, err := http.NewRequest(http.MethodPost, backendURL+"/trade/order", bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+userTok)
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}
