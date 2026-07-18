// cmd/server/main.go runs the Dex-Backend wallet auth service.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dex/dex-backend/internal/api"
	"github.com/dex/dex-backend/internal/auth"
	"github.com/dex/dex-backend/internal/chain"
	"github.com/dex/dex-backend/internal/db"
	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/repo"
	"github.com/joho/godotenv"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file, using env vars")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := db.New(ctx, os.Getenv("POSTGRES_SERVICE_URI"))
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		slog.Error("JWT_SECRET not set")
		os.Exit(1)
	}

	userRepo := repo.NewUserRepo(pool)
	ledgerRepo := repo.NewLedgerRepo(pool)
	adminRepo := repo.NewAdminRepo(pool)
	p2pRepo := repo.NewP2PRepo(pool)
	engineClient := engineclient.New()

	srv := &api.Server{
		Nonces:       auth.NewNonceStore(),
		JWT:          auth.NewJWTIssuer(jwtSecret, 7*24*time.Hour),
		Users:        userRepo,
		Log:          slog.Default(),
		SecureCookie: os.Getenv("COOKIE_SECURE") != "false",
		TrustedProxy: os.Getenv("TRUSTED_PROXY"),
	}
	go srv.Nonces.Run(ctx)

	walletSrv := &api.WalletServer{
		Server:       srv,
		Ledger:       ledgerRepo,
		Admins:       parseAdminAddresses(os.Getenv("ADMIN_WALLET_ADDRESSES")),
		EngineSecret: os.Getenv("ENGINE_SHARED_SECRET"),
		EngineClient: engineClient,
	}
	if walletSrv.EngineSecret == "" {
		slog.Warn("ENGINE_SHARED_SECRET not set, /internal/balance/* disabled")
	}
	adminSrv := &api.AdminServer{
		Server:        srv,
		Admin:         adminRepo,
		P2P:           p2pRepo,
		AdminLoginID:  os.Getenv("ADMIN_LOGIN_ID"),
		AdminPassword: os.Getenv("ADMIN_PASSWORD"),
	}
	if adminSrv.AdminLoginID == "" || adminSrv.AdminPassword == "" {
		slog.Warn("ADMIN_LOGIN_ID/ADMIN_PASSWORD not set, /admin/login disabled (set ADMIN_PASSWORD to a bcrypt hash)")
	}
	p2pSrv := &api.P2PServer{Server: srv, P2P: p2pRepo}
	go p2pRepo.RunMaintenance(ctx, engineClient)

	if vaultAddress := os.Getenv("DEXVAULT_ADDRESS"); vaultAddress != "" {
		chainClient, err := chain.NewClient(ctx, os.Getenv("FUJI_RPC_URL"), vaultAddress, os.Getenv("USDC_ADDRESS"))
		if err != nil {
			slog.Error("failed to connect to Fuji RPC", "err", err)
			os.Exit(1)
		}

		startBlock, err := strconv.ParseUint(os.Getenv("DEXVAULT_DEPLOY_BLOCK"), 10, 64)
		if err != nil {
			startBlock = 0
		}

		listener := &chain.Listener{
			Client:       chainClient,
			Pool:         pool,
			Users:        userRepo,
			Ledger:       ledgerRepo,
			Log:          slog.Default(),
			StartBlock:   startBlock,
			EngineClient: engineClient,
		}
		go listener.Run(ctx)

		if treasuryKey := os.Getenv("TREASURY_PRIVATE_KEY"); treasuryKey != "" {
			chainID, err := strconv.ParseInt(os.Getenv("FUJI_CHAIN_ID"), 10, 64)
			if err != nil {
				chainID = 43113
			}
			signer, err := chain.NewSigner(chainClient, treasuryKey, chainID)
			if err != nil {
				slog.Error("failed to init treasury signer", "err", err)
				os.Exit(1)
			}
			walletSrv.Signer = signer
		} else {
			slog.Warn("TREASURY_PRIVATE_KEY not set, /admin/withdraw-approve disabled")
		}
	} else {
		slog.Warn("DEXVAULT_ADDRESS not set, chain listener and withdrawal approval disabled")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/auth/nonce", srv.Nonce)
	mux.HandleFunc("/auth/login", srv.Login)
	mux.HandleFunc("/auth/logout", srv.Logout)
	mux.HandleFunc("/auth/me", srv.Me)
	mux.HandleFunc("/admin/login", adminSrv.Login)
	mux.HandleFunc("/admin/dashboard", adminSrv.Dashboard)
	mux.HandleFunc("/admin/profile", adminSrv.Profile)
	mux.HandleFunc("/admin/p2p/price", adminSrv.P2PPrice)
	mux.HandleFunc("/admin/p2p/appeals", adminSrv.P2PAppeals)
	mux.HandleFunc("/admin/p2p/appeals/resolve", adminSrv.ResolveP2PAppeal)
	mux.HandleFunc("/wallet/balance", walletSrv.Balance)
	mux.HandleFunc("/wallet/withdraw-request", walletSrv.WithdrawRequest)
	mux.HandleFunc("/admin/withdraw-approve", walletSrv.AdminApproveWithdrawal)
	mux.HandleFunc("/admin/withdraw-recover", walletSrv.AdminRecoverWithdrawal)
	mux.HandleFunc("/internal/balance/lock", walletSrv.InternalLockBalance)
	mux.HandleFunc("/internal/balance/unlock", walletSrv.InternalUnlockBalance)
	mux.HandleFunc("/internal/balance/settle", walletSrv.InternalSettleBalance)
	mux.HandleFunc("/internal/balance/credit", walletSrv.InternalCreditBalance)
	mux.HandleFunc("/admin/engine-backfill", walletSrv.AdminEngineBackfill)
	mux.HandleFunc("/internal/engine-backfill", walletSrv.InternalEngineBackfill)
	mux.HandleFunc("/p2p/price", p2pSrv.Price)
	mux.HandleFunc("/p2p/listings", p2pSrv.Listings)
	mux.HandleFunc("/p2p/my-listings", p2pSrv.MyListings)
	mux.HandleFunc("/p2p/buy", p2pSrv.Buy)
	mux.HandleFunc("/p2p/orders", p2pSrv.Orders)
	mux.HandleFunc("/p2p/orders/paid", p2pSrv.MarkPaid)
	mux.HandleFunc("/p2p/orders/cancel", p2pSrv.CancelOrder)
	mux.HandleFunc("/p2p/orders/release", p2pSrv.ReleaseOrder)
	mux.HandleFunc("/p2p/orders/appeal", p2pSrv.AppealOrder)
	mux.HandleFunc("/p2p/listings/cancel", p2pSrv.CancelListing)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	origin := os.Getenv("CORS_ORIGIN")
	if origin == "" {
		origin = "http://localhost:5173"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	httpSrv := &http.Server{
		Addr:         ":" + port,
		Handler:      api.CORS(origin, mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		slog.Info("dex-backend listening", "port", port)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func parseAdminAddresses(raw string) map[string]bool {
	admins := map[string]bool{}
	for _, addr := range strings.Split(raw, ",") {
		addr = strings.ToLower(strings.TrimSpace(addr))
		if addr != "" {
			admins[addr] = true
		}
	}
	return admins
}
