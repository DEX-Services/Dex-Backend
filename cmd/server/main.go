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
	engineClient := engineclient.New()

	srv := &api.Server{
		Nonces: auth.NewNonceStore(),
		JWT:    auth.NewJWTIssuer(jwtSecret, 7*24*time.Hour),
		Users:  userRepo,
		Log:    slog.Default(),
	}

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

	if vaultAddress := os.Getenv("DEXVAULT_ADDRESS"); vaultAddress != "" {
		chainClient, err := chain.NewClient(ctx, os.Getenv("FUJI_RPC_URL"), vaultAddress)
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
	mux.HandleFunc("/wallet/balance", walletSrv.Balance)
	mux.HandleFunc("/wallet/withdraw-request", walletSrv.WithdrawRequest)
	mux.HandleFunc("/admin/withdraw-approve", walletSrv.AdminApproveWithdrawal)
	mux.HandleFunc("/internal/balance/lock", walletSrv.InternalLockBalance)
	mux.HandleFunc("/internal/balance/unlock", walletSrv.InternalUnlockBalance)
	mux.HandleFunc("/internal/balance/settle", walletSrv.InternalSettleBalance)
	mux.HandleFunc("/admin/engine-backfill", walletSrv.AdminEngineBackfill)
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

	httpSrv := &http.Server{Addr: ":" + port, Handler: api.CORS(origin, mux)}

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
