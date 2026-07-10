// cmd/server/main.go runs the Dex-Backend wallet auth service.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dex/dex-backend/internal/api"
	"github.com/dex/dex-backend/internal/auth"
	"github.com/dex/dex-backend/internal/db"
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

	srv := &api.Server{
		Nonces: auth.NewNonceStore(),
		JWT:    auth.NewJWTIssuer(jwtSecret, 7*24*time.Hour),
		Users:  repo.NewUserRepo(pool),
		Log:    slog.Default(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/auth/nonce", srv.Nonce)
	mux.HandleFunc("/auth/login", srv.Login)
	mux.HandleFunc("/auth/logout", srv.Logout)
	mux.HandleFunc("/auth/me", srv.Me)
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
