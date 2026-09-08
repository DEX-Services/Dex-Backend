// Command clear-chain-data is a DEV-ONLY tool for local testing.
//
// It deletes the users, balances, and ledger entries that the on-chain
// deposit listener (internal/chain/listener.go) created from real vault
// Deposit events, WITHOUT touching chain_cursor. That distinction matters:
//
//   - The listener resumes from chain_cursor on every backend restart and
//     replays any on-chain deposit at or after that block. That replay is
//     the correct, production-appropriate behavior — real deposits must
//     always re-sync even if the local app DB was wiped, exactly like a
//     real exchange reconciles its ledger against the chain it indexes.
//   - This script is the opposite of that: an explicit, manual, opt-in way
//     to blank out the *local reflection* of those deposits for a clean-
//     looking dev screen, while leaving chain_cursor alone so the very
//     next backend start simply re-syncs the same deposits back in. If you
//     never run this script, nothing changes — the listener's normal
//     auto-sync behavior is untouched.
//
// This does NOT and CANNOT touch the chain itself. The Deposit events are
// permanent on-chain history; nothing run here, or anywhere, erases them.
//
// Usage (run from Dex-Backend, after ./run.sh, any time you want a clean
// dashboard for local testing):
//
//	go run ./cmd/clear-chain-data          # dry run: lists what would be removed
//	go run ./cmd/clear-chain-data --apply  # actually deletes it
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dex/dex-backend/internal/db"
	"github.com/joho/godotenv"
)

// chainWalletType is the wallet_type FindOrCreate stamps on every user the
// deposit listener auto-creates (see chain/listener.go: FindOrCreate(ctx,
// userAddr.Hex(), "metamask")). It's the one reliable marker separating
// chain-derived users from any other signup path.
const chainWalletType = "metamask"

func main() {
	_ = godotenv.Load()
	apply := len(os.Args) > 1 && os.Args[1] == "--apply"

	ctx := context.Background()
	pool, err := db.New(ctx, os.Getenv("POSTGRES_SERVICE_URI"))
	if err != nil {
		fmt.Println("connect error:", err)
		os.Exit(1)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx,
		`SELECT id, wallet_address, created_at FROM users WHERE wallet_type = $1 ORDER BY created_at`,
		chainWalletType)
	if err != nil {
		fmt.Println("query error:", err)
		os.Exit(1)
	}
	type user struct {
		id, addr string
		created  any
	}
	var users []user
	for rows.Next() {
		var u user
		if err := rows.Scan(&u.id, &u.addr, &u.created); err != nil {
			fmt.Println("scan error:", err)
			os.Exit(1)
		}
		users = append(users, u)
	}
	rows.Close()

	if len(users) == 0 {
		fmt.Println("No chain-derived (metamask) users found — nothing to clear.")
		return
	}

	fmt.Printf("Chain-derived users (wallet_type = %q):\n", chainWalletType)
	for _, u := range users {
		fmt.Printf("  %-14s %-44s created %v\n", u.id, u.addr, u.created)
	}

	if !apply {
		fmt.Println("\nDry run only — chain_cursor is never touched by this tool either way.")
		fmt.Println("Re-run with --apply to delete these users and their balances/ledger entries.")
		fmt.Println("The on-chain deposits themselves are permanent; the next backend restart")
		fmt.Println("will simply re-sync and recreate these same rows from chain_cursor onward.")
		return
	}

	var deleted, skipped int
	for _, u := range users {
		// users(id) cascades into user_balances, ledger_entries, user_sessions,
		// admin_profiles (ON DELETE CASCADE), but p2p_listings/p2p_orders are
		// ON DELETE RESTRICT — a user who placed a real P2P listing/order won't
		// delete cleanly. Report it rather than aborting the whole run.
		if _, err := pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, u.id); err != nil {
			fmt.Printf("  skip %s: %v\n", u.id, err)
			skipped++
			continue
		}
		deleted++
	}

	fmt.Printf("\nDeleted %d chain-derived user(s) (cascaded balances/ledger entries); %d skipped.\n", deleted, skipped)
	if skipped > 0 {
		fmt.Println("Skipped users likely have P2P listings/orders referencing them (ON DELETE RESTRICT).")
	}
	fmt.Println("chain_cursor left untouched — restart the backend and it will resync these deposits from chain as usual.")
}
