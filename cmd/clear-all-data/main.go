// Command clear-all-data is a DEV-ONLY tool for a full local reset.
//
// It TRUNCATEs every application table this platform uses — across
// Dex-Backend, the matching-engine, and the bots service, since all three
// point at the same Postgres database (dexdb) — while leaving every
// table's SCHEMA (columns, constraints, indexes) completely untouched.
// This is "clear the data, not the tables": after running with --apply,
// every table still exists exactly as it did, just empty, ready for a
// fresh dev/test session with zero users, balances, orders, or history.
//
// This does NOT touch Kafka or Redis — those are cleared separately (see
// the operator's own notes on this reset; Kafka topics and Redis keys have
// no analogous "TRUNCATE, keep the topic" tool here since neither system
// has an app-owned schema to preserve).
//
// This is IRREVERSIBLE. There is no soft-delete, no backup taken by this
// tool, and TRUNCATE does not go through Postgres MVCC in a way that lets
// you get the rows back afterward. Only ever run this against a database
// nobody else depends on — confirmed dev-only, single-operator use before
// this tool was written.
//
// Usage (services must be STOPPED first — see ./run.sh stop at the
// workspace root; nothing here starts or stops them):
//
//	go run ./cmd/clear-all-data          # dry run: lists every table that would be truncated
//	go run ./cmd/clear-all-data --apply  # actually truncates them all, in one transaction
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/dex/dex-backend/internal/db"
	"github.com/joho/godotenv"
)

// tables is every application table currently defined across the three
// services sharing this database, grouped by which repo owns/creates it
// (purely for readability here — TRUNCATE ... CASCADE below doesn't care
// about ordering or ownership, it resolves every foreign-key dependent
// itself in one statement). Kept as an explicit list rather than querying
// information_schema at runtime so a run of this tool is reviewable in a
// diff and can't silently pick up some future unrelated table.
var tables = []string{
	// Dex-Backend
	"users",
	"user_balances",
	"ledger_entries",
	"user_sessions",
	"admin_profiles",
	"chain_cursor",
	"p2p_listings",
	"p2p_orders",
	"p2p_order_events",
	"p2p_order_messages",
	"p2p_order_proofs",
	"p2p_payment_accounts",
	"p2p_price_history",
	"p2p_wallet_balances",
	"p2p_wallet_entries",
	"p2p_admin_wallet_balances",
	"p2p_admin_wallet_entries",
	"platform_treasury_balances",
	"platform_treasury_entries",
	"referral_codes",
	"referral_config",
	"affiliate_links",
	"user_referral_links",
	// Shared between Dex-Backend and the matching-engine (both migrate it)
	"fee_config",
	"fee_tiers",
	"user_fee_subscriptions",
	// matching-engine
	"symbol_configs",
	"option_instruments",
	"combo_instruments",
	"option_positions",
	"orders",
	"trades",
	"events",
	"event_outbox",
	"funding_payments",
	"realized_pnl",
	"iv_snapshots",
	// bots
	"bots",
	"market_makers",
	"mm_funding_ledger",
}

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

	// Confirm every listed table actually exists before doing anything —
	// a typo'd or since-renamed table name should be reported, not silently
	// skipped by TRUNCATE (which errors on a missing table rather than
	// no-op, so this dry-run check surfaces that clearly up front instead
	// of failing mid-transaction on --apply).
	var present, missing []string
	for _, t := range tables {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)`,
			t,
		).Scan(&exists)
		if err != nil {
			fmt.Println("check error:", err)
			os.Exit(1)
		}
		if exists {
			present = append(present, t)
		} else {
			missing = append(missing, t)
		}
	}

	fmt.Printf("Tables to truncate (%d):\n", len(present))
	for _, t := range present {
		fmt.Printf("  %s\n", t)
	}
	if len(missing) > 0 {
		fmt.Printf("\nNot found in this database, skipped (%d): %s\n", len(missing), strings.Join(missing, ", "))
	}

	if !apply {
		fmt.Println("\nDry run only — nothing was touched.")
		fmt.Println("Re-run with --apply to TRUNCATE all of the above in one transaction.")
		fmt.Println("This is IRREVERSIBLE: every row in every listed table is gone, table structure is untouched.")
		return
	}

	if len(present) == 0 {
		fmt.Println("\nNothing to truncate.")
		return
	}

	// One statement, one transaction: TRUNCATE ... CASCADE resolves every
	// foreign-key dependent among these tables itself (no need to hand-order
	// them), RESTART IDENTITY resets any serial/identity columns back to 1
	// so a fresh dev session doesn't inherit old id gaps.
	stmt := fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", strings.Join(quoteAll(present), ", "))
	if _, err := pool.Exec(ctx, stmt); err != nil {
		fmt.Println("truncate error:", err)
		os.Exit(1)
	}
	fmt.Printf("\nTruncated %d table(s). Every table's schema is untouched — only the rows are gone.\n", len(present))
}

func quoteAll(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = `"` + n + `"`
	}
	return out
}
