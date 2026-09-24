// dbcheck is a one-off diagnostic: prints every user's total and available
// (total minus locked) balances from Postgres user_balances, to locate the
// ledger desync (Postgres funded, engine shows available=0). Read-only.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("POSTGRES_SERVICE_URI"))
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `
		SELECT user_id,
		       "USDC", "USDC_locked",
		       "BI2XUSD", "BI2XUSD_locked",
		       "BI2X", "BI2X_locked"
		FROM user_balances
		WHERE "USDC" > 0 OR "BI2XUSD" > 0 OR "BI2X" > 0
		ORDER BY user_id`)
	if err != nil {
		fmt.Println("query:", err)
		os.Exit(1)
	}
	defer rows.Close()

	fmt.Printf("%-40s %14s %14s %14s %14s %14s %14s\n",
		"USER", "USDC", "USDC_avail", "BI2XUSD", "B2XUSD_avail", "BI2X", "BI2X_avail")
	for rows.Next() {
		var id string
		var usdc, usdcL, b2xusd, b2xusdL, bi2x, bi2xL string
		if err := rows.Scan(&id, &usdc, &usdcL, &b2xusd, &b2xusdL, &bi2x, &bi2xL); err != nil {
			fmt.Println("scan:", err)
			os.Exit(1)
		}
		fmt.Printf("%-40s %14s %14s %14s %14s %14s %14s\n", id, usdc, usdcL, b2xusd, b2xusdL, bi2x, bi2xL)
	}
	if err := rows.Err(); err != nil {
		fmt.Println("rows:", err)
		os.Exit(1)
	}
}
