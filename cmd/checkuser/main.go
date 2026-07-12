package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dex/dex-backend/internal/db"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load("/Users/trimplingroup/Desktop/Dex Everything/Dex-Backend/.env")
	ctx := context.Background()
	pool, err := db.New(ctx, os.Getenv("POSTGRES_SERVICE_URI"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, `SELECT id, wallet_address, "USDC", "USDC_locked" FROM users u JOIN user_balances b ON u.id = b.user_id WHERE wallet_address ILIKE $1`, "0x2d98%")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, addr, usdc, locked string
		rows.Scan(&id, &addr, &usdc, &locked)
		fmt.Println(id, addr, usdc, locked)
	}
}
