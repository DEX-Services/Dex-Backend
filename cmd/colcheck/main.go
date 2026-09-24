package main

import (
	"context"
	"fmt"
	"os"


	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("POSTGRES_SERVICE_URI"))
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	var total int
	conn.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='user_balances'`).Scan(&total)
	fmt.Println("user_balances column count:", total)

	var usersCols int
	conn.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='users'`).Scan(&usersCols)
	fmt.Println("users column count:", usersCols)

	rowsu, err := conn.Query(ctx, `
		SELECT column_name, COUNT(*) AS n
		FROM information_schema.columns
		WHERE table_schema='public' AND table_name='users'
		GROUP BY column_name
		HAVING COUNT(*) > 1
		ORDER BY n DESC`)
	if err != nil {
		panic(err)
	}
	type dupu struct {
		name string
		n    int
	}
	var dupsu []dupu
	for rowsu.Next() {
		var d dupu
		rowsu.Scan(&d.name, &d.n)
		dupsu = append(dupsu, d)
	}
	rowsu.Close()
	fmt.Println("users duplicated column names:", len(dupsu))
	for i, d := range dupsu {
		if i < 25 {
			fmt.Printf("   %-30s x%d\n", d.name, d.n)
		}
	}
	if len(dupsu) > 25 {
		fmt.Printf("   ... and %d more\n", len(dupsu)-25)
	}

	rows, err := conn.Query(ctx, `
		SELECT column_name, COUNT(*) AS n
		FROM information_schema.columns
		WHERE table_schema='public' AND table_name='user_balances'
		GROUP BY column_name
		HAVING COUNT(*) > 1
		ORDER BY n DESC, column_name`)
	if err != nil {
		panic(err)
	}
	type dup struct {
		name string
		n    int
	}
	var dups []dup
	for rows.Next() {
		var d dup
		rows.Scan(&d.name, &d.n)
		dups = append(dups, d)
	}
	rows.Close()
	fmt.Println("duplicated column names:", len(dups))
	for i, d := range dups {
		if i < 20 {
			fmt.Printf("   %-30s x%d\n", d.name, d.n)
		}
	}
	if len(dups) > 20 {
		fmt.Printf("   ... and %d more\n", len(dups)-20)
	}

	// Which tables have an absurd column count?
	rows2, err := conn.Query(ctx, `
		SELECT table_schema, table_name, COUNT(*) n
		FROM information_schema.columns
		WHERE table_schema='public'
		GROUP BY 1,2
		ORDER BY n DESC
		LIMIT 25`)
	if err != nil {
		panic(err)
	}
	fmt.Println("\ntop tables by column count:")
	var names []string
	for rows2.Next() {
		var sch, tbl string
		var n int
		rows2.Scan(&sch, &tbl, &n)
		names = append(names, fmt.Sprintf("   %s.%s : %d", sch, tbl, n))
	}
	rows2.Close()
	for _, s := range names {
		fmt.Println(s)
	}
}
