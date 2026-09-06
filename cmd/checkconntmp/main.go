package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	uri := os.Getenv("POSTGRES_SERVICE_URI")
	conn, err := pgx.Connect(context.Background(), uri)
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), `
		SELECT pid, (now() - xact_start)::text, (now() - query_start)::text, coalesce(wait_event_type,''), coalesce(wait_event,''), left(query, 100)
		FROM pg_stat_activity
		WHERE datname = current_database() AND state = 'idle in transaction'
		ORDER BY xact_start ASC`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var xactAge, queryAge, waitType, waitEvent, query string
		rows.Scan(&pid, &xactAge, &queryAge, &waitType, &waitEvent, &query)
		fmt.Printf("pid=%d xact_age=%v query_age=%v wait=%v/%v query=%v\n", pid, xactAge, queryAge, waitType, waitEvent, query)
	}
}
