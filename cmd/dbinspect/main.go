package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

// read-only inspection: table count, sizes, row counts, index stats, bloat signals.
func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("POSTGRES_SERVICE_URI"))
	if err != nil {
		fmt.Println("CONNECT ERROR:", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	fmt.Println("connected (read-only session — no writes performed)")
	fmt.Println()

	// 1. Server version + database size
	var version string
	var dbSize string
	_ = conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	_ = conn.QueryRow(ctx, "SELECT pg_size_pretty(pg_database_size(current_database()))").Scan(&dbSize)
	fmt.Println("=== SERVER ===")
	fmt.Println(version)
	fmt.Println("database size:", dbSize)
	fmt.Println()

	// 2. Table count in public schema
	var tableCount int
	_ = conn.QueryRow(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'").Scan(&tableCount)
	fmt.Printf("=== TABLES: %d in public schema ===\n", tableCount)
	fmt.Println()

	// 3. Per-table size + row estimate, largest first
	rows, err := conn.Query(ctx, `
		SELECT c.relname,
		       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
		       pg_size_pretty(pg_relation_size(c.oid)) AS heap_size,
		       pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
		       COALESCE(c.reltuples::bigint, 0) AS est_rows
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'r'
		ORDER BY pg_total_relation_size(c.oid) DESC`)
	if err != nil {
		fmt.Println("table stats query error:", err)
		return
	}
	defer rows.Close()
	fmt.Println("=== TABLE SIZES (largest first) ===")
	fmt.Printf("%-40s %12s %12s %12s %14s\n", "TABLE", "TOTAL", "HEAP", "INDEXES", "~ROWS")
	for rows.Next() {
		var name, total, heap, idx string
		var estRows int64
		if err := rows.Scan(&name, &total, &heap, &idx, &estRows); err != nil {
			fmt.Println("scan error:", err)
			return
		}
		fmt.Printf("%-40s %12s %12s %12s %14d\n", name, total, heap, idx, estRows)
	}
	fmt.Println()

	// 4. Biggest indexes overall (bloat indicators: index >> heap)
	idxRows, err := conn.Query(ctx, `
		SELECT c.relname AS index_name,
		       t.relname AS table_name,
		       pg_size_pretty(pg_relation_size(c.oid)) AS size,
		       CASE WHEN pg_relation_size(t.oid) > 0
		            THEN round(pg_relation_size(c.oid)::numeric / pg_relation_size(t.oid), 1)
		            ELSE 0 END AS idx_vs_heap
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_index i ON i.indexrelid = c.oid
		JOIN pg_class t ON t.oid = i.indrelid
		WHERE n.nspname = 'public'
		ORDER BY pg_relation_size(c.oid) DESC
		LIMIT 15`)
	if err != nil {
		fmt.Println("index stats query error:", err)
		return
	}
	defer idxRows.Close()
	fmt.Println("=== TOP 15 INDEXES (idx_vs_heap > 1 suggests bloat/rebuild candidate) ===")
	fmt.Printf("%-45s %-30s %12s %12s\n", "INDEX", "TABLE", "SIZE", "IDX/HEAP")
	for idxRows.Next() {
		var idxName, tblName, size string
		var ratio float64
		if err := idxRows.Scan(&idxName, &tblName, &size, &ratio); err != nil {
			fmt.Println("scan error:", err)
			return
		}
		fmt.Printf("%-45s %-30s %12s %12.1f\n", idxName, tblName, size, ratio)
	}
	fmt.Println()

	// 5. Dead tuples (bloat signal) + last vacuum/analyze per table
	deadRows, err := conn.Query(ctx, `
		SELECT relname, n_live_tup, n_dead_tup,
		       pg_size_pretty(pg_total_relation_size(relid)),
		       last_vacuum, last_autovacuum, last_analyze
		FROM pg_stat_user_tables
		ORDER BY n_dead_tup DESC
		LIMIT 15`)
	if err != nil {
		fmt.Println("dead tuple query error:", err)
		return
	}
	defer deadRows.Close()
	fmt.Println("=== DEAD TUPLES / VACUUM STATE (top 15 by dead tuples) ===")
	fmt.Printf("%-40s %12s %12s %12s\n", "TABLE", "LIVE", "DEAD", "TOTAL SIZE")
	for deadRows.Next() {
		var name string
		var live, dead int64
		var size string
		var lastVacuum, lastAuto, lastAnalyze *interface{}
		if err := deadRows.Scan(&name, &live, &dead, &size, &lastVacuum, &lastAuto, &lastAnalyze); err != nil {
			// nullable timestamps can be nil — scan into interface{} pointers
			fmt.Println("scan error:", err)
			return
		}
		fmt.Printf("%-40s %12d %12d %12s\n", name, live, dead, size)
	}
	fmt.Println()

	// 6. Connection state
	var totalConns, activeConns int
	_ = conn.QueryRow(ctx, "SELECT count(*), count(*) FILTER (WHERE state='active') FROM pg_stat_activity").Scan(&totalConns, &activeConns)
	fmt.Println("=== CONNECTIONS ===")
	fmt.Printf("total: %d, active: %d\n", totalConns, activeConns)
	var maxConn int
	_ = conn.QueryRow(ctx, "SHOW max_connections").Scan(&maxConn)
	fmt.Printf("max_connections setting: %d\n", maxConn)
}
