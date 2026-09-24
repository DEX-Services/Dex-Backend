package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

// Runs a .sql file's statements one at a time to find the statement that
// raises 54011 (too many columns). Rolled back via ROLLBACK on failure.
func main() {
	_ = godotenv.Load()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("POSTGRES_SERVICE_URI"))
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	stmts := splitStatements(string(raw))
	fmt.Printf("statements to run: %d\n\n", len(stmts))

	for i, s := range stmts {
		trimmed := strings.TrimSpace(s)
		if trimmed == "" {
			continue
		}
		head := strings.Join(strings.Fields(trimmed), " ")
		if len(head) > 100 {
			head = head[:100] + "..."
		}
		if _, err := conn.Exec(ctx, trimmed); err != nil {
			fmt.Printf("### FAILED at #%d\n%s\n\nerror: %v\n", i, trimmed, err)
			return
		}
		fmt.Printf("ok  #%d  %s\n", i, head)
	}
	fmt.Println("\nALL STATEMENTS OK")
}

func splitStatements(sql string) []string {
	var out []string
	var cur strings.Builder
	i := 0
	for i < len(sql) {
		if sql[i] == '$' {
			j := i + 1
			for j < len(sql) && (sql[j] == '_' || (sql[j] >= 'a' && sql[j] <= 'z') || (sql[j] >= 'A' && sql[j] <= 'Z') || (sql[j] >= '0' && sql[j] <= '9')) {
				j++
			}
			if j < len(sql) && sql[j] == '$' {
				tag := sql[i : j+1]
				end := strings.Index(sql[j+1:], tag)
				if end >= 0 {
					cur.WriteString(sql[i : j+1+end+len(tag)])
					i = j + 1 + end + len(tag)
					continue
				}
			}
		}
		if sql[i] == '\'' {
			j := i + 1
			for j < len(sql) {
				if sql[j] == '\'' {
					if j+1 < len(sql) && sql[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			cur.WriteString(sql[i : j+1])
			i = j + 1
			continue
		}
		if sql[i] == ';' {
			out = append(out, cur.String())
			cur.Reset()
			i++
			continue
		}
		cur.WriteByte(sql[i])
		i++
	}
	if strings.TrimSpace(cur.String()) != "" {
		out = append(out, cur.String())
	}
	return out
}
