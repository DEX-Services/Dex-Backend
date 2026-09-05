package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/dex/dex-backend/internal/auth"
)

func call(tok, path string) {
	req, _ := http.NewRequest("GET", "http://localhost:8081"+path, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("err", err)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out any
	json.Unmarshal(b, &out)
	fmt.Printf("%s -> %s\n", path, string(b))
}

func main() {
	iss := auth.NewJWTIssuer(os.Getenv("JWT_SECRET"), 24*time.Hour)
	user := os.Args[1]
	tok, _, _ := iss.Issue(user, user)
	for _, p := range os.Args[2:] {
		call(tok, p)
	}
}
