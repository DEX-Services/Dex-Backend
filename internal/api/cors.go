package api

import (
	"net/http"
	"strings"
)

// CORS wraps handler with permissive-but-credentialed CORS. origins is a
// comma-separated allowlist (e.g. "http://localhost:8080,http://localhost:8082");
// the request's Origin header is echoed back only if it matches an entry, since
// Access-Control-Allow-Origin can't be a wildcard when credentials are allowed.
func CORS(origins string, next http.Handler) http.Handler {
	allowed := make(map[string]bool)
	for _, o := range strings.Split(origins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			allowed[o] = true
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
