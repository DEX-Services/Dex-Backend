// Package propfirmclient calls the standalone BitDX Prop Firm backend's
// POST /internal/provision endpoint — the one integration point between
// this exchange and that separate service/database (PROP_FIRM_PLAN.md
// section 2's trust boundary: the prop-firm backend owns its own Postgres,
// its own auth, its own trading engine; this exchange only ever tells it
// "this purchase happened, provision an account for it").
package propfirmclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// Client calls the prop-firm backend's internal provisioning endpoint. A
// nil-baseURL Client (built when PROPFIRM_BACKEND_URL or
// PROPFIRM_INTERNAL_SECRET is unset) reports Enabled()==false so callers can
// fail the purchase clearly instead of silently no-op'ing money movement —
// unlike engineclient's ledger-sync bridge, a purchase's provisioning step
// is not optional best-effort work, so there is no silent-disable path here.
type Client struct {
	baseURL string
	secret  string
	http    *http.Client
}

// New builds a Client from PROPFIRM_BACKEND_URL / PROPFIRM_INTERNAL_SECRET
// env vars. If either is unset, Enabled() reports false.
func New() *Client {
	base := os.Getenv("PROPFIRM_BACKEND_URL")
	secret := os.Getenv("PROPFIRM_INTERNAL_SECRET")
	if base == "" || secret == "" {
		slog.Warn("PROPFIRM_BACKEND_URL or PROPFIRM_INTERNAL_SECRET not set, prop-firm purchase flow disabled")
		return &Client{}
	}
	return &Client{
		baseURL: base,
		secret:  secret,
		http: &http.Client{
			Timeout: 20 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        20,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// NewForTest builds a Client pointed at an arbitrary base URL/secret/http
// client, for tests that need to stub the prop-firm backend.
func NewForTest(baseURL, secret string, httpClient *http.Client) *Client {
	return &Client{baseURL: baseURL, secret: secret, http: httpClient}
}

// Enabled reports whether this client can actually reach the prop-firm
// backend.
func (c *Client) Enabled() bool {
	return c != nil && c.baseURL != ""
}

type provisionRequest struct {
	ExternalRef        string  `json:"externalRef"`
	PackageID          string  `json:"packageId"`
	ExchangeAccountRef *string `json:"exchangeAccountRef,omitempty"`
}

// ProvisionResult mirrors the prop-firm backend's provisionResponse (see its
// internal/api/provision.go) — Status is "" for a freshly-minted account and
// "already_fulfilled" when externalRef was already provisioned by a prior
// call (Username/Password are empty in that case: a plaintext password is
// only ever returned once, by the original call).
type ProvisionResult struct {
	Status    string `json:"status,omitempty"`
	Username  string `json:"username,omitempty"`
	Password  string `json:"password,omitempty"`
	AccountID string `json:"accountId"`
}

// Provision calls POST /internal/provision. externalRef must be a stable,
// unique identifier for this purchase (the pf_purchases row this creates on
// the prop-firm side dedups on it) — callers should pass the exchange-side
// purchase record's own ID so a retried call after a network error is safe:
// the prop-firm backend recognizes the same externalRef and returns
// {status:"already_fulfilled"} instead of minting a second account.
func (c *Client) Provision(ctx context.Context, externalRef, packageID, exchangeAccountRef string) (*ProvisionResult, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("prop-firm backend not configured")
	}
	body, err := json.Marshal(provisionRequest{
		ExternalRef:        externalRef,
		PackageID:          packageID,
		ExchangeAccountRef: &exchangeAccountRef,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/provision", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", c.secret)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("propfirmclient provision: %w", err)
	}
	defer resp.Body.Close()

	var result ProvisionResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("propfirmclient provision: decode response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("propfirmclient provision: status %d", resp.StatusCode)
	}
	return &result, nil
}
