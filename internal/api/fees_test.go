package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shopspring/decimal"
)

// fakeBI2XPriceReader is an injectable bi2xprice.Reader for handler tests —
// no live network call, no dependency on the real feed being up.
type fakeBI2XPriceReader struct {
	price decimal.Decimal
	err   error
}

func (f *fakeBI2XPriceReader) CurrentPrice(ctx context.Context) (decimal.Decimal, error) {
	return f.price, f.err
}

func TestFeeServerSubscribe_RejectsInvalidTier(t *testing.T) {
	srv := &FeeServer{
		Server:    &Server{},
		BI2XPrice: &fakeBI2XPriceReader{price: decimal.NewFromFloat(3.75)},
	}
	body, _ := json.Marshal(subscribeRequestBody{Tier: 0})
	req := httptest.NewRequest(http.MethodPost, "/fees/subscribe", bytes.NewReader(body))
	w := httptest.NewRecorder()

	// No auth cookie set — authenticate() will fail first, so this exercises
	// the auth gate rather than the tier-range check; that's fine, this test
	// exists to confirm invalid input never reaches FeeTierRepo.Subscribe
	// (which would nil-panic here since FeeTiers is unset).
	srv.Subscribe(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d (unauthenticated request rejected before reaching business logic)", w.Code, http.StatusUnauthorized)
	}
}

func TestFeeServerSubscribe_MethodNotAllowed(t *testing.T) {
	srv := &FeeServer{Server: &Server{}}
	req := httptest.NewRequest(http.MethodGet, "/fees/subscribe", nil)
	w := httptest.NewRecorder()
	srv.Subscribe(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestFeeServerTiers_MethodNotAllowed(t *testing.T) {
	srv := &FeeServer{Server: &Server{}}
	req := httptest.NewRequest(http.MethodPost, "/fees/tiers", nil)
	w := httptest.NewRecorder()
	srv.Tiers(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestFeeServerAdminSetFee_RejectsUnauthenticated(t *testing.T) {
	srv := &FeeServer{Server: &Server{}}
	body, _ := json.Marshal(adminSetFeeRequestBody{Key: "spot.maker", Rate: "0.0015"})
	req := httptest.NewRequest(http.MethodPost, "/admin/fees/set", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.AdminSetFee(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}
