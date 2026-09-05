package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/models"
	"github.com/dex/dex-backend/internal/repo"
)

type P2PServer struct {
	*Server
	P2P    *repo.P2PRepo
	Engine *engineclient.Client
}
type createListingRequest struct {
	Asset          string   `json:"asset"`
	Side           string   `json:"side"`
	AmountRaw      string   `json:"amountRaw"`
	PaymentMethods []string `json:"paymentMethods"`
	Username       string   `json:"username"`
}
type buyListingRequest struct {
	ListingID      string `json:"listingId"`
	AmountRaw      string `json:"amountRaw"`
	PaymentMethod  string `json:"paymentMethod"`
	IdempotencyKey string `json:"idempotencyKey"`
}
type p2pProfileRequest struct {
	Username string `json:"username"`
}
type cancelListingRequest struct {
	ListingID string `json:"listingId"`
}
type fundP2PWalletRequest struct {
	Asset          string `json:"asset"`
	AmountRaw      string `json:"amountRaw"`
	IdempotencyKey string `json:"idempotencyKey"`
}
type orderActionRequest struct {
	OrderID string `json:"orderId"`
}

func requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return false
	}
	return true
}
func (s *P2PServer) claims(w http.ResponseWriter, r *http.Request) (string, bool) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "connect and authenticate a wallet first")
		return "", false
	}
	return claims.UserID, true
}

func (s *P2PServer) Price(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	asset := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("asset")))
	if asset == "" {
		asset = "USDB"
	}
	price, err := s.P2P.PriceFor(r.Context(), asset)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"price": price})
}

// Wallet: GET /p2p/wallet returns the dedicated USDB P2P wallet.
func (s *P2PServer) Wallet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	balances, err := s.P2P.WalletBalances(r.Context(), userID)
	if err != nil {
		s.Log.Error("load p2p wallet failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load P2P wallet")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"balance": balances[0], "balances": balances})
}

// FundWallet: POST /p2p/wallet/fund moves main-wallet USDB into P2P.
func (s *P2PServer) FundWallet(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req fundP2PWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	asset := strings.ToUpper(strings.TrimSpace(req.Asset))
	if asset == "" {
		asset = "USDB"
	}
	balance, moved, err := s.P2P.FundWalletAsset(r.Context(), userID, asset, req.AmountRaw, req.IdempotencyKey)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	if moved && s.Engine != nil {
		engineclient.Async("p2p wallet fund debit", func(ctx context.Context) error {
			return s.Engine.Debit(ctx, userID, asset, req.AmountRaw)
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"balance": balance})
}

func (s *P2PServer) Listings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		listings, err := s.P2P.Listings(r.Context(), "", true)
		if err != nil {
			s.Log.Error("list p2p listings failed", "err", err)
			writeError(w, http.StatusInternalServerError, "could not load listings")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"listings": listings})
		return
	}
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req createListingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	asset := strings.ToUpper(strings.TrimSpace(req.Asset))
	if asset == "" {
		asset = "USDB"
	}
	listing, err := s.P2P.CreateListingWithDetails(r.Context(), userID, req.Side, asset, req.AmountRaw, req.PaymentMethods, req.Username)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"listing": listing})
}

func (s *P2PServer) MyListings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	listings, err := s.P2P.Listings(r.Context(), userID, false)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load listings")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"listings": listings})
}

func (s *P2PServer) Buy(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req buyListingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	order, err := s.P2P.CreateOrderWithPayment(r.Context(), userID, req.ListingID, req.AmountRaw, req.PaymentMethod, req.IdempotencyKey)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	if order.LegacyMainDebit && s.Engine != nil {
		engineclient.Async("legacy p2p order debit", func(ctx context.Context) error {
			return s.Engine.Debit(ctx, order.SellerID, order.Asset, order.AmountRaw)
		})
	}
	writeJSON(w, http.StatusCreated, map[string]any{"order": order})
}

func (s *P2PServer) Profile(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	if r.Method == http.MethodGet {
		profile, err := s.P2P.P2PProfile(r.Context(), userID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not load P2P profile")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"profile": profile})
		return
	}
	if !requirePost(w, r) {
		return
	}
	var req p2pProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	profile, err := s.P2P.EstablishP2PUsername(r.Context(), userID, req.Username)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profile": profile})
}

func p2pErrorStatus(err error) int {
	switch {
	case errors.Is(err, repo.ErrP2PNotFound), errors.Is(err, repo.ErrP2POrderNotFound):
		return http.StatusNotFound
	case errors.Is(err, repo.ErrP2PSelfPurchase), errors.Is(err, repo.ErrP2PForbidden):
		return http.StatusForbidden
	case errors.Is(err, repo.ErrP2PUnavailable), errors.Is(err, repo.ErrP2PInvalidState),
		errors.Is(err, repo.ErrP2PExpired), errors.Is(err, repo.ErrP2PIdempotencyKey):
		return http.StatusConflict
	default:
		return http.StatusBadRequest
	}
}

func (s *P2PServer) orderAction(w http.ResponseWriter, r *http.Request, action func(context.Context, string, string) (*models.P2POrder, error)) {
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req orderActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.OrderID) == "" {
		writeError(w, http.StatusBadRequest, "orderId is required")
		return
	}
	order, err := action(r.Context(), userID, req.OrderID)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"order": order})
}

func (s *P2PServer) MarkPaid(w http.ResponseWriter, r *http.Request) {
	s.orderAction(w, r, s.P2P.MarkPaid)
}

func (s *P2PServer) ReleaseOrder(w http.ResponseWriter, r *http.Request) {
	s.orderAction(w, r, s.P2P.ReleaseOrder)
}

func (s *P2PServer) CancelOrder(w http.ResponseWriter, r *http.Request) {
	s.orderAction(w, r, s.P2P.CancelOrder)
}

func (s *P2PServer) Orders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	orders, err := s.P2P.Orders(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load orders")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": orders})
}

func (s *P2PServer) CancelListing(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req cancelListingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.P2P.CancelListing(r.Context(), userID, req.ListingID); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, repo.ErrP2PNotFound) {
			status = http.StatusNotFound
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}
