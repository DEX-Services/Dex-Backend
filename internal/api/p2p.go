package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
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
	MinOrderFiat   string   `json:"minOrderFiat"`
	MaxOrderFiat   string   `json:"maxOrderFiat"`
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
	Reason  string `json:"reason"`
}
type paymentAccountRequest struct {
	Method            string `json:"method"`
	AccountName       string `json:"accountName"`
	AccountIdentifier string `json:"accountIdentifier"`
	BankName          string `json:"bankName"`
	IFSCCode          string `json:"ifscCode"`
	Instructions      string `json:"instructions"`
}
type orderMessageRequest struct {
	OrderID string `json:"orderId"`
	Body    string `json:"body"`
}
type appealRequest struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}
type markPaidRequest struct {
	OrderID            string `json:"orderId"`
	OwnAccountAttested bool   `json:"ownAccountAttested"`
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
	listing, err := s.P2P.CreateListingWithLimits(r.Context(), userID, req.Side, asset, req.AmountRaw, req.PaymentMethods, req.Username, req.MinOrderFiat, req.MaxOrderFiat)
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
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req markPaidRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	order, err := s.P2P.MarkPaidWithAttestation(r.Context(), userID, req.OrderID, req.OwnAccountAttested)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"order": order})
}

func (s *P2PServer) ReleaseOrder(w http.ResponseWriter, r *http.Request) {
	s.orderAction(w, r, s.P2P.ReleaseOrder)
}

func (s *P2PServer) CancelOrder(w http.ResponseWriter, r *http.Request) {
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
	order, err := s.P2P.CancelOrderWithReason(r.Context(), userID, req.OrderID, req.Reason)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"order": order})
}

func (s *P2PServer) OrderDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	orderID := strings.TrimSpace(r.URL.Query().Get("orderId"))
	if orderID == "" {
		writeError(w, http.StatusBadRequest, "orderId is required")
		return
	}
	order, err := s.P2P.Order(r.Context(), userID, orderID)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"order": order})
}

func (s *P2PServer) PaymentAccounts(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	if r.Method == http.MethodGet {
		accounts, err := s.P2P.PaymentAccounts(r.Context(), userID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not load payment accounts")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"accounts": accounts})
		return
	}
	if !requirePost(w, r) {
		return
	}
	var req paymentAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	account, err := s.P2P.UpsertPaymentAccount(r.Context(), userID, req.Method, req.AccountName, req.AccountIdentifier, req.Instructions, req.BankName, req.IFSCCode)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"account": account})
}

func (s *P2PServer) OrderMessages(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	if r.Method == http.MethodGet {
		orderID := strings.TrimSpace(r.URL.Query().Get("orderId"))
		items, err := s.P2P.OrderMessages(r.Context(), userID, orderID)
		if err != nil {
			writeError(w, p2pErrorStatus(err), err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"messages": items})
		return
	}
	if !requirePost(w, r) {
		return
	}
	var req orderMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	item, err := s.P2P.AddOrderMessage(r.Context(), userID, req.OrderID, req.Body)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"message": item})
}

func (s *P2PServer) OrderProofs(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	if r.Method == http.MethodGet {
		orderID := strings.TrimSpace(r.URL.Query().Get("orderId"))
		items, err := s.P2P.OrderProofs(r.Context(), userID, orderID)
		if err != nil {
			writeError(w, p2pErrorStatus(err), err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"proofs": items})
		return
	}
	if !requirePost(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 6<<20)
	if err := r.ParseMultipartForm(6 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "proof upload must be at most 5 MB")
		return
	}
	orderID := strings.TrimSpace(r.FormValue("orderId"))
	file, header, err := r.FormFile("proof")
	if err != nil {
		writeError(w, http.StatusBadRequest, "proof file is required")
		return
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (5<<20)+1))
	if err != nil || len(data) > 5<<20 {
		writeError(w, http.StatusBadRequest, "proof upload must be at most 5 MB")
		return
	}
	mimeType := http.DetectContentType(data)
	proof, err := s.P2P.AddOrderProof(r.Context(), userID, orderID, filepath.Base(header.Filename), mimeType, data)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"proof": proof})
}

func (s *P2PServer) OrderProofDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	file, err := s.P2P.OrderProofFile(r.Context(), userID, strings.TrimSpace(r.URL.Query().Get("proofId")))
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	w.Header().Set("Content-Type", file.MimeType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", filepath.Base(file.FileName)))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(file.Data)
}

func (s *P2PServer) OrderEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	items, err := s.P2P.OrderEvents(r.Context(), userID, strings.TrimSpace(r.URL.Query().Get("orderId")))
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": items})
}

func (s *P2PServer) AppealOrder(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req appealRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	order, err := s.P2P.AppealOrder(r.Context(), userID, req.OrderID, req.Reason)
	if err != nil {
		writeError(w, p2pErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"order": order})
}

func (s *P2PServer) CancelAppeal(w http.ResponseWriter, r *http.Request) {
	s.orderAction(w, r, s.P2P.CancelAppeal)
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
