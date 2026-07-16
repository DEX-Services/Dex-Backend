package api

import (
	"encoding/json"
	"errors"
	"github.com/dex/dex-backend/internal/repo"
	"net/http"
)

type P2PServer struct {
	*Server
	P2P *repo.P2PRepo
}
type createListingRequest struct {
	AmountRaw     string `json:"amountRaw"`
	PaymentMethod string `json:"paymentMethod"`
}
type buyListingRequest struct {
	ListingID string `json:"listingId"`
	AmountRaw string `json:"amountRaw"`
}
type cancelListingRequest struct {
	ListingID string `json:"listingId"`
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
	price, err := s.P2P.TodayPrice(r.Context())
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"price": price})
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
	listing, err := s.P2P.CreateListing(r.Context(), userID, req.AmountRaw, req.PaymentMethod)
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
	order, err := s.P2P.Buy(r.Context(), userID, req.ListingID, req.AmountRaw)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, repo.ErrP2PNotFound) {
			status = http.StatusNotFound
		}
		if errors.Is(err, repo.ErrP2PSelfPurchase) {
			status = http.StatusForbidden
		}
		if errors.Is(err, repo.ErrP2PUnavailable) {
			status = http.StatusConflict
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"order": order})
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
