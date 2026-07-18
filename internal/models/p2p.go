package models

import "time"

type P2PPrice struct {
	Asset        string    `json:"asset"`
	FiatCurrency string    `json:"fiatCurrency"`
	Price        string    `json:"price"`
	PriceDate    string    `json:"priceDate"`
	CreatedAt    time.Time `json:"createdAt"`
}
type P2PListing struct {
	ID            string    `json:"id"`
	SellerID      string    `json:"sellerId"`
	SellerAddress string    `json:"sellerAddress"`
	Asset         string    `json:"asset"`
	AmountRaw     string    `json:"amountRaw"`
	RemainingRaw  string    `json:"remainingRaw"`
	Price         string    `json:"price"`
	FiatCurrency  string    `json:"fiatCurrency"`
	PaymentMethod string    `json:"paymentMethod"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}
type P2POrder struct {
	ID                 string     `json:"id"`
	ListingID          string     `json:"listingId"`
	SellerID           string     `json:"sellerId"`
	BuyerID            string     `json:"buyerId"`
	Asset              string     `json:"asset"`
	AmountRaw          string     `json:"amountRaw"`
	Price              string     `json:"price"`
	FiatCurrency       string     `json:"fiatCurrency"`
	GrossAmount        string     `json:"grossAmount"`
	BuyerFee           string     `json:"buyerFee"`
	SellerFee          string     `json:"sellerFee"`
	BuyerPayable       string     `json:"buyerPayable"`
	SellerReceivable   string     `json:"sellerReceivable"`
	PaymentMethod      string     `json:"paymentMethod"`
	Status             string     `json:"status"`
	ExpiresAt          time.Time  `json:"expiresAt"`
	UpdatedAt          time.Time  `json:"updatedAt"`
	CancellationReason string     `json:"cancellationReason,omitempty"`
	CompletedAt        *time.Time `json:"completedAt,omitempty"`
	CreatedAt          time.Time  `json:"createdAt"`
}
