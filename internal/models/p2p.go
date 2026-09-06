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
	ID                 string    `json:"id"`
	CreatorID          string    `json:"creatorId"`
	Username           string    `json:"username"`
	Side               string    `json:"side"`
	Asset              string    `json:"asset"`
	AmountRaw          string    `json:"amountRaw"`
	RemainingRaw       string    `json:"remainingRaw"`
	Price              string    `json:"price"`
	FiatCurrency       string    `json:"fiatCurrency"`
	MinOrderFiat       string    `json:"minOrderFiat"`
	MaxOrderFiat       string    `json:"maxOrderFiat"`
	PaymentMethods     []string  `json:"paymentMethods"`
	Status             string    `json:"status"`
	CompletedOrders    int       `json:"completedOrders"`
	CompletedAmountRaw string    `json:"completedAmountRaw"`
	CompletedOrders30d int       `json:"completedOrders30d"`
	CompletionRate30d  string    `json:"completionRate30d"`
	RatedOrders30d     int       `json:"ratedOrders30d"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

type P2PProfile struct {
	Username string `json:"username"`
}
type P2POrder struct {
	ID                      string     `json:"id"`
	ListingID               string     `json:"listingId"`
	SellerID                string     `json:"sellerId"`
	BuyerID                 string     `json:"buyerId"`
	Asset                   string     `json:"asset"`
	AmountRaw               string     `json:"amountRaw"`
	EscrowRaw               string     `json:"escrowRaw"`
	Price                   string     `json:"price"`
	FiatCurrency            string     `json:"fiatCurrency"`
	GrossAmount             string     `json:"grossAmount"`
	BuyerFee                string     `json:"buyerFee"`
	SellerFee               string     `json:"sellerFee"`
	BuyerPayable            string     `json:"buyerPayable"`
	SellerReceivable        string     `json:"sellerReceivable"`
	BuyerFeeRaw             string     `json:"buyerFeeRaw"`
	SellerFeeRaw            string     `json:"sellerFeeRaw"`
	BuyerCreditRaw          string     `json:"buyerCreditRaw"`
	SellerDebitRaw          string     `json:"sellerDebitRaw"`
	PaymentMethod           string     `json:"paymentMethod"`
	PaymentAccountName      string     `json:"paymentAccountName"`
	PaymentAccountID        string     `json:"paymentAccountIdentifier"`
	PaymentInstructions     string     `json:"paymentInstructions,omitempty"`
	Status                  string     `json:"status"`
	ExpiresAt               time.Time  `json:"expiresAt"`
	PaymentMarkedAt         *time.Time `json:"paymentMarkedAt,omitempty"`
	BuyerOwnAccountAttested bool       `json:"buyerOwnAccountAttested"`
	AppealAvailableAt       *time.Time `json:"appealAvailableAt,omitempty"`
	AppealedBy              string     `json:"appealedBy,omitempty"`
	AppealReason            string     `json:"appealReason,omitempty"`
	AppealedAt              *time.Time `json:"appealedAt,omitempty"`
	UpdatedAt               time.Time  `json:"updatedAt"`
	CancellationReason      string     `json:"cancellationReason,omitempty"`
	CompletedAt             *time.Time `json:"completedAt,omitempty"`
	CreatedAt               time.Time  `json:"createdAt"`

	// LegacyMainDebit is internal response metadata used only to keep the
	// matching-engine mirror consistent when an order consumes a pre-P2P-wallet
	// listing. It is never serialized to clients.
	LegacyMainDebit bool `json:"-"`
}

type P2PPaymentAccount struct {
	ID                string    `json:"id"`
	Method            string    `json:"method"`
	AccountName       string    `json:"accountName"`
	AccountIdentifier string    `json:"accountIdentifier"`
	Instructions      string    `json:"instructions,omitempty"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

type P2POrderProof struct {
	ID        string    `json:"id"`
	FileName  string    `json:"fileName"`
	MimeType  string    `json:"mimeType"`
	SizeBytes int64     `json:"sizeBytes"`
	CreatedAt time.Time `json:"createdAt"`
}

type P2POrderProofFile struct {
	P2POrderProof
	Data []byte `json:"-"`
}

type P2POrderMessage struct {
	ID             string    `json:"id"`
	SenderID       string    `json:"senderId,omitempty"`
	SenderUsername string    `json:"senderUsername"`
	Body           string    `json:"body"`
	System         bool      `json:"system"`
	CreatedAt      time.Time `json:"createdAt"`
}

type P2POrderEvent struct {
	ID        string         `json:"id"`
	ActorID   string         `json:"actorId,omitempty"`
	Kind      string         `json:"kind"`
	Metadata  map[string]any `json:"metadata"`
	CreatedAt time.Time      `json:"createdAt"`
}

type P2PWalletBalance struct {
	Asset        string `json:"asset"`
	AvailableRaw string `json:"availableRaw"`
	ReservedRaw  string `json:"reservedRaw"`
	TotalRaw     string `json:"totalRaw"`
}
