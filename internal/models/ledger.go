package models

import "time"

type LedgerEntry struct {
	ID            string    `json:"id"`
	UserID        string    `json:"userId"`
	WalletAddress string    `json:"walletAddress"`
	Kind          string    `json:"kind"`
	Token         string    `json:"token"`
	Amount        string    `json:"amount"`
	TxHash        *string   `json:"txHash,omitempty"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"createdAt"`
}

const (
	LedgerKindDeposit            = "deposit"
	LedgerKindWithdrawalRequest  = "withdrawal_request"
	LedgerKindWithdrawalApproved = "withdrawal_approved"
	LedgerStatusPending          = "pending"
	LedgerStatusConfirmed        = "confirmed"
)
