// Package models holds shared data types for the auth service.
package models

import "time"

type User struct {
	ID            string     `json:"id"`
	WalletAddress string     `json:"walletAddress"`
	WalletType    string     `json:"walletType"`
	CreatedAt     time.Time  `json:"createdAt"`
	LastLoginAt   *time.Time `json:"lastLoginAt,omitempty"`
}
