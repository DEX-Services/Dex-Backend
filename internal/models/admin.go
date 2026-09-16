package models

import "time"

type AdminProfile struct {
	LoginID   string    `json:"loginId"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Phone     string    `json:"phone"`
	Role      string    `json:"role"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type AdminSummary struct {
	TotalUsers          int64              `json:"totalUsers"`
	ActiveUsers24h      int64              `json:"activeUsers24h"`
	OpenSessions        int64              `json:"openSessions"`
	TotalLedgerEntries  int64              `json:"totalLedgerEntries"`
	ConfirmedLedgerRaw  string             `json:"confirmedLedgerRaw"`
	PendingWithdrawals  int64              `json:"pendingWithdrawals"`
	P2PFeeWalletRaw     string             `json:"p2pFeeWalletRaw"`
	TotalBalances       []AdminTokenTotal  `json:"totalBalances"`
	TopUsers            []AdminTopUser     `json:"topUsers"`
	RecentLedgerEntries []AdminLedgerEntry `json:"recentLedgerEntries"`
	RecentUsers         []AdminRecentUser  `json:"recentUsers"`
}

type AdminTokenTotal struct {
	Token  string `json:"token"`
	Amount string `json:"amount"`
	Locked string `json:"locked"`
}

type AdminTopUser struct {
	UserID        string     `json:"userId"`
	WalletAddress string     `json:"walletAddress"`
	WalletType    string     `json:"walletType"`
	EntryCount    int64      `json:"entryCount"`
	TotalRaw      string     `json:"totalRaw"`
	LastLoginAt   *time.Time `json:"lastLoginAt,omitempty"`
}

type AdminLedgerEntry struct {
	ID            string    `json:"id"`
	UserID        string    `json:"userId"`
	WalletAddress string    `json:"walletAddress"`
	Kind          string    `json:"kind"`
	Token         string    `json:"token"`
	Amount        string    `json:"amount"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"createdAt"`
}

type AdminRecentUser struct {
	ID            string     `json:"id"`
	WalletAddress string     `json:"walletAddress"`
	WalletType    string     `json:"walletType"`
	CreatedAt     time.Time  `json:"createdAt"`
	LastLoginAt   *time.Time `json:"lastLoginAt,omitempty"`
}

// BI2XAllocationBalance is one category's current remaining quantity out of
// the fixed 500,000,000 BI2X total supply (see db.ensureBI2XAllocationTables
// for the starting split). RemainingQty is a plain whole-token decimal
// string (e.g. "368000000"), not a raw on-chain-scaled amount.
type BI2XAllocationBalance struct {
	Category     string    `json:"category"`
	RemainingQty string    `json:"remainingQty"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

// BI2XAllocationHistoryEntry is one recorded burn/distribution against a
// category, which also decremented that category's BI2XAllocationBalance by
// AmountQty at the time it was recorded.
type BI2XAllocationHistoryEntry struct {
	ID        int64     `json:"id"`
	Category  string    `json:"category"`
	AmountQty string    `json:"amountQty"`
	EventDate time.Time `json:"eventDate"`
	Note      string    `json:"note,omitempty"`
	CreatedBy string    `json:"createdBy,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}
