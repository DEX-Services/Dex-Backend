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
