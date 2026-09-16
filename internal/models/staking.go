package models

import "time"

// StakingPosition is one user's stake — see repo.LedgerRepo.StakeBI2X/
// RedeemBI2X and db.ensureStakingTables for the full mechanics. PrincipalRaw
// and any interest figures are raw integer strings at the platform's
// standard balanceRawScale (6), same convention as every other balance
// field in this API.
type StakingPosition struct {
	ID           string     `json:"id"`
	Asset        string     `json:"asset"`
	PrincipalRaw string     `json:"principalRaw"`
	AprBps       int        `json:"aprBps"`
	StartedAt    time.Time  `json:"startedAt"`
	Status       string     `json:"status"`
	ClosedAt     *time.Time `json:"closedAt,omitempty"`
}

// StakingEvent is one permanent record of a stake or redeem action against a
// position (see db.ensureStakingTables's staking_events table).
type StakingEvent struct {
	ID           int64     `json:"id"`
	PositionID   string    `json:"positionId"`
	Kind         string    `json:"kind"`
	PrincipalRaw string    `json:"principalRaw"`
	InterestRaw  string    `json:"interestRaw"`
	CreatedAt    time.Time `json:"createdAt"`
}
