package models

import "time"

// SipSwpPlan is one user's recurring Spot investment (SIP, buy) or
// withdrawal (SWP, sell) plan — see db.ensureSipSwpTables and
// repo.SipSwpRepo. AmountUsdRaw/TotalUsdRaw are raw integer strings at the
// platform's standard balanceRawScale (6), same convention as every other
// balance figure in this API; the actual token quantity bought/sold each
// cycle is computed at execution time from the live price, not stored here
// (see SipSwpExecution.QtyRaw for what a specific cycle actually traded).
type SipSwpPlan struct {
	ID                  string    `json:"id"`
	Kind                string    `json:"kind"` // "SIP" or "SWP"
	Name                string    `json:"name"`
	Asset               string    `json:"asset"`        // base asset bought (SIP) or sold (SWP), e.g. "BI2X"
	QuoteAsset          string    `json:"quoteAsset"`   // cash asset debited (SIP) or credited (SWP), e.g. "BI2XUSD"
	AmountUsdRaw        string    `json:"amountUsdRaw"` // per-cycle USD amount
	Frequency           string    `json:"frequency"`    // DAILY | WEEKLY | MONTHLY | YEARLY
	DayOfPeriod         *int      `json:"dayOfPeriod,omitempty"`
	StartDate           string    `json:"startDate"` // YYYY-MM-DD
	EndDate             *string   `json:"endDate,omitempty"`
	Status              string    `json:"status"` // active | paused | cancelled | completed
	NextRunDate         string    `json:"nextRunDate"`
	ExecutionsCompleted int       `json:"executionsCompleted"`
	TotalUsdRaw         string    `json:"totalUsdRaw"`
	CreatedAt           time.Time `json:"createdAt"`
	UpdatedAt           time.Time `json:"updatedAt"`
}

// SipSwpExecution is one permanent record of a plan's scheduled cycle,
// whether it actually traded or not — see SipSwpPlan's doc comment and
// repo.SipSwpRepo.RecordExecution. ScheduledDate is the date this cycle was
// DUE (the worker's idempotency key alongside PlanID), not necessarily the
// timestamp it actually ran — CreatedAt is that.
type SipSwpExecution struct {
	ID            string    `json:"id"`
	PlanID        string    `json:"planId"`
	ScheduledDate string    `json:"scheduledDate"`
	Status        string    `json:"status"` // completed | skipped | failed
	OrderID       *string   `json:"orderId,omitempty"`
	AmountUsdRaw  string    `json:"amountUsdRaw"`
	QtyRaw        *string   `json:"qtyRaw,omitempty"`
	Price         *string   `json:"price,omitempty"`
	SkipReason    *string   `json:"skipReason,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
}
