package models

import "time"

// PropFirmPurchase is one exchange-side record of a BitDX Prop Firm
// challenge purchase — prop_firm_purchases (db.ensurePropFirmPurchasesTable).
// PriceBI2XUSD is a raw integer string at the platform's standard
// balanceRawScale (6), same convention as every other balance field. This
// is entirely separate from the prop-firm backend's own pf_purchases row;
// see db.ensurePropFirmPurchasesTable's comment for why the two are kept
// distinct.
type PropFirmPurchase struct {
	ID                string    `json:"id"`
	UserID            string    `json:"userId"`
	PackageID         string    `json:"packageId"`
	PriceBI2XUSD      string    `json:"priceBi2xusd"`
	Status            string    `json:"status"`
	PropFirmAccountID *string   `json:"propFirmAccountId,omitempty"`
	FailReason        *string   `json:"failReason,omitempty"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}
