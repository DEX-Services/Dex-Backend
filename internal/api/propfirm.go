package api

import (
	"encoding/json"
	"net/http"

	"github.com/dex/dex-backend/internal/propfirmclient"
	"github.com/dex/dex-backend/internal/repo"
)

// PropFirmServer implements POST /prop-firm/purchase — the one integration
// point between this exchange and the standalone BitDX Prop Firm backend
// (PROP_FIRM_PLAN.md section 3). It debits the buyer's real BI2XUSD wallet,
// records the purchase, and calls the prop-firm backend's
// POST /internal/provision server-to-server, returning real, one-time
// credentials to the caller — replacing the exchange frontend's previous
// "Simulate verified payment" placeholder.
type PropFirmServer struct {
	*Server
	Ledger    *repo.LedgerRepo
	Purchases *repo.PropFirmPurchaseRepo
	PropFirm  *propfirmclient.Client
}

// packageCatalog is the exact (track, size) -> (packageId, priceBi2xusd)
// table the prop-firm backend itself seeds (see PropFirm Backend's
// internal/db/seed.go catalogPrices/accountSizes) and that
// Dex New Frontend/src/lib/propFirmPlans.ts mirrors for display. It is
// duplicated here, not fetched live, so a purchase's price is always
// validated against a value THIS service trusts, never one the browser
// sends or a value read from a service this handler is about to pay for
// provisioning from — a client-supplied price would let anyone buy a
// $100,000 account for the price of a $5,000 one.
type packageInfo struct {
	packageID    string
	priceBI2XUSD string // human-decimal string, e.g. "239" for $239
}

var packageCatalog = map[string]map[int]packageInfo{
	"1step": {
		5000:   {"1step-5000", "69"},
		10000:  {"1step-10000", "119"},
		25000:  {"1step-25000", "239"},
		50000:  {"1step-50000", "399"},
		100000: {"1step-100000", "749"},
	},
	"2step": {
		5000:   {"2step-5000", "59"},
		10000:  {"2step-10000", "109"},
		25000:  {"2step-25000", "219"},
		50000:  {"2step-50000", "349"},
		100000: {"2step-100000", "699"},
	},
	"instant": {
		5000:   {"instant-5000", "399"},
		10000:  {"instant-10000", "799"},
		25000:  {"instant-25000", "1999"},
		50000:  {"instant-50000", "3999"},
		100000: {"instant-100000", "7999"},
	},
}

type propFirmPurchaseRequest struct {
	// Track is "1step" | "2step" | "instant" — matches the prop-firm
	// backend's own track values exactly, not the exchange frontend's
	// display labels ("One-Step"/"Two-Step"/"Instant Funding"); the
	// frontend maps its labels to this before calling.
	Track       string `json:"track"`
	AccountSize int    `json:"accountSize"`
}

// Purchase handles POST /prop-firm/purchase.
func (s *PropFirmServer) Purchase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if s.PropFirm == nil || !s.PropFirm.Enabled() {
		writeError(w, http.StatusServiceUnavailable, "prop-firm purchases are not available right now")
		return
	}

	var req propFirmPurchaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	sizes, ok := packageCatalog[req.Track]
	if !ok {
		writeError(w, http.StatusBadRequest, "unknown track")
		return
	}
	pkg, ok := sizes[req.AccountSize]
	if !ok {
		writeError(w, http.StatusBadRequest, "unknown account size for this track")
		return
	}

	ctx := r.Context()

	// priceRaw is the raw integer (6-decimal scale) amount the ledger
	// actually debits — see engineclient.RawUnitScale's doc comment for the
	// exact incident class this scale mismatch caused before; priceBI2XUSD
	// here is a whole-dollar human amount (e.g. "239"), so appending 6
	// zeros is exact, not an approximation.
	priceRaw := pkg.priceBI2XUSD + "000000"

	purchase, err := s.Purchases.Create(ctx, claims.UserID, pkg.packageID, priceRaw)
	if err != nil {
		s.Log.Error("prop-firm purchase record failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to record purchase")
		return
	}

	if err := s.Ledger.DebitBalance(ctx, claims.UserID, "BI2XUSD", priceRaw); err != nil {
		// No refund bookkeeping needed here: the debit itself never
		// happened, so there is nothing to reverse. The purchase row stays
		// 'pending' — a distinct case from MarkRefundNeeded below (debit
		// succeeded, provisioning didn't).
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := s.PropFirm.Provision(ctx, purchase.ID, pkg.packageID, claims.UserID)
	if err != nil {
		s.Log.Error("prop-firm provisioning failed after debit", "purchaseId", purchase.ID, "err", err)
		if markErr := s.Purchases.MarkRefundNeeded(ctx, purchase.ID, err.Error()); markErr != nil {
			s.Log.Error("failed to mark prop-firm purchase refund_needed", "purchaseId", purchase.ID, "err", markErr)
		}
		writeError(w, http.StatusBadGateway, "payment was taken but account setup failed — contact support for a refund, reference: "+purchase.ID)
		return
	}

	if err := s.Purchases.MarkFulfilled(ctx, purchase.ID, result.AccountID); err != nil {
		s.Log.Error("failed to mark prop-firm purchase fulfilled", "purchaseId", purchase.ID, "err", err)
	}

	if result.Status == "already_fulfilled" {
		// Should not happen on a fresh purchase (a new purchase.ID is a
		// fresh externalRef every time), but if it ever does, there is no
		// plaintext password to return — surface that honestly rather than
		// silently returning empty credentials.
		writeError(w, http.StatusConflict, "this purchase was already provisioned; contact support for your credentials")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"purchaseId":        purchase.ID,
		"propFirmAccountId": result.AccountID,
		"username":          result.Username,
		"password":          result.Password,
	})
}
