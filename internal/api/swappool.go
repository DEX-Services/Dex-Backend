package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

// SwapPoolStatus: GET /admin/swap-pool
// Returns every asset's current swappable/reserve balances (human-unit),
// for the admin swap-pool dashboard — mirrors BI2XAllocationTotals'
// "list current state" shape (admin.go).
func (s *AdminServer) SwapPoolStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	balances, err := s.Ledger.AllSwapPoolBalances(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load swap pool balances")
		return
	}
	type poolRow struct {
		Asset     string `json:"asset"`
		Swappable string `json:"swappable"`
		Reserve   string `json:"reserve"`
	}
	// Fixed, ordered asset list rather than iterating the map directly, so
	// the response has a stable row order the frontend can render without
	// its own sort.
	assets := []string{"USDT", "USDC"}
	rows := make([]poolRow, 0, len(assets))
	for _, asset := range assets {
		bal, ok := balances[asset]
		if !ok {
			continue
		}
		swappable, err := rawToHumanUnits(bal.SwappableRaw)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "convert swap pool balance: "+err.Error())
			return
		}
		reserve, err := rawToHumanUnits(bal.ReserveRaw)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "convert swap pool balance: "+err.Error())
			return
		}
		rows = append(rows, poolRow{Asset: asset, Swappable: swappable, Reserve: reserve})
	}
	writeJSON(w, http.StatusOK, map[string]any{"pools": rows})
}

type swapPoolTopUpRequest struct {
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
}

// AdminTopUpSwapPool: POST /admin/swap-pool/topup {asset, amount}
// Credits amount (human-unit) into asset's SWAPPABLE pool only. There is no
// "direction" field here, unlike AdjustUserBalance — this endpoint can only
// ever increase swappable_raw. It cannot move funds from reserve into
// swappable and cannot decrease either balance: admin manually adding fresh
// swap liquidity is the only operation this exposes, matching the product
// requirement that reserve is a one-way accumulation, never spent back out.
func (s *AdminServer) AdminTopUpSwapPool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	var req swapPoolTopUpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	asset := strings.ToUpper(strings.TrimSpace(req.Asset))
	if asset != "USDT" && asset != "USDC" {
		writeError(w, http.StatusBadRequest, "asset must be USDT or USDC")
		return
	}
	raw, err := toRawUnits(req.Amount)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// adminLoginID ("admin") is a fixed login constant, not a row in the
	// users table — swap_pool_entries.account_id has a REFERENCES users(id)
	// foreign key, so passing it through would violate that constraint.
	// Passed as empty here (repo.LedgerRepo.AdminTopUpSwapPool stores NULL
	// for an empty account_id via nullIfEmpty), same as every other
	// admin-triggered entry in this codebase that has no real user to
	// attribute the row to.
	if err := s.Ledger.AdminTopUpSwapPool(r.Context(), asset, raw, ""); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	swappableRaw, reserveRaw, err := s.Ledger.SwapPoolBalance(r.Context(), asset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "load swap pool balance: "+err.Error())
		return
	}
	swappable, err := rawToHumanUnits(swappableRaw)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "convert swap pool balance: "+err.Error())
		return
	}
	reserve, err := rawToHumanUnits(reserveRaw)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "convert swap pool balance: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"asset": asset, "swappable": swappable, "reserve": reserve})
}
