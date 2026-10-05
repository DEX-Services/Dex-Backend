package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/models"
	"github.com/dex/dex-backend/internal/repo"
)

// SipSwpServer handles SIP (Systematic Investment Plan, recurring Spot BUY)
// and SWP (Systematic Withdrawal Plan, recurring Spot SELL) plans —
// previously Dex New Frontend/src/pages/SIP.tsx was 100% frontend mock
// data with no backend at all. Embeds TradeServer rather than just holding
// a reference to it so RunDueExecutions (the background worker) can call
// TradeServer's own submitOrderReconciled — the exact same per-account
// slot + balance-drift-reconcile safety path a manual order goes through
// (see submitOrderReconciled's doc comment in trade.go).
type SipSwpServer struct {
	*Server
	Trade *TradeServer
	Plans *repo.SipSwpRepo
}

func (s *SipSwpServer) claims(w http.ResponseWriter, r *http.Request) (string, bool) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return "", false
	}
	return claims.UserID, true
}

func sipSwpErrorStatus(err error) int {
	switch {
	case errors.Is(err, repo.ErrSipSwpNotFound):
		return http.StatusNotFound
	case errors.Is(err, repo.ErrSipSwpNotActive):
		return http.StatusConflict
	case errors.Is(err, repo.ErrSipSwpInvalidKind),
		errors.Is(err, repo.ErrSipSwpInvalidFrequency),
		errors.Is(err, repo.ErrSipSwpInvalidDayOfPeriod),
		errors.Is(err, repo.ErrSipSwpInvalidAmount),
		errors.Is(err, repo.ErrSipSwpInvalidDates):
		return http.StatusBadRequest
	default:
		return http.StatusBadRequest
	}
}

type createSipSwpPlanRequest struct {
	Kind         string  `json:"kind"`
	Name         string  `json:"name"`
	Asset        string  `json:"asset"`
	QuoteAsset   string  `json:"quoteAsset"`
	AmountUsdRaw string  `json:"amountUsdRaw"`
	Frequency    string  `json:"frequency"`
	DayOfPeriod  *int    `json:"dayOfPeriod,omitempty"`
	StartDate    string  `json:"startDate"`
	EndDate      *string `json:"endDate,omitempty"`
}

// CreatePlan: POST /sip/plans
func (s *SipSwpServer) CreatePlan(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req createSipSwpPlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = fmt.Sprintf("%s Plan", strings.ToUpper(req.Kind))
	}
	plan, err := s.Plans.CreatePlan(r.Context(), userID, req.Kind, name, req.Asset, req.QuoteAsset, req.AmountUsdRaw, req.Frequency, req.DayOfPeriod, req.StartDate, req.EndDate)
	if err != nil {
		writeError(w, sipSwpErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"plan": plan})
}

// ListPlans: GET /sip/plans
func (s *SipSwpServer) ListPlans(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	plans, err := s.Plans.ListPlans(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load plans")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"plans": plans})
}

// GetPlan: GET /sip/plans/get?planId=...
func (s *SipSwpServer) GetPlan(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	planID := strings.TrimSpace(r.URL.Query().Get("planId"))
	if planID == "" {
		writeError(w, http.StatusBadRequest, "planId is required")
		return
	}
	plan, err := s.Plans.GetPlan(r.Context(), userID, planID)
	if err != nil {
		writeError(w, sipSwpErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"plan": plan})
}

// ListExecutions: GET /sip/plans/executions?planId=...&limit=...
func (s *SipSwpServer) ListExecutions(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	planID := strings.TrimSpace(r.URL.Query().Get("planId"))
	if planID == "" {
		writeError(w, http.StatusBadRequest, "planId is required")
		return
	}
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		fmt.Sscanf(v, "%d", &limit)
	}
	executions, err := s.Plans.ListExecutions(r.Context(), userID, planID, limit)
	if err != nil {
		writeError(w, sipSwpErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"executions": executions})
}

type planActionRequest struct {
	PlanID string `json:"planId"`
}

func (s *SipSwpServer) planAction(w http.ResponseWriter, r *http.Request, action func(context.Context, string, string) error) {
	if !requirePost(w, r) {
		return
	}
	userID, ok := s.claims(w, r)
	if !ok {
		return
	}
	var req planActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.PlanID) == "" {
		writeError(w, http.StatusBadRequest, "planId is required")
		return
	}
	if err := action(r.Context(), userID, req.PlanID); err != nil {
		writeError(w, sipSwpErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// PausePlan: POST /sip/plans/pause {planId}
func (s *SipSwpServer) PausePlan(w http.ResponseWriter, r *http.Request) {
	s.planAction(w, r, s.Plans.PausePlan)
}

// ResumePlan: POST /sip/plans/resume {planId}
func (s *SipSwpServer) ResumePlan(w http.ResponseWriter, r *http.Request) {
	s.planAction(w, r, s.Plans.ResumePlan)
}

// CancelPlan: POST /sip/plans/cancel {planId}
func (s *SipSwpServer) CancelPlan(w http.ResponseWriter, r *http.Request) {
	s.planAction(w, r, s.Plans.CancelPlan)
}

// ─── Background worker ──────────────────────────────────────────────────────

// sipSwpPollInterval: a plan's cadence is day-granularity ("the 5th of
// every month"), not a precise time-of-day, so the worker only needs to
// notice "today" has arrived for a plan sometime during that day — polling
// every 10 minutes is frequent enough for that and cheap enough to run
// forever, and means a brief server restart never causes a missed cycle
// (DuePlans just catches it on the next tick after restart).
const sipSwpPollInterval = 10 * time.Minute

// RunSipSwpWorker polls for due SIP/SWP plans and executes them, same
// ticker-loop shape as RunWithdrawalWatchdog (wallet.go) — blocks until ctx
// is done, intended to run in its own goroutine from cmd/server/main.go.
func (s *SipSwpServer) RunSipSwpWorker(ctx context.Context) {
	ticker := time.NewTicker(sipSwpPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.executeDuePlans(ctx)
		}
	}
}

func (s *SipSwpServer) executeDuePlans(ctx context.Context) {
	plans, err := s.Plans.DuePlans(ctx, time.Now())
	if err != nil {
		s.Log.Error("sip/swp worker: could not load due plans", "err", err)
		return
	}
	for _, plan := range plans {
		// One plan's failure never blocks another's — each cycle is fully
		// independent, same reasoning as BalancesByArea fetching each area
		// separately.
		s.executeOnePlan(ctx, plan)
	}
}

// spotMarketSymbol builds the Spot pair symbol this codebase's engine
// expects (e.g. "BI2X-BI2XUSD") from a plan's base/quote assets.
func spotMarketSymbol(asset, quoteAsset string) string {
	return asset + "-" + quoteAsset
}

// executeOnePlan attempts exactly one due cycle for plan, then
// unconditionally advances its scheduling state (completed, skipped, OR
// failed alike — the confirmed product decision: a failed/skipped cycle is
// never retried later the same day, it simply tries again next cycle).
// scheduledDate is plan.NextRunDate, parsed once by the caller — this is
// the date RecordExecution's UNIQUE(plan_id, scheduled_date) constraint
// keys on, so a crash-and-retry between this function's DuePlans read and
// its own completion can't double-execute the same cycle (the second
// attempt's RecordExecution simply fails that constraint and this function
// treats it as already handled).
func (s *SipSwpServer) executeOnePlan(ctx context.Context, plan models.SipSwpPlan) {
	scheduledDate, err := time.Parse("2006-01-02", plan.NextRunDate)
	if err != nil {
		s.Log.Error("sip/swp worker: invalid next_run_date", "planId", plan.ID, "nextRunDate", plan.NextRunDate, "err", err)
		return
	}

	in := repo.SipSwpExecutionInput{
		PlanID:        plan.ID,
		UserID:        plan.UserID,
		ScheduledDate: scheduledDate,
		AmountUsdRaw:  plan.AmountUsdRaw,
	}

	symbol := spotMarketSymbol(plan.Asset, plan.QuoteAsset)
	priceStr, priceErr := s.Trade.Engine.SpotMidPrice(ctx, symbol)
	var qty *big.Rat
	if priceErr == nil {
		price, ok := new(big.Rat).SetString(priceStr)
		if !ok || price.Sign() <= 0 {
			priceErr = fmt.Errorf("invalid price %q for %s", priceStr, symbol)
		} else {
			usd, ok := humanRatFromRaw(plan.AmountUsdRaw)
			if !ok {
				priceErr = fmt.Errorf("invalid amountUsdRaw %q", plan.AmountUsdRaw)
			} else {
				qty = new(big.Rat).Quo(usd, price)
			}
		}
	}

	var (
		status     = "completed"
		orderID    *string
		qtyStr     *string
		priceOut   *string
		skipReason *string
		usdDelta   = plan.AmountUsdRaw
	)

	if priceErr != nil {
		status = "skipped"
		reason := priceErr.Error()
		skipReason = &reason
		usdDelta = "0"
	} else {
		side := "BUY"
		if plan.Kind == "SWP" {
			side = "SELL"
			// SWP sells a fixed USD amount's worth of the base asset —
			// same qty-from-USD conversion as SIP's buy side (confirmed
			// product decision), just the opposite order side.
		}
		qtyDecimal := qty.FloatString(8)
		order, err := s.Trade.submitOrderReconciled(ctx, plan.UserID, engineclient.TradeOrder{
			AccountID: plan.UserID, Symbol: symbol, Market: "SPOT", Side: side, Type: "MARKET", Qty: qtyDecimal,
		})
		if err != nil {
			status = "skipped"
			reason := err.Error()
			skipReason = &reason
			usdDelta = "0"
		} else {
			orderID = &order.OrderID
			qtyStr = &qtyDecimal
			priceOut = &priceStr
		}
	}

	in.Status = status
	in.OrderID = orderID
	in.QtyRaw = qtyStr
	in.Price = priceOut
	in.SkipReason = skipReason

	tx, err := s.Plans.Begin(ctx)
	if err != nil {
		s.Log.Error("sip/swp worker: could not begin tx", "planId", plan.ID, "err", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := s.Plans.RecordExecution(ctx, tx, in); err != nil {
		if errors.Is(err, repo.ErrSipSwpExecutionAlreadyRecorded) {
			// Already handled by an earlier attempt (e.g. a previous tick
			// that crashed after RecordExecution but before
			// AdvancePlanAfterExecution/commit) — nothing left to do.
			return
		}
		s.Log.Error("sip/swp worker: could not record execution", "planId", plan.ID, "err", err)
		return
	}
	if err := s.Plans.AdvancePlanAfterExecution(ctx, tx, plan.ID, usdDelta); err != nil {
		s.Log.Error("sip/swp worker: could not advance plan", "planId", plan.ID, "err", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		s.Log.Error("sip/swp worker: could not commit execution", "planId", plan.ID, "err", err)
	}
}

// humanRatFromRaw parses a raw 6-decimal integer string (this platform's
// standard balance scale) into a big.Rat of its human-decimal value, e.g.
// "1000000" -> 1.
func humanRatFromRaw(raw string) (*big.Rat, bool) {
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return nil, false
	}
	return new(big.Rat).SetFrac(n, big.NewInt(1_000_000)), true
}
