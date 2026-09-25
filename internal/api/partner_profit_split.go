package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/dex/dex-backend/internal/repo"
)

// RunDailyPartnerProfitSplit is the background job body for the partner
// profit-sharing feature: once per UTC calendar day, it sums the PREVIOUS
// day's platform profit (ReferralRepo.FeeRevenueTotals' same revenue
// streams, but bounded to that one day — see PartnerRepo.ProfitPoolForDay)
// and splits it evenly across every partner_accounts row, recording one row
// per partner in partner_profit_splits.
//
// "Previous day" (not "today so far") is deliberate: a day isn't final
// until it's over, so splitting today's still-accumulating profit would
// give partners a number that changes if the job somehow ran again later —
// partner_profit_splits is meant to be a fixed, append-only ledger.
//
// Called on a fixed interval (hourly, mirroring RetryPropFirmProvisioning)
// from a ticker goroutine in cmd/server/main.go, not a precise
// midnight-only trigger — AlreadySplit makes every call after the first
// successful one for a given day a cheap no-op, so an hourly cadence is
// both simple and safe to restart/redeploy around.
func RunDailyPartnerProfitSplit(ctx context.Context, partners *repo.PartnerRepo, log *slog.Logger) {
	now := time.Now().UTC()
	yesterday := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).Add(-24 * time.Hour)

	already, err := partners.AlreadySplit(ctx, yesterday)
	if err != nil {
		log.Error("partner profit split: check already-split failed", "err", err, "day", yesterday)
		return
	}
	if already {
		return
	}

	totalRaw, err := partners.ProfitPoolForDay(ctx, yesterday)
	if err != nil {
		log.Error("partner profit split: compute profit pool failed", "err", err, "day", yesterday)
		return
	}

	if err := partners.RecordDailySplit(ctx, yesterday, totalRaw); err != nil {
		log.Error("partner profit split: record daily split failed", "err", err, "day", yesterday, "totalRaw", totalRaw)
		return
	}
	log.Info("partner profit split recorded", "day", yesterday, "totalRaw", totalRaw)
}
