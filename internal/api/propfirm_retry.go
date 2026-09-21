package api

import (
	"context"
	"log/slog"

	"github.com/dex/dex-backend/internal/models"
	"github.com/dex/dex-backend/internal/propfirmclient"
	"github.com/dex/dex-backend/internal/repo"
)

// PropFirmRetryMaxAttempts is how many provisioning retries a purchase gets
// before the background job gives up and refunds instead. A purchase's
// FIRST provisioning attempt happens synchronously inside the original
// POST /prop-firm/purchase request (PropFirmServer.Purchase); this job only
// ever retries a purchase that already failed that first attempt and was
// marked 'refund_needed'.
const PropFirmRetryMaxAttempts = 5

// RetryPropFirmProvisioning is the background job body for the
// retry-then-refund policy (PROP_FIRM_PLAN.md §3's previously-open
// "pending/retry/refund bookkeeping" gap): every purchase still in
// 'refund_needed' with fewer than PropFirmRetryMaxAttempts retries gets one
// more attempt at POST /internal/provision; every purchase that has already
// exhausted its retries gets refunded instead. Called on a fixed interval
// from a ticker goroutine in cmd/server/main.go, mirroring the existing
// ExpirePendingOrders pattern there.
//
// Each purchase is handled independently — one purchase's error is logged
// and does not stop the batch, since a single stuck row must never block
// every other trader's retry/refund from proceeding.
func RetryPropFirmProvisioning(ctx context.Context, purchases *repo.PropFirmPurchaseRepo, ledger *repo.LedgerRepo, propFirm *propfirmclient.Client, log *slog.Logger) {
	retryable, err := purchases.ListRefundNeeded(ctx, PropFirmRetryMaxAttempts, 50)
	if err != nil {
		log.Error("prop-firm retry: list refund_needed failed", "err", err)
	}
	for _, p := range retryable {
		if err := purchases.IncrementRetryAttempt(ctx, p.ID); err != nil {
			log.Error("prop-firm retry: increment retry attempt failed", "purchaseId", p.ID, "err", err)
			continue
		}
		result, err := propFirm.Provision(ctx, p.ID, p.PackageID, p.UserID)
		if err != nil {
			log.Warn("prop-firm retry: provisioning still failing", "purchaseId", p.ID, "attempt", p.RetryCount+1, "err", err)
			continue
		}
		if result.Status == "already_fulfilled" {
			// The original synchronous attempt must have actually succeeded
			// on the prop-firm side before this service ever saw the
			// response (e.g. the response was lost to a timeout after the
			// account was already created) — no plaintext password to
			// return here, but the purchase is not actually stuck, so mark
			// it resolved rather than continuing to retry a purchase that
			// already has a real account.
			if err := purchases.MarkFulfilled(ctx, p.ID, result.AccountID); err != nil {
				log.Error("prop-firm retry: mark fulfilled (already_fulfilled) failed", "purchaseId", p.ID, "err", err)
			}
			continue
		}
		if err := purchases.MarkFulfilled(ctx, p.ID, result.AccountID); err != nil {
			log.Error("prop-firm retry: mark fulfilled failed", "purchaseId", p.ID, "err", err)
			continue
		}
		log.Info("prop-firm retry: provisioning succeeded", "purchaseId", p.ID, "attempt", p.RetryCount+1)
	}

	exhausted, err := purchases.ListExhaustedRetries(ctx, PropFirmRetryMaxAttempts, 50)
	if err != nil {
		log.Error("prop-firm retry: list exhausted retries failed", "err", err)
		return
	}
	for _, p := range exhausted {
		refundPropFirmPurchase(ctx, purchases, ledger, p, log)
	}
}

// refundPropFirmPurchase credits p's price back to the buyer's real BI2XUSD
// wallet and marks the purchase 'refunded'. ClaimForRefund's atomic
// status-guarded UPDATE runs FIRST, before the wallet credit — if the claim
// doesn't succeed (already refunded, or raced by another pass), the credit
// is never issued, so this can never double-pay a refund even under
// concurrent retry-job runs.
func refundPropFirmPurchase(ctx context.Context, purchases *repo.PropFirmPurchaseRepo, ledger *repo.LedgerRepo, p models.PropFirmPurchase, log *slog.Logger) {
	claimed, err := purchases.ClaimForRefund(ctx, p.ID)
	if err != nil {
		log.Error("prop-firm refund: claim failed", "purchaseId", p.ID, "err", err)
		return
	}
	if !claimed {
		// Already refunded (or no longer in refund_needed) by a prior pass
		// or a manual admin action — never credit again.
		return
	}
	if err := ledger.CreditBalance(ctx, p.UserID, "BI2XUSD", p.PriceBI2XUSD); err != nil {
		// The purchase is now marked 'refunded' but the credit failed —
		// this is a real, serious inconsistency (the claim and the credit
		// are not in one transaction, since they cross a package boundary
		// the ledger's transaction helpers don't span). Logged loudly for
		// manual reconciliation; the alternative — crediting before
		// claiming — would risk a double-credit instead, which is worse
		// (paying a customer twice) than a rare, loud, manually-fixable
		// under-credit.
		log.Error("prop-firm refund: purchase marked refunded but wallet credit FAILED — manual reconciliation required", "purchaseId", p.ID, "userId", p.UserID, "amount", p.PriceBI2XUSD, "err", err)
		return
	}
	log.Info("prop-firm refund: credited buyer after exhausted provisioning retries", "purchaseId", p.ID, "userId", p.UserID, "amount", p.PriceBI2XUSD, "attempts", p.RetryCount)
}
