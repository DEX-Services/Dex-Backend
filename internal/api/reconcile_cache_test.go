package api

import (
	"testing"
	"time"
)

// TestReconcileVerifiedCache covers the per-account+asset TTL cache added
// for PERFORMANCE-CODE-REVIEW-FINDINGS.md item #8: reconcileOrderBalance
// previously paid a full three-way round trip (two Postgres reads + one
// engine call) on every single order, even for an account trading in a fast
// burst with nothing ever actually out of sync. These are pure in-memory map
// operations, so no live Postgres/engine is needed to exercise them
// directly — reconcileOrderBalance itself (which does need both) is covered
// by the existing live-Postgres integration tests in ledger_test.go/
// trade_test.go, unaffected by this cache since it only ever adds an early
// return before those reads, never changes what they'd find.
func TestReconcileVerifiedCache(t *testing.T) {
	s := &TradeServer{}

	if s.reconcileRecentlyVerified("acct1", "USDC") {
		t.Fatal("a fresh TradeServer with no cache entries must report nothing as recently verified")
	}

	s.markReconcileVerified("acct1", "USDC")
	if !s.reconcileRecentlyVerified("acct1", "USDC") {
		t.Fatal("expected acct1/USDC to be recently verified immediately after marking")
	}

	// A different asset for the same account must not share the cache entry
	// — reconcileOrderBalance's whole job is per-asset drift, so caching
	// across assets would mask real drift in an asset that was never
	// actually checked.
	if s.reconcileRecentlyVerified("acct1", "USDT") {
		t.Fatal("a different asset for the same account must not be considered verified")
	}
	// A different account for the same asset must likewise not share the
	// entry.
	if s.reconcileRecentlyVerified("acct2", "USDC") {
		t.Fatal("a different account must not be considered verified")
	}

	// Asset lookup must be case-insensitive, since reconcileCacheKey
	// upper-cases it the same way settlementAssetForOrder's callers do.
	if !s.reconcileRecentlyVerified("acct1", "usdc") {
		t.Fatal("expected case-insensitive asset match against a previously marked entry")
	}

	// Invalidation must remove the entry outright, not just leave it to
	// expire — a real correction means the drift that just happened could
	// recur immediately, so the very next order must re-check from scratch.
	s.invalidateReconcileVerified("acct1", "USDC")
	if s.reconcileRecentlyVerified("acct1", "USDC") {
		t.Fatal("expected invalidateReconcileVerified to drop the cached entry immediately")
	}

	// Invalidating an entry that was never set must not panic (the nil-map
	// case — invalidateReconcileVerified can run before any
	// markReconcileVerified call ever populated the map).
	fresh := &TradeServer{}
	fresh.invalidateReconcileVerified("acct3", "USDC")
}

// TestReconcileVerifiedCache_TTLExpires confirms a cached "verified" entry
// stops being trusted once reconcileVerifiedTTL has elapsed, so a deposit or
// engine restart that happens after the cached check is still caught on the
// very next order past the TTL window — the cache only ever narrows the
// window in which reconciliation is skipped, it never disables it.
func TestReconcileVerifiedCache_TTLExpires(t *testing.T) {
	s := &TradeServer{
		reconcileVerified: map[string]time.Time{
			reconcileCacheKey("acct1", "USDC"): time.Now().Add(-reconcileVerifiedTTL - time.Second),
		},
	}
	if s.reconcileRecentlyVerified("acct1", "USDC") {
		t.Fatal("an entry older than reconcileVerifiedTTL must not be considered recently verified")
	}
}
