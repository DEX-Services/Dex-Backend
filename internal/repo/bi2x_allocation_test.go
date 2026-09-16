package repo

import (
	"context"
	"math/big"
	"testing"
	"time"
)

func mustBigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("not a valid integer: %q", s)
	}
	return n
}

// TestBI2XAllocationSeed confirms every expected category exists and that
// no category has ever gone negative. Deliberately does NOT assert exact
// starting quantities: other tests in this file (and any admin action
// against a shared test database) legitimately decrement these running
// totals, so only structural/invariant properties are checked here, not a
// point-in-time snapshot.
func TestBI2XAllocationSeed(t *testing.T) {
	pool := testPool(t)
	repo := NewBI2XAllocationRepo(pool)
	ctx := context.Background()

	totals, err := repo.CurrentTotals(ctx)
	if err != nil {
		t.Fatalf("current totals: %v", err)
	}
	wantCategories := []string{
		"Initial Burn", "Team Reserve", "Community", "Airdrop",
		"Marketing", "Treasury Reserve", "Initial Liquidity", "Staking Reward",
	}
	if len(totals) != len(wantCategories) {
		t.Fatalf("got %d categories, want %d", len(totals), len(wantCategories))
	}
	seen := map[string]bool{}
	for _, b := range totals {
		seen[b.Category] = true
		if mustBigInt(t, b.RemainingQty).Sign() < 0 {
			t.Fatalf("category %q has a negative remaining quantity: %s", b.Category, b.RemainingQty)
		}
	}
	for _, cat := range wantCategories {
		if !seen[cat] {
			t.Fatalf("expected category %q not present in totals", cat)
		}
	}
}

// TestAddHistoryEntry_DecrementsRunningTotal is a regression test for the
// core behavior requested: recording a distribution/burn against a category
// must both create a permanent history row AND reduce that category's
// current remaining quantity by the same amount, atomically.
func TestAddHistoryEntry_DecrementsRunningTotal(t *testing.T) {
	pool := testPool(t)
	repo := NewBI2XAllocationRepo(pool)
	ctx := context.Background()

	totalsBefore, err := repo.CurrentTotals(ctx)
	if err != nil {
		t.Fatalf("totals before: %v", err)
	}
	var before string
	for _, b := range totalsBefore {
		if b.Category == "Staking Reward" {
			before = b.RemainingQty
		}
	}
	if before == "" {
		t.Fatal("Staking Reward category not found")
	}

	entry, err := repo.AddHistoryEntry(ctx, "Staking Reward", "2000000", "test distribution", "admin", time.Now())
	if err != nil {
		t.Fatalf("add history entry: %v", err)
	}
	if entry.Category != "Staking Reward" || entry.AmountQty != "2000000" {
		t.Fatalf("unexpected entry: %+v", entry)
	}

	totalsAfter, err := repo.CurrentTotals(ctx)
	if err != nil {
		t.Fatalf("totals after: %v", err)
	}
	var after string
	for _, b := range totalsAfter {
		if b.Category == "Staking Reward" {
			after = b.RemainingQty
		}
	}
	beforeN, afterN := mustBigInt(t, before), mustBigInt(t, after)
	beforeN.Sub(beforeN, mustBigInt(t, "2000000"))
	if beforeN.Cmp(afterN) != 0 {
		t.Fatalf("remaining quantity after distribution = %s, want %s (before %s minus 2000000)", after, beforeN.String(), before)
	}

	history, err := repo.History(ctx, 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	found := false
	for _, h := range history {
		if h.ID == entry.ID {
			found = true
			if h.Note != "test distribution" || h.CreatedBy != "admin" {
				t.Fatalf("history entry fields = %+v, want note/createdBy preserved", h)
			}
		}
	}
	if !found {
		t.Fatal("recorded entry not present in History()")
	}
}

// TestAddHistoryEntry_RejectsAmountExceedingRemaining confirms a
// distribution can never exceed what's actually left in a category — the
// running total must never go negative.
func TestAddHistoryEntry_RejectsAmountExceedingRemaining(t *testing.T) {
	pool := testPool(t)
	repo := NewBI2XAllocationRepo(pool)
	ctx := context.Background()

	totals, err := repo.CurrentTotals(ctx)
	if err != nil {
		t.Fatalf("totals: %v", err)
	}
	var remaining string
	for _, b := range totals {
		if b.Category == "Airdrop" {
			remaining = b.RemainingQty
		}
	}
	tooMuch := mustBigInt(t, remaining)
	tooMuch.Add(tooMuch, mustBigInt(t, "1"))

	if _, err := repo.AddHistoryEntry(ctx, "Airdrop", tooMuch.String(), "", "admin", time.Now()); err == nil {
		t.Fatal("expected AddHistoryEntry to reject an amount exceeding the category's remaining quantity")
	}

	// Rejected: the running total must be unchanged.
	after, err := repo.CurrentTotals(ctx)
	if err != nil {
		t.Fatalf("totals after rejected attempt: %v", err)
	}
	for _, b := range after {
		if b.Category == "Airdrop" && b.RemainingQty != remaining {
			t.Fatalf("Airdrop remaining changed after a rejected distribution: was %s, now %s", remaining, b.RemainingQty)
		}
	}
}

// TestAddHistoryEntry_UnknownCategoryErrors confirms an unrecognized
// category is rejected rather than silently creating a new row.
func TestAddHistoryEntry_UnknownCategoryErrors(t *testing.T) {
	pool := testPool(t)
	repo := NewBI2XAllocationRepo(pool)
	ctx := context.Background()

	if _, err := repo.AddHistoryEntry(ctx, "Not A Real Category", "1", "", "admin", time.Now()); err == nil {
		t.Fatal("expected AddHistoryEntry to reject an unknown category")
	}
}
