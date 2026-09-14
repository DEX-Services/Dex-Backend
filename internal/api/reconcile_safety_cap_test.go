package api

import (
	"math/big"
	"testing"
)

// TestReconcileDeltaExceedsSafetyCap is a regression test for a live
// incident: reconcileOrderBalance debited a user's entire real BI2X balance
// to zero as a "drift correction" — see reconcileDeltaExceedsSafetyCap's
// doc comment for the full incident context and the two earlier, related
// incidents already documented around reconcileOrderBalance itself.
func TestReconcileDeltaExceedsSafetyCap(t *testing.T) {
	rat := func(s string) *big.Rat {
		r, ok := new(big.Rat).SetString(s)
		if !ok {
			t.Fatalf("invalid test rational %q", s)
		}
		return r
	}

	cases := []struct {
		name           string
		dbAmount       string
		engineAmount   string
		delta          string // dbAmount - engineAmount, passed explicitly to mirror the real call site
		wantExceedsCap bool
	}{
		{
			name:     "tiny dust-scale delta on a large balance always allowed",
			dbAmount: "1000", engineAmount: "999.995", delta: "0.005",
			wantExceedsCap: false,
		},
		{
			name:     "dust floor itself (0.01) allowed, not treated as exceeding",
			dbAmount: "5", engineAmount: "4.99", delta: "0.01",
			wantExceedsCap: false,
		},
		{
			name:     "just over dust floor but still within 2% of balance allowed",
			dbAmount: "100", engineAmount: "98.5", delta: "1.5", // 1.5% of 100
			wantExceedsCap: false,
		},
		{
			name:     "delta at exactly 2% of balance allowed",
			dbAmount: "100", engineAmount: "98", delta: "2",
			wantExceedsCap: false,
		},
		{
			name:     "delta just over 2% of balance blocked",
			dbAmount: "100", engineAmount: "97.9", delta: "2.1",
			wantExceedsCap: true,
		},
		{
			name:     "the actual incident: whole balance wiped to zero is blocked",
			dbAmount: "0", engineAmount: "4.06936", delta: "-4.06936",
			wantExceedsCap: true,
		},
		{
			name:     "whole balance wiped the other direction (phantom credit) also blocked",
			dbAmount: "500", engineAmount: "0", delta: "500",
			wantExceedsCap: true,
		},
		{
			name:     "both sides zero, no delta — never reached in practice (caller returns early on delta==0), but must not exceed cap if it were",
			dbAmount: "0", engineAmount: "0", delta: "0",
			wantExceedsCap: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reconcileDeltaExceedsSafetyCap(rat(tc.dbAmount), rat(tc.engineAmount), rat(tc.delta))
			if got != tc.wantExceedsCap {
				t.Fatalf("reconcileDeltaExceedsSafetyCap(db=%s, engine=%s, delta=%s) = %v, want %v",
					tc.dbAmount, tc.engineAmount, tc.delta, got, tc.wantExceedsCap)
			}
		})
	}
}
