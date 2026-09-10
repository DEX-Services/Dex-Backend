package api

import "testing"

// TestHasLiveReservation is a regression test for a real, reproducible bug:
// reconcileOrderBalance used to correct drift (Engine.Debit/Credit) for any
// account, regardless of whether it had live reservations. risk.Ledger.Debit
// releases reservation "up to the debited amount" with no notion of which
// order that reservation belongs to — so a drift-correction Debit could
// silently release the engine's own reservation tracking for a DIFFERENT,
// currently-resting order at the exact moment the matching engine's
// independent settlement call (triggered by a different account's matching
// order) was relying on that same reservation being intact. The account's
// real Postgres lock is untouched by that Debit, so settlement's own
// locked-balance check then failed "insufficient locked ...", halting the
// entire symbol for every account — reproduced under real concurrent
// multi-account load with no restart or outage involved.
//
// The fix: never run the correction while the engine reports ANY live
// reservation for the asset. This is the pure decision function that guards
// that call in reconcileOrderBalance.
func TestHasLiveReservation(t *testing.T) {
	cases := []struct {
		name     string
		reserved string
		want     bool
	}{
		{"zero reservation allows correction", "0", false},
		{"positive reservation blocks correction", "100.5", true},
		{"tiny positive reservation still blocks correction", "0.000001", true},
		{"empty string treated as no reservation", "", false},
		{"malformed string treated as no reservation (fails open to allow correction)", "not-a-number", false},
		{"negative reservation (should never happen) does not block", "-5", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := hasLiveReservation(tc.reserved)
			if got != tc.want {
				t.Fatalf("hasLiveReservation(%q) = %v, want %v", tc.reserved, got, tc.want)
			}
		})
	}
}
