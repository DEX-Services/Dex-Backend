package engineclient

import "testing"

// TestRawToHumanUnits is a regression test for a live incident: a real
// on-chain deposit's raw amount was passed straight to Credit with no
// conversion, inflating an account's engine-side balance by 1,000,000×
// (a real 31.00004-token deposit became 31,000,040 in the engine's
// in-memory ledger). See RawToHumanUnits' own doc comment for the full
// incident writeup.
func TestRawToHumanUnits(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "the actual incident amount", raw: "31000040", want: "31.00004"},
		{name: "whole number, no fractional part", raw: "71000000", want: "71"},
		{name: "smallest unit", raw: "1", want: "0.000001"},
		{name: "zero", raw: "0", want: "0"},
		{name: "large realistic deposit", raw: "1000000000000", want: "1000000"},
		{name: "invalid input", raw: "not-a-number", wantErr: true},
		{name: "empty input", raw: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RawToHumanUnits(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("RawToHumanUnits(%q) = %q, nil; want an error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("RawToHumanUnits(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("RawToHumanUnits(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}
