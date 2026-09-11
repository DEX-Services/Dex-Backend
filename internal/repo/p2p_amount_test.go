package repo

import "testing"

func TestValidateP2PAmountAllowsRawBIUSDBPrecision(t *testing.T) {
	for _, raw := range []string{"1", "1000000", "10123456"} {
		if err := validateP2PAmount(raw); err != nil {
			t.Fatalf("validateP2PAmount(%q) returned %v", raw, err)
		}
	}
}
