package tokenunits

import "testing"

func TestUSDCMetadata(t *testing.T) {
	asset, err := Get("USDC")
	if err != nil {
		t.Fatal(err)
	}
	if asset.Decimals != 6 || asset.ScaleRaw != "1000000" {
		t.Fatalf("unexpected USDC metadata: %+v", asset)
	}
}
