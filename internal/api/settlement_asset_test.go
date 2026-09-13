package api

import "testing"

// TestSettlementAssetForOrder_Options is a regression test for a real bug:
// reconcileOrderBalance used to derive the settlement asset with a 2-part
// SplitN(symbol, "-", 2) split, which only works for spot/futures'
// BASE-QUOTE symbols. An option instrument's 5-part
// BASE-QUOTE-STRIKE-EXPIRY-TYPE symbol (e.g. "BTC-BI2XUSD-55000-20260917-CALL")
// split that way took "BI2XUSD-55000-20260917-CALL" as the asset — never a
// real balance column — so every options BUY order failed "unsupported
// asset" before ever reaching the engine. A SELL order happened to work by
// accident (parts[0], "BTC", is a real asset) which is why the bug wasn't
// caught by testing only one side.
func TestSettlementAssetForOrder_Options(t *testing.T) {
	cases := []struct {
		name   string
		symbol string
		market string
		side   string
		want   string
	}{
		{"call buy", "BTC-BI2XUSD-55000-20260917-CALL", "OPTIONS", "BUY", "BI2XUSD"},
		{"call sell (writer)", "BTC-BI2XUSD-55000-20260917-CALL", "OPTIONS", "SELL", "BI2XUSD"},
		{"put buy", "BTC-BI2XUSD-60000-20260917-PUT", "OPTIONS", "BUY", "BI2XUSD"},
		{"put sell (writer)", "BTC-BI2XUSD-60000-20260917-PUT", "OPTIONS", "SELL", "BI2XUSD"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := settlementAssetForOrder(tc.symbol, tc.market, tc.side)
			if !ok {
				t.Fatalf("settlementAssetForOrder(%q, %q, %q) returned ok=false", tc.symbol, tc.market, tc.side)
			}
			if got != tc.want {
				t.Fatalf("settlementAssetForOrder(%q, %q, %q) = %q, want %q", tc.symbol, tc.market, tc.side, got, tc.want)
			}
		})
	}
}

func TestSettlementAssetForOrder_SpotAndFutures(t *testing.T) {
	cases := []struct {
		name   string
		symbol string
		market string
		side   string
		want   string
	}{
		{"spot buy draws quote", "BTC-BI2XUSD", "SPOT", "BUY", "BI2XUSD"},
		{"spot sell draws base", "BTC-BI2XUSD", "SPOT", "SELL", "BTC"},
		{"futures buy draws quote", "BTC-BI2XUSD", "FUTURES", "BUY", "BI2XUSD"},
		{"futures sell ALSO draws quote (margin, not base)", "BTC-BI2XUSD", "FUTURES", "SELL", "BI2XUSD"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := settlementAssetForOrder(tc.symbol, tc.market, tc.side)
			if !ok || got != tc.want {
				t.Fatalf("settlementAssetForOrder(%q, %q, %q) = (%q, %v), want (%q, true)", tc.symbol, tc.market, tc.side, got, ok, tc.want)
			}
		})
	}
}

func TestSettlementAssetForOrder_MalformedSymbolFailsClosed(t *testing.T) {
	if _, ok := settlementAssetForOrder("notasymbol", "SPOT", "BUY"); ok {
		t.Fatal("expected ok=false for a symbol with no separator")
	}
	if _, ok := settlementAssetForOrder("BTC-BI2XUSD-1", "OPTIONS", "BUY"); ok {
		t.Fatal("expected ok=false for a too-short options symbol (fewer than 5 parts)")
	}
}
