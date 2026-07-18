package tokenunits

import "fmt"

type Asset struct {
	Symbol   string
	Decimals int
	ScaleRaw string
}

var assets = map[string]Asset{
	"USDC": {Symbol: "USDC", Decimals: 6, ScaleRaw: "1000000"},
}

func Get(symbol string) (Asset, error) {
	asset, ok := assets[symbol]
	if !ok {
		return Asset{}, fmt.Errorf("unsupported token %q", symbol)
	}
	return asset, nil
}
