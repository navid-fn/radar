package scraper

import "strings"

// SymbolRule defines how to normalize a symbol suffix
type SymbolRule struct {
	Suffix      string
	Replacement string
}

// DefaultRules for common exchanges
var DefaultSymbolRules = map[string][]SymbolRule{
	"nobitex": {
		{Suffix: "IRT", Replacement: "/IRT"},
		{Suffix: "USDT", Replacement: "/USDT"},
	},
	"wallex": {
		{Suffix: "TMN", Replacement: "/IRT"},
		{Suffix: "USDT", Replacement: "/USDT"},
	},
	"ramzinex": {
		{Suffix: "IRR", Replacement: "/IRT"},
		{Suffix: "USDT", Replacement: "/USDT"},
	},
	"bitpin": {
		{Suffix: "_IRT", Replacement: "/IRT"},
		{Suffix: "_USDT", Replacement: "/USDT"},
	},
	"tabdeal": {
		{Suffix: "IRT", Replacement: "/IRT"},
		{Suffix: "USDT", Replacement: "/USDT"},
	},
}

// NormalizeSymbol cleans a symbol based on exchange-specific rules
func NormalizeSymbol(exchange, symbol string) string {
	rules, ok := DefaultSymbolRules[exchange]
	if !ok {
		return symbol
	}

	for _, rule := range rules {
		if strings.HasSuffix(symbol, rule.Suffix) {
			base := strings.TrimSuffix(symbol, rule.Suffix)
			return base + rule.Replacement
		}
	}

	return symbol
}

// NormalizePrice adjusts price for IRT pairs (divide by 10 for Rial to Toman)
func NormalizePrice(symbol string, price float64) float64 {
	if strings.HasSuffix(symbol, "/IRT") {
		return price / 10
	}
	return price
}
