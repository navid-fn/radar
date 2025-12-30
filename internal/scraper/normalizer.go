package scraper

import "strings"

// SymbolRule defines a transformation rule for normalizing exchange-specific symbols.
// Exchanges use different formats: BTCIRT, BTC_IRT, BTCTMN, etc.
// We normalize all to standard format: BTC/IRT, BTC/USDT
type SymbolRule struct {
	// Suffix is the exchange-specific suffix to match (e.g., "TMN", "_IRT")
	Suffix string

	// Replacement is the normalized suffix (e.g., "/IRT", "/USDT")
	Replacement string
}

// DefaultSymbolRules maps exchange names to their normalization rules.
// Add new exchanges here when implementing new scrapers.
var DefaultSymbolRules = map[string][]SymbolRule{
	"nobitex": {
		{Suffix: "IRT", Replacement: "/IRT"},
		{Suffix: "USDT", Replacement: "/USDT"},
	},
	"wallex": {
		{Suffix: "TMN", Replacement: "/IRT"}, // Wallex uses TMN for Toman
		{Suffix: "USDT", Replacement: "/USDT"},
	},
	"ramzinex": {
		{Suffix: "IRR", Replacement: "/IRT"}, // Ramzinex uses IRR for Rial
		{Suffix: "USDT", Replacement: "/USDT"},
	},
	"bitpin": {
		{Suffix: "_IRT", Replacement: "/IRT"}, // Bitpin uses underscore separator
		{Suffix: "_USDT", Replacement: "/USDT"},
	},
	"tabdeal": {
		{Suffix: "IRT", Replacement: "/IRT"},
		{Suffix: "USDT", Replacement: "/USDT"},
	},
}

// NormalizeSymbol converts an exchange-specific symbol to our standard format.
// Example: "BTCTMN" (wallex) -> "BTC/IRT"
// Example: "BTC_USDT" (bitpin) -> "BTC/USDT"
// Returns the original symbol if no matching rule is found.
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

// NormalizePrice adjusts prices for IRT (Iranian Toman) pairs.
// Iranian exchanges report prices in Rial (10x Toman), so we divide by 10.
// This ensures consistent Toman-based pricing across all exchanges.
func NormalizePrice(symbol string, price float64) float64 {
	if strings.HasSuffix(symbol, "/IRT") {
		return price / 10
	}
	return price
}
