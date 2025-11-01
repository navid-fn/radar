package coingecko

import (
	"testing"
)

func TestFilterUSDPairs(t *testing.T) {
	cgc := NewCoinGeckoCrawler()

	testCases := []struct {
		name     string
		tickers  []Ticker
		expected int
	}{
		{
			name: "Filter USDT pairs",
			tickers: []Ticker{
				{Base: "BTC", Target: "USDT"},
				{Base: "ETH", Target: "USDT"},
				{Base: "BTC", Target: "BNB"},
			},
			expected: 2,
		},
		{
			name: "Filter multiple USD currencies",
			tickers: []Ticker{
				{Base: "BTC", Target: "USDT"},
				{Base: "ETH", Target: "USDC"},
				{Base: "SOL", Target: "BUSD"},
				{Base: "ADA", Target: "BNB"},
			},
			expected: 3,
		},
		{
			name: "Case insensitive filtering",
			tickers: []Ticker{
				{Base: "BTC", Target: "usdt"},
				{Base: "ETH", Target: "USDT"},
				{Base: "SOL", Target: "Usdt"},
			},
			expected: 3,
		},
		{
			name:     "Empty input",
			tickers:  []Ticker{},
			expected: 0,
		},
		{
			name: "No USD pairs",
			tickers: []Ticker{
				{Base: "BTC", Target: "BNB"},
				{Base: "ETH", Target: "BTC"},
			},
			expected: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := cgc.filterUSDPairs(tc.tickers)
			if len(result) != tc.expected {
				t.Errorf("Expected %d USD pairs, got %d", tc.expected, len(result))
			}
		})
	}
}

func TestTickersToSnapshots(t *testing.T) {
	cgc := NewCoinGeckoCrawler()

	tickers := []Ticker{
		{
			Base:   "BTC",
			Target: "USDT",
			Volume: 1000.0,
			ConvertedVolume: ConvertedVolume{
				USD: 111000000.0,
			},
			ConvertedLast: ConvertedPrice{
				USD: 111000.0,
			},
			CoinID:           "bitcoin",
			TrustScore:       "green",
			BidAskSpread:     0.01,
			IsAnomaly:        false,
			IsStale:          false,
			CoinMarketCapUSD: 2000000000000.0,
		},
		{
			Base:   "ETH",
			Target: "USDT",
			Volume: 5000.0,
			ConvertedVolume: ConvertedVolume{
				USD: 20000000.0,
			},
			ConvertedLast: ConvertedPrice{
				USD: 4000.0,
			},
			CoinID:           "ethereum",
			TrustScore:       "green",
			BidAskSpread:     0.02,
			IsAnomaly:        false,
			IsStale:          false,
			CoinMarketCapUSD: 500000000000.0,
		},
	}

	snapshots := cgc.tickersToSnapshots(tickers)

	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots, got %d", len(snapshots))
	}

	btcSnapshot, exists := snapshots["BTC/USDT"]
	if !exists {
		t.Fatal("BTC/USDT snapshot not found")
	}

	if btcSnapshot.CoinID != "bitcoin" {
		t.Errorf("Expected coin_id 'bitcoin', got '%s'", btcSnapshot.CoinID)
	}

	if btcSnapshot.VolumeUSD != 111000000.0 {
		t.Errorf("Expected volume_usd 111000000.0, got %f", btcSnapshot.VolumeUSD)
	}

	if btcSnapshot.LastPriceUSD != 111000.0 {
		t.Errorf("Expected price 111000.0, got %f", btcSnapshot.LastPriceUSD)
	}

	if btcSnapshot.Exchange != "binance" {
		t.Errorf("Expected exchange 'binance', got '%s'", btcSnapshot.Exchange)
	}
}

func TestCalculateVolumeChanges(t *testing.T) {
	cgc := NewCoinGeckoCrawler()

	// Setup previous snapshots
	cgc.previousSnapshots = map[string]*VolumeSnapshot{
		"BTC/USDT": {
			PairID:       "BTC/USDT",
			CoinID:       "bitcoin",
			BaseSymbol:   "BTC",
			QuoteSymbol:  "USDT",
			VolumeUSD:    1000000.0,
			LastPriceUSD: 100000.0,
		},
		"ETH/USDT": {
			PairID:       "ETH/USDT",
			CoinID:       "ethereum",
			BaseSymbol:   "ETH",
			QuoteSymbol:  "USDT",
			VolumeUSD:    500000.0,
			LastPriceUSD: 4000.0,
		},
	}

	// Current snapshots with changes
	currentSnapshots := map[string]*VolumeSnapshot{
		"BTC/USDT": {
			PairID:       "BTC/USDT",
			CoinID:       "bitcoin",
			BaseSymbol:   "BTC",
			QuoteSymbol:  "USDT",
			VolumeUSD:    1200000.0, // +20%
			LastPriceUSD: 105000.0,
		},
		"ETH/USDT": {
			PairID:       "ETH/USDT",
			CoinID:       "ethereum",
			BaseSymbol:   "ETH",
			QuoteSymbol:  "USDT",
			VolumeUSD:    450000.0, // -10%
			LastPriceUSD: 3900.0,
		},
		"SOL/USDT": {
			PairID:       "SOL/USDT",
			CoinID:       "solana",
			BaseSymbol:   "SOL",
			QuoteSymbol:  "USDT",
			VolumeUSD:    300000.0, // New pair, no change calculated
			LastPriceUSD: 150.0,
		},
	}

	changes := cgc.calculateVolumeChanges(currentSnapshots)

	// Should have 2 changes (BTC and ETH), not SOL (new pair)
	if len(changes) != 2 {
		t.Errorf("Expected 2 volume changes, got %d", len(changes))
	}

	// Find BTC change
	var btcChange *VolumeChange
	for i := range changes {
		if changes[i].PairID == "BTC/USDT" {
			btcChange = &changes[i]
			break
		}
	}

	if btcChange == nil {
		t.Fatal("BTC/USDT change not found")
	}

	if btcChange.AbsoluteChangeUSD != 200000.0 {
		t.Errorf("Expected absolute change 200000.0, got %f", btcChange.AbsoluteChangeUSD)
	}

	if btcChange.PercentChange == nil {
		t.Fatal("Expected percent change to be non-nil")
	}

	expectedPercent := 20.0
	if *btcChange.PercentChange != expectedPercent {
		t.Errorf("Expected percent change %.2f%%, got %.2f%%", expectedPercent, *btcChange.PercentChange)
	}

	// Find ETH change
	var ethChange *VolumeChange
	for i := range changes {
		if changes[i].PairID == "ETH/USDT" {
			ethChange = &changes[i]
			break
		}
	}

	if ethChange == nil {
		t.Fatal("ETH/USDT change not found")
	}

	if ethChange.AbsoluteChangeUSD != -50000.0 {
		t.Errorf("Expected absolute change -50000.0, got %f", ethChange.AbsoluteChangeUSD)
	}

	if ethChange.PercentChange == nil {
		t.Fatal("Expected percent change to be non-nil")
	}

	expectedEthPercent := -10.0
	if *ethChange.PercentChange != expectedEthPercent {
		t.Errorf("Expected percent change %.2f%%, got %.2f%%", expectedEthPercent, *ethChange.PercentChange)
	}
}

func TestCalculateVolumeChangesZeroPrevious(t *testing.T) {
	cgc := NewCoinGeckoCrawler()

	// Previous volume is zero
	cgc.previousSnapshots = map[string]*VolumeSnapshot{
		"BTC/USDT": {
			PairID:       "BTC/USDT",
			VolumeUSD:    0.0,
			LastPriceUSD: 100000.0,
		},
	}

	currentSnapshots := map[string]*VolumeSnapshot{
		"BTC/USDT": {
			PairID:       "BTC/USDT",
			VolumeUSD:    1000000.0,
			LastPriceUSD: 105000.0,
		},
	}

	changes := cgc.calculateVolumeChanges(currentSnapshots)

	if len(changes) != 1 {
		t.Fatalf("Expected 1 volume change, got %d", len(changes))
	}

	change := changes[0]

	if change.AbsoluteChangeUSD != 1000000.0 {
		t.Errorf("Expected absolute change 1000000.0, got %f", change.AbsoluteChangeUSD)
	}

	// Percent change should be nil when previous volume is 0
	if change.PercentChange != nil {
		t.Errorf("Expected percent change to be nil for zero previous volume, got %f", *change.PercentChange)
	}
}

