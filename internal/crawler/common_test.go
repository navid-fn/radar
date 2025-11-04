package crawler

import (
	"testing"
)

func TestNewConfig(t *testing.T) {
	config := NewConfig("testexchange", 10)

	if config.ExchangeName != "testexchange" {
		t.Errorf("Expected ExchangeName 'testexchange', got '%s'", config.ExchangeName)
	}

	if config.MaxSubsPerConnection != 10 {
		t.Errorf("Expected MaxSubsPerConnection 10, got %d", config.MaxSubsPerConnection)
	}

	if config.Logger == nil {
		t.Error("Expected Logger to be initialized")
	}

	// Test defaults
	if config.KafkaBroker == "" {
		t.Error("Expected KafkaBroker to have default value")
	}
}

func TestNewConfigDefaults(t *testing.T) {
	config := NewConfig("testexchange", 0)

	if config.KafkaTopic != DefaultKafkaTopic {
		t.Errorf("Expected default KafkaTopic '%s', got '%s'", DefaultKafkaTopic, config.KafkaTopic)
	}

	if config.MaxSubsPerConnection != MaxSubsPerConnection {
		t.Errorf("Expected default MaxSubsPerConnection %d, got %d", MaxSubsPerConnection, config.MaxSubsPerConnection)
	}
}

func TestChunkMarkets(t *testing.T) {
	markets := []string{"BTC", "ETH", "XRP", "ADA", "DOT", "LINK", "UNI"}

	tests := []struct {
		name      string
		chunkSize int
		expected  int
	}{
		{"Chunk by 2", 2, 4},
		{"Chunk by 3", 3, 3},
		{"Chunk by 5", 5, 2},
		{"Chunk by 10", 10, 1},
		{"Chunk by 1", 1, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := ChunkMarkets(markets, tt.chunkSize)
			if len(chunks) != tt.expected {
				t.Errorf("Expected %d chunks, got %d", tt.expected, len(chunks))
			}

			// Verify all markets are present
			totalMarkets := 0
			for _, chunk := range chunks {
				totalMarkets += len(chunk)
			}
			if totalMarkets != len(markets) {
				t.Errorf("Expected %d total markets, got %d", len(markets), totalMarkets)
			}
		})
	}
}

func TestNewLogger(t *testing.T) {
	logger := NewLogger()

	if logger == nil {
		t.Error("Expected logger to be initialized")
	}
}

func TestNewBaseCrawler(t *testing.T) {
	config := NewConfig("testexchange", 10)
	baseCrawler := NewBaseCrawler(config)

	if baseCrawler.Config != config {
		t.Error("Expected Config to be set")
	}

	if baseCrawler.Logger == nil {
		t.Error("Expected Logger to be initialized")
	}

	if baseCrawler.KafkaProducer != nil {
		t.Error("Expected KafkaProducer to be nil before initialization")
	}
}
