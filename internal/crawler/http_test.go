package crawler

import (
	"testing"
	"time"
)

func TestNewTradeTracker(t *testing.T) {
	tracker := NewTradeTracker()

	if tracker == nil {
		t.Error("Expected tracker to be initialized")
	}

	if tracker.seenTradeHashes == nil {
		t.Error("Expected seenTradeHashes map to be initialized")
	}
}

func TestTradeTrackerMarkAndCheck(t *testing.T) {
	tracker := NewTradeTracker()
	symbol := "BTCUSDT"
	tradeHash := "abc123"

	// Initially should not be processed
	if tracker.IsTradeProcessed(symbol, tradeHash) {
		t.Error("Trade should not be marked as processed initially")
	}

	// Mark as processed
	tracker.MarkTradeProcessed(symbol, tradeHash)

	// Should now be processed
	if !tracker.IsTradeProcessed(symbol, tradeHash) {
		t.Error("Trade should be marked as processed")
	}

	// Different hash should not be processed
	if tracker.IsTradeProcessed(symbol, "xyz789") {
		t.Error("Different trade hash should not be marked as processed")
	}
}

func TestCreateTradeHash(t *testing.T) {
	// Test that same inputs create same hash
	hash1 := CreateTradeHash(12345, "100.50", "1.5", "buy")
	hash2 := CreateTradeHash(12345, "100.50", "1.5", "buy")

	if hash1 != hash2 {
		t.Error("Same inputs should create same hash")
	}

	// Test that different inputs create different hash
	hash3 := CreateTradeHash(12346, "100.50", "1.5", "buy")

	if hash1 == hash3 {
		t.Error("Different inputs should create different hash")
	}
}

func TestNewIDTracker(t *testing.T) {
	tracker := NewIDTracker(1*time.Second, 30*time.Second, 1*time.Second)

	if tracker == nil {
		t.Error("Expected tracker to be initialized")
	}

	if tracker.lastSeenTradeID == nil {
		t.Error("Expected lastSeenTradeID map to be initialized")
	}
}

func TestIDTrackerGetAndUpdate(t *testing.T) {
	tracker := NewIDTracker(1*time.Second, 30*time.Second, 1*time.Second)
	symbol := "BTCUSDT"

	// Initially should be 0
	if id := tracker.GetLastSeenID(symbol); id != 0 {
		t.Errorf("Expected initial ID 0, got %d", id)
	}

	// Update ID
	tracker.UpdateLastSeenID(symbol, 100)

	// Should return updated ID
	if id := tracker.GetLastSeenID(symbol); id != 100 {
		t.Errorf("Expected ID 100, got %d", id)
	}

	// Update with smaller ID should not change
	tracker.UpdateLastSeenID(symbol, 50)

	if id := tracker.GetLastSeenID(symbol); id != 100 {
		t.Errorf("Expected ID to remain 100, got %d", id)
	}

	// Update with larger ID should change
	tracker.UpdateLastSeenID(symbol, 150)

	if id := tracker.GetLastSeenID(symbol); id != 150 {
		t.Errorf("Expected ID 150, got %d", id)
	}
}

func TestIDTrackerSleepDuration(t *testing.T) {
	minSleep := 1 * time.Second
	maxSleep := 30 * time.Second
	increment := 1 * time.Second

	tracker := NewIDTracker(minSleep, maxSleep, increment)
	symbol := "BTCUSDT"

	// Get default sleep
	defaultSleep := 5 * time.Second
	sleep := tracker.GetSleepDuration(symbol, defaultSleep)

	if sleep != defaultSleep {
		t.Errorf("Expected default sleep %v, got %v", defaultSleep, sleep)
	}

	// Increase sleep
	tracker.IncreaseSleep(symbol, increment, maxSleep, defaultSleep)
	sleep = tracker.GetSleepDuration(symbol, defaultSleep)

	if sleep != defaultSleep+increment {
		t.Errorf("Expected sleep %v, got %v", defaultSleep+increment, sleep)
	}

	// Decrease sleep
	tracker.DecreaseSleep(symbol, increment, minSleep, defaultSleep)
	sleep = tracker.GetSleepDuration(symbol, defaultSleep)

	if sleep != defaultSleep {
		t.Errorf("Expected sleep back to %v, got %v", defaultSleep, sleep)
	}
}

func TestDefaultHTTPConfig(t *testing.T) {
	baseURL := "https://api.example.com"
	requestsPerSecond := 10.0

	config := DefaultHTTPConfig(baseURL, requestsPerSecond)

	if config.BaseURL != baseURL {
		t.Errorf("Expected BaseURL '%s', got '%s'", baseURL, config.BaseURL)
	}

	if config.RateLimiter == nil {
		t.Error("Expected RateLimiter to be initialized")
	}

	if config.PollingDelay != 1*time.Second {
		t.Errorf("Expected PollingDelay 1s, got %v", config.PollingDelay)
	}
}








