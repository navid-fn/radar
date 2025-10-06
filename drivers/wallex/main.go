package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/navid-fn/radar/depth"
	"github.com/navid-fn/radar/trades"
	"github.com/sirupsen/logrus"
)

// Configuration constants
const (
	WallexAPIURL         = "https://api.wallex.ir/hector/web/v1/markets"
	DefaultKafkaBroker   = "localhost:9092"
	MaxSubsPerConnection = 40
)

// Market represents a trading market from the API
type Market struct {
	Symbol string `json:"symbol"`
}

// APIResponse represents the response from Wallex API
type APIResponse struct {
	Result struct {
		Markets []Market `json:"markets"`
	} `json:"result"`
}

// WallexProducer handles the main application logic
type WallexProducer struct {
	mode             string
	kafkaTopic       string
	kafkaProducer    *kafka.Producer
	logger           *logrus.Logger
	kafkaBroker      string
	throttleInterval time.Duration
}

// NewWallexProducer creates a new instance of WallexProducer
func NewWallexProducer(mode, kafkaTopic string, throttleInterval time.Duration) *WallexProducer {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = DefaultKafkaBroker
	}

	return &WallexProducer{
		mode:             mode,
		kafkaTopic:       kafkaTopic,
		logger:           logger,
		kafkaBroker:      kafkaBroker,
		throttleInterval: throttleInterval,
	}
}

// initKafkaProducer initializes the Kafka producer
func (wp *WallexProducer) initKafkaProducer() error {
	config := kafka.ConfigMap{
		"bootstrap.servers": wp.kafkaBroker,
	}

	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	wp.kafkaProducer = producer
	wp.logger.Info("Kafka Producer initialized successfully")
	return nil
}

// getMarkets fetches all trading markets from Wallex API
func (wp *WallexProducer) getMarkets() ([]string, error) {
	resp, err := http.Get(WallexAPIURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets from Wallex API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	var apiResponse APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("error decoding API response: %w", err)
	}

	markets := make([]string, len(apiResponse.Result.Markets))
	for i, market := range apiResponse.Result.Markets {
		markets[i] = market.Symbol
	}

	// Sort for consistent chunking
	sort.Strings(markets)

	wp.logger.Infof("Fetched %d unique markets from Wallex API", len(markets))
	return markets, nil
}

// chunkMarkets splits markets into smaller chunks
func (wp *WallexProducer) chunkMarkets(markets []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(markets); i += chunkSize {
		end := i + chunkSize
		if end > len(markets) {
			end = len(markets)
		}
		chunks = append(chunks, markets[i:end])
	}
	return chunks
}

// deliveryReport handles Kafka message delivery reports
func (wp *WallexProducer) deliveryReport() {
	go func() {
		for e := range wp.kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					wp.logger.Errorf("Message delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()
}

// Run starts the main application
func (wp *WallexProducer) Run() error {
	// Initialize Kafka producer
	if err := wp.initKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer wp.kafkaProducer.Close()

	// Start delivery report handler
	wp.deliveryReport()

	// Fetch markets
	markets, err := wp.getMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	// Chunk markets
	marketChunks := wp.chunkMarkets(markets, MaxSubsPerConnection)
	wp.logger.Infof("Divided %d markets into %d chunks of ~%d", len(markets), len(marketChunks), MaxSubsPerConnection)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		wp.logger.Info("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	// Start workers based on mode
	var wg sync.WaitGroup

	switch wp.mode {
	case "trades":
		wp.logger.Info("Starting in TRADES mode (real-time)")
		tradesWorker := trades.NewTradesWorker(wp.kafkaProducer, wp.logger, wp.kafkaTopic)
		for _, chunk := range marketChunks {
			wg.Add(1)
			go tradesWorker.Run(ctx, chunk, &wg)
		}

	case "depth":
		wp.logger.Infof("Starting in DEPTH mode (throttled: %v)", wp.throttleInterval)
		// For depth, we can subscribe to both sellDepth and buyDepth
		// For simplicity, let's use sellDepth (you can extend this to support both)
		depthWorker := depth.NewDepthWorker(wp.kafkaProducer, wp.logger, wp.kafkaTopic, "sellDepth", wp.throttleInterval)
		for _, chunk := range marketChunks {
			wg.Add(1)
			go depthWorker.Run(ctx, chunk, &wg)
		}

	default:
		return fmt.Errorf("invalid mode: %s (must be 'trades' or 'depth')", wp.mode)
	}

	wp.logger.Info("All workers started, waiting for completion...")
	wg.Wait()
	wp.logger.Info("All workers completed")

	return nil
}

func main() {
	var mode string
	var throttleSeconds int

	flag.StringVar(&mode, "mode", "trades", "Mode to run: 'trades' or 'depth'")
	flag.IntVar(&throttleSeconds, "throttle", 7, "Throttle interval in seconds for depth mode (default: 7)")
	flag.Parse()

	// Validate mode
	if mode != "trades" && mode != "depth" {
		fmt.Fprintf(os.Stderr, "Error: mode must be 'trades' or 'depth', got: %s\n", mode)
		flag.Usage()
		os.Exit(1)
	}

	// Determine Kafka topic based on mode
	var kafkaTopic string
	switch mode {
	case "trades":
		kafkaTopic = "radar_trades"
	case "depth":
		kafkaTopic = "radar_depths"
	}

	throttleInterval := time.Duration(throttleSeconds) * time.Second

	producer := NewWallexProducer(mode, kafkaTopic, throttleInterval)

	if err := producer.Run(); err != nil {
		producer.logger.Fatalf("Application failed: %v", err)
	}
}
