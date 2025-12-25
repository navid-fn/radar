package ingester

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/models"
	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"
	"nobitex/radar/internal/storage"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type Ingester struct {
	reader       *kafka.Reader
	TradeStorage storage.TradeStorage
	workerCount  int
	batchSize    int
	batchTimeout time.Duration
	messagesChan chan kafka.Message
	wg           sync.WaitGroup
	logger       *slog.Logger
}

func NewIngester(cfg *configs.Config, tradeStorage storage.TradeStorage) *Ingester {
	kafkaConfig := cfg.KafkaConfigs
	reader := kafka.NewReader(*configs.GetKafkaReaderConfig(&kafkaConfig))

	return &Ingester{
		reader:       reader,
		TradeStorage: tradeStorage,
		workerCount:  cfg.KafkaConfigs.WorkerCount,
		batchSize:    cfg.KafkaConfigs.BatchSize,
		batchTimeout: time.Duration(cfg.KafkaConfigs.BatchTimeoutSeconds) * time.Second,
		messagesChan: make(chan kafka.Message, cfg.KafkaConfigs.WorkerCount*2),
		logger:       cfg.Logger,
	}
}

func (ig *Ingester) Start(ctx context.Context) error {
	ig.logger.Info("Starting Kafka consumer",
		"workers", ig.workerCount,
		"topic", ig.reader.Config().Topic,
		"groupID", ig.reader.Config().GroupID,
		"batchSize", ig.batchSize,
		"batchTimeout", ig.batchTimeout)

	for i := range ig.workerCount {
		ig.wg.Add(1)
		go ig.worker(ctx, i+1)
	}

	go ig.readMessages(ctx)

	<-ctx.Done()
	ig.logger.Info("Shutting down consumer...")

	close(ig.messagesChan)

	ig.wg.Wait()

	if err := ig.reader.Close(); err != nil {
		ig.logger.Error("Error closing reader", "error", err)
		return err
	}

	ig.logger.Info("Kafka consumer shut down cleanly")
	return nil
}

func (ig *Ingester) readMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := ig.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				ig.logger.Error("Error fetching message", "error", err)
				continue
			}

			select {
			case ig.messagesChan <- m:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (ig *Ingester) worker(ctx context.Context, workerID int) {
	defer ig.wg.Done()

	ig.logger.Info("Worker started",
		"workerID", workerID,
		"batchSize", ig.batchSize,
		"timeout", ig.batchTimeout)

	batch := make([]*models.Trade, 0, ig.batchSize)
	messagesToCommit := make([]kafka.Message, 0, ig.batchSize)
	ticker := time.NewTicker(ig.batchTimeout)
	defer ticker.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		if err := ig.TradeStorage.CreateTrades(batch); err != nil {
			ig.logger.Error("Error creating batch",
				"workerID", workerID,
				"batchSize", len(batch),
				"error", err)
		} else {
			if err := ig.reader.CommitMessages(ctx, messagesToCommit...); err != nil {
				ig.logger.Error("Error committing messages", "workerID", workerID, "error", err)
			} else {
				ig.logger.Info("Successfully inserted batch",
					"workerID", workerID,
					"batchSize", len(batch))
			}
		}

		batch = batch[:0]
		messagesToCommit = messagesToCommit[:0]
		ticker.Reset(ig.batchTimeout)
	}

	for {
		select {
		case msg, ok := <-ig.messagesChan:
			if !ok {
				flushBatch()
				ig.logger.Info("Worker stopped", "workerID", workerID)
				return
			}

			trades, err := ig.parseMessage(msg)
			if err != nil {
				ig.logger.Error("Error parsing message",
					"workerID", workerID,
					"offset", msg.Offset,
					"error", err)
				continue
			}

			batch = append(batch, trades...)
			messagesToCommit = append(messagesToCommit, msg)

			if len(batch) >= ig.batchSize {
				flushBatch()
			}

		case <-ticker.C:
			if len(batch) > 0 {
				ig.logger.Info("Flushing partial batch due to timeout",
					"workerID", workerID,
					"batchSize", len(batch))
				flushBatch()
			}

		case <-ctx.Done():
			flushBatch()
			ig.logger.Info("Worker stopped", "workerID", workerID)
			return
		}
	}
}

func (c *Ingester) parseMessage(msg kafka.Message) ([]*models.Trade, error) {
	// Try single message first (most common case)
	if trades, err := c.parseProtobufSingle(msg.Value); err == nil {
		return trades, nil
	}

	// Try batch message
	if trades, err := c.parseProtobufBatch(msg.Value); err == nil {
		return trades, nil
	}

	return nil, fmt.Errorf("failed to parse message")
}

func (ig *Ingester) parseProtobufBatch(data []byte) ([]*models.Trade, error) {
	var batch pb.TradeDataBatch
	if err := proto.Unmarshal(data, &batch); err != nil {
		return nil, err
	}

	if len(batch.Trades) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	// Validate first trade to ensure it's not garbage data
	if !isValidTradeData(batch.Trades[0]) {
		return nil, fmt.Errorf("invalid batch data")
	}

	var trades []*models.Trade
	for _, td := range batch.Trades {
		kd := scraper.FromProto(td)
		trade, err := ig.transformKafkaDataToTrade(kd)
		if err != nil {
			ig.logger.Error("Error transforming trade", "error", err)
			continue
		}
		trades = append(trades, trade)
	}

	if len(trades) == 0 {
		return nil, fmt.Errorf("no valid trades")
	}

	return trades, nil
}

func (ig *Ingester) parseProtobufSingle(data []byte) ([]*models.Trade, error) {
	var singleTrade pb.TradeData
	if err := proto.Unmarshal(data, &singleTrade); err != nil {
		return nil, err
	}

	if !isValidTradeData(&singleTrade) {
		return nil, fmt.Errorf("invalid trade data")
	}

	kd := scraper.FromProto(&singleTrade)
	trade, err := ig.transformKafkaDataToTrade(kd)
	if err != nil {
		return nil, err
	}

	return []*models.Trade{trade}, nil
}

func isValidTradeData(td *pb.TradeData) bool {
	if td == nil {
		return false
	}
	if td.Id == "" {
		return false
	}
	if td.Exchange == "" {
		return false
	}
	if td.Symbol == "" {
		return false
	}
	if td.Price <= 0 || td.Price > 1e15 {
		return false
	}
	if td.Volume <= 0 || td.Volume > 1e15 {
		return false
	}
	return true
}

func (ig *Ingester) transformKafkaDataToTrade(kd *scraper.KafkaData) (*models.Trade, error) {
	eventTime, err := ig.parseTime(kd.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to parse time '%s': %w", kd.Time, err)
	}

	trade := &models.Trade{
		TradeID:     kd.ID,
		Source:      kd.Exchange,
		Symbol:      kd.Symbol,
		Side:        kd.Side,
		Price:       kd.Price,
		BaseAmount:  kd.Volume,
		QuoteAmount: kd.Quantity,
		EventTime:   eventTime,
		InsertedAt:  time.Now(),
		USDTPrice:   kd.USDTPrice,
	}

	return trade, nil
}

func (c *Ingester) parseTime(timeStr string) (time.Time, error) {
	timeFormats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	}

	for _, format := range timeFormats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse time with any known format")
}
