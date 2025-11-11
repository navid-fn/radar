package consumer

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"nobitex/radar/internal/consumer/config"
	"nobitex/radar/internal/crawler"
	"nobitex/radar/internal/model"
	"nobitex/radar/internal/repository"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader       *kafka.Reader
	repo         repository.TradeRepository
	workerCount  int
	batchSize    int
	batchTimeout time.Duration
	messagesChan chan kafka.Message
	logger       *slog.Logger
	wg           sync.WaitGroup
}

func NewConsumer(cfg *config.Config, repo repository.TradeRepository, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(cfg.KafkaConfig)

	return &Consumer{
		reader:       reader,
		repo:         repo,
		workerCount:  cfg.WorkerCount,
		batchSize:    cfg.BatchSize,
		batchTimeout: time.Duration(cfg.BatchTimeoutSeconds) * time.Second,
		messagesChan: make(chan kafka.Message, cfg.WorkerCount*2),
		logger:       logger,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Kafka consumer",
		"workers", c.workerCount,
		"topic", c.reader.Config().Topic,
		"groupID", c.reader.Config().GroupID,
		"batchSize", c.batchSize,
		"batchTimeout", c.batchTimeout)

	// Start worker pool
	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i+1)
	}

	go c.readMessages(ctx)

	<-ctx.Done()
	c.logger.Info("Shutting down consumer...")

	close(c.messagesChan)

	c.wg.Wait()

	if err := c.reader.Close(); err != nil {
		c.logger.Error("Error closing reader", "error", err)
		return err
	}

	c.logger.Info("Kafka consumer shut down cleanly")
	return nil
}

func (c *Consumer) readMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Error("Error fetching message", "error", err)
				continue
			}

			select {
			case c.messagesChan <- m:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *Consumer) worker(ctx context.Context, workerID int) {
	defer c.wg.Done()

	c.logger.Info("Worker started",
		"workerID", workerID,
		"batchSize", c.batchSize,
		"timeout", c.batchTimeout)

	batch := make([]*model.Trade, 0, c.batchSize)
	messagesToCommit := make([]kafka.Message, 0, c.batchSize)
	ticker := time.NewTicker(c.batchTimeout)
	defer ticker.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		if err := c.repo.CreateTrades(batch); err != nil {
			c.logger.Error("Error creating batch",
				"workerID", workerID,
				"batchSize", len(batch),
				"error", err)
		} else {
			if err := c.reader.CommitMessages(ctx, messagesToCommit...); err != nil {
				c.logger.Error("Error committing messages", "workerID", workerID, "error", err)
			} else {
				c.logger.Info("Successfully inserted batch",
					"workerID", workerID,
					"batchSize", len(batch))
			}
		}

		batch = batch[:0]
		messagesToCommit = messagesToCommit[:0]
		ticker.Reset(c.batchTimeout)
	}

	for {
		select {
		case msg, ok := <-c.messagesChan:
			if !ok {
				flushBatch()
				c.logger.Info("Worker stopped", "workerID", workerID)
				return
			}

			trades, err := c.parseMessage(msg)
			if err != nil {
				c.logger.Error("Error parsing message",
					"workerID", workerID,
					"offset", msg.Offset,
					"error", err)
				continue
			}

			batch = append(batch, trades...)
			messagesToCommit = append(messagesToCommit, msg)

			if len(batch) >= c.batchSize {
				flushBatch()
			}

		case <-ticker.C:
			if len(batch) > 0 {
				c.logger.Info("Flushing partial batch due to timeout",
					"workerID", workerID,
					"batchSize", len(batch))
				flushBatch()
			}

		case <-ctx.Done():
			flushBatch()
			c.logger.Info("Worker stopped", "workerID", workerID)
			return
		}
	}
}

func (c *Consumer) parseMessage(msg kafka.Message) ([]*model.Trade, error) {
	var trades []*model.Trade

	var kafkaDataArray []crawler.KafkaData
	if err := json.Unmarshal(msg.Value, &kafkaDataArray); err == nil && len(kafkaDataArray) > 0 {
		for _, kd := range kafkaDataArray {
			trade, err := c.transformKafkaDataToTrade(kd)
			if err != nil {
				c.logger.Error("Error transforming KafkaData to Trade", "error", err, "data", kd)
				continue
			}
			trades = append(trades, trade)
		}
		if len(trades) > 0 {
			return trades, nil
		}
		return nil, fmt.Errorf("no valid trades found in array")
	}

	var kafkaData crawler.KafkaData
	if err := json.Unmarshal(msg.Value, &kafkaData); err != nil {
		return nil, fmt.Errorf("failed to parse message as KafkaData: %w", err)
	}

	trade, err := c.transformKafkaDataToTrade(kafkaData)
	if err != nil {
		return nil, err
	}

	trades = append(trades, trade)
	return trades, nil
}

func (c *Consumer) transformKafkaDataToTrade(kd crawler.KafkaData) (*model.Trade, error) {
	eventTime, err := c.parseTime(kd.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to parse time '%s': %w", kd.Time, err)
	}

	tradeID := kd.ID
	if tradeID == "" {
		tradeID = c.generateTradeID(kd)
	}

	trade := &model.Trade{
		TradeID:     tradeID,
		Source:      kd.Exchange,
		Symbol:      kd.Symbol,
		Side:        kd.Side,
		Price:       kd.Price,
		BaseAmount:  kd.Volume,
		QuoteAmount: kd.Quantity,
		EventTime:   eventTime,
		InsertedAt:  time.Now(),
	}

	return trade, nil
}

func (c *Consumer) parseTime(timeStr string) (time.Time, error) {
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

func (c *Consumer) generateTradeID(kd crawler.KafkaData) string {
	uniqueString := fmt.Sprintf("%s-%s-%s-%f-%f-%s",
		kd.Exchange,
		kd.Symbol,
		kd.Time,
		kd.Price,
		kd.Volume,
		kd.Side,
	)

	hash := sha1.Sum([]byte(uniqueString))
	return hex.EncodeToString(hash[:])
}
