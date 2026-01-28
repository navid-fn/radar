package wallex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type WallexDepthWS struct {
	sender *scraper.Sender
	logger *slog.Logger
}

func NewWallexDepthScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *WallexDepthWS {
	return &WallexDepthWS{
		sender: scraper.NewSender(kafkaWriter, logger),
		logger: logger.With("scraper", "wallex-depth-ws"),
	}
}

func (w *WallexDepthWS) Name() string { return "wallex" }

func (w *WallexDepthWS) Run(ctx context.Context) error {
	markets, err := fetchMarkets(w.logger)
	if err != nil {
		return err
	}
	if len(markets) == 0 {
		return fmt.Errorf("no markets found")
	}

	chunks := scraper.ChunkSlice(markets, maxSymbolsPerConnDepth)
	scraper.RunWorkers(ctx, chunks, w.Name(), func() *scraper.WSClient {
		return w.createClient()
	}, w.logger)

	return nil
}

func (w *WallexDepthWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnSubscribe: w.onSubscribe,
		OnMessage:   w.onMessage,
	}
	return scraper.NewWSClient(config, handler, w.sender, w.logger)
}

func (w *WallexDepthWS) onSubscribe(conn *websocket.Conn, symbols []string) error {
	for _, sym := range symbols {
		msg := []any{"subscribe", map[string]string{"channel": sym + "@sellDepth"}}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}

		msg = []any{"subscribe", map[string]string{"channel": sym + "@buyDepth"}}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	w.logger.Info("Subscribed", "count", len(symbols))
	return nil
}

func (w *WallexDepthWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	msgStr := strings.TrimSpace(string(message))
	if strings.Contains(msgStr, `"sid":`) {
		return nil, nil
	}

	var raw []json.RawMessage
	if json.Unmarshal(message, &raw) != nil || len(raw) < 2 {
		return nil, nil
	}

	var channel string
	json.Unmarshal(raw[0], &channel)

	var data []map[string]float64
	if err := json.Unmarshal(raw[1], &data); err != nil {
		fmt.Println("Error parsing orders:", err)
		return nil, nil
	}

	parts := strings.Split(channel, "@")
	symbol := scraper.NormalizeSymbol("wallex", parts[0])
	side := "buy"
	if strings.Contains(parts[1], "sell") {
		side = "sell"
	}

	var depths []*pb.OrderLevel

	var bids []*pb.OrderLevel
	var asks []*pb.OrderLevel

	for _, b := range data {
		depths = append(depths, &pb.OrderLevel{Price: b["price"], Volume: b["quantity"]})
	}
	if side == "sell" {
		bids = depths
	} else {
		asks = depths
	}

	now := time.Now().UTC().Format(time.RFC3339)
	snapshot := &pb.OrderBookSnapshot{
		Id:         scraper.GenerateSnapShotID("wallex", symbol, now),
		Exchange:   "wallex",
		Symbol:     symbol,
		LastUpdate: now,
		Asks:       asks,
		Bids:       bids,
	}

	return proto.Marshal(snapshot)
}
