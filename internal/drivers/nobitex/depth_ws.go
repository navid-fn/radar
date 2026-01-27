//	{
//	  "asks": [
//	    ["35077909990", "0.009433"],
//	    ["35078000000", "0.000274"],
//	    ["35078009660", "0.00057"]
//	  ],
//	  "bids": [
//	    ["35020080080", "0.185784"],
//	    ["35020070060", "0.086916"],
//	    ["35020030010", "0.000071"]
//	  ],
//	  "lastTradePrice": "35077909990",
//	  "lastUpdate": 1726581829816
//	}
package nobitex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type NobitexDepthWS struct {
	sender *scraper.Sender
	logger *slog.Logger
}

func NewNobitexDepthScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *NobitexDepthWS {
	return &NobitexDepthWS{
		sender: scraper.NewSender(kafkaWriter, logger),
		logger: logger.With("scraper", "nobitex-depth-ws"),
	}
}

func (n *NobitexDepthWS) Name() string { return "nobitex" }

func (n *NobitexDepthWS) Run(ctx context.Context) error {
	n.logger.Info("Starting Nobitex WebSocket scraper")
	markets, err := fetchMarkets(n.logger)

	if err != nil {
		return fmt.Errorf("fetch markets: %w", err)
	}
	if len(markets) == 0 {
		return fmt.Errorf("no markets found")
	}

	chunks := scraper.ChunkSlice(markets, maxSymbolsPerConn)
	scraper.RunWorkers(ctx, chunks, "nobitex", func() *scraper.WSClient {
		return n.createClient()
	}, n.logger)

	return nil
}

func (n *NobitexDepthWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnConnect:   n.onConnect,
		OnSubscribe: n.onSubscribe,
		OnMessage:   n.onMessage,
	}
	return scraper.NewWSClient(config, handler, n.sender, n.logger)
}

func (n *NobitexDepthWS) onConnect(conn *websocket.Conn) error {
	return conn.WriteJSON(map[string]any{"id": 1, "connect": map[string]any{}})
}

func (n *NobitexDepthWS) onSubscribe(conn *websocket.Conn, symbols []string) error {
	for _, sym := range symbols {
		msg := map[string]any{"id": 2, "subscribe": map[string]any{"channel": "public:orderbook-" + sym}}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	n.logger.Info("Subscribed", "count", len(symbols))
	return nil
}

func (n *NobitexDepthWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	var depthSnapshot *pb.OrderBookSnapshot
	scanner := bufio.NewScanner(bytes.NewReader(message))
	for scanner.Scan() {
		if snapshot := n.parseLine(conn, scanner.Bytes()); snapshot != nil {
			depthSnapshot = snapshot
		}
	}
	if depthSnapshot == nil {
		return nil, nil
	}
	return proto.Marshal(depthSnapshot)
}

func (n *NobitexDepthWS) parseLine(conn *websocket.Conn, line []byte) *pb.OrderBookSnapshot {
	if len(line) == 0 {
		return nil
	}

	var msg map[string]any
	if json.Unmarshal(line, &msg) != nil {
		return nil
	}

	if len(msg) == 0 {
		conn.WriteJSON(map[string]any{})
		return nil
	}
	if _, ok := msg["ping"]; ok {
		conn.WriteJSON(map[string]any{"pong": map[string]any{}})
		return nil
	}
	if _, ok := msg["connect"]; ok {
		return nil
	}
	if _, ok := msg["subscribe"]; ok {
		return nil
	}

	push, _ := msg["push"].(map[string]any)
	if push == nil {
		return nil
	}

	pub, _ := push["pub"].(map[string]any)
	if pub == nil {
		return nil
	}

	dataMap, _ := pub["data"].(map[string]any)
	if dataMap == nil {
		return nil
	}

	jsonBytes, err := json.Marshal(dataMap)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	var data depthResponse
	json.Unmarshal(jsonBytes, &data)

	channel, _ := push["channel"].(string)
	symbol := ""
	if len(channel) > 17 {
		symbol = channel[17:]
	}

	return n.createDepth(data, symbol)
}

func (n *NobitexDepthWS) createDepth(data depthResponse, symbol string) *pb.OrderBookSnapshot {
	lastUpdateInt := data.LastUpdate.(float64)
	lastUpdate := scraper.TimestampToRFC3339(int64(lastUpdateInt))
	cleanedSymbol := scraper.NormalizeSymbol("nobitex", symbol)

	asks := []*pb.OrderLevel{}
	for _, a := range data.Asks {
		price, _ := strconv.ParseFloat(a[0], 64)
		if price == 0 {
			continue
		}
		volume, _ := strconv.ParseFloat(a[0], 64)
		if volume == 0 {
			continue
		}

		asks = append(asks, &pb.OrderLevel{Price: price, Volume: volume})
	}

	bids := []*pb.OrderLevel{}
	for _, b := range data.Bids {
		price, _ := strconv.ParseFloat(b[0], 64)
		if price == 0 {
			continue
		}
		volume, _ := strconv.ParseFloat(b[0], 64)
		if volume == 0 {
			continue
		}
		bids = append(asks, &pb.OrderLevel{Price: price, Volume: volume})
	}
	return &pb.OrderBookSnapshot{
		Id:         scraper.GenerateSnapShotID("nobitex", cleanedSymbol, lastUpdate),
		LastUpdate: lastUpdate,
		Bids:       bids,
		Asks:       asks,
		Symbol:     cleanedSymbol,
		Exchange:   "nobitex",
	}
}
