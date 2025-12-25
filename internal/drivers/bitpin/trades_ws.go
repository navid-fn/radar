package bitpin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
)

const (
	BitpinAPIURL         = "https://api.bitpin.ir/api/v1/mkt/markets/"
	BitpinWsUrl          = "wss://centrifugo.bitpin.ir/connection/websocket"
	MaxSubsPerConnection = 100
)

type BitpinScraper struct {
	*scraper.BaseScraper
	wsWorker *scraper.BaseWebSocketWorker

	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewBitpinScraper(cfg *configs.Config) *BitpinScraper {
	config := scraper.NewConfig("bitpin", MaxSubsPerConnection, cfg)
	baseScraper := scraper.NewBaseScraper(config)

	bs := &BitpinScraper{
		BaseScraper: baseScraper,
		usdtPrice:   getLatestUSDTPrice(),
	}

	bs.Logger.Info("INFO", "USDT PRICE:", bs.usdtPrice)
	wsConfig := scraper.DefaultWebSocketConfig(BitpinWsUrl)
	wsConfig.DisableClientPing = true
	bs.wsWorker = scraper.NewBaseWebSocketWorker(wsConfig, bs.Logger, bs.SendToKafka)
	bs.wsWorker.SendToKafkaCtx = bs.SendToKafkaWithContext

	bs.wsWorker.OnConnect = func(conn *websocket.Conn) error {
		connectMsg := map[string]any{
			"id":      1,
			"connect": map[string]any{},
		}
		if err := bs.wsWorker.WriteJSON(conn, connectMsg); err != nil {
			return fmt.Errorf("failed to send connect message: %w", err)
		}
		return nil
	}

	bs.wsWorker.OnMessage = func(conn *websocket.Conn, message []byte) ([]byte, error) {
		var centrifugoMsg map[string]any
		if err := json.Unmarshal(message, &centrifugoMsg); err != nil {
			bs.Logger.Debug("Failed to parse message", "error", err)
			return nil, nil
		}

		// Handle Centrifugo pings - can be either {} (empty object) or {"ping": {}}
		if len(centrifugoMsg) == 0 || centrifugoMsg["ping"] != nil {
			pongMsg := map[string]any{}
			if err := bs.wsWorker.WriteJSON(conn, pongMsg); err != nil {
				bs.Logger.Error("Failed to send PONG", "error", err)
			}
			return nil, nil
		}

		if _, hasConnect := centrifugoMsg["connect"]; hasConnect {
			return nil, nil
		}

		if _, hasSubscribe := centrifugoMsg["subscribe"]; hasSubscribe {
			return nil, nil
		}

		if push, hasPush := centrifugoMsg["push"]; hasPush {
			pushData, ok := push.(map[string]any)
			if !ok {
				return nil, nil
			}

			channel, _ := pushData["channel"].(string)
			pub, _ := pushData["pub"].(map[string]any)
			if pub == nil {
				return nil, nil
			}

			dataField, _ := pub["data"].(map[string]any)
			if dataField == nil {
				return nil, nil
			}

			event, _ := dataField["event"].(string)
			if event != "matches_update" {
				return nil, nil
			}

			symbol := ""
			if len(channel) > 8 {
				symbol = channel[8:]
			}

			matchesData, _ := dataField["matches"].([]any)
			if matchesData == nil {
				return nil, nil
			}

			matchTime, _ := dataField["event_time"].(string)

			var messageToSend []scraper.KafkaData
			for _, m := range matchesData {
				match, ok := m.(map[string]any)
				if !ok {
					continue
				}

				id, _ := match["id"].(string)
				priceStr, _ := match["price"].(string)
				volumeStr, _ := match["base_amount"].(string)
				side, _ := match["side"].(string)

				volume, _ := strconv.ParseFloat(volumeStr, 64)
				price, _ := strconv.ParseFloat(priceStr, 64)
				cleanedSymbol := cleanSymbol(symbol)
				if cleanedSymbol == "USDT/IRT" {
					bs.usdtMu.Lock()
					bs.usdtPrice = price
					bs.usdtMu.Unlock()
				}

				data := scraper.KafkaData{
					ID:        id,
					Exchange:  "bitpin",
					Side:      side,
					Volume:    volume,
					Price:     price,
					Quantity:  volume * price,
					Symbol:    cleanedSymbol,
					Time:      matchTime,
					USDTPrice: bs.usdtPrice,
				}
				messageToSend = append(messageToSend, data)
			}

			if len(messageToSend) > 0 {
				protoData, err := scraper.SerializeProtoBatch(messageToSend)
				if err != nil {
					bs.Logger.Error("Failed to serialize kafka data", "error", err)
					return nil, nil
				}
				return protoData, nil
			}
		}

		return nil, nil
	}

	bs.wsWorker.OnSubscribe = func(conn *websocket.Conn, symbols []string) error {

		for i, symbol := range symbols {
			subscriptionMsg := map[string]any{
				"id": i + 2,
				"subscribe": map[string]any{
					"channel": fmt.Sprintf("matches:%s", symbol),
				},
			}
			if err := bs.wsWorker.WriteJSON(conn, subscriptionMsg); err != nil {
				return fmt.Errorf("failed to send subscription message for %s: %w", symbol, err)
			}
		}

		bs.Logger.Info("Subscribed to symbols", "count", len(symbols))
		return nil
	}

	return bs
}

func (bs *BitpinScraper) Name() string {
	return "bitpin"
}

func (bs *BitpinScraper) FetchMarkets() ([]string, error) {
	var markets []string

	resp, err := http.Get(BitpinAPIURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets from Bitpin API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading API response: %w", err)
	}

	var apiResponse []market
	if err = json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	for _, market := range apiResponse {
		if market.Tradeable {
			markets = append(markets, market.Symbol)
		}
	}

	sort.Strings(markets)

	bs.Logger.Info("Fetched unique markets from Bitpin API", "count", len(markets))
	return markets, nil
}

func (bs *BitpinScraper) Run(ctx context.Context) error {
	bs.Logger.Info("Starting Bitpin Crawler...")

	if err := bs.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer bs.CloseKafkaProducer()

	markets, err := bs.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	marketChunks := scraper.ChunkMarkets(markets, bs.Config.MaxSubsPerConnection)
	bs.Logger.Info("Divided markets into chunks",
		"total", len(markets),
		"chunks", len(marketChunks),
		"chunkSize", bs.Config.MaxSubsPerConnection)

	return scraper.RunWithGracefulShutdown(bs.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for i, chunk := range marketChunks {
			wg.Add(1)
			go bs.wsWorker.RunWorker(ctx, chunk, wg, "BitpinWorker")

			if i < len(marketChunks)-1 {
				time.Sleep(1 * time.Second)
			}
		}
	})
}
