package scraper

import (
	"google.golang.org/protobuf/proto"
	pb "nobitex/radar/internal/proto"
)

type KafkaData struct {
	ID        string  `json:"ID"`
	Exchange  string  `json:"exchange"`
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Volume    float64 `json:"volume"`
	Quantity  float64 `json:"quantity"`
	Side      string  `json:"side"`
	Time      string  `json:"time"`
	USDTPrice float64 `json:"usdt_price"`
}

func (kd *KafkaData) ToProto() *pb.TradeData {
	return &pb.TradeData{
		Id:        kd.ID,
		Exchange:  kd.Exchange,
		Symbol:    kd.Symbol,
		Price:     kd.Price,
		Volume:    kd.Volume,
		Quantity:  kd.Quantity,
		Side:      kd.Side,
		Time:      kd.Time,
		UsdtPrice: kd.USDTPrice,
	}
}

func FromProto(td *pb.TradeData) *KafkaData {
	return &KafkaData{
		ID:        td.Id,
		Exchange:  td.Exchange,
		Symbol:    td.Symbol,
		Price:     td.Price,
		Volume:    td.Volume,
		Quantity:  td.Quantity,
		Side:      td.Side,
		Time:      td.Time,
		USDTPrice: td.UsdtPrice,
	}
}

// SerializeProto serializes a single KafkaData to protobuf bytes
func SerializeProto(kd *KafkaData) ([]byte, error) {
	protoData := kd.ToProto()
	return proto.Marshal(protoData)
}

// SerializeProtoBatch serializes multiple KafkaData to protobuf bytes
func SerializeProtoBatch(kafkaDataList []KafkaData) ([]byte, error) {
	var trades []*pb.TradeData
	for i := range kafkaDataList {
		trades = append(trades, kafkaDataList[i].ToProto())
	}

	batch := &pb.TradeDataBatch{
		Trades: trades,
	}

	return proto.Marshal(batch)
}
