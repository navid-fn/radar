package scraper

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"
)

func GenerateTradeID(kd *KafkaData) string {
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

func TurnTimeStampToTime(timestampMs int64, turnToIR bool) *time.Time {
	utcTime := time.UnixMilli(timestampMs).UTC()

	if !turnToIR {
		return &utcTime
	}

	loc, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		fmt.Printf("Error loading location: %v\n", err)
		return nil
	}
	tehranTime := utcTime.In(loc)
	return &tehranTime
}
