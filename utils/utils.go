package utils

import (
	"fmt"
	"time"
)

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
