package utils

import (
	"fmt"
	"time"
)

func TurnTimeStampToIR(timestampMs int64) *time.Time {
    utcTime := time.UnixMilli(timestampMs).UTC()
    
    loc, err := time.LoadLocation("Asia/Tehran")
    if err != nil {
        fmt.Printf("Error loading location: %v\n", err)
        return nil
    }
    tehranTime := utcTime.In(loc)
    return &tehranTime
}
