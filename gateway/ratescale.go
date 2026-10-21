// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gateway

import (
	"math"
	"time"
)

// ScaleRateToIntegerPerPeriod converts a requests-per-second rate into the
// (integer rate, window) pair the sliding-window limiter takes, never rounding
// up past reqsPerSec. A whole rate keeps a 1-second window; a fractional rate
// uses Floor(rps*60) per minute, which bounds the burst a wide window would
// allow, and below one call a minute Floor(rps*3600) per hour. It is the
// algorithm of the Bitbucket gateway's private copy.
func ScaleRateToIntegerPerPeriod(reqsPerSec float64) (int64, time.Duration) {
	if !(reqsPerSec > 0) || math.IsInf(reqsPerSec, 0) {
		// Config validation rejects these; keep the limiter usable regardless.
		return 1, time.Second
	}
	if reqsPerSec == math.Floor(reqsPerSec) {
		return int64(reqsPerSec), time.Second
	}
	if perMinute := int64(math.Floor(reqsPerSec * 60)); perMinute >= 1 {
		return perMinute, time.Minute
	}
	perHour := int64(math.Floor(reqsPerSec * 3600))
	if perHour < 1 {
		perHour = 1
	}
	return perHour, time.Hour
}
