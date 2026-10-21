// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// perHourFromPeriod derives the effective hourly cap a (rate, period) pair
// enforces — what the vendor's hourly budget actually sees.
func perHourFromPeriod(t *testing.T, rate int64, period time.Duration) int64 {
	t.Helper()
	switch period {
	case time.Second:
		return rate * 3600
	case time.Minute:
		return rate * 60
	case time.Hour:
		return rate
	default:
		t.Fatalf("unexpected period %v; test needs an explicit derivation rule", period)
		return 0
	}
}

// TestScaleRateToIntegerPerPeriod pins down the float→(rate, period) conversion
// the limiter relies on. The limiter is a sliding-window counter, so this pair
// fixes two things at once: the hourly total (must stay under the vendor cap)
// and the per-window burst (the window is the only burst bound there is).
func TestScaleRateToIntegerPerPeriod(t *testing.T) {
	tests := []struct {
		name       string
		rps        float64
		wantRate   int64
		wantPeriod time.Duration
		// derivedReqPerHour is the effective hourly cap once the limiter applies
		// (rate, period). Asserts the configured rate translates to the correct
		// long-window cap — this is what the vendor compares against.
		derivedReqPerHour int64
	}{
		{
			name:              "GitHub-style >=1 rps stays at 1s window",
			rps:               30,
			wantRate:          30,
			wantPeriod:        time.Second,
			derivedReqPerHour: 30 * 3600,
		},
		{
			name:              "exactly 1 rps stays at 1s window",
			rps:               1,
			wantRate:          1,
			wantPeriod:        time.Second,
			derivedReqPerHour: 3600,
		},
		{
			name:              "fractional rate scales into the per-minute window",
			rps:               0.5,
			wantRate:          30, // floor(0.5 * 60)
			wantPeriod:        time.Minute,
			derivedReqPerHour: 1800,
		},
		{
			// Premium Bitbucket cap 10000/hr -> 2.7777 rps. Floor at minute
			// granularity keeps the hourly total under the vendor cap while
			// also capping any single minute at 166.
			name:              "fractional >=1 rps (observed cap) stays under vendor cap",
			rps:               10000.0 / 3600.0,
			wantRate:          166, // floor(2.7778 * 60)
			wantPeriod:        time.Minute,
			derivedReqPerHour: 9960,
		},
		{
			// GitHub enterprise 12500/hr -> 3.47 rps.
			name:              "GitHub enterprise observed cap stays under vendor cap",
			rps:               12500.0 / 3600.0,
			wantRate:          208, // floor(3.4722 * 60)
			wantPeriod:        time.Minute,
			derivedReqPerHour: 12480,
		},
		{
			name:              "Bitbucket configured rate scales to a per-minute allowance",
			rps:               0.5556,
			wantRate:          33, // floor(0.5556 * 60)
			wantPeriod:        time.Minute,
			derivedReqPerHour: 1980,
		},
		{
			name:              "Floor avoids the float64 0.28 → 16.8 → 17 trap",
			rps:               0.28,
			wantRate:          16, // floor(0.28000000000000003 * 60) = 16
			wantPeriod:        time.Minute,
			derivedReqPerHour: 960,
		},
		{
			// Below 1 req/min the minute window would floor to zero and
			// disable the limiter, so the hour window is the only one that can
			// still represent the rate without rounding up.
			name:              "sub-per-minute rate widens back to the hour window",
			rps:               0.01,
			wantRate:          36, // floor(0.01 * 3600)
			wantPeriod:        time.Hour,
			derivedReqPerHour: 36,
		},
		{
			name:              "very small rate stays at 1 req/hr floor (limiter usable)",
			rps:               0.0001,
			wantRate:          1,
			wantPeriod:        time.Hour,
			derivedReqPerHour: 1,
		},
		{
			name:              "zero rate falls back to 1/sec (avoids divide-by-zero, keeps limiter usable)",
			rps:               0,
			wantRate:          1,
			wantPeriod:        time.Second,
			derivedReqPerHour: 3600,
		},
		{
			name:              "negative rate falls back to 1/sec (misconfig guard)",
			rps:               -5,
			wantRate:          1,
			wantPeriod:        time.Second,
			derivedReqPerHour: 3600,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotRate, gotPeriod := scaleRateToIntegerPerPeriod(tt.rps)
			assert.Equal(t, tt.wantRate, gotRate, "rate")
			assert.Equal(t, tt.wantPeriod, gotPeriod, "period")
			assert.Equal(t, tt.derivedReqPerHour, perHourFromPeriod(t, gotRate, gotPeriod), "derived req/hr")
		})
	}
}

// TestScaleRateNeverRoundsUp guards the one invariant the scaler owns: Floor
// must never enforce a rate ABOVE what the operator configured.
//
// Note this is deliberately not "stays under Bitbucket's 1000/hr cap" — the
// configured floor sits above that cap on purpose (see gateway/defaults.yaml),
// with the vendor's 429s bounding the overshoot. Asserting the vendor cap here
// would encode a policy the config explicitly rejects.
func TestScaleRateNeverRoundsUp(t *testing.T) {
	// Includes the value actually configured in gateway/defaults.yaml.
	for _, configured := range []float64{0.5556, 0.2777, 0.28, 0.5, 3.47, 10000.0 / 3600.0} {
		rate, period := scaleRateToIntegerPerPeriod(configured)
		perHour := perHourFromPeriod(t, rate, period)
		intended := int64(configured * 3600)
		assert.LessOrEqual(t, perHour, intended,
			"configured RPS %.4f enforces %d req/hr, above the %d/hr intended — Floor rounded up",
			configured, perHour, intended)
	}
}

// TestScaleRateBoundsSingleMinuteBurst pins the burst
// bound documented on scaleRateToIntegerPerPeriod: a fractional rate must land
// in a window of at most a minute, because the window width is the only thing
// capping what a single minute can carry.
func TestScaleRateBoundsSingleMinuteBurst(t *testing.T) {
	// A rate at or above 1 req/min must never be expressed over a window
	// wider than a minute, whatever hourly figure it was derived from.
	for _, rps := range []float64{0.2667, 0.2777, 0.28, 0.5, 0.5556, 10000.0 / 3600.0} {
		rate, period := scaleRateToIntegerPerPeriod(rps)
		assert.LessOrEqual(t, period, time.Minute,
			"rps %.4f scaled to a %v window; an hour-wide window would let the whole budget burst at once", rps, period)

		// Requests admissible in any single minute.
		var perMinute int64
		switch period {
		case time.Second:
			perMinute = rate * 60
		case time.Minute:
			perMinute = rate
		}
		assert.LessOrEqual(t, perMinute, int64(rps*60)+1,
			"rps %.4f admits %d requests in one minute", rps, perMinute)
	}
}
