// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slackgateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestScaleRateToIntegerPerPeriod proves the fractional-RPS scaling never
// rounds up past the caller's intended rate: math.Ceil(0.33) would silently
// become 1 req/s = 60/min instead of ~20/min and let real Slack 429s through.
func TestScaleRateToIntegerPerPeriod(t *testing.T) {
	tests := []struct {
		name       string
		reqsPerSec float64
		wantRate   int64
		wantPeriod time.Duration
	}{
		{
			name:       "slack Tier 2 fractional rate widens to an hourly window",
			reqsPerSec: 0.33,
			wantRate:   1188,
			wantPeriod: time.Hour,
		},
		{
			name:       "whole RPS keeps a one-second window",
			reqsPerSec: 20,
			wantRate:   20,
			wantPeriod: time.Second,
		},
		{
			name:       "zero is a misconfiguration and floors to a 1/s minimum",
			reqsPerSec: 0,
			wantRate:   1,
			wantPeriod: time.Second,
		},
		{
			name:       "negative is a misconfiguration and floors to a 1/s minimum",
			reqsPerSec: -5,
			wantRate:   1,
			wantPeriod: time.Second,
		},
		{
			// Below 1/3600, even Floor(rps*3600) rounds to 0, which the
			// function then bumps to a 1-per-hour floor so the limiter stays
			// usable — this is the one case that intentionally exceeds the
			// configured rate; it is exempted from the overrun check below.
			name:       "a rate below 1/3600 still floors to a usable 1-per-hour limiter",
			reqsPerSec: 0.0001,
			wantRate:   1,
			wantPeriod: time.Hour,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rate, period := scaleRateToIntegerPerPeriod(tt.reqsPerSec)
			require.Equal(t, tt.wantRate, rate)
			require.Equal(t, tt.wantPeriod, period)
			// The enforced effective rate must never exceed the configured
			// intent: rounding up would enforce 1/s (60/min) against a
			// ~20/min budget.
			// Exempt the sub-1/3600 floor case (see its own comment above).
			if tt.reqsPerSec >= 1.0/3600 {
				effectivePerSec := float64(rate) / period.Seconds()
				require.LessOrEqualf(t, effectivePerSec, tt.reqsPerSec+1e-9,
					"effective rate %.6f/s must not exceed the configured %.6f/s", effectivePerSec, tt.reqsPerSec)
			}
		})
	}
}
