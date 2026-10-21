// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"
)

// observedCapStaleness is how long an observed vendor cap is applied after it
// was observed. Past it the configured rate applies, as for a group with no
// observation; the connectors re-stamp an unchanged cap daily.
const observedCapStaleness = 7 * 24 * time.Hour

// withObservedCaps scopes gw to the configured rate of each endpoint group,
// lowered to the hourly cap the connector recorded for the group in
// settings.vendor_rate_limits when that cap is lower and fresh. An observed
// cap never raises the configured rate. Settings with no usable observation
// leave gw as it is.
func withObservedCaps(ctx context.Context, gw sdk.VendorGateway, endpoints map[string]gateway.APIEndpointConfig, settings json.RawMessage, now time.Time) sdk.VendorGateway {
	var s struct {
		VendorRateLimits map[string]pacing.VendorRateLimitObservation `json:"vendor_rate_limits"`
	}
	if len(settings) == 0 {
		return gw
	}
	if err := json.Unmarshal(settings, &s); err != nil || len(s.VendorRateLimits) == 0 {
		return gw
	}
	groups := make([]string, 0, len(endpoints))
	for g := range endpoints {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	configs := make([]sdk.RateLimitConfig, 0, len(groups))
	lowered := false
	for _, g := range groups {
		ep := endpoints[g]
		rc := ep.ToRateLimitConfig()
		if obs, ok := s.VendorRateLimits[g]; ok && obs.PerHour > 0 {
			at, err := time.Parse(time.RFC3339, obs.ObservedAt)
			if err == nil && now.Sub(at) <= observedCapStaleness {
				if rps := float64(obs.PerHour) / 3600; rps < rc.RequestsPerSecond {
					rc.RequestsPerSecond = rps
					if rc.BurstSize > obs.PerHour {
						rc.BurstSize = obs.PerHour
					}
					lowered = true
					logger.Debug(ctx, "pacing a group at its observed vendor cap", zap.String("group", g), zap.Int("per_hour", obs.PerHour))
				}
			}
		}
		configs = append(configs, rc)
	}
	if !lowered {
		return gw
	}
	return gw.WithRateLimits(configs)
}
