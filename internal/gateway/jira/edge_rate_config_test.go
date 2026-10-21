// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A fractional rate must never round up past what the operator configured:
// 0.2 req/s is one call every five seconds, not one a second. The GitHub,
// Bitbucket, Slack and Linear gateways scale to an integer per longer period
// and round down; Jira rounds up with math.Ceil.
func TestEdge_FractionalRateIsNotRoundedUp(t *testing.T) {
	cfg, err := buildRateLimitConfigFromFramework([]sdk.RateLimitConfig{{Group: "jira-rest", RequestsPerSecond: 0.2}})
	assert.NoError(t, err)
	rl := cfg.ResourceMap["jira-rest"]
	perSecond := float64(rl.Rate) / rl.Period.Seconds()
	assert.LessOrEqual(t, perSecond, 0.2, "configured 0.2 req/s, limiter allows %d per %v", rl.Rate, rl.Period)
	assert.Greater(t, rl.Rate, int64(0))
}
