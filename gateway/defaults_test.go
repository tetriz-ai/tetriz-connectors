// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The embedded defaults parse strictly and give every rate-limited vendor at
// least one endpoint with a positive rate.
func TestDefaultConfigCoversVendors(t *testing.T) {
	c := DefaultConfig()
	require.NotNil(t, c)
	for name, eps := range map[string][]APIEndpointConfig{
		"GitHub": c.GitHub, "GitLab": c.GitLab, "Bitbucket": c.Bitbucket, "Jira": c.Jira,
		"Slack": c.Slack, "Cursor": c.Cursor,
		"Anthropic": c.Anthropic, "Codex": c.Codex,
	} {
		require.NotEmpty(t, eps, name)
		for _, e := range eps {
			assert.Greater(t, e.RequestsPerSecond, 0.0, name+"/"+e.Group)
			assert.GreaterOrEqual(t, e.BurstSize, 1, name+"/"+e.Group)
		}
	}
}

func TestParseConfigRejectsUnknownFields(t *testing.T) {
	_, err := ParseConfig([]byte("GitHub:\n  - Group: x\n    RequestPerSecond: 1\n"))
	assert.Error(t, err)
}

// A rate of zero or below is rejected at load, naming the vendor and group.
func TestParseConfigRejectsNonPositiveRate(t *testing.T) {
	for _, rate := range []string{"0", "-1"} {
		_, err := ParseConfig([]byte("Jira:\n  - Group: jira-rest\n    BaseURL: https://api.atlassian.com\n    RequestsPerSecond: " + rate + "\n    BurstSize: 1\n"))
		if assert.Error(t, err, rate) {
			assert.Contains(t, err.Error(), `Jira group "jira-rest": RequestsPerSecond must be above 0`)
		}
	}
}
