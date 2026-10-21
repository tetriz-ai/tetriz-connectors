// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// A simulated organization on a configured base other than api.atlassian.com still
// routes onto the simulator root: the base path is kept and the host is dropped.
func TestSimulatorKeepsTheCustomBasePath(t *testing.T) {
	cfg := gateway.DefaultConfig()
	for i := range cfg.Jira {
		if cfg.Jira[i].Group == "jira-rest" {
			cfg.Jira[i].BaseURL = "https://atlassian.example.com"
		}
	}
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	gw, err := NewJiraVendorGatewayForSimulator(&mockDoer{statusCode: 200}, "t", nil, "http://sim:8080")
	require.NoError(t, err)
	vr, err := gw.Factory(&connectorpb.JiraListProjectsRequest{CloudId: "c"})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(vr.URL(), "http://sim:8080/jira/ex/jira/c/"), "got %s", vr.URL())
}
