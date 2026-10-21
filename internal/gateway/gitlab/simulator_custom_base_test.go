// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// A simulated organization on a configured base other than gitlab.com, such as a
// self-managed instance, still routes onto the simulator root: the base path is kept
// and the host is dropped.
func TestSimulatorKeepsTheCustomBasePath(t *testing.T) {
	cfg := gateway.DefaultConfig()
	for i := range cfg.GitLab {
		if cfg.GitLab[i].Group == "gitlab-rest" {
			cfg.GitLab[i].BaseURL = "https://gitlab.example.com/api/v4"
		}
	}
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	gw, err := NewGitLabVendorGatewayForSimulator(&mockDoer{statusCode: 200}, "t", nil, nil, nil, "http://sim:8080")
	require.NoError(t, err)
	vr, err := gw.Factory(&connectorpb.GitLabListProjectsRequest{GroupId: 1, Page: 1, PerPage: 10})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(vr.URL(), "http://sim:8080/gitlab/api/v4/"), "got %s", vr.URL())
}
