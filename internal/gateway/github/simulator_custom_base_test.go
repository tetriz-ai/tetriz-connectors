// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ghgateway

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// A simulated organization on a configured base other than api.github.com, such as
// a GitHub Enterprise server, still routes onto the simulator root: the base path
// is kept and the host is dropped.
func TestSimulatorKeepsTheCustomBasePath(t *testing.T) {
	cfg := gateway.DefaultConfig()
	for i := range cfg.GitHub {
		if cfg.GitHub[i].Group == "github-rest" {
			cfg.GitHub[i].BaseURL = "https://ghe.example.com/api/v3"
		}
	}
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	gw, err := NewGitHubVendorGatewayForSimulator(&stubHTTPDoer{inner: http.DefaultClient}, "t", nil, nil, "http://sim:8080")
	require.NoError(t, err)
	vr, err := gw.Factory(&connectorpb.ListOrgReposRequest{Organization: "acme"})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(vr.URL(), "http://sim:8080/github/api/v3/"), "got %s", vr.URL())
}
