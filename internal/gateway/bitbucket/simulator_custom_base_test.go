// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// A simulated organization on a configured base other than api.bitbucket.org still
// routes onto the simulator root: the base path is kept and the host is dropped.
func TestSimulatorKeepsTheCustomBasePath(t *testing.T) {
	cfg := gateway.DefaultConfig()
	for i := range cfg.Bitbucket {
		if cfg.Bitbucket[i].Group == "bitbucket-rest" {
			cfg.Bitbucket[i].BaseURL = "https://bitbucket.example.com/2.0"
		}
	}
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	gw, err := NewBitbucketVendorGatewayForSimulator(&mockDoer{statusCode: 200}, "t", nil, nil, nil, "http://sim:8080")
	require.NoError(t, err)
	vr, err := gw.Factory(&connectorpb.BitbucketListReposRequest{Workspace: "ws"})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(vr.URL(), "http://sim:8080/bitbucket/2.0/"), "got %s", vr.URL())
}
