// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
)

func TestGitLabRESTBase_SimulatorRouting(t *testing.T) {
	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)

	const nonSimOrg = "00000000-0000-0000-0000-000000000000"
	require.False(t, cfg.IsSimulatorOrg(nonSimOrg), "guard: fixture org must not be a simulator org")

	require.Equal(t, defaultGitLabRESTBase, gitlabRESTBase(nonSimOrg))
	parsed, err := url.Parse(gitlabRESTBase(nonSimOrg))
	require.NoError(t, err)
	require.Equal(t, "gitlab.com", parsed.Host)

	if len(cfg.SimulatorOrgIDs) == 0 {
		t.Fatal("the test gateway config routes no org to the simulator")
	}
	simOrg := cfg.SimulatorOrgIDs[0]

	wantBase := cfg.SimulatorBaseURL + "/gitlab/api/v4"
	require.Equal(t, wantBase, gitlabRESTBase(simOrg))
	simParsed, err := url.Parse(gitlabRESTBase(simOrg))
	require.NoError(t, err)
	require.NotEqual(t, "gitlab.com", simParsed.Host,
		"simulator host must not stay pinned at gitlab.com")
}
