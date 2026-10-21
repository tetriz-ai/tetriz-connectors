// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
)

// TestBitbucketRESTBase_SimulatorRouting pins the simulator routing of the three
// direct-URL operations (fetch_pr_diffs, fetch_branches, fetch_repo_tree) build
// their base from bitbucketRESTBase, which returns the real Bitbucket base for a
// normal org and the simulator base (with the "/bitbucket/2.0" prefix) for an
// org in SimulatorOrgIDs — and bitbucketRESTHost tracks whichever base is chosen,
// so the paginated `next`-host guard accepts the configured host and rejects any
// other.
func TestBitbucketRESTBase_SimulatorRouting(t *testing.T) {
	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)

	const nonSimOrg = "00000000-0000-0000-0000-000000000000"
	require.False(t, cfg.IsSimulatorOrg(nonSimOrg), "guard: fixture org must not be a simulator org")

	// Non-simulator org → real Bitbucket base + host, unchanged.
	require.Equal(t, defaultBitbucketRESTBase, bitbucketRESTBase(nonSimOrg))
	require.Equal(t, defaultBitbucketRESTHost, bitbucketRESTHost(nonSimOrg))

	if len(cfg.SimulatorOrgIDs) == 0 {
		t.Fatal("the test gateway config routes no org to the simulator")
	}
	simOrg := cfg.SimulatorOrgIDs[0]

	// Simulator org → simulator base under the "/bitbucket/2.0" prefix.
	wantBase := cfg.SimulatorBaseURL + "/bitbucket/2.0"
	require.Equal(t, wantBase, bitbucketRESTBase(simOrg))

	// The host guard must follow the simulator's host, not api.bitbucket.org,
	// or a multi-page simulator response would be rejected mid-pagination.
	wantHost, err := url.Parse(cfg.SimulatorBaseURL)
	require.NoError(t, err)
	require.Equal(t, wantHost.Host, bitbucketRESTHost(simOrg))
	require.NotEqual(t, defaultBitbucketRESTHost, bitbucketRESTHost(simOrg),
		"simulator host must differ from the real host so the next-guard follows simulator pages")
}
