// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// memUpdater is the runner's ConfigUpdater over the memory store.
type memUpdater struct{ m *memstore.Store }

func (u memUpdater) UpdateSettings(ctx context.Context, org string, id sdk.ConnectorID, s json.RawMessage) error {
	cfg, err := u.m.Get(ctx, org, id)
	if err != nil {
		return err
	}
	cfg.Settings = s
	return u.m.Save(ctx, cfg)
}

func (u memUpdater) PatchSettings(ctx context.Context, org string, id sdk.ConnectorID, fn func(json.RawMessage) (json.RawMessage, error)) error {
	return u.m.PatchSettings(ctx, org, id, fn)
}

// An operator's settings change made while a run is in flight survives the
// run's end-of-run writes: they patch only the keys the operation owns.
func TestEndOfRunWritesKeepAConcurrentSettingsChange(t *testing.T) {
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-000000000001"
	mr := miniredis.RunT(t)
	bl := ratelimiter.NewBudgetLimiter(redis.NewClient(&redis.Options{Addr: mr.Addr()}), ratelimiter.GitHubAdapter{})
	require.NoError(t, bl.UpdateFromResponse(ctx, org, http.Header{"X-Ratelimit-Remaining": {"4500"}, "X-Ratelimit-Limit": {"5000"},
		"X-Ratelimit-Reset": {strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)}, "X-Ratelimit-Resource": {"core"}}))

	m := memstore.New()
	start, _ := json.Marshal(GitHubSettings{Organization: "old-org"})
	require.NoError(t, m.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github", Settings: start, Enabled: true}))
	snapshot, err := m.Get(ctx, org, "github") // what the run loaded
	require.NoError(t, err)

	// While the run is in flight, the operator changes the organization.
	require.NoError(t, m.PatchSettings(ctx, org, "github", func(json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"organization":"new-org","operator_key":"kept"}`), nil
	}))

	params := &sdk.ExecutionParams{Config: snapshot, ConfigUpdater: memUpdater{m}, BudgetLimiter: bl}
	persistObservedRateLimitCap(ctx, params, ratelimiter.BucketGitHubGraphQL, ratelimiter.BucketGitHubCore)
	var snapSettings GitHubSettings
	require.NoError(t, json.Unmarshal(snapshot.Settings, &snapSettings))
	saveDiscoveredRepos(ctx, params, snapSettings, []repoInfo{{FullName: "new-org/a", DefaultBranch: "main", DatabaseID: 1}})

	after, err := m.Get(ctx, org, "github")
	require.NoError(t, err)
	var stored map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(after.Settings, &stored))
	assert.JSONEq(t, `"new-org"`, string(stored["organization"]), "the operator's change survives")
	assert.JSONEq(t, `"kept"`, string(stored["operator_key"]), "a key the connector does not model survives")
	assert.Contains(t, stored, "vendor_rate_limits")
	assert.Contains(t, stored, "discovered_repos")
}
