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
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func init() {
	logger.Init("test", nil)
}

type recordingConfigUpdater struct {
	calls            int
	lastSettingsJSON json.RawMessage
}

func (r *recordingConfigUpdater) UpdateSettings(_ context.Context, _ string, _ sdk.ConnectorID, settings json.RawMessage) error {
	r.calls++
	r.lastSettingsJSON = settings
	return nil
}

// PatchSettings is the race-safe persist path used by the production
// fetch_prs/fetch_commits persist helpers. The stub runs the mutator
// against the recorded settings, replays the result, and
// captures it so tests can assert on the final shape.
func (r *recordingConfigUpdater) PatchSettings(_ context.Context, _ string, _ sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	current := r.lastSettingsJSON
	if len(current) == 0 {
		current = json.RawMessage(`{}`)
	}
	next, err := mutator(current)
	if err != nil {
		return err
	}
	r.calls++
	r.lastSettingsJSON = next
	return nil
}

// TestObservedRateLimit_GitHub_EndToEnd confirms per-bucket observations
// land under their matching gateway group, so the gateway factory can
// scale only the observed groups instead of fanning a single cap across
// REST + GraphQL + search.
func TestObservedRateLimit_GitHub_EndToEnd(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	bl := ratelimiter.NewBudgetLimiter(rc, ratelimiter.GitHubAdapter{})

	const orgID = "org-gh-enterprise"
	ctx := context.Background()

	// Two observations land — graphql (12500/hr) and core (5000/hr). Each
	// should be recorded under its own group key.
	resetAt := time.Now().Add(45 * time.Minute).Unix()
	require.NoError(t, bl.UpdateFromResponse(ctx, orgID, http.Header{
		"X-Ratelimit-Remaining": []string{"11000"},
		"X-Ratelimit-Limit":     []string{"12500"},
		"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt, 10)},
		"X-Ratelimit-Resource":  []string{"graphql"},
	}))
	require.NoError(t, bl.UpdateFromResponse(ctx, orgID, http.Header{
		"X-Ratelimit-Remaining": []string{"4500"},
		"X-Ratelimit-Limit":     []string{"5000"},
		"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt, 10)},
		"X-Ratelimit-Resource":  []string{"core"},
	}))

	initialSettings, _ := json.Marshal(GitHubSettings{Organization: "acme-corp"})
	upd := &recordingConfigUpdater{lastSettingsJSON: initialSettings} // the stored row
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: orgID,
			Settings:       initialSettings,
		},
		ConfigUpdater: upd,
		BudgetLimiter: bl,
	}

	persistObservedRateLimitCap(ctx, params, ratelimiter.BucketGitHubGraphQL, ratelimiter.BucketGitHubCore)

	require.Equal(t, 1, upd.calls)
	var persisted GitHubSettings
	require.NoError(t, json.Unmarshal(upd.lastSettingsJSON, &persisted))
	assert.Equal(t, "acme-corp", persisted.Organization)
	require.Contains(t, persisted.VendorRateLimits, "github-graphql")
	require.Contains(t, persisted.VendorRateLimits, "github-rest")
	assert.Equal(t, 12500, persisted.VendorRateLimits["github-graphql"].PerHour, "graphql carries its own cap, not core's")
	assert.Equal(t, 5000, persisted.VendorRateLimits["github-rest"].PerHour, "rest carries its own cap")
	for group, entry := range persisted.VendorRateLimits {
		require.NotEmpty(t, entry.ObservedAt, "group %s missing observed_at", group)
		observedAt, err := time.Parse(time.RFC3339, entry.ObservedAt)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), observedAt, 5*time.Second)
	}
}
