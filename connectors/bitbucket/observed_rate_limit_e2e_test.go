// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

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
	lastOrgID        string
	lastConnectorID  sdk.ConnectorID
	lastSettingsJSON json.RawMessage
}

func (r *recordingConfigUpdater) UpdateSettings(_ context.Context, orgID string, connectorID sdk.ConnectorID, settings json.RawMessage) error {
	r.calls++
	r.lastOrgID = orgID
	r.lastConnectorID = connectorID
	r.lastSettingsJSON = settings
	return nil
}

// PatchSettings is the race-safe persist path used by persistFetchPRsSettings /
// persistFetchCommitsSettings in real use. The stub records the result of
// running the mutator against `lastSettingsJSON` so tests can assert exactly
// what the persist helper would have written, without needing a live DB.
func (r *recordingConfigUpdater) PatchSettings(_ context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	current := r.lastSettingsJSON
	if len(current) == 0 {
		current = json.RawMessage(`{}`)
	}
	next, err := mutator(current)
	if err != nil {
		return err
	}
	// Mirror the no-op semantics of the real DAO: when the mutator returned
	// the input unchanged, there's nothing to write. Tests that assert
	// `calls == 0` for the cold-start path depend on this — they pre-seed
	// `lastSettingsJSON` from the start-of-run snapshot.
	if bytes.Equal(current, next) {
		return nil
	}
	r.calls++
	r.lastOrgID = orgID
	r.lastConnectorID = connectorID
	r.lastSettingsJSON = next
	return nil
}

// TestObservedRateLimit_EndToEnd exercises the full observed-rate-limit loop in one process:
//  1. Vendor response with X-RateLimit-Limit lands; BudgetLimiter.UpdateFromResponse seeds Redis.
//  2. persistFetchPRsSettings reads the snapshot and writes it through ConfigUpdater
//     into Settings.VendorRateLimits keyed by the matching gateway group.
//  3. The resulting Settings JSON carries a per-group observation entry — exactly
//     the shape the scheduler gateway factory consumes via applyObservedRateLimit
//     on the next run.
func TestObservedRateLimit_EndToEnd(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	bl := ratelimiter.NewBudgetLimiter(rc, ratelimiter.BitbucketAdapter{})

	const orgID = "org-premium-1"
	ctx := context.Background()

	resetAt := time.Now().Add(45 * time.Minute).Unix()
	require.NoError(t, bl.UpdateFromResponse(ctx, orgID, http.Header{
		"X-Ratelimit-Remaining": []string{"8200"},
		"X-Ratelimit-Limit":     []string{"10000"},
		"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt, 10)},
	}))

	gotLimit, _, ok, err := bl.GetLimit(ctx, orgID, ratelimiter.BucketDefault)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 10000, gotLimit)

	initialSettings, err := json.Marshal(BitbucketSettings{Workspace: "acme"})
	require.NoError(t, err)
	upd := &recordingConfigUpdater{} // lastSettingsJSON seeded below from params.Config.Settings
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: orgID,
			Settings:       initialSettings,
		},
		ConfigUpdater: upd,
		BudgetLimiter: bl,
	}
	upd.lastSettingsJSON = params.Config.Settings

	persistFetchPRsSettings(ctx, params, bitbucketBucketToGroup, nil)

	require.Equal(t, 1, upd.calls, "expected single UpdateSettings write")
	assert.Equal(t, orgID, upd.lastOrgID)
	assert.Equal(t, sdk.ConnectorID("bitbucket"), upd.lastConnectorID)

	var persisted BitbucketSettings
	require.NoError(t, json.Unmarshal(upd.lastSettingsJSON, &persisted))
	assert.Equal(t, "acme", persisted.Workspace, "non-cap fields preserved")
	require.Contains(t, persisted.VendorRateLimits, "bitbucket-rest")
	entry := persisted.VendorRateLimits["bitbucket-rest"]
	assert.Equal(t, 10000, entry.PerHour)
	require.NotEmpty(t, entry.ObservedAt)
	observedAt, err := time.Parse(time.RFC3339, entry.ObservedAt)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), observedAt, 5*time.Second)

	// Idempotent: running again immediately should not write again.
	params.Config.Settings = upd.lastSettingsJSON
	persistFetchPRsSettings(ctx, params, bitbucketBucketToGroup, nil)
	assert.Equal(t, 1, upd.calls, "idempotent: no rewrite when cap unchanged inside refresh window")
}

// TestObservedRateLimit_RefreshAfterDay confirms persistFetchPRsSettings
// re-stamps the per-group observation once a day even when the cap is
// unchanged, so a stable workspace never drifts past the 7-day staleness
// window enforced by the gateway factory.
func TestObservedRateLimit_RefreshAfterDay(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	bl := ratelimiter.NewBudgetLimiter(rc, ratelimiter.BitbucketAdapter{})
	const orgID = "org-stable-1"
	ctx := context.Background()

	resetAt := time.Now().Add(30 * time.Minute).Unix()
	require.NoError(t, bl.UpdateFromResponse(ctx, orgID, http.Header{
		"X-Ratelimit-Remaining": []string{"500"},
		"X-Ratelimit-Limit":     []string{"1000"},
		"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt, 10)},
	}))

	prior := BitbucketSettings{
		Workspace: "acme",
		VendorRateLimits: map[string]pacing.VendorRateLimitObservation{
			"bitbucket-rest": {PerHour: 1000, ObservedAt: time.Now().Add(-30 * time.Hour).UTC().Format(time.RFC3339)},
		},
	}
	priorJSON, err := json.Marshal(prior)
	require.NoError(t, err)

	upd := &recordingConfigUpdater{} // lastSettingsJSON seeded below from params.Config.Settings
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: orgID,
			Settings:       priorJSON,
		},
		ConfigUpdater: upd,
		BudgetLimiter: bl,
	}
	upd.lastSettingsJSON = params.Config.Settings
	persistFetchPRsSettings(ctx, params, bitbucketBucketToGroup, nil)
	require.Equal(t, 1, upd.calls, "expected refresh write after 24h since prior observation")

	var refreshed BitbucketSettings
	require.NoError(t, json.Unmarshal(upd.lastSettingsJSON, &refreshed))
	require.Contains(t, refreshed.VendorRateLimits, "bitbucket-rest")
	entry := refreshed.VendorRateLimits["bitbucket-rest"]
	assert.Equal(t, 1000, entry.PerHour)
	stamped, err := time.Parse(time.RFC3339, entry.ObservedAt)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), stamped, 5*time.Second)
}

// TestObservedRateLimit_SkipsWhenSnapshotMissing covers the cold-start path:
// no prior vendor response means BudgetLimiter has nothing AND there are no
// caches to write — persistFetchPRsSettings must be a no-op (callers fall
// back to the yml floor on the next run).
func TestObservedRateLimit_SkipsWhenSnapshotMissing(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	bl := ratelimiter.NewBudgetLimiter(rc, ratelimiter.BitbucketAdapter{})

	initialSettings, _ := json.Marshal(BitbucketSettings{Workspace: "acme"})
	upd := &recordingConfigUpdater{} // lastSettingsJSON seeded below from params.Config.Settings
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-cold",
			Settings:       initialSettings,
		},
		ConfigUpdater: upd,
		BudgetLimiter: bl,
	}
	upd.lastSettingsJSON = params.Config.Settings
	persistFetchPRsSettings(context.Background(), params, bitbucketBucketToGroup, nil)
	assert.Equal(t, 0, upd.calls, "no snapshot + no cache → no DB write")
}

// TestPersistFetchPRsSettings_CombinedWrite confirms a single UpdateSettings
// call carries both the per-group rate-limit observation and the per-repo
// lifecycle diagnostics: a single read-modify-write of params.Config.Settings,
// so neither deferred write can overwrite the other.
func TestPersistFetchPRsSettings_CombinedWrite(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	bl := ratelimiter.NewBudgetLimiter(rc, ratelimiter.BitbucketAdapter{})
	const orgID = "org-combined"
	ctx := context.Background()

	resetAt := time.Now().Add(30 * time.Minute).Unix()
	require.NoError(t, bl.UpdateFromResponse(ctx, orgID, http.Header{
		"X-Ratelimit-Remaining": []string{"7500"},
		"X-Ratelimit-Limit":     []string{"10000"},
		"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt, 10)},
	}))

	initialSettings, _ := json.Marshal(BitbucketSettings{
		Workspace:       "acme",
		DiscoveredRepos: []BitbucketDiscoveredRepo{{FullName: "acme/repo"}},
	})
	upd := &recordingConfigUpdater{} // lastSettingsJSON seeded below from params.Config.Settings
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: orgID,
			Settings:       initialSettings,
		},
		ConfigUpdater: upd,
		BudgetLimiter: bl,
	}
	upd.lastSettingsJSON = params.Config.Settings

	// A diagnostic-only repo update (Status UNSPECIFIED) stamps LastHTTPStatus onto
	// the existing repo — the other field-source that must merge in the same write.
	repoStateUpdates := map[string]BitbucketDiscoveredRepo{
		"acme/repo": {FullName: "acme/repo", LastErrorAt: "2026-05-01T00:00:00Z", LastHTTPStatus: 500},
	}

	persistFetchPRsSettings(ctx, params, bitbucketBucketToGroup, repoStateUpdates)
	require.Equal(t, 1, upd.calls, "expected single UpdateSettings with both fields")

	var persisted BitbucketSettings
	require.NoError(t, json.Unmarshal(upd.lastSettingsJSON, &persisted))
	require.Contains(t, persisted.VendorRateLimits, "bitbucket-rest")
	assert.Equal(t, 10000, persisted.VendorRateLimits["bitbucket-rest"].PerHour, "cap preserved alongside repo state")
	require.Len(t, persisted.DiscoveredRepos, 1)
	assert.Equal(t, 500, persisted.DiscoveredRepos[0].LastHTTPStatus, "repo lifecycle diagnostics written in the same call")
}
