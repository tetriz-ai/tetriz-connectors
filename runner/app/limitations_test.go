// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

// Tests of how the gateway factory paces a connector.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/inprocess"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	rlstore "github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/store"
	gatewaypb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The GitHub and Bitbucket connectors record the vendor's observed hourly cap
// in Settings.vendor_rate_limits, and the gateway factory paces the group at
// it when it is lower than the configured rate. Here the recorded cap is one
// request an hour: the first request goes out, and the next are held back
// locally instead of reaching the vendor.
func TestObservedRateLimitIsApplied(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer srv.Close()

	const org = "00000000-0000-4000-8000-00000000f00d"
	cfg := gateway.DefaultConfig()
	cfg.SimulatorOrgIDs = []string{org}
	cfg.SimulatorBaseURL = srv.URL
	for i := range cfg.GitHub {
		cfg.GitHub[i].RequestsPerSecond, cfg.GitHub[i].BurstSize = 100, 100
	}
	gateway.SetConfig(cfg)
	defer gateway.SetConfig(gateway.DefaultConfig())

	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	factory := newGatewayFactory(gatewayDeps{rlStore: rlstore.NewSlidingWindowLogWithRedis(rc), plainHTTP: httpDoer{client: srv.Client()}})

	settings, _ := json.Marshal(map[string]any{
		"organization":       "acme",
		"vendor_rate_limits": map[string]any{"github-graphql": map[string]any{"per_hour": 1, "observed_at": time.Now().UTC().Format(time.RFC3339)}},
	})
	gw, err := factory(context.Background(), &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github", Settings: settings})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = gw.ForGroup("github-graphql").Execute(ctx, &gatewaypb.GitHubGraphQLRequest{Query: "{ viewer { login } }"})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err := gw.ForGroup("github-graphql").Execute(ctx, &gatewaypb.GitHubGraphQLRequest{Query: "{ viewer { login } }"})
		var se *sdk.SyncError
		require.ErrorAs(t, err, &se, "the observed cap is spent")
		assert.Equal(t, sdk.ErrKindRateLimit, se.Kind)
	}
	assert.Equal(t, int32(1), hits.Load(), "only the first request reached the vendor")
}

// A stale observation (older than the staleness window), or one above the
// configured rate, leaves the configured rate in force.
func TestObservedRateLimitIgnoresStaleOrHigherCaps(t *testing.T) {
	endpoints := map[string]gateway.APIEndpointConfig{"github-graphql": {Group: "github-graphql", RequestsPerSecond: 2, BurstSize: 2}}
	gw := &recordingGateway{}
	now := time.Now().UTC()
	for name, obs := range map[string]map[string]any{
		"stale":  {"per_hour": 1, "observed_at": now.Add(-8 * 24 * time.Hour).Format(time.RFC3339)},
		"higher": {"per_hour": 100000, "observed_at": now.Format(time.RFC3339)},
	} {
		settings, _ := json.Marshal(map[string]any{"vendor_rate_limits": map[string]any{"github-graphql": obs}})
		got := withObservedCaps(context.Background(), gw, endpoints, settings, now)
		assert.Same(t, gw, got, name)
		assert.Nil(t, gw.configs, name)
	}
	settings, _ := json.Marshal(map[string]any{"vendor_rate_limits": map[string]any{"github-graphql": map[string]any{"per_hour": 3600, "observed_at": now.Format(time.RFC3339)}}})
	withObservedCaps(context.Background(), gw, endpoints, settings, now)
	require.Len(t, gw.configs, 1)
	assert.InDelta(t, 1.0, gw.configs[0].RequestsPerSecond, 1e-9, "a fresh lower cap is applied")
}

type recordingGateway struct {
	sdk.VendorGateway
	configs []sdk.RateLimitConfig
}

func (g *recordingGateway) WithRateLimits(c []sdk.RateLimitConfig) sdk.VendorGateway {
	g.configs = c
	return g
}

// The factory's GitLab gateways, real and simulator, carry the GitLab budget,
// so its RateLimit-* headers reach the budget fetch_mr_diffs checks.
func TestGatewayFactoryGivesGitLabItsBudget(t *testing.T) {
	const org = "00000000-0000-4000-8000-00000000f00e"
	reset := time.Now().Add(time.Hour).Unix()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("RateLimit-Remaining", "1")
		w.Header().Set("RateLimit-Limit", "2000")
		w.Header().Set("RateLimit-Reset", strconv.FormatInt(reset, 10))
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	cfg := gateway.DefaultConfig()
	cfg.SimulatorOrgIDs = []string{org}
	cfg.SimulatorBaseURL = srv.URL
	gateway.SetConfig(cfg)
	defer gateway.SetConfig(gateway.DefaultConfig())

	stores := inprocess.New()
	budgets := stores.BudgetLimiters()
	factory := newGatewayFactory(gatewayDeps{rlStore: stores.RateLimits, budgets: budgets, plainHTTP: httpDoer{client: srv.Client()}})
	gw, err := factory(context.Background(), &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "gitlab", Settings: json.RawMessage(`{}`)})
	require.NoError(t, err)

	ctx := reqctx.WithOrgID(context.Background(), org)
	_, err = gw.ForGroup(constants.GroupGitLabREST).Execute(ctx, &gatewaypb.GitLabListProjectsRequest{GroupId: 1})
	require.NoError(t, err)
	allowed, _, err := budgets["gitlab"].Allow(ctx, org, ratelimiter.BucketDefault, ratelimiter.GitLabCoreFloor)
	require.NoError(t, err)
	assert.False(t, allowed, "the gateway recorded GitLab's budget")
}
