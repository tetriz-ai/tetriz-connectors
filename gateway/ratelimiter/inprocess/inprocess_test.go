// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package inprocess_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/inprocess"
	anthropicgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/anthropic"
	bitbucketgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/bitbucket"
	codexgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/codex"
	cursorgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/cursor"
	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/gatewaytest"
	githubgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/github"
	gitlabgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/gitlab"
	jiragw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/jira"
	lineargw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/linear"
	slackgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/slack"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// This package had no tests, so it carries its own guard.
func TestMain(m *testing.M) {
	logger.Init("test", nil)
	gatewaytest.Install()
	os.Exit(testguard.Run(m))
}

// fakeDoer answers every request itself, so no test reaches a vendor.
type fakeDoer struct {
	calls  atomic.Int64
	status int
	header http.Header
}

func (d *fakeDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	d.calls.Add(1)
	status := d.status
	if status == 0 {
		status = http.StatusOK
	}
	h := d.header.Clone()
	if h == nil {
		h = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader("{}")), Request: req}, nil
}

func groups(m map[string]gateway.APIEndpointConfig) []string {
	out := make([]string, 0, len(m))
	for g := range m {
		out = append(out, g)
	}
	return out
}

// Every built-in gateway the runner builds, except Kiro (which talks to AWS
// and takes no store), runs a request through its rate limiter on the
// in-process stores, with no Redis anywhere, and none panics on a nil Store.
func TestEveryBuiltInGatewayRunsWithoutRedis(t *testing.T) {
	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)
	s := inprocess.New()
	budgets := s.BudgetLimiters()
	doer := &fakeDoer{}

	type built struct {
		gw     sdk.VendorGateway
		groups []string
	}
	builds := map[string]func() (sdk.VendorGateway, error){
		"github": func() (sdk.VendorGateway, error) {
			return githubgw.NewGitHubVendorGateway(doer, "t", s.RateLimits, budgets["github"])
		},
		"github-simulator": func() (sdk.VendorGateway, error) {
			return githubgw.NewGitHubVendorGatewayForSimulator(doer, "t", s.RateLimits, budgets["github"], gatewaytest.SimulatorBaseURL)
		},
		"gitlab": func() (sdk.VendorGateway, error) {
			return gitlabgw.NewGitLabVendorGateway(doer, s.RateLimits, s.GitLabCircuit, nil)
		},
		"gitlab-simulator": func() (sdk.VendorGateway, error) {
			return gitlabgw.NewGitLabVendorGatewayForSimulator(doer, "t", s.RateLimits, s.GitLabCircuit, nil, gatewaytest.SimulatorBaseURL)
		},
		"bitbucket": func() (sdk.VendorGateway, error) {
			return bitbucketgw.NewBitbucketVendorGateway(doer, s.RateLimits, s.BitbucketCircuit, budgets["bitbucket"])
		},
		"bitbucket-simulator": func() (sdk.VendorGateway, error) {
			return bitbucketgw.NewBitbucketVendorGatewayForSimulator(doer, "t", s.RateLimits, s.BitbucketCircuit, budgets["bitbucket"], gatewaytest.SimulatorBaseURL)
		},
		"jira": func() (sdk.VendorGateway, error) { return jiragw.NewJiraVendorGateway(doer, s.RateLimits) },
		"jira-simulator": func() (sdk.VendorGateway, error) {
			return jiragw.NewJiraVendorGatewayForSimulator(doer, "t", s.RateLimits, gatewaytest.SimulatorBaseURL)
		},
		"linear": func() (sdk.VendorGateway, error) { return lineargw.NewLinearVendorGateway(doer, s.RateLimits) },
		"slack":  func() (sdk.VendorGateway, error) { return slackgw.NewSlackVendorGateway(doer, "t", s.RateLimits) },
		"cursor": func() (sdk.VendorGateway, error) { return cursorgw.NewCursorVendorGateway(doer, "t", s.RateLimits) },
		"claude-code": func() (sdk.VendorGateway, error) {
			return anthropicgw.NewAnthropicVendorGateway(doer, "t", s.RateLimits)
		},
		"codex": func() (sdk.VendorGateway, error) { return codexgw.NewCodexVendorGateway(doer, "t", s.RateLimits) },
	}
	groupsFor := map[string][]string{
		"github": groups(cfg.GitHubEndpoints()), "gitlab": groups(cfg.GitLabEndpoints()),
		"bitbucket": groups(cfg.BitbucketEndpoints()), "jira": groups(cfg.JiraEndpoints()),
		"slack":  groups(cfg.SlackEndpoints()),
		"cursor": groups(cfg.CursorEndpoints()), "claude-code": groups(cfg.AnthropicEndpoints()),
		"codex": groups(cfg.CodexEndpoints()),
	}

	ctx := reqctx.WithOrgID(context.Background(), "org-inprocess")
	for name, build := range builds {
		t.Run(name, func(t *testing.T) {
			gw, err := build()
			require.NoError(t, err)
			b := built{gw: gw, groups: groupsFor[strings.TrimSuffix(name, "-simulator")]}
			if len(b.groups) == 0 {
				b.groups = []string{""} // the gateway's own default group
			}
			for _, g := range b.groups {
				scoped := b.gw
				if g != "" {
					scoped = b.gw.ForGroup(g)
				}
				before := doer.calls.Load()
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://vendor.invalid/x", nil)
				require.NoError(t, err)
				resp, err := scoped.Do(ctx, req)
				require.NoError(t, err, "group %q", g)
				_ = resp.Body.Close()
				assert.Equal(t, before+1, doer.calls.Load(), "group %q reached the doer", g)
			}
		})
	}
}

// The in-process stores are not inert: the rate limit, the budget snapshot
// and the Bitbucket circuit breaker each take effect.
func TestInProcessStoresTakeEffect(t *testing.T) {
	ctx := reqctx.WithOrgID(context.Background(), "org-effect")
	newReq := func() *http.Request {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://vendor.invalid/x", nil)
		require.NoError(t, err)
		return req
	}

	t.Run("rate limit", func(t *testing.T) {
		s := inprocess.New()
		gw, err := slackgw.NewSlackVendorGateway(&fakeDoer{}, "t", s.RateLimits)
		require.NoError(t, err)
		scoped := gw.WithRateLimits([]sdk.RateLimitConfig{{Group: "tiny", RequestsPerSecond: 2, BurstSize: 2}}).ForGroup("tiny")
		var limited int
		for range 10 {
			resp, err := scoped.Do(ctx, newReq())
			var se *sdk.SyncError
			if errors.As(err, &se) && se.Kind == sdk.ErrKindRateLimit {
				limited++
				continue
			}
			require.NoError(t, err)
			_ = resp.Body.Close()
		}
		assert.Positive(t, limited, "ten calls at once exceed two a second")
	})

	t.Run("budget snapshot", func(t *testing.T) {
		s := inprocess.New()
		reset := time.Now().Add(time.Hour)
		doer := &fakeDoer{header: http.Header{
			"X-Ratelimit-Remaining": {"100"},
			"X-Ratelimit-Limit":     {"5000"},
			"X-Ratelimit-Reset":     {strconv.FormatInt(reset.Unix(), 10)},
			"X-Ratelimit-Resource":  {"core"},
		}}
		budgets := s.BudgetLimiters()
		gw, err := githubgw.NewGitHubVendorGateway(doer, "t", s.RateLimits, budgets["github"])
		require.NoError(t, err)
		cfg, err := gateway.LoadConfig()
		require.NoError(t, err)
		resp, err := gw.ForGroup(groups(cfg.GitHubEndpoints())[0]).Do(ctx, newReq())
		require.NoError(t, err)
		_ = resp.Body.Close()
		lim, _, ok, err := budgets["github"].GetLimit(ctx, "org-effect", ratelimiter.BucketGitHubCore)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, 5000, lim)
		allowed, _, err := budgets["github"].Allow(ctx, "org-effect", ratelimiter.BucketGitHubCore, ratelimiter.GitHubCoreFloor)
		require.NoError(t, err)
		assert.False(t, allowed, "100 remaining is under the core floor")
	})

	t.Run("bitbucket circuit breaker", func(t *testing.T) {
		s := inprocess.New()
		doer := &fakeDoer{status: http.StatusTooManyRequests, header: http.Header{"Retry-After": {"1"}}}
		gw, err := bitbucketgw.NewBitbucketVendorGateway(doer, s.RateLimits, s.BitbucketCircuit, nil)
		require.NoError(t, err)
		scoped := gw.ForGroup("bitbucket-rest")
		for range 3 {
			resp, err := scoped.Do(ctx, newReq())
			if err == nil {
				_ = resp.Body.Close()
			}
		}
		before := doer.calls.Load()
		_, err = scoped.Do(ctx, newReq())
		var se *sdk.SyncError
		require.ErrorAs(t, err, &se)
		assert.Equal(t, sdk.ErrKindRateLimit, se.Kind)
		assert.Equal(t, before, doer.calls.Load(), "an open circuit fails fast without calling the vendor")
	})
}
