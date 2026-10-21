// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

//go:build stress

// Stress simulation of the Bitbucket rate limit across many repositories and pull requests.
// scripts/ci.sh runs it in its own step of the test job, about five seconds. Run with:
//   go test -tags=stress -v -run TestRateLimitStormSimulation ./connectors/bitbucket/...
//
// What it asserts: under workloads of 7-50 repos × 50-100 PRs × N cycles ×
// small churn between cycles, the per-PR change cache plus
// the schedule stagger cut warm-state vendor call counts well below the
// bitbucket-rest 0.5556 RPS / 2000 req/hr / ~500-calls-per-15-min drain budget.
//
// Cold-start (cache empty) deliberately ISN'T asserted to fit within one
// cycle's budget — it's a one-time cost per deploy / cache wipe. The
// simulation reports the wall-clock drain time so reviewers see what to
// expect during the first cycle post-deploy.

package bitbucket

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

const (
	// drainBudgetPerCycle is the per-15-min drain capacity of the
	// bitbucket-rest group (0.5556 RPS × 900s ≈ 500 calls). Per the params
	// gateway configuration.
	drainBudgetPerCycle = 500
	bitbucketRESTRPS    = 0.5556
)

type simState struct {
	prs []bbPullRequest
}

// advanceChurn picks `churn` fraction of PRs (prs[0..n-1]) and bumps their
// UpdatedOn to `now`. The chosen set is deterministic so the simulation is
// reproducible and reviewers can reason about it.
func (s *simState) advanceChurn(now time.Time, churn float64) {
	n := int(float64(len(s.prs)) * churn)
	for i := 0; i < n; i++ {
		s.prs[i].UpdatedOn = now
	}
}

// runOneCycleReviews wires a counting mock VendorGateway through the actual
// FetchReviewsOp.Execute and returns (list_calls, activity_calls). The activity
// watermark cache lives in connector_fetch_cache, so the caller controls warm vs
// cold by passing a persistent fc (warm across cycles) or a fresh one (cold each
// cycle).
func runOneCycleReviews(t *testing.T, repos []string, states map[string]*simState, fc sdk.FetchCache) (int, int) {
	t.Helper()
	var listCalls, activityCalls int
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				listCalls++
				key := r.GetWorkspace() + "/" + r.GetRepoSlug()
				st, ok := states[key]
				if !ok {
					return bbPRsResponse(nil, false), nil
				}
				return bbPRsResponse(st.prs, false), nil
			case *connectorpb.BitbucketListPRActivityRequest:
				activityCalls++
				return bbActivityResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-stress",
			Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "t"}),
			Settings:       mustJSON(BitbucketSettings{Workspace: "workspace", Repositories: repos}),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
		FetchCache:    fc,
	}
	_, err := (&FetchReviewsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	return listCalls, activityCalls
}

// drainSeconds reports the wall-clock time required to clear `calls` calls
// through the bitbucket-rest token bucket. Useful for reporting cold-start
// cost: at 0.5556 RPS, 5000 calls ≈ 2.5 hours.
func drainSeconds(calls int) float64 {
	return float64(calls) / bitbucketRESTRPS
}

func TestRateLimitStormSimulation(t *testing.T) {
	scenarios := []struct {
		name          string
		repos         int
		prsPerRepo    int
		cycles        int
		churnPerCycle float64
	}{
		{name: "large-workspace scenario: 7 repos × 50 PRs × 5% churn × 5 cycles", repos: 7, prsPerRepo: 50, cycles: 5, churnPerCycle: 0.05},
		{name: "mid org: 25 repos × 75 PRs × 3% churn × 5 cycles", repos: 25, prsPerRepo: 75, cycles: 5, churnPerCycle: 0.03},
		{name: "big org: 50 repos × 100 PRs × 2% churn × 5 cycles", repos: 50, prsPerRepo: 100, cycles: 5, churnPerCycle: 0.02},
		{name: "high churn outlier: 7 repos × 50 PRs × 30% churn × 5 cycles", repos: 7, prsPerRepo: 50, cycles: 5, churnPerCycle: 0.30},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)

			repos := make([]string, sc.repos)
			states := make(map[string]*simState, sc.repos)
			for i := 0; i < sc.repos; i++ {
				ws := "workspace"
				slug := fmt.Sprintf("repo-%03d", i)
				repos[i] = ws + "/" + slug
				prs := make([]bbPullRequest, sc.prsPerRepo)
				for j := 0; j < sc.prsPerRepo; j++ {
					prs[j] = bbPullRequest{
						ID:        i*1000 + j,
						Title:     fmt.Sprintf("PR-%d-%d", i, j),
						State:     "OPEN",
						CreatedOn: now.Add(-30 * 24 * time.Hour),
						UpdatedOn: now,
					}
				}
				states[repos[i]] = &simState{prs: prs}
			}

			// --- BEFORE simulation: no persistent cache — a fresh (empty) fetch
			// cache each cycle, so every PR is a cold miss and re-fetches /activity. ---
			var beforeColdStart, beforeWarmTotal int
			for c := 0; c < sc.cycles; c++ {
				_, actC := runOneCycleReviews(t, repos, states, newFakeFetchCache())
				if c == 0 {
					beforeColdStart = actC
				} else {
					beforeWarmTotal += actC
				}
				for _, st := range states {
					st.advanceChurn(now.Add(time.Duration(c+1)*15*time.Minute), sc.churnPerCycle)
				}
			}

			// Reset PR state for the "after" run.
			for _, repoName := range repos {
				for j := range states[repoName].prs {
					states[repoName].prs[j].UpdatedOn = now
				}
			}

			// --- AFTER simulation: one fetch cache persists across cycles, so only
			// the churned PRs (bumped UpdatedOn) miss and re-fetch /activity. ---
			var afterColdStart, afterWarmTotal int
			afterCache := newFakeFetchCache()
			for c := 0; c < sc.cycles; c++ {
				_, actC := runOneCycleReviews(t, repos, states, afterCache)
				if c == 0 {
					afterColdStart = actC
				} else {
					afterWarmTotal += actC
				}
				for _, st := range states {
					st.advanceChurn(now.Add(time.Duration(c+1)*15*time.Minute), sc.churnPerCycle)
				}
			}

			warmCycles := sc.cycles - 1
			beforeWarmPerCycle := beforeWarmTotal / warmCycles
			afterWarmPerCycle := afterWarmTotal / warmCycles

			t.Logf("")
			t.Logf("=== %s ===", sc.name)
			t.Logf("Cold-start /activity calls       — before: %5d   after: %5d", beforeColdStart, afterColdStart)
			t.Logf("Warm-state /activity (per cycle) — before: %5d   after: %5d   drain budget: %d", beforeWarmPerCycle, afterWarmPerCycle, drainBudgetPerCycle)
			if beforeWarmPerCycle > 0 {
				t.Logf("Warm-state reduction:                                  %.1f%%", 100.0*float64(beforeWarmPerCycle-afterWarmPerCycle)/float64(beforeWarmPerCycle))
			}
			t.Logf("Cold-start drain time            — before: %s   after: %s", fmt.Sprintf("%4.1f min", drainSeconds(beforeColdStart)/60), fmt.Sprintf("%4.1f min", drainSeconds(afterColdStart)/60))
			t.Logf("")

			// Assertions (warm state only — cold start is a one-time deploy cost):
			if afterWarmPerCycle >= drainBudgetPerCycle {
				t.Errorf("after-fix WARM /activity calls per cycle = %d ≥ drain budget %d — fix does NOT hold for this workload",
					afterWarmPerCycle, drainBudgetPerCycle)
			}
			if beforeWarmPerCycle <= afterWarmPerCycle {
				t.Errorf("expected warm-state reduction; before=%d after=%d", beforeWarmPerCycle, afterWarmPerCycle)
			}
		})
	}
}
