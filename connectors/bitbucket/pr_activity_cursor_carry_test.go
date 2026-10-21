// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	bitbucketpb "github.com/tetriz-ai/tetriz-connectors/gen/bitbucket/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	tu "github.com/tetriz-ai/tetriz-connectors/testkit"
)

// The two per-PR-cache ops share one cursor shape and one carry-forward contract, so
// every case below runs against both. A fix applied to only one of them is the shape
// this table exists to catch.
type prActivityOp struct {
	name      string
	cacheType string
	execute   func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error)
}

func prActivityOps() []prActivityOp {
	return []prActivityOp{
		{"fetch_reviews", CacheTypePRActivity, (&FetchReviewsOp{}).Execute},
		{"fetch_pr_comments", CacheTypePRComments, (&FetchPRCommentsOp{}).Execute},
	}
}

// prActivityGateway serves a PR list for whichever repo is asked for, plus empty
// activity/comment sub-resources. Empty sub-resources are the point: they leave
// latestDate at `since`, which is the state in which a synthetic first-sync floor
// would be written back as though it were observed progress.
func prActivityGateway(prUpdatedOn time.Time) *mockVendorGateway {
	return &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{{
					ID: 42, State: "OPEN",
					CreatedOn: prUpdatedOn.Add(-2 * time.Hour),
					UpdatedOn: prUpdatedOn,
				}}, false), nil
			case *connectorpb.BitbucketListPRActivityRequest:
				return bbActivityResponse(nil, false), nil
			case *connectorpb.BitbucketListPRCommentsRequest:
				return bbCommentsResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
}

// bareCursor builds the plain-timestamp composite cursor shape these two ops write
// (buildCompositeCursor), so a test starts from a cursor the op itself could have left.
func bareCursor(t *testing.T, watermarks map[string]time.Time) *sdk.Cursor {
	t.Helper()
	parts := make(map[string]json.RawMessage, len(watermarks))
	for repo, ts := range watermarks {
		raw, err := json.Marshal(ts.UTC())
		require.NoError(t, err)
		parts[repo] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// cursorTime reads one repo's watermark back out of a returned cursor.
func cursorTime(t *testing.T, cursor *sdk.Cursor, repo string) (time.Time, bool) {
	t.Helper()
	require.NotNil(t, cursor)
	parts, err := sdk.CompositeFromCursor(cursor)
	require.NoError(t, err)
	raw, ok := parts[repo]
	if !ok {
		return time.Time{}, false
	}
	var ts time.Time
	require.NoError(t, json.Unmarshal(raw, &ts))
	return ts, true
}

// TestPRActivityFirstSyncDoesNotPersistTheLookbackFloor pins the rule that a cursor
// entry may only ever hold a timestamp vendor data actually carried.
//
// On a first sync `since` is a synthetic now-minus-lookback floor. A repo whose PRs
// yield no comments or activity observes nothing, so it must record nothing — writing
// `since` back would publish a query bound as observed progress, and because the next
// run reads that value as its own floor and a warm probe cache then skips every PR, the
// floor rewrites itself on every tick. The entry freezes a full lookback in the past,
// ages a day per day, and trips ConnectorCursorOlderThanLookbackFloor ~40 days after
// onboarding with nothing able to move it.
//
// The assertion is deliberately "no entry at all" rather than a bound on its age: any
// value here is wrong, and an age assertion would still pass for a floor written one
// day short of the threshold.
func TestPRActivityFirstSyncDoesNotPersistTheLookbackFloor(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)

	const repo = "myworkspace/myrepo"

	for _, op := range prActivityOps() {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			// No cursor: this is the first sync, the run that mints the floor.
			params := testParams(tu.NewChannelSink(), prActivityGateway(now), nil)
			withFetchCache(params, newFakeFetchCache())

			next, err := op.execute(context.Background(), params)
			require.NoError(t, err)

			// With nothing observed for the only repo there is no watermark map to
			// build, so the op returns the incoming cursor — nil here. That is the
			// strongest form of "recorded nothing"; anything else must carry no entry
			// for this repo.
			if next == nil {
				return
			}
			got, ok := cursorTime(t, next, repo)
			assert.False(t, ok,
				"a repo that observed no %s must leave its watermark unset; got %s, which is the synthetic first-sync floor persisted as though vendor data carried it",
				op.name, got)
		})
	}
}

// TestPRActivityCursorSurvivesAnUnvisitedRepo pins the carry-forward contract that
// keeps a per-repo watermark from being erased.
//
// buildCompositeCursor REPLACES the whole cursor blob, so a repo the run does not
// visit is not merely left alone — its watermark is dropped. The next run then reads
// that repo as a first sync and firstSyncSince hands it a synthetic now-defaultLookback
// floor, which a warm fetch cache turns into a permanently pinned cursor (nothing is
// re-fetched, so latestDate never rises above the floor and the floor is written back
// every run). A repo goes unvisited for ordinary reasons — an unparseable full name, or
// one transient absence from the workspace listing marking it DELETED — so this is not
// an exotic path.
func TestPRActivityCursorSurvivesAnUnvisitedRepo(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	// A real vendor-shaped watermark, well inside the lookback window.
	stored := now.Add(-72 * time.Hour)

	const visited = "myworkspace/visited"
	const unvisited = "myworkspace/unvisited"

	for _, op := range prActivityOps() {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			params := testParams(tu.NewChannelSink(), prActivityGateway(now), bareCursor(t, map[string]time.Time{
				visited:   stored,
				unvisited: stored,
			}))
			// Discovery mode. `unvisited` is DELETED, so fetchAllRepos filters it out of
			// this run while it stays in DiscoveredRepos awaiting a fetch_repos
			// reconciliation that can restore it — precisely the repo whose progress
			// must survive the run that skipped it.
			params.Config.Settings = mustJSON(BitbucketSettings{
				Workspace: "myworkspace",
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: visited},
					{FullName: unvisited, Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED},
				},
			})
			params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
			withFetchCache(params, newFakeFetchCache())

			next, err := op.execute(context.Background(), params)
			require.NoError(t, err)

			got, ok := cursorTime(t, next, unvisited)
			require.True(t, ok, "an unvisited repo's watermark must be carried forward, not erased — dropping it makes the next run a first sync against the lookback floor")
			assert.True(t, got.Equal(stored), "carried-forward watermark must be unchanged: got %s want %s", got, stored)
		})
	}
}

// TestFetchCommitsCursorSurvivesAnUnvisitedRepo covers the same carry-forward contract on
// fetch_commits. The carry lives in the shared composite-cursor shape rather than in the
// per-PR-cache ops, so every op building a cursor that way inherits it; fetch_commits is on an hourly schedule and would otherwise re-walk a year of
// commits for any repo a run skipped.
func TestFetchCommitsCursorSurvivesAnUnvisitedRepo(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	stored := now.Add(-72 * time.Hour)

	const visited = "myworkspace/visited"
	const unvisited = "myworkspace/unvisited"

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListCommitsRequest:
				return bbCommitsResponse([]bbCommit{{Hash: "c1", Date: now.Add(-time.Hour)}}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	params := testParams(tu.NewChannelSink(), gw, bareCursor(t, map[string]time.Time{
		visited:   stored,
		unvisited: stored,
	}))
	// Discovery mode: the run visits only `visited`, while DELETED `unvisited` stays a
	// repo the config knows about, so it is carryable rather than forgotten.
	params.Config.Settings = mustJSON(BitbucketSettings{
		Workspace: "myworkspace",
		DiscoveredRepos: []BitbucketDiscoveredRepo{
			{FullName: visited},
			{FullName: unvisited, Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED},
		},
		DisableCommitDiffstat: true,
	})
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, newFakeFetchCache())

	next, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	got, ok := cursorTime(t, next, unvisited)
	require.True(t, ok, "fetch_commits must carry an unvisited repo's watermark forward too")
	assert.True(t, got.Equal(stored), "carried-forward watermark must be unchanged: got %s want %s", got, stored)
}

// TestCursorCarryForwardDropsAForgottenRepo is the bound on the carry. A repo the config
// no longer lists at all — renamed, or removed from an explicit Repositories list — is
// gone for good, and carrying its watermark anyway would grow cursor_data with no path
// that ever prunes it.
func TestCursorCarryForwardDropsAForgottenRepo(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	stored := now.Add(-72 * time.Hour)

	const live = "myworkspace/live"
	const forgotten = "myworkspace/renamed-away"

	for _, op := range prActivityOps() {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			params := testParams(tu.NewChannelSink(), prActivityGateway(now), bareCursor(t, map[string]time.Time{
				live:      stored,
				forgotten: stored,
			}))
			// `forgotten` appears in the cursor but in neither Repositories nor
			// DiscoveredRepos — the config has no record of it at all.
			params.Config.Settings = mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{live},
			})
			params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
			withFetchCache(params, newFakeFetchCache())

			next, err := op.execute(context.Background(), params)
			require.NoError(t, err)

			_, ok := cursorTime(t, next, forgotten)
			assert.False(t, ok, "a repo the config no longer knows about must be dropped, else cursor_data grows unboundedly")
			_, liveOK := cursorTime(t, next, live)
			assert.True(t, liveOK, "the live repo must still be present")
		})
	}
}

// TestCursorCarryForwardIgnoresStaleDiscoveryUnderAManualAllowlist pins that the carry
// mirrors fetchAllRepos's precedence. An explicit Repositories allowlist short-circuits
// discovery there, so an org that narrowed from auto-discovery to a manual list can still
// hold a stale DiscoveredRepos cache. Those repos are deliberately not synced, and no run
// will ever visit them to advance or drop their entry — so carrying their watermarks would
// accrue keys in cursor_data and hold the oldest-entry age gauge open indefinitely.
func TestCursorCarryForwardIgnoresStaleDiscoveryUnderAManualAllowlist(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	stored := now.Add(-72 * time.Hour)

	const allowed = "myworkspace/allowed"
	const droppedFromAllowlist = "myworkspace/no-longer-synced"

	for _, op := range prActivityOps() {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			params := testParams(tu.NewChannelSink(), prActivityGateway(now), bareCursor(t, map[string]time.Time{
				allowed:              stored,
				droppedFromAllowlist: stored,
			}))
			// Manual allowlist names only `allowed`; the discovery cache is stale and
			// must not make the dropped repo carryable.
			params.Config.Settings = mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{allowed},
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: allowed},
					{FullName: droppedFromAllowlist},
				},
			})
			params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
			withFetchCache(params, newFakeFetchCache())

			next, err := op.execute(context.Background(), params)
			require.NoError(t, err)

			_, ok := cursorTime(t, next, droppedFromAllowlist)
			assert.False(t, ok, "a repo excluded by the manual allowlist must not be carried from the stale discovery cache")
			_, allowedOK := cursorTime(t, next, allowed)
			assert.True(t, allowedOK, "the allowlisted repo must still be present")
		})
	}
}

// TestPRActivityCursorNeverRegressesWithWarmCache is the failed-then-successful-run
// guard. A run that aborts partway flushes its fetch-cache pages but never persists a
// cursor; the next run therefore meets a warm cache, skips every PR at the freshness
// gate, and observes no new data at all. The cursor it writes must still be the
// watermark it started from — never a synthetic floor derived from time.Now().
func TestPRActivityCursorNeverRegressesWithWarmCache(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	stored := now.Add(-72 * time.Hour)
	const repo = "myworkspace/myrepo"
	const prKey = repo + "#42"

	for _, op := range prActivityOps() {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			var subResourceCalls int
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.BitbucketListPullRequestsRequest:
						return bbPRsResponse([]bbPullRequest{{
							ID: 42, State: "OPEN",
							CreatedOn: now.Add(-96 * time.Hour),
							UpdatedOn: now.Add(-48 * time.Hour),
						}}, false), nil
					case *connectorpb.BitbucketListPRActivityRequest:
						subResourceCalls++
						return bbActivityResponse(nil, false), nil
					case *connectorpb.BitbucketListPRCommentsRequest:
						subResourceCalls++
						return bbCommentsResponse(nil, false), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}
			// Warm cache: the aborted run already probed this PR at its current
			// UpdatedOn, so the freshness gate skips it.
			fake := newFakeFetchCache()
			fake.seed(op.cacheType, prKey, newPRProbeEntry(now.Add(-48*time.Hour), time.Time{}))

			params := testParams(tu.NewChannelSink(), gw, bareCursor(t, map[string]time.Time{repo: stored}))
			params.Config.Settings = mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{repo},
			})
			params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
			withFetchCache(params, fake)

			next, err := op.execute(context.Background(), params)
			require.NoError(t, err)
			require.Zero(t, subResourceCalls, "warm cache must skip the per-PR sub-resource call — otherwise this is not exercising the regression path")

			got, ok := cursorTime(t, next, repo)
			require.True(t, ok)
			assert.False(t, got.Before(stored), "cursor moved BACKWARD from %s to %s: a cursor is monotonic by contract, and a regression re-paginates the whole lookback window every tick", stored, got)
			// Pin the value too, not just the direction: equal-to-stored is the correct
			// outcome, whereas "not before" alone would also admit a bogus jump forward.
			assert.True(t, got.Equal(stored), "a run that observed no new data must leave the watermark exactly where it was: got %s want %s", got, stored)
		})
	}
}
