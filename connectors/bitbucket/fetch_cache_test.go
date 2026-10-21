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

// fakeFetchCache is an in-memory sdk.FetchCache for op-level tests. It
// records GetMany/PutMany traffic and applies writes to a per-cacheType store so a
// PutMany followed by a GetMany round-trips, letting a test exercise both the
// read-from-table and write-to-table paths in one run.
type fakeFetchCache struct {
	store    map[string]map[string]json.RawMessage
	getCalls int
	putCalls []putRecord
}

type putRecord struct {
	cacheType string
	entries   map[string]json.RawMessage
}

func newFakeFetchCache() *fakeFetchCache {
	return &fakeFetchCache{store: map[string]map[string]json.RawMessage{}}
}

func (f *fakeFetchCache) GetMany(_ context.Context, cacheType string, keys []string) (map[string]json.RawMessage, error) {
	f.getCalls++
	out := map[string]json.RawMessage{}
	for _, k := range keys {
		if v, ok := f.store[cacheType][k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func (f *fakeFetchCache) PutMany(_ context.Context, cacheType string, entries map[string]json.RawMessage) error {
	cp := make(map[string]json.RawMessage, len(entries))
	for k, v := range entries {
		cp[k] = v
	}
	f.putCalls = append(f.putCalls, putRecord{cacheType: cacheType, entries: cp})
	if f.store[cacheType] == nil {
		f.store[cacheType] = map[string]json.RawMessage{}
	}
	for k, v := range entries {
		f.store[cacheType][k] = v
	}
	return nil
}

func (f *fakeFetchCache) seed(cacheType, key string, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	if f.store[cacheType] == nil {
		f.store[cacheType] = map[string]json.RawMessage{}
	}
	f.store[cacheType][key] = raw
}

// putEntries flattens every PutMany call for one cacheType into a single map.
func (f *fakeFetchCache) putEntries(cacheType string) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, p := range f.putCalls {
		if p.cacheType == cacheType {
			for k, v := range p.entries {
				out[k] = v
			}
		}
	}
	return out
}

// putCallsFor returns the PutMany calls recorded for one cacheType, in order — so a
// test can assert the flush CADENCE (one call per page), not just the flattened set.
func (f *fakeFetchCache) putCallsFor(cacheType string) []putRecord {
	var out []putRecord
	for _, p := range f.putCalls {
		if p.cacheType == cacheType {
			out = append(out, p)
		}
	}
	return out
}

// withFetchCache attaches the fetch-cache capability to params. Reads and writes
// against connector_fetch_cache are unconditional whenever the capability is present.
func withFetchCache(params *sdk.ExecutionParams, fc sdk.FetchCache) {
	params.FetchCache = fc
}

// probeEntryFrom decodes a stored per-PR probe entry. Assertions go through this so
// they pin the probe's MEANING (which watermark, which covered window) rather than the
// wire encoding; the encoding itself is pinned once by TestPRProbeEntryDecodes.
func probeEntryFrom(t *testing.T, raw json.RawMessage) prProbeEntry {
	t.Helper()
	var entry prProbeEntry
	require.NoError(t, json.Unmarshal(raw, &entry))
	return entry
}

// --- fetch_prs: a cache miss re-fetches from the vendor and writes the table ---

// mergedPR is a final-state PR so both the diffstat and first-commit caches engage.
func mergedPR(now time.Time) bbPullRequest {
	return bbPullRequest{ID: 41, Title: "Fix bug", State: "MERGED", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now.Add(-time.Hour)}
}

// prsMissGateway serves the PR list plus the two enrichment endpoints (diffstat +
// PR commits) so a cache miss re-fetches. It counts diffstat calls so a test can
// assert a table hit skipped the vendor.
func prsMissGateway(now time.Time, diffstatCalls *int) *mockVendorGateway {
	return &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{mergedPR(now)}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				*diffstatCalls++
				return bbDiffstatResponse(10, 5), nil
			case *connectorpb.BitbucketListPRCommitsRequest:
				return bbPRCommitsResponse([]bbCommit{{Hash: "c1", Date: now.Add(-3 * time.Hour)}}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
}

// TestFetchPRsOp_MissWritesTable proves an empty cache is a miss: the diffstat +
// first-commit are re-fetched from the vendor and the fresh entries are upserted to
// connector_fetch_cache.
func TestFetchPRsOp_MissWritesTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const key = "myworkspace/myrepo#41"

	var diffstatCalls int
	gw := prsMissGateway(now, &diffstatCalls)
	fake := newFakeFetchCache()
	params := testParams(tu.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", Repositories: []string{"myworkspace/myrepo"}})
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, fake)

	_, err := (&FetchPRsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, 1, diffstatCalls, "empty cache is a miss — the diffstat is re-fetched")
	assert.Contains(t, fake.putEntries(CacheTypePRDiffstat), key, "the fresh diffstat entry is upserted to the table")
	assert.Contains(t, fake.putEntries(CacheTypePRFirstCommit), key, "the fresh first-commit entry is upserted to the table")
}

// TestFetchPRsOp_ServesTable proves a table hit is served without a vendor diffstat
// call and produces no dirty entry (a byte-equal re-run is a no-op), while a table
// miss falls through to the vendor.
func TestFetchPRsOp_ServesTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const key = "myworkspace/myrepo#41"

	t.Run("table hit skips vendor and produces no dirty entry", func(t *testing.T) {
		var diffstatCalls, commitCalls int
		gw := &mockVendorGateway{
			executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				switch req.(type) {
				case *connectorpb.BitbucketListPullRequestsRequest:
					return bbPRsResponse([]bbPullRequest{mergedPR(now)}, false), nil
				case *connectorpb.BitbucketGetPRDiffstatRequest:
					diffstatCalls++
					return bbDiffstatResponse(10, 5), nil
				case *connectorpb.BitbucketListPRCommitsRequest:
					commitCalls++
					return bbPRCommitsResponse([]bbCommit{{Hash: "c1", Date: now.Add(-3 * time.Hour)}}, false), nil
				default:
					return nil, fmt.Errorf("unexpected request type: %T", req)
				}
			},
		}
		fake := newFakeFetchCache()
		fake.seed(CacheTypePRDiffstat, key, PRDiffstatCacheEntry{FilesStr: `["a.go"]`, FilesStatStr: `[{"path":"a.go","additions":10,"deletions":5}]`, Additions: 10, Deletions: 5})
		fake.seed(CacheTypePRFirstCommit, key, now.Add(-3*time.Hour).Format(time.RFC3339))
		// The chained commits call answers both the date and the commit list, so both
		// entries must be warm for it to be skipped. Seeding only the date leaves the
		// list unresolved, and an unresolved list is what makes the call anyway.
		fake.seed(CacheTypePRCommitSHAs, key, "c1")

		params := testParams(tu.NewChannelSink(), gw, nil)
		params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
		withFetchCache(params, fake)

		_, err := (&FetchPRsOp{}).Execute(context.Background(), params)
		require.NoError(t, err)

		assert.Zero(t, diffstatCalls, "diffstat table hit must skip the vendor call")
		assert.Zero(t, commitCalls, "first-commit table hit must skip the vendor call")
		assert.Empty(t, fake.putEntries(CacheTypePRDiffstat), "a hit produces no dirty diffstat entry")
		assert.Empty(t, fake.putEntries(CacheTypePRFirstCommit), "a hit produces no dirty first-commit entry")
		assert.Empty(t, fake.putEntries(CacheTypePRCommitSHAs), "nor a dirty commit-list entry")
	})

	t.Run("table miss falls through to vendor", func(t *testing.T) {
		var diffstatCalls int
		gw := prsMissGateway(now, &diffstatCalls)
		fake := newFakeFetchCache() // empty → miss
		params := testParams(tu.NewChannelSink(), gw, nil)
		params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
		withFetchCache(params, fake)

		_, err := (&FetchPRsOp{}).Execute(context.Background(), params)
		require.NoError(t, err)

		assert.Equal(t, 1, diffstatCalls, "table miss must re-fetch from the vendor")
		assert.Contains(t, fake.putEntries(CacheTypePRDiffstat), key, "the miss's fresh entry must be upserted to the table")
	})
}

// --- fetch_commits: a cache miss re-fetches the sha diffstat and writes the table ---

func TestFetchCommitsOp_MissWritesTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const key = "myworkspace/myrepo#c1"
	discovered := []BitbucketDiscoveredRepo{{FullName: "myworkspace/myrepo", DefaultBranch: "main", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE}}

	var diffstatCalls int
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListCommitsRequest:
				return bbCommitsResponse([]bbCommit{{Hash: "c1", Date: now}}, false), nil
			case *connectorpb.BitbucketGetCommitDiffstatRequest:
				diffstatCalls++
				return bbCommitDiffstatResponse([]bbDiffstatEntry{{LinesAdded: 4, LinesRemoved: 1, Status: "modified"}}, ""), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	fake := newFakeFetchCache()
	params := testParams(tu.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", DiscoveredRepos: discovered})
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, fake)

	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Equal(t, 1, diffstatCalls, "commit diffstat miss must re-fetch")
	assert.Contains(t, fake.putEntries(CacheTypeCommitDiffstat), key)
}

// --- fetch_reviews / fetch_pr_comments: a miss upserts the PR's UpdatedOn watermark ---

func TestPRActivityTimestampCacheWritesTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const key = "myworkspace/myrepo#42"

	type opUnderTest struct {
		name      string
		cacheType string
		run       func(p *sdk.ExecutionParams) error
	}
	ops := []opUnderTest{
		{"fetch_reviews", CacheTypePRActivity, func(p *sdk.ExecutionParams) error {
			_, err := (&FetchReviewsOp{}).Execute(context.Background(), p)
			return err
		}},
		{"fetch_pr_comments", CacheTypePRComments, func(p *sdk.ExecutionParams) error {
			_, err := (&FetchPRCommentsOp{}).Execute(context.Background(), p)
			return err
		}},
	}

	// Gateway serves an open PR plus empty activity/comment sub-resources, so the
	// cache gate is a miss and the PR's UpdatedOn becomes a dirty timestamp.
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{{ID: 42, State: "OPEN", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now}}, false), nil
			case *connectorpb.BitbucketListPRActivityRequest:
				return bbActivityResponse(nil, false), nil
			case *connectorpb.BitbucketListPRCommentsRequest:
				return bbCommentsResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			fake := newFakeFetchCache()
			params := testParams(tu.NewChannelSink(), gw, nil)
			params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", Repositories: []string{"myworkspace/myrepo"}})
			params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
			withFetchCache(params, fake)

			require.NoError(t, op.run(params))

			put := fake.putEntries(op.cacheType)
			require.Contains(t, put, key, "timestamp dirty entry must be upserted")
			assert.Equal(t, now.Format(time.RFC3339), probeEntryFrom(t, put[key]).Watermark, "dirty value is the PR's UpdatedOn")
		})
	}
}

// TestFetchReviewsOp_ServesTable proves a primed table timestamp hit skips the
// /activity call.
func TestFetchReviewsOp_ServesTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var activityCalls int
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{{ID: 42, State: "OPEN", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now}}, false), nil
			case *connectorpb.BitbucketListPRActivityRequest:
				activityCalls++
				return bbActivityResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	fake := newFakeFetchCache()
	fake.seed(CacheTypePRActivity, "myworkspace/myrepo#42", newPRProbeEntry(now, time.Time{}))

	params := testParams(tu.NewChannelSink(), gw, nil)
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, fake)

	_, err := (&FetchReviewsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, activityCalls, "table timestamp hit must skip the /activity call")
	assert.Empty(t, fake.putEntries(CacheTypePRActivity), "a hit produces no dirty timestamp entry")
}

// TestFetchReviewsOp_PerRepoDirtyNotResent guards against write amplification: the
// activity dirty set is flushed per page and cleared on success, so a later flush
// carries only its own new entries and never re-upserts an earlier flush's (which
// would make the per-run write O(n^2) and churn the whole table each sync). Here
// each repo is a single page, so the two flushes are the two repos' entries; the
// multi-page cadence is pinned by TestFetchReviewsOp_PerPageFlush.
func TestFetchReviewsOp_PerRepoDirtyNotResent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	// Same PR id in each repo; the activity key is repo-full-name-scoped, so the
	// two repos produce distinct keys — a later flush must not re-send an earlier
	// repo's key.
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{{ID: 42, State: "OPEN", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now}}, false), nil
			case *connectorpb.BitbucketListPRActivityRequest:
				return bbActivityResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	fake := newFakeFetchCache()
	params := testParams(tu.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(BitbucketSettings{
		Workspace:    "myworkspace",
		Repositories: []string{"myworkspace/repo1", "myworkspace/repo2"},
	})
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, fake)

	_, err := (&FetchReviewsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	const repo1Key = "myworkspace/repo1#42"
	const repo2Key = "myworkspace/repo2#42"
	require.Len(t, fake.putCalls, 2, "one PutMany per page (each repo is a single page here)")
	assert.Contains(t, fake.putCalls[0].entries, repo1Key)
	assert.NotContains(t, fake.putCalls[0].entries, repo2Key)
	// repo2's flush must NOT re-carry repo1's key.
	assert.Contains(t, fake.putCalls[1].entries, repo2Key)
	assert.NotContains(t, fake.putCalls[1].entries, repo1Key, "dirty set must be cleared after each flush (no re-upsert of earlier entries)")
}

// --- per-page write-back (a hard kill mid-run keeps completed pages' work) ---

// TestFetchPRsOp_PerPageFlush proves the table write-back is per PR-page, not one
// end-of-run flush: a two-page repo yields one PutMany per page per cache_type, and
// page 1's entry is durable in the store BEFORE page 2 is fetched — so a hard kill
// mid-run keeps every completed page's work.
func TestFetchPRsOp_PerPageFlush(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	pr1 := bbPullRequest{ID: 1, Title: "p1", State: "MERGED", CreatedOn: now.Add(-3 * time.Hour), UpdatedOn: now.Add(-1 * time.Hour)}
	pr2 := bbPullRequest{ID: 2, Title: "p2", State: "MERGED", CreatedOn: now.Add(-3 * time.Hour), UpdatedOn: now.Add(-2 * time.Hour)}
	const key1, key2 = "myworkspace/myrepo#1", "myworkspace/myrepo#2"

	fake := newFakeFetchCache()
	// fetch_prs is the only op with a per-page cursor checkpoint (onPage), so assert
	// the invariant directly: the checkpoint must never fire for a page whose cache
	// write-back has not already landed. A reordering that checkpointed before the
	// flush would trip this. (Without a real Checkpoint wired, onPage is nil and this
	// ordering goes unexercised — the gap staff-judgment flagged.)
	var checkpointCalls int
	checkpoint := func(_ context.Context, _ *sdk.Cursor) error {
		checkpointCalls++
		assert.Contains(t, fake.store[CacheTypePRDiffstat], key1,
			"cursor checkpoint fired before page 1's cache write-back landed")
		return nil
	}
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				if r.GetPage() <= 1 {
					return bbPRsResponse([]bbPullRequest{pr1}, true), nil
				}
				assert.Contains(t, fake.store[CacheTypePRDiffstat], key1,
					"page 1's diffstat must be durable in the store BEFORE page 2 is fetched")
				return bbPRsResponse([]bbPullRequest{pr2}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return bbDiffstatResponse(10, 5), nil
			case *connectorpb.BitbucketListPRCommitsRequest:
				return bbPRCommitsResponse([]bbCommit{{Hash: "c", Date: now.Add(-3 * time.Hour)}}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	params := testParamsMultiRepo(tu.NewChannelSink(), gw, nil, []string{"myworkspace/myrepo"}, checkpoint)
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, fake)

	_, err := (&FetchPRsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	require.Positive(t, checkpointCalls, "the per-page cursor checkpoint must have fired (else the ordering assertion is vacuous)")

	diffstatPuts := fake.putCallsFor(CacheTypePRDiffstat)
	require.Len(t, diffstatPuts, 2, "one pr_diffstat PutMany per page (not a single end-of-run flush)")
	assert.Contains(t, diffstatPuts[0].entries, key1)
	assert.NotContains(t, diffstatPuts[0].entries, key2, "page 1's flush carries only page 1")
	assert.Contains(t, diffstatPuts[1].entries, key2)
	assert.NotContains(t, diffstatPuts[1].entries, key1, "cleared after page 1 — page 2 does not re-send it")
}

// TestFetchCommitsOp_PerPageFlush mirrors the PR proof for the sha-diffstat cache:
// a two-page commit listing flushes commit_diffstat once per page, page 1 durable
// before page 2 is fetched.
func TestFetchCommitsOp_PerPageFlush(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	discovered := []BitbucketDiscoveredRepo{{FullName: "myworkspace/myrepo", DefaultBranch: "main", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE}}
	const key1, key2 = "myworkspace/myrepo#c1", "myworkspace/myrepo#c2"

	fake := newFakeFetchCache()
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.BitbucketListCommitsRequest:
				if r.GetPage() <= 1 {
					return bbCommitsResponse([]bbCommit{{Hash: "c1", Date: now.Add(-1 * time.Hour)}}, true), nil
				}
				assert.Contains(t, fake.store[CacheTypeCommitDiffstat], key1,
					"page 1's commit diffstat must be flushed BEFORE page 2 is fetched")
				return bbCommitsResponse([]bbCommit{{Hash: "c2", Date: now.Add(-2 * time.Hour)}}, false), nil
			case *connectorpb.BitbucketGetCommitDiffstatRequest:
				return bbCommitDiffstatResponse([]bbDiffstatEntry{{LinesAdded: 4, LinesRemoved: 1, Status: "modified"}}, ""), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	params := testParams(tu.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", DiscoveredRepos: discovered})
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, fake)

	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	puts := fake.putCallsFor(CacheTypeCommitDiffstat)
	require.Len(t, puts, 2, "one commit_diffstat PutMany per page")
	assert.Contains(t, puts[0].entries, key1)
	assert.NotContains(t, puts[0].entries, key2)
	assert.Contains(t, puts[1].entries, key2)
	assert.NotContains(t, puts[1].entries, key1, "cleared after page 1")
}

// TestFetchReviewsOp_PerPageFlush pins the watermark op's per-page cadence. This
// matters most for fetch_reviews: it has NO intermediate cursor checkpoint, so the
// watermark is the sole guard against re-fetching a PR's paginated /activity — a
// per-page flush bounds a hard-kill re-fetch to one page, not the whole repo.
func TestFetchReviewsOp_PerPageFlush(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	pr1 := bbPullRequest{ID: 1, State: "OPEN", CreatedOn: now.Add(-3 * time.Hour), UpdatedOn: now.Add(-1 * time.Hour)}
	pr2 := bbPullRequest{ID: 2, State: "OPEN", CreatedOn: now.Add(-3 * time.Hour), UpdatedOn: now.Add(-2 * time.Hour)}
	const key1, key2 = "myworkspace/myrepo#1", "myworkspace/myrepo#2"

	fake := newFakeFetchCache()
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				if r.GetPage() <= 1 {
					return bbPRsResponse([]bbPullRequest{pr1}, true), nil
				}
				assert.Contains(t, fake.store[CacheTypePRActivity], key1,
					"page 1's activity watermark must be flushed BEFORE page 2 is fetched")
				return bbPRsResponse([]bbPullRequest{pr2}, false), nil
			case *connectorpb.BitbucketListPRActivityRequest:
				return bbActivityResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	params := testParams(tu.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", Repositories: []string{"myworkspace/myrepo"}})
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, fake)

	_, err := (&FetchReviewsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	puts := fake.putCallsFor(CacheTypePRActivity)
	require.Len(t, puts, 2, "one pr_activity PutMany per page")
	assert.Contains(t, puts[0].entries, key1)
	assert.NotContains(t, puts[0].entries, key2)
	assert.Contains(t, puts[1].entries, key2)
	assert.NotContains(t, puts[1].entries, key1, "cleared after page 1")
}

// TestFetchPRCommentsOp_PerPageFlush mirrors the reviews proof for the
// pr_comments watermark — same "no intermediate cursor checkpoint" shape, so the
// per-page write-back is its only in-run durability signal.
func TestFetchPRCommentsOp_PerPageFlush(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	pr1 := bbPullRequest{ID: 1, State: "OPEN", CreatedOn: now.Add(-3 * time.Hour), UpdatedOn: now.Add(-1 * time.Hour)}
	pr2 := bbPullRequest{ID: 2, State: "OPEN", CreatedOn: now.Add(-3 * time.Hour), UpdatedOn: now.Add(-2 * time.Hour)}
	const key1, key2 = "myworkspace/myrepo#1", "myworkspace/myrepo#2"

	fake := newFakeFetchCache()
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				if r.GetPage() <= 1 {
					return bbPRsResponse([]bbPullRequest{pr1}, true), nil
				}
				assert.Contains(t, fake.store[CacheTypePRComments], key1,
					"page 1's comments watermark must be flushed BEFORE page 2 is fetched")
				return bbPRsResponse([]bbPullRequest{pr2}, false), nil
			case *connectorpb.BitbucketListPRCommentsRequest:
				return bbCommentsResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	params := testParams(tu.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", Repositories: []string{"myworkspace/myrepo"}})
	params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
	withFetchCache(params, fake)

	_, err := (&FetchPRCommentsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	puts := fake.putCallsFor(CacheTypePRComments)
	require.Len(t, puts, 2, "one pr_comments PutMany per page")
	assert.Contains(t, puts[0].entries, key1)
	assert.NotContains(t, puts[0].entries, key2)
	assert.Contains(t, puts[1].entries, key2)
	assert.NotContains(t, puts[1].entries, key1, "cleared after page 1")
}
