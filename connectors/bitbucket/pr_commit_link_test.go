// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/vcs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// prLinkRun drives FetchPRsOp over one pull request whose commit page returns the given
// hashes, against a caller-supplied cache, and returns the emitted fetch_prs metadata
// alongside how many times the chained commit call was made.
func prLinkRun(t *testing.T, fake *fakeFetchCache, hashes []string) (meta []map[string]string, prCommitCalls int) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)

	commits := make([]bbCommit, 0, len(hashes))
	for _, h := range hashes {
		commits = append(commits, bbCommit{Hash: h, Date: now.Add(-2 * time.Hour)})
	}

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 42, Title: "Fix the thing", State: "MERGED", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return bbDiffstatResponse(5, 2), nil
			case *connectorpb.BitbucketListPRCommitsRequest:
				prCommitCalls++
				return bbPRCommitsResponse(commits, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.FetchCache = fake

	_, err := (&FetchPRsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	for _, r := range sink.Records() {
		if r.OperationID == "fetch_prs" {
			meta = append(meta, r.Metadata)
		}
	}
	require.NotEmpty(t, meta, "the pull request must be emitted")
	return meta, prCommitCalls
}

// TestFetchPRs_WarmDateCacheStillResolvesTheCommitLink pins a gap that
// would be permanent and silent rather than transient.
//
// first_commit_at and the commit list are answered by one chained call but stored as two
// independent cache entries. Gating that call on the date alone strands every pull request
// whose date was already cached when the commit list was first recorded — and because the
// date is immutable and its entry never expires, such a pull request would never call
// again. Its link would be permanently absent, with no error, no retry and no watermark
// hold to signal it.
//
// The set this repairs is the pull requests the incremental walk still visits: fetch_prs
// stops at the first record older than the stored watermark, so one that never updates
// again is never revisited and this gate never runs for it. Reaching those needs a
// backfill, which no change inside the walk can substitute for.
func TestFetchPRs_WarmDateCacheStillResolvesTheCommitLink(t *testing.T) {
	fake := newFakeFetchCache()
	// The state this defect needs: the date resolved on some earlier run, the list never did.
	fake.seed(CacheTypePRFirstCommit, "myworkspace/myrepo#42",
		time.Now().UTC().Add(-24*time.Hour).Format(time.RFC3339))

	meta, calls := prLinkRun(t, fake, []string{"aaaaaaaaaaaa1111", "bbbbbbbbbbbb2222"})

	assert.Equal(t, 1, calls,
		"a cold commit-list entry must still make the call, however warm the date entry is")
	assert.Equal(t, "aaaaaaaaaaaa,bbbbbbbbbbbb", meta[0][vcs.MetaKeyPRCommitSHAs],
		"the link must be recorded even though the date came from cache")
	assert.NotEmpty(t, fake.putEntries(CacheTypePRCommitSHAs),
		"and persisted, so the next run short-circuits on both entries together")
}

// TestFetchPRs_BothEntriesWarmSkipsTheCall is the other half of the same rule: once both
// are resolved the call is skipped, and the link still reaches the record from cache. A
// re-emitted pull request must not lose the link — the record upsert assigns metadata
// wholesale, so an omitted value erases what an earlier run stored.
func TestFetchPRs_BothEntriesWarmSkipsTheCall(t *testing.T) {
	fake := newFakeFetchCache()
	key := "myworkspace/myrepo#42"
	fake.seed(CacheTypePRFirstCommit, key, time.Now().UTC().Add(-24*time.Hour).Format(time.RFC3339))
	fake.seed(CacheTypePRCommitSHAs, key, "aaaaaaaaaaaa,bbbbbbbbbbbb")

	meta, calls := prLinkRun(t, fake, []string{"aaaaaaaaaaaa1111"})

	assert.Equal(t, 0, calls, "both entries warm must cost no provider request")
	assert.Equal(t, "aaaaaaaaaaaa,bbbbbbbbbbbb", meta[0][vcs.MetaKeyPRCommitSHAs],
		"the cached link must still reach the record, or the re-emit erases it")
}

// TestFetchPRs_PullRequestWithNoCommitsStopsRefetching pins the sentinel. A pull request
// the provider answered with no commits is resolved, not unattempted, so its empty entry
// must be stored — otherwise every later run repeats a call whose answer cannot change.
func TestFetchPRs_PullRequestWithNoCommitsStopsRefetching(t *testing.T) {
	fake := newFakeFetchCache()

	_, calls := prLinkRun(t, fake, nil)
	require.Equal(t, 1, calls)

	entries := fake.putEntries(CacheTypePRCommitSHAs)
	require.Contains(t, entries, "myworkspace/myrepo#42",
		"an answered call with no commits must still record its entry, or it refetches forever")
	assert.JSONEq(t, `""`, string(entries["myworkspace/myrepo#42"]),
		"and that entry is empty — resolved, with nothing to link")
}
