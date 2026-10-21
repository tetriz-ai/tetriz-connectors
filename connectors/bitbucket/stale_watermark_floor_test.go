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

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	tu "github.com/tetriz-ai/tetriz-connectors/testkit"
)

// syntheticPin builds a watermark carrying the fingerprint of a minted first-sync
// floor: aged beyond defaultLookback AND carrying sub-microsecond precision, which
// time.Now() produces and no Bitbucket vendor timestamp (microsecond-or-coarser) can.
func syntheticPin(now time.Time) time.Time {
	return now.Add(-defaultLookback - 40*24*time.Hour).Truncate(time.Second).Add(123456789 * time.Nanosecond)
}

// realAged builds a watermark equally old but with vendor precision — the shape of a
// REAL watermark on a repo whose sync was dark long enough to age past the floor.
func realAged(now time.Time) time.Time {
	return now.Add(-defaultLookback - 40*24*time.Hour).Truncate(time.Millisecond)
}

// TestPRActivityDropsPinnedWatermark pins the self-heal for the residue of synthetic
// first-sync floors minted before the pin fixes: a pinned watermark aged
// beyond defaultLookback is treated as absent — the run fetches from the ordinary
// first-sync window and the entry is not written back. Only a provable PIN drops:
// a REAL watermark equally old is kept, because dropping it would move the next
// scan's lower bound forward and silently skip the window a dark sync never consumed.
func TestPRActivityDropsPinnedWatermark(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)

	const repo = "myworkspace/myrepo"

	tests := []struct {
		name      string
		watermark time.Time
		wantKept  bool
	}{
		{name: "synthetic pin beyond the floor is dropped", watermark: syntheticPin(now), wantKept: false},
		{name: "real watermark beyond the floor is kept", watermark: realAged(now), wantKept: true},
		{name: "watermark inside the floor is kept", watermark: now.Add(-2 * 24 * time.Hour), wantKept: true},
	}

	for _, op := range prActivityOps() {
		for _, tt := range tests {
			t.Run(op.name+"/"+tt.name, func(t *testing.T) {
				t.Parallel()
				// Empty sub-resources: the run observes nothing for the repo, so the
				// only thing that can appear in the returned cursor is what the loader
				// chose to keep of the incoming watermark.
				params := testParams(tu.NewChannelSink(), prActivityGateway(now), bareCursor(t, map[string]time.Time{repo: tt.watermark}))
				withFetchCache(params, newFakeFetchCache())

				next, err := op.execute(context.Background(), params)
				require.NoError(t, err)

				got, ok := cursorTime(t, next, repo)
				if tt.wantKept {
					require.True(t, ok, "a non-pinned watermark must survive a run that observed nothing")
					assert.WithinDuration(t, tt.watermark, got, time.Second)
					return
				}
				assert.False(t, ok,
					"a pinned watermark beyond the first-sync floor must be dropped, not carried; got %s", got)
			})
		}
	}
}

// TestFetchPRsDropsPinnedWatermark pins the same contract end-to-end for the
// struct-shaped fetch_prs cursor, including the write side: a dropped pin must not be
// re-minted by the empty iteration (latestUpdated collapses to the synthetic `since`
// floor, which must not be persisted as observed progress), and the nothing-to-write
// fallback must not resurrect the incoming cursor.
func TestFetchPRsDropsPinnedWatermark(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	const repo = "myworkspace/myrepo"

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	entry, err := json.Marshal(repoCursorState{Timestamp: syntheticPin(now)})
	require.NoError(t, err)
	params := testParams(tu.NewChannelSink(), gw, sdk.NewCompositeCursor(map[string]json.RawMessage{repo: entry}))
	withFetchCache(params, newFakeFetchCache())

	next, execErr := (&FetchPRsOp{}).Execute(context.Background(), params)
	require.NoError(t, execErr)

	require.NotNil(t, next, "the erasure must persist as an empty cursor, not fall back to the incoming one")
	parts, partsErr := sdk.CompositeFromCursor(next)
	require.NoError(t, partsErr)
	assert.NotContains(t, parts, repo,
		"a dropped pin must stay dropped: an empty iteration must not re-mint the synthetic floor")
}

// TestFetchCommitsDropsPinnedWatermark covers the bare-timestamp op that seeds its
// watermark from `since`: the drop must not be undone by writing the fresh floor back
// on an empty iteration.
func TestFetchCommitsDropsPinnedWatermark(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	const repo = "myworkspace/myrepo"

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListCommitsRequest:
				return &connectorpb.BitbucketListCommitsResponse{}, nil
			case *connectorpb.BitbucketGetRepoRequest:
				return &connectorpb.BitbucketGetRepoResponse{Repo: []byte(`{"mainbranch":{"name":"main"}}`)}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	params := testParams(tu.NewChannelSink(), gw, bareCursor(t, map[string]time.Time{repo: syntheticPin(now)}))
	withFetchCache(params, newFakeFetchCache())

	next, execErr := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, execErr)

	require.NotNil(t, next, "the erasure must persist as an empty cursor, not fall back to the incoming one")
	parts, partsErr := sdk.CompositeFromCursor(next)
	require.NoError(t, partsErr)
	assert.NotContains(t, parts, repo,
		"a dropped pin must stay dropped: an empty iteration must not re-mint the synthetic floor")
}

// TestLoadCursorStatesDropsPinnedEntries pins the loader rules for the struct-shaped
// fetch_prs / fetch_prs_data cursor. Only a PLAIN pinned entry is dropped: resume
// state (PageCursor) or a repair floor (HoldFloor) marks an iteration whose
// bookkeeping is live, and a REAL aged watermark is never dropped at all.
func TestLoadCursorStatesDropsPinnedEntries(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	pin := syntheticPin(now)
	fresh := now.Add(-3 * 24 * time.Hour)

	entry := func(t *testing.T, v any) json.RawMessage {
		t.Helper()
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		return raw
	}

	tests := []struct {
		name        string
		raw         json.RawMessage
		wantKept    bool
		wantDropped bool
	}{
		{name: "pinned plain struct entry is dropped", raw: entry(t, repoCursorState{Timestamp: pin}), wantDropped: true},
		{name: "real aged watermark is kept", raw: entry(t, repoCursorState{Timestamp: realAged(now)}), wantKept: true},
		{name: "pinned entry mid-pagination is kept", raw: entry(t, repoCursorState{Timestamp: pin, PageCursor: "3", Since: fresh}), wantKept: true},
		{name: "pinned entry under a repair hold is kept", raw: entry(t, repoCursorState{Timestamp: pin, HoldFloor: pin.Add(24 * time.Hour)}), wantKept: true},
		{name: "fresh struct entry is kept", raw: entry(t, repoCursorState{Timestamp: fresh}), wantKept: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const repo = "myworkspace/myrepo"
			cursor := sdk.NewCompositeCursor(map[string]json.RawMessage{repo: tt.raw})

			states, droppedPinned := loadCursorStates(context.Background(), cursor)
			_, kept := states[repo]
			assert.Equal(t, tt.wantKept, kept)
			assert.Equal(t, tt.wantDropped, droppedPinned,
				"droppedPinned must report exactly the deliberate erasures, so the nothing-to-write fallback cannot resurrect them")
		})
	}
}

// TestCarryForwardDropsPinnedWatermarks pins the write-back half for the
// bare-timestamp ops: a pinned entry must not be re-emitted for a repo the run does
// not visit, or the carry contract re-pins what the loader dropped — and the drop is
// reported, so an all-dropped cursor is persisted as empty rather than resurrected by
// the nothing-to-write fallback.
func TestCarryForwardDropsPinnedWatermarks(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	fresh := now.Add(-6 * time.Hour)

	cursor := bareCursor(t, map[string]time.Time{
		"myworkspace/pinned": syntheticPin(now),
		"myworkspace/aged":   realAged(now),
		"myworkspace/recent": fresh,
	})

	carried, droppedPinned := carryForwardCursorTimes(context.Background(), cursor,
		[]string{"myworkspace/pinned", "myworkspace/aged", "myworkspace/recent"})

	assert.True(t, droppedPinned)
	assert.NotContains(t, carried, "myworkspace/pinned")
	assert.Contains(t, carried, "myworkspace/aged", "a real watermark is never dropped, however old")
	require.Contains(t, carried, "myworkspace/recent")
	assert.WithinDuration(t, fresh, carried["myworkspace/recent"], time.Second)
}
