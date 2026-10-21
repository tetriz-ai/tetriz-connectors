// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// The invariant these tests guard: a PR whose enrichment could not be resolved
// must never be persisted with zeroed file stats AND skipped by the cursor. That
// combination is unrecoverable — the row reads as a genuinely empty PR and no
// later run revisits it, because `since` has already moved past its updated_on.
//
// The PR is still EMITTED on enrichment failure — that is deliberate elsewhere
// (TestFetchPRsOp_EnrichmentFailureRetainsOriginalPayload,
// TestFetchPRsOp_CommitsFetchFailureDoesNotDropPR): present-with-zeros beats
// missing. What these tests pin is that the zeros are TRANSIENT — the cursor
// holds so the next run repairs them.
//
// Bitbucket pages newest→oldest, so the fixtures below put the failing PR at the
// OLDER timestamp: the watermark must be pulled back to it even though a newer
// PR in the same page enriched cleanly.

// enrichFailFixture drives a single-repo executor where the diffstat and/or
// PR-commits call fails for one specific PR id.
type enrichFailFixture struct {
	prs               []bbPullRequest
	diffstatFailPRID  int64
	diffstatFailErr   error
	prCommitsFailPRID int64
	prCommitsFailErr  error
	commits           []bbCommit
}

func enrichFailExecutor(fixture enrichFailFixture) func(context.Context, proto.Message) (proto.Message, error) {
	return func(_ context.Context, request proto.Message) (proto.Message, error) {
		switch typedRequest := request.(type) {
		case *connectorpb.BitbucketListPullRequestsRequest:
			return bbPRsResponse(fixture.prs, false), nil
		case *connectorpb.BitbucketGetPRDiffstatRequest:
			if fixture.diffstatFailErr != nil && typedRequest.GetPrId() == fixture.diffstatFailPRID {
				return nil, fixture.diffstatFailErr
			}
			return bbDiffstatResponse(10, 5), nil
		case *connectorpb.BitbucketListPRCommitsRequest:
			if fixture.prCommitsFailErr != nil && typedRequest.GetPrId() == fixture.prCommitsFailPRID {
				return nil, fixture.prCommitsFailErr
			}
			return bbPRCommitsResponse(fixture.commits, false), nil
		default:
			return nil, fmt.Errorf("unexpected request type: %T", request)
		}
	}
}

func TestFetchPRsOp_DegradedEnrichmentHoldsCursor(t *testing.T) {
	t.Parallel()

	newer := time.Now().UTC().Truncate(time.Second)
	older := newer.Add(-2 * time.Hour)
	created := newer.Add(-24 * time.Hour)
	commitAt := newer.Add(-20 * time.Hour)

	// Newest-first, matching Bitbucket's ?sort=-updated_on ordering.
	prs := []bbPullRequest{
		{ID: 200, State: "OPEN", CreatedOn: created, UpdatedOn: newer},
		{ID: 100, State: "OPEN", CreatedOn: created, UpdatedOn: older},
	}

	rateLimited := &sdk.SyncError{
		ConnectorID: "bitbucket",
		OperationID: "fetch_prs",
		Kind:        sdk.ErrKindRateLimit,
		Cause:       errors.New("rate limit exceeded for group bitbucket-rest"),
		Retryable:   true,
	}
	notFound := &sdk.SyncError{
		ConnectorID: "bitbucket",
		OperationID: "fetch_prs",
		Kind:        sdk.ErrKindNotFound,
		Cause:       errors.New("diffstat gone"),
	}

	// Every case emits both PRs — the discriminator is the CURSOR.
	allEmitted := []string{
		"myworkspace/myrepo#PR-200#opened", "myworkspace/myrepo#PR-200#snapshot",
		"myworkspace/myrepo#PR-100#opened", "myworkspace/myrepo#PR-100#snapshot",
	}

	testCases := []struct {
		name                 string
		fixture              enrichFailFixture
		wantCursor           time.Time
		wantPR100Additions   *int // nil = key absent (enrichment failed)
		wantPR100FirstCommit bool
		reason               string
	}{
		{
			name: "rate-limited diffstat on the older PR holds the cursor at that PR",
			fixture: enrichFailFixture{
				prs:              prs,
				diffstatFailPRID: 100,
				diffstatFailErr:  rateLimited,
				commits:          []bbCommit{{Hash: "c1", Date: commitAt}},
			},
			wantCursor:           older,
			wantPR100Additions:   nil, // original payload retained this run…
			wantPR100FirstCommit: true,
			reason:               "cursor must not pass PR-100, whose zeroed stats are indistinguishable from an empty PR",
		},
		{
			name: "rate-limited PR-commits on the older PR holds the cursor at that PR",
			fixture: enrichFailFixture{
				prs:               prs,
				prCommitsFailPRID: 100,
				prCommitsFailErr:  rateLimited,
				commits:           []bbCommit{{Hash: "c1", Date: commitAt}},
			},
			wantCursor:           older,
			wantPR100Additions:   intPtr(10),
			wantPR100FirstCommit: false,
			reason:               "first_commit_at is only written on first success, so a transient miss must be retried",
		},
		{
			name: "non-transient diffstat failure lets the cursor advance",
			fixture: enrichFailFixture{
				prs:              prs,
				diffstatFailPRID: 100,
				diffstatFailErr:  notFound,
				commits:          []bbCommit{{Hash: "c1", Date: commitAt}},
			},
			wantCursor:           newer,
			wantPR100Additions:   nil,
			wantPR100FirstCommit: true,
			reason:               "a permanent per-PR failure would pin the repo and re-walk it every tick if it held the cursor",
		},
		{
			// An untyped error is usually a code-level bug (unexpected response
			// type, malformed payload) that recurs on every tick. Holding for it
			// would pin the repo and replay the window forever — the exact API
			// burn this change exists to stop.
			name: "untyped (non-SyncError) enrichment failure lets the cursor advance",
			fixture: enrichFailFixture{
				prs:              prs,
				diffstatFailPRID: 100,
				diffstatFailErr:  errors.New("unexpected response type *connectorv1.SomethingElse"),
				commits:          []bbCommit{{Hash: "c1", Date: commitAt}},
			},
			wantCursor:           newer,
			wantPR100Additions:   nil,
			wantPR100FirstCommit: true,
			reason:               "an untyped failure recurs every tick; holding the cursor would replay the window forever",
		},
		{
			name: "clean enrichment advances the cursor to the newest PR",
			fixture: enrichFailFixture{
				prs:     prs,
				commits: []bbCommit{{Hash: "c1", Date: commitAt}},
			},
			wantCursor:           newer,
			wantPR100Additions:   intPtr(10),
			wantPR100FirstCommit: true,
			reason:               "baseline: no degradation means no hold",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			gateway := &mockVendorGateway{executeFunc: enrichFailExecutor(testCase.fixture)}
			sink := testkit.NewChannelSink()
			params := testParams(sink, gateway, nil)

			op := &FetchPRsOp{}
			cursor, err := op.Execute(context.Background(), params)
			require.NoError(t, err, "per-PR enrichment failures must not fail the whole repo")
			require.NotNil(t, cursor)

			// The effective lower bound of the NEXT run: the watermark, pulled back by
			// a repair floor when one is set. Timestamp itself is never clamped.
			state := existingCursorStates(context.Background(), cursor)["myworkspace/myrepo"]
			effective := applyRepairFloor(state.Timestamp, state.HoldFloor)
			assert.Equal(t, testCase.wantCursor, effective.UTC(), testCase.reason)

			emitted := make([]string, 0, len(sink.Records()))
			var pr100 *sdk.RawRecord
			for _, record := range sink.Records() {
				emitted = append(emitted, record.ExternalID)
				if record.ExternalID == "myworkspace/myrepo#PR-100#snapshot" {
					pr100 = record
				}
			}
			assert.ElementsMatch(t, allEmitted, emitted,
				"a failed enrichment must not drop the PR — only hold the cursor")

			require.NotNil(t, pr100)
			var payload map[string]any
			require.NoError(t, json.Unmarshal(pr100.Payload, &payload))
			// On enrichment failure enrichPR hands back the ORIGINAL payload, so
			// injectLineCounts never runs and the key is absent entirely — that
			// absence, not a zero, is the "degraded" signal downstream.
			if testCase.wantPR100Additions == nil {
				assert.NotContains(t, payload, "additions",
					"failed enrichment must leave the original payload untouched: "+testCase.reason)
			} else {
				assert.EqualValues(t, *testCase.wantPR100Additions, payload["additions"], testCase.reason)
			}
			_, hasFirstCommit := pr100.Metadata["first_commit_at"]
			assert.Equal(t, testCase.wantPR100FirstCommit, hasFirstCommit, testCase.reason)
		})
	}
}

// A held cursor is only useful if the retry actually repairs the row, so drive
// two runs against the same op: run 1 is rate-limited on the older PR, run 2
// succeeds and must emit it with real stats.
func TestFetchPRsOp_DegradedPRRepairedOnNextRun(t *testing.T) {
	t.Parallel()

	newer := time.Now().UTC().Truncate(time.Second)
	older := newer.Add(-2 * time.Hour)
	created := newer.Add(-24 * time.Hour)

	prs := []bbPullRequest{
		{ID: 200, State: "OPEN", CreatedOn: created, UpdatedOn: newer},
		{ID: 100, State: "OPEN", CreatedOn: created, UpdatedOn: older},
	}
	rateLimited := &sdk.SyncError{
		ConnectorID: "bitbucket", OperationID: "fetch_prs",
		Kind: sdk.ErrKindRateLimit, Cause: errors.New("429"), Retryable: true,
	}

	// Run 1: PR-100's diffstat is rate-limited.
	gateway1 := &mockVendorGateway{executeFunc: enrichFailExecutor(enrichFailFixture{
		prs: prs, diffstatFailPRID: 100, diffstatFailErr: rateLimited,
		commits: []bbCommit{{Hash: "c1", Date: created}},
	})}
	sink1 := testkit.NewChannelSink()
	op := &FetchPRsOp{}
	cursor1, err := op.Execute(context.Background(), testParams(sink1, gateway1, nil))
	require.NoError(t, err)
	run1 := existingCursorStates(context.Background(), cursor1)["myworkspace/myrepo"]
	require.Equal(t, older, run1.HoldFloor.UTC(),
		"run 1 must set a repair floor at the degraded PR")

	// Run 2: same cursor, limiter has recovered. PR-100 is back in range and
	// enriches cleanly.
	gateway2 := &mockVendorGateway{executeFunc: enrichFailExecutor(enrichFailFixture{
		prs: prs, commits: []bbCommit{{Hash: "c1", Date: created}},
	})}
	sink2 := testkit.NewChannelSink()
	cursor2, err := op.Execute(context.Background(), testParams(sink2, gateway2, cursor1))
	require.NoError(t, err)

	repaired := false
	for _, record := range sink2.Records() {
		if record.ExternalID == "myworkspace/myrepo#PR-100#snapshot" {
			repaired = true
			// injectLineCounts writes the resolved diffstat aggregates onto the
			// payload; a downstream consumer reads lines from there. Zeroes
			// here would mean the empty-default path had run again.
			var payload map[string]any
			require.NoError(t, json.Unmarshal(record.Payload, &payload))
			assert.EqualValues(t, 10, payload["additions"],
				"the retry must persist real diffstat aggregates, not the empty defaults")
			assert.EqualValues(t, 5, payload["deletions"])
		}
	}
	assert.True(t, repaired, "PR-100 must be re-fetched and emitted on the run after the throttle clears")
	run2 := existingCursorStates(context.Background(), cursor2)["myworkspace/myrepo"]
	assert.Equal(t, newer, run2.Timestamp.UTC(), "once nothing is degraded the cursor advances to the newest PR")
	assert.True(t, run2.HoldFloor.IsZero(), "and the repair floor is cleared")
}

func TestHoldWatermarkAndRepairFloor(t *testing.T) {
	t.Parallel()

	base := time.Now().UTC().Truncate(time.Second)
	earlier := base.Add(-time.Hour)

	t.Run("holdWatermark keeps the oldest non-zero candidate", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, base, holdWatermark(time.Time{}, base), "first candidate seeds the marker")
		assert.Equal(t, earlier, holdWatermark(base, earlier), "older candidate wins")
		assert.Equal(t, earlier, holdWatermark(earlier, base), "newer candidate does not move it")
		assert.Equal(t, base, holdWatermark(base, time.Time{}),
			"a zero updated_on must not pin the cursor at the epoch and force a full re-walk")
		assert.True(t, holdWatermark(time.Time{}, time.Time{}).IsZero())
	})

	t.Run("applyRepairFloor pulls the lower bound back to the floor", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, base, applyRepairFloor(base, time.Time{}), "no floor leaves the bound alone")
		assert.Equal(t, earlier, applyRepairFloor(base, earlier), "bound pulled back to the floor")
		assert.Equal(t, earlier, applyRepairFloor(earlier, base),
			"a floor newer than the bound cannot push it forward")
		assert.Equal(t, base, applyRepairFloor(base, base), "equal values need no adjustment")
	})
}

func intPtr(value int) *int { return &value }

// The hold MUST be bounded. ErrKindRateLimit is raised by our own local token bucket
// as well as by a vendor 429, and a locally-throttled org never self-clears — so an
// unbounded hold pins the repo while the re-walked window grows every tick, needing
// more budget and deepening the throttle it is riding out. After
// maxConsecutiveCursorHolds the cursor must advance instead, leaving a gap that
// a cursor rewind can repair.
func TestApplyCursorHold_BoundsTheHold(t *testing.T) {
	t.Parallel()

	watermark := time.Now().UTC().Truncate(time.Second)
	degraded := watermark.Add(-2 * time.Hour)

	t.Run("holds and counts up to the limit, then releases", func(t *testing.T) {
		t.Parallel()

		state := repoCursorState{Timestamp: watermark}
		for hold := 1; hold <= maxConsecutiveCursorHolds; hold++ {
			require.True(t, applyCursorHold(context.Background(), "ws/a", &state, degraded), "hold %d must modify the state", hold)
			assert.Equal(t, degraded, state.HoldFloor.UTC(), "hold %d must set the repair floor", hold)
			assert.Equal(t, watermark, state.Timestamp.UTC(), "hold %d must NOT touch the watermark", hold)
			assert.Equal(t, hold, state.HoldCount)
		}

		// One more run: the limit is reached, so the floor must be RELEASED.
		assert.True(t, applyCursorHold(context.Background(), "ws/a", &state, degraded))
		assert.True(t, state.HoldFloor.IsZero(),
			"at the limit the repair floor must be dropped — a stuck repo is worse than a repairable gap")
		assert.Zero(t, state.HoldCount, "the streak resets after giving up")
		assert.Equal(t, degraded, state.HoldReleasedAt.UTC(), "the abandoned point is recorded")

		// And it must NOT restart a streak on the same PR next tick.
		assert.False(t, applyCursorHold(context.Background(), "ws/a", &state, degraded),
			"re-holding the abandoned point would cycle 0 -> limit -> 0 forever")
	})

	t.Run("a clean run breaks the streak", func(t *testing.T) {
		t.Parallel()

		state := repoCursorState{Timestamp: watermark, HoldCount: 3, HoldFloor: degraded}
		assert.True(t, applyCursorHold(context.Background(), "ws/a", &state, time.Time{}))
		assert.Zero(t, state.HoldCount, "nothing degraded means the hold streak is broken")
		assert.True(t, state.HoldFloor.IsZero(), "a clean run clears the repair floor")
		assert.Equal(t, watermark, state.Timestamp.UTC(), "a clean run must not move the watermark")
	})

	t.Run("no degradation and no streak is a no-op", func(t *testing.T) {
		t.Parallel()

		state := repoCursorState{Timestamp: watermark}
		assert.False(t, applyCursorHold(context.Background(), "ws/a", &state, time.Time{}))
		assert.Equal(t, watermark, state.Timestamp.UTC())
	})

	t.Run("a degraded PR newer than the watermark cannot push the cursor forward", func(t *testing.T) {
		t.Parallel()

		state := repoCursorState{Timestamp: degraded}
		assert.True(t, applyCursorHold(context.Background(), "ws/a", &state, watermark))
		assert.Equal(t, degraded, state.Timestamp.UTC(), "the watermark is never rewritten by a hold")
		assert.Equal(t, 1, state.HoldCount)
	})
}

// The per-page checkpoint must persist the TRUE high-water mark. Clamping it there
// broke the priorWatermark monotonic invariant: a resumed run walks only OLDER pages,
// so a clamped Timestamp became the ceiling and the interrupted run's real progress
// was lost, replaying that window forever.
func TestFetchPRsOp_PerPageCheckpointKeepsTrueWatermark(t *testing.T) {
	t.Parallel()

	newer := time.Now().UTC().Truncate(time.Second)
	older := newer.Add(-2 * time.Hour)
	created := newer.Add(-24 * time.Hour)

	rateLimited := &sdk.SyncError{
		ConnectorID: "bitbucket", OperationID: "fetch_prs",
		Kind: sdk.ErrKindRateLimit, Cause: errors.New("429"), Retryable: true,
	}
	gateway := &mockVendorGateway{executeFunc: enrichFailExecutor(enrichFailFixture{
		prs: []bbPullRequest{
			{ID: 200, State: "OPEN", CreatedOn: created, UpdatedOn: newer},
			{ID: 100, State: "OPEN", CreatedOn: created, UpdatedOn: older},
		},
		diffstatFailPRID: 100, diffstatFailErr: rateLimited,
		commits: []bbCommit{{Hash: "c1", Date: created}},
	})}

	recorder := &checkpointRecorder{}
	sink := testkit.NewChannelSink()
	params := testParamsMultiRepo(sink, gateway, nil, []string{"myworkspace/myrepo"}, recorder.fn)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotEmpty(t, recorder.calls, "the per-page checkpoint must have fired")

	// The FIRST checkpoint is the per-page one, written from inside fetchPRsForRepo.
	perPage := decodedRepoEntries(t, recorder.calls[0])["myworkspace/myrepo"]
	assert.Equal(t, newer, perPage.UTC(),
		"the per-page checkpoint must carry the true high-water mark, not the hold — a resume treats it as priorWatermark")

	// The final write keeps the TRUE watermark and expresses the hold as a floor.
	final := existingCursorStates(context.Background(), recorder.calls[len(recorder.calls)-1])["myworkspace/myrepo"]
	assert.Equal(t, newer, final.Timestamp.UTC(), "Timestamp must stay the true high-water mark")
	assert.Equal(t, older, final.HoldFloor.UTC(), "the hold is expressed as a repair floor, not a clamped watermark")
}

// --- throttle-faithful harness ---
//
// The tests above all run with a NIL Checkpoint and a single page, so they only ever
// exercise the clean-return path. That path does not reach the DB when a run fails:
// orchestrator.go sends execErr to failRun and never persists Execute's returned
// cursor. Under a real throttle the repo-level LIST call is itself rate-limited, so
// the ONLY write that lands is the per-page checkpoint — which is exactly the write
// these tests pin.

// pagedFailFixture serves page 1 from prs (with a next-page link), fails the page-2
// LIST call, and fails one PR's diffstat — the shape of a throttled run.
type pagedFailFixture struct {
	page1      []bbPullRequest
	failPRID   int64
	enrichErr  error
	listPage2  error
	commitDate time.Time
}

func pagedFailExecutor(f pagedFailFixture) func(context.Context, proto.Message) (proto.Message, error) {
	return func(_ context.Context, request proto.Message) (proto.Message, error) {
		switch typed := request.(type) {
		case *connectorpb.BitbucketListPullRequestsRequest:
			if typed.GetPage() >= 2 {
				return nil, f.listPage2
			}
			return bbPRsResponse(f.page1, true), nil // hasNext: page 2 follows
		case *connectorpb.BitbucketGetPRDiffstatRequest:
			if f.enrichErr != nil && typed.GetPrId() == f.failPRID {
				return nil, f.enrichErr
			}
			return bbDiffstatResponse(10, 5), nil
		case *connectorpb.BitbucketListPRCommitsRequest:
			return bbPRCommitsResponse([]bbCommit{{Hash: "c1", Date: f.commitDate}}, false), nil
		default:
			return nil, fmt.Errorf("unexpected request type: %T", request)
		}
	}
}

func rateLimitErr() *sdk.SyncError {
	return &sdk.SyncError{
		ConnectorID: "bitbucket", OperationID: "fetch_prs",
		Kind: sdk.ErrKindRateLimit, Cause: errors.New("rate limit exceeded for group bitbucket-rest"),
		Retryable: true,
	}
}

// runThrottledTick drives one Execute against a throttled multi-page fixture and
// returns the LAST cursor the Checkpoint actually persisted — which, on a failing
// run, is the only state that survives.
func runThrottledTick(t *testing.T, inputCursor *sdk.Cursor, page1 []bbPullRequest, failPRID int64, commitDate time.Time) (*sdk.Cursor, error) {
	t.Helper()

	gateway := &mockVendorGateway{executeFunc: pagedFailExecutor(pagedFailFixture{
		page1: page1, failPRID: failPRID, enrichErr: rateLimitErr(),
		listPage2: rateLimitErr(), commitDate: commitDate,
	})}
	recorder := &checkpointRecorder{}
	params := testParamsMultiRepo(testkit.NewChannelSink(), gateway, inputCursor,
		[]string{"myworkspace/myrepo"}, recorder.fn)

	op := &FetchPRsOp{}
	_, execErr := op.Execute(context.Background(), params)
	if len(recorder.calls) == 0 {
		return nil, execErr
	}
	return recorder.calls[len(recorder.calls)-1], execErr
}

// The hold must survive a run that FAILS mid-pagination. Execute's returned cursor is
// discarded by the orchestrator on error, so if the hold is not in the persisted
// per-page checkpoint it is lost — and the degraded PR sits permanently behind the
// cursor, which is the whole defect this change exists to prevent.
func TestFetchPRsOp_HoldSurvivesMidPaginationFailure(t *testing.T) {
	t.Parallel()

	newer := time.Now().UTC().Truncate(time.Second)
	older := newer.Add(-2 * time.Hour)
	created := newer.Add(-24 * time.Hour)

	page1 := []bbPullRequest{
		{ID: 200, State: "OPEN", CreatedOn: created, UpdatedOn: newer},
		{ID: 100, State: "OPEN", CreatedOn: created, UpdatedOn: older},
	}

	persisted, execErr := runThrottledTick(t, nil, page1, 100, created)
	require.Error(t, execErr, "the page-2 LIST failure must fail the run")
	require.NotNil(t, persisted, "the failing run must still persist the hold")

	state := existingCursorStates(context.Background(), persisted)["myworkspace/myrepo"]
	assert.Equal(t, older, state.HoldFloor.UTC(),
		"the PERSISTED state must carry a repair floor at the degraded PR — Execute's return is dropped on error, so only this write survives")
}

// The bound counts consecutive holds, so the streak must survive the same failing-run
// path. If a per-page write drops it, the streak resets every tick and the give-up
// limit is unreachable — leaving the unbounded pin the bound exists to prevent.
func TestFetchPRsOp_HoldStreakSurvivesMidPaginationFailure(t *testing.T) {
	t.Parallel()

	newer := time.Now().UTC().Truncate(time.Second)
	older := newer.Add(-2 * time.Hour)
	created := newer.Add(-24 * time.Hour)
	page1 := []bbPullRequest{
		{ID: 200, State: "OPEN", CreatedOn: created, UpdatedOn: newer},
		{ID: 100, State: "OPEN", CreatedOn: created, UpdatedOn: older},
	}

	// Two consecutive throttled ticks, feeding each run the cursor the last one left.
	first, _ := runThrottledTick(t, nil, page1, 100, created)
	require.NotNil(t, first)
	second, _ := runThrottledTick(t, first, page1, 100, created)
	require.NotNil(t, second, "a resumed run that fails before completing a page must still persist the hold")

	state := existingCursorStates(context.Background(), second)["myworkspace/myrepo"]
	assert.Equal(t, 2, state.HoldCount,
		"two consecutive held runs must count 2; a per-page write that rebuilds the state without the streak resets it every tick and the bound never fires")
}

// A repair floor must OUTRANK a mid-pagination resume, or the hold silently
// false-clears.
//
// The failure path persists the floor and the page checkpoint together. So a run that
// degrades a PR on page 1 and then fails on page 2 leaves PageCursor="2". If the next
// run honours that page cursor it resumes AT page 2, never re-reads page 1, finishes
// clean, reports no degradation — and applyCursorHold clears the floor as if the PR
// had been repaired. The degraded PR is then permanently behind the cursor: the exact
// defect this whole change exists to prevent, reintroduced through the resume path.
func TestFetchPRsOp_RepairFloorOutranksMidPaginationResume(t *testing.T) {
	t.Parallel()

	newer := time.Now().UTC().Truncate(time.Second)
	older := newer.Add(-2 * time.Hour)
	created := newer.Add(-24 * time.Hour)
	page1 := []bbPullRequest{
		{ID: 200, State: "OPEN", CreatedOn: created, UpdatedOn: newer},
		{ID: 100, State: "OPEN", CreatedOn: created, UpdatedOn: older},
	}

	// Run 1: PR-100 on page 1 is rate-limited, then the page-2 LIST fails.
	first, execErr := runThrottledTick(t, nil, page1, 100, created)
	require.Error(t, execErr)
	require.NotNil(t, first)
	firstState := existingCursorStates(context.Background(), first)["myworkspace/myrepo"]
	require.Equal(t, older, firstState.HoldFloor.UTC(), "run 1 must hold a floor at PR-100")
	require.NotEmpty(t, firstState.PageCursor, "run 1 must also leave a mid-pagination page cursor — that collision is the point")

	// Run 2: nothing is throttled. Record which PRs actually get re-enriched.
	// No lock: operations.go has no errgroup — Execute walks repos, pages and PRs
	// sequentially, so the gateway is only ever called from this goroutine.
	var diffstatFor []int64
	gateway := &mockVendorGateway{executeFunc: func(_ context.Context, request proto.Message) (proto.Message, error) {
		switch typed := request.(type) {
		case *connectorpb.BitbucketListPullRequestsRequest:
			if typed.GetPage() >= 2 {
				return bbPRsResponse(nil, false), nil
			}
			return bbPRsResponse(page1, true), nil
		case *connectorpb.BitbucketGetPRDiffstatRequest:
			diffstatFor = append(diffstatFor, typed.GetPrId())
			return bbDiffstatResponse(10, 5), nil
		case *connectorpb.BitbucketListPRCommitsRequest:
			return bbPRCommitsResponse([]bbCommit{{Hash: "c1", Date: created}}, false), nil
		default:
			return nil, fmt.Errorf("unexpected request type: %T", request)
		}
	}}
	params := testParamsMultiRepo(testkit.NewChannelSink(), gateway, first,
		[]string{"myworkspace/myrepo"}, nil)
	op := &FetchPRsOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	// The load-bearing assertion: page 1 was re-walked, so the degraded PR was
	// genuinely re-enriched. Resuming at page 2 would leave this empty.
	assert.Contains(t, diffstatFor, int64(100),
		"the degraded PR sits on page 1; honouring the stale page cursor would skip it and the floor would clear as if repaired")

	state := existingCursorStates(context.Background(), cursor)["myworkspace/myrepo"]
	assert.True(t, state.HoldFloor.IsZero(), "the floor clears only because the PR was actually repaired")
	assert.Empty(t, state.PageCursor, "a clean finish clears the resume state")
	assert.Equal(t, newer, state.Timestamp.UTC(), "and the watermark advances to the newest PR")
}

// Giving up must actually move the cursor. When the degraded PR IS the newest one,
// clamping to it is a no-op, so a release that only zeroes the counter leaves the
// repo re-walking and re-degrading the same PR forever — the pin, with a reset count.
func TestApplyCursorHold_ReleaseAdvancesEvenWhenDegradedIsNewest(t *testing.T) {
	t.Parallel()

	watermark := time.Now().UTC().Truncate(time.Second)

	state := repoCursorState{Timestamp: watermark, HoldFloor: watermark, HoldCount: maxConsecutiveCursorHolds}
	applyCursorHold(context.Background(), "ws/a", &state, watermark)

	assert.True(t, state.HoldFloor.IsZero(),
		"on release the repair floor must be dropped so the next run is not pulled back to the same PR")
	assert.Equal(t, watermark, state.HoldReleasedAt.UTC())
	assert.Zero(t, state.HoldCount)
	// The decisive property: it must not re-hold the same point.
	assert.False(t, applyCursorHold(context.Background(), "ws/a", &state, watermark),
		"re-holding the abandoned PR re-walks and re-degrades it every tick — the pin, with a reset counter")
}

// An aggregate can be INCOMPLETE while enrichPR returns nil: an unparseable diffstat
// entry is skipped, or pagination hits its cap. Two behaviours are pinned here, and
// the split between them is the point.
//
// It must NOT be cached: a final-state cache HIT would re-serve the under-count on
// every later fetch, so a fix to the parser or the page cap could never take effect.
//
// It must NOT hold the cursor: both causes are deterministic, so a re-fetch degrades
// identically. Holding would replay the window until the bound gives up and then raise
// an Error that no retry can clear — the same livelock
// isRetryableEnrichFailure refuses untyped errors to avoid.
func TestFetchPRsOp_NilErrorDegradationSkipsCacheWithoutHoldingCursor(t *testing.T) {
	t.Parallel()

	newer := time.Now().UTC().Truncate(time.Second)
	older := newer.Add(-2 * time.Hour)
	created := newer.Add(-24 * time.Hour)

	// PR-100 (MERGED, so cache-eligible) gets a diffstat page carrying one entry that
	// will not unmarshal — enrichPR skips it and returns nil.
	gateway := &mockVendorGateway{executeFunc: func(_ context.Context, request proto.Message) (proto.Message, error) {
		switch typed := request.(type) {
		case *connectorpb.BitbucketListPullRequestsRequest:
			return bbPRsResponse([]bbPullRequest{
				{ID: 200, State: "OPEN", CreatedOn: created, UpdatedOn: newer},
				{ID: 100, State: "MERGED", CreatedOn: created, UpdatedOn: older},
			}, false), nil
		case *connectorpb.BitbucketGetPRDiffstatRequest:
			if typed.GetPrId() == 100 {
				return &connectorpb.BitbucketGetPRDiffstatResponse{
					DiffstatEntries: [][]byte{[]byte(`{"lines_added": "not-a-number"}`)},
				}, nil
			}
			return bbDiffstatResponse(10, 5), nil
		case *connectorpb.BitbucketListPRCommitsRequest:
			return bbPRCommitsResponse([]bbCommit{{Hash: "c1", Date: created}}, false), nil
		default:
			return nil, fmt.Errorf("unexpected request type: %T", request)
		}
	}}

	cache := &recordingFetchCache{}
	params := testParams(testkit.NewChannelSink(), gateway, nil)
	params.FetchCache = cache

	op := &FetchPRsOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	state := existingCursorStates(context.Background(), cursor)["myworkspace/myrepo"]
	assert.True(t, state.HoldFloor.IsZero(),
		"a deterministic under-count must NOT hold a repair floor — re-fetching reproduces it exactly")
	assert.Equal(t, newer, state.Timestamp.UTC(),
		"the cursor must advance to the newest PR")
	// PR-100 is the only cache-eligible PR on the page (200 is OPEN, so isFinalPRState
	// excludes it), which is what makes an empty write set the right assertion.
	assert.NotContains(t, cache.written, "myworkspace/myrepo#100",
		"an incomplete aggregate must NOT be cached — a final-state cache hit would re-serve the wrong number forever")
}

// recordingFetchCache records which diffstat keys were written back.
type recordingFetchCache struct {
	written []string
}

func (c *recordingFetchCache) GetMany(_ context.Context, _ string, _ []string) (map[string]json.RawMessage, error) {
	return map[string]json.RawMessage{}, nil
}

func (c *recordingFetchCache) PutMany(_ context.Context, cacheType string, entries map[string]json.RawMessage) error {
	if cacheType == CacheTypePRDiffstat {
		for key := range entries {
			c.written = append(c.written, key)
		}
	}
	return nil
}
