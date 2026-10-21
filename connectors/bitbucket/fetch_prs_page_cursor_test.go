// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	sdkmock "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// marshalCursorPart wraps json.Marshal for inline test-table construction
// where *testing.T is not yet in scope. The existing mustMarshalJSON helper
// in operations_test.go takes *testing.T; this variant panics on error so
// table-init expressions stay compact.
func marshalCursorPart(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshalCursorPart: %v", err))
	}
	return raw
}

// expectGatewayDispatch wires the generated MockVendorGateway as the
// VendorGateway seen by FetchPRsOp.Execute. ForGroup is constrained to the
// production code's only group identifier so a future regression that
// dispatches on a different group (e.g. an accidental graphql/rest mix-up)
// trips the mock instead of silently routing through. Execute uses Any() on
// the proto argument because request-type discrimination happens inside the
// dispatch closure — gomock's matchers can't express "switch on Go type" as
// cleanly. WithRateLimits is intentionally NOT expected: fetch_prs never
// calls it (that lives in the scheduler's gatewayfactory), so the
// no-expectation default trips the test if production starts calling it.
//
// Returns the mock so callers can layer additional expectations if needed.
func expectGatewayDispatch(ctrl *gomock.Controller, dispatch func(ctx context.Context, req proto.Message) (proto.Message, error)) *sdkmock.MockVendorGateway {
	gateway := sdkmock.NewMockVendorGateway(ctrl)
	gateway.EXPECT().ForGroup(gomock.Eq(constants.GroupBitbucketREST)).Return(gateway).AnyTimes()
	gateway.EXPECT().Execute(gomock.Any(), gomock.Any()).DoAndReturn(dispatch).AnyTimes()
	return gateway
}

// --- TestRepoCursorStateRoundTrip ---

// Single table-driven verification of the cursor codec: each input shape (full
// state struct, struct with empty Timestamp, an entry that is not an object,
// corrupt blob) must round-trip into the in-memory repoCursorState the way the
// Execute loop expects, or be dropped.
func TestRepoCursorStateRoundTrip(t *testing.T) {
	t.Parallel()

	canonicalTimestamp := time.Date(2026, 5, 21, 8, 30, 0, 0, time.UTC)
	earlierTimestamp := canonicalTimestamp.Add(-24 * time.Hour)

	testCases := []struct {
		name           string
		rawCursorParts map[string]json.RawMessage
		repoKey        string
		wantState      repoCursorState
		wantPresent    bool // does existingCursorStates return a state with non-zero Timestamp under this repoKey
	}{
		{
			name: "a bare timestamp is not an entry and is dropped like a corrupt one",
			rawCursorParts: map[string]json.RawMessage{
				"myworkspace/repoA": marshalCursorPart(canonicalTimestamp),
			},
			repoKey:     "myworkspace/repoA",
			wantPresent: false,
		},
		{
			name: "new state-struct format with PageCursor + Since round-trips intact",
			rawCursorParts: map[string]json.RawMessage{
				"myworkspace/repoA": marshalCursorPart(repoCursorState{
					Timestamp:  canonicalTimestamp,
					PageCursor: "3",
					Since:      earlierTimestamp,
				}),
			},
			repoKey: "myworkspace/repoA",
			wantState: repoCursorState{
				Timestamp:  canonicalTimestamp,
				PageCursor: "3",
				Since:      earlierTimestamp,
			},
			wantPresent: true,
		},
		{
			name: "state struct with zero Timestamp falls through to time.Time path; reader returns empty when neither path is valid",
			rawCursorParts: map[string]json.RawMessage{
				// {"timestamp":"0001-01-01T00:00:00Z","page_cursor":"5"} — zero
				// Timestamp triggers the fallback path, but the same blob
				// doesn't parse as a bare time.Time either (it's an object).
				// Result: empty state.
				"myworkspace/repoA": json.RawMessage(`{"timestamp":"0001-01-01T00:00:00Z","page_cursor":"5"}`),
			},
			repoKey:     "myworkspace/repoA",
			wantState:   repoCursorState{},
			wantPresent: false,
		},
		{
			name: "missing repo key returns empty state with no error",
			rawCursorParts: map[string]json.RawMessage{
				"myworkspace/repoA": marshalCursorPart(canonicalTimestamp),
			},
			repoKey:     "myworkspace/repoB",
			wantState:   repoCursorState{},
			wantPresent: false,
		},
		{
			name: "corrupt per-repo blob is silently treated as empty (no panic)",
			rawCursorParts: map[string]json.RawMessage{
				"myworkspace/repoA": json.RawMessage(`"not-a-timestamp-or-state"`),
			},
			repoKey:     "myworkspace/repoA",
			wantState:   repoCursorState{},
			wantPresent: false,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			cursor := sdk.NewCompositeCursor(testCase.rawCursorParts)

			allStates := existingCursorStates(context.Background(), cursor)
			gotState := allStates[testCase.repoKey]
			assert.Equal(t, testCase.wantState, gotState)
			if testCase.wantPresent {
				assert.Contains(t, allStates, testCase.repoKey,
					"existingCursorStates must include the repo when the value parses")
			} else {
				_, hasKey := allStates[testCase.repoKey]
				assert.False(t, hasKey,
					"existingCursorStates must omit unparseable / empty-Timestamp entries")
			}
		})
	}

	t.Run("buildStateCursor → existingCursorStates round-trips a populated map", func(t *testing.T) {
		t.Parallel()
		inputStates := map[string]repoCursorState{
			"myworkspace/repoA": {Timestamp: canonicalTimestamp},
			"myworkspace/repoB": {
				Timestamp:  canonicalTimestamp,
				PageCursor: "4",
				Since:      earlierTimestamp,
			},
		}
		gotStates := existingCursorStates(context.Background(), buildStateCursor(context.Background(), inputStates))
		assert.Equal(t, inputStates, gotStates,
			"round-trip must preserve every field for every repo")
	})
}

// --- TestFetchPRsOp_Execute_PageCursorResume ---

// Verifies the mid-pagination resume path: when the input cursor carries a
// non-empty PageCursor + Since for a repo, the next fetch must (a) start
// the list call at the recorded page number — not page 1 — and (b) use the
// recorded Since as the early-exit boundary, NOT the newer Timestamp watermark
// (which would silently skip the older pages the interrupted run was working
// through; see [repoCursorState] docs).
//
// After the resumed iteration finishes cleanly, the cursor write must clear
// PageCursor + Since so the NEXT run starts fresh at page 1 with
// `since = firstSyncSince(Timestamp)`.
func TestFetchPRsOp_Execute_PageCursorResume(t *testing.T) {
	t.Parallel()

	priorSince := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	stalePageMaxTimestamp := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name                   string
		inputCursor            *sdk.Cursor
		expectFirstPage        int32     // page number the connector must request first
		expectClearAfter       bool      // after Execute, cursor should have PageCursor="" and Since=zero
		expectTimestampAtLeast time.Time // returned Timestamp must be >= this floor (prevents cursor regression on resume)
	}{
		{
			name: "resume with PageCursor=3 + Since starts at page 3 (NOT page 1) AND preserves prior watermark",
			inputCursor: sdk.NewCompositeCursor(map[string]json.RawMessage{
				"myworkspace/repoA": marshalCursorPart(repoCursorState{
					Timestamp:  stalePageMaxTimestamp, // page-1 max of interrupted run
					PageCursor: "3",
					Since:      priorSince,
				}),
			}),
			expectFirstPage:        3,
			expectClearAfter:       true,
			expectTimestampAtLeast: stalePageMaxTimestamp, // resumed iteration walks OLDER pages; final cursor must NOT regress below the interrupted run's page-1 max
		},
		{
			name: "a bare timestamp entry is dropped, so the walk starts at page 1",
			inputCursor: sdk.NewCompositeCursor(map[string]json.RawMessage{
				"myworkspace/repoA": marshalCursorPart(stalePageMaxTimestamp),
			}),
			expectFirstPage:  1,
			expectClearAfter: true,
		},
		{
			name:             "no input cursor for repo: fresh iteration starting at page 1",
			inputCursor:      nil,
			expectFirstPage:  1,
			expectClearAfter: true,
		},
		{
			name: "malformed state with PageCursor set but Since=zero falls back to fresh iteration at page 1",
			inputCursor: sdk.NewCompositeCursor(map[string]json.RawMessage{
				"myworkspace/repoA": marshalCursorPart(repoCursorState{
					Timestamp:  stalePageMaxTimestamp,
					PageCursor: "3",
					// Since intentionally left zero — using a zero-time as the
					// loop's early-exit floor would disable the cursor-based
					// stop and walk the repo's entire pagination history. The
					// defensive guard in Execute must drop the (untrustworthy)
					// PageCursor when Since is unset, treating the state as
					// fresh and starting at page 1 with since = Timestamp.
					Since: time.Time{},
				}),
			}),
			expectFirstPage:        1,
			expectClearAfter:       true,
			expectTimestampAtLeast: stalePageMaxTimestamp,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			var firstObservedPage atomic.Int32
			pageRequests := make([]int32, 0, 4)
			var pageRequestMu atomic.Bool // serialise appends defensively even though the loop is sequential

			singlePRPayload := bbPullRequest{
				ID: 1, State: "OPEN",
				CreatedOn: priorSince.Add(time.Hour),
				UpdatedOn: priorSince.Add(2 * time.Hour),
			}

			gateway := expectGatewayDispatch(ctrl, func(_ context.Context, req proto.Message) (proto.Message, error) {
				switch typedRequest := req.(type) {
				case *connectorpb.BitbucketListPullRequestsRequest:
					recordedPage := typedRequest.GetPage()
					firstObservedPage.CompareAndSwap(0, recordedPage)
					for !pageRequestMu.CompareAndSwap(false, true) {
						runtime.Gosched()
					}
					pageRequests = append(pageRequests, recordedPage)
					pageRequestMu.Store(false)
					// Return a single-page payload regardless of page number. The
					// connector terminates because hasNext=false; that's enough
					// to exercise the resume-page semantics without needing a
					// realistic multi-page corpus.
					return bbPRsResponse([]bbPullRequest{singlePRPayload}, false), nil
				case *connectorpb.BitbucketGetPRDiffstatRequest:
					return bbDiffstatResponse(0, 0), nil
				case *connectorpb.BitbucketListPRCommitsRequest:
					return bbPRCommitsResponse(nil, false), nil
				default:
					return nil, fmt.Errorf("unexpected request type: %T", req)
				}
			})

			sink := testkit.NewChannelSink()
			params := testParamsMultiRepo(sink, gateway, testCase.inputCursor,
				[]string{"myworkspace/repoA"}, nil)

			op := &FetchPRsOp{}
			gotCursor, gotErr := op.Execute(context.Background(), params)
			require.NoError(t, gotErr)
			require.NotNil(t, gotCursor)

			require.NotEmpty(t, pageRequests, "gateway must be invoked at least once")
			assert.Equal(t, testCase.expectFirstPage, pageRequests[0],
				"resume must dispatch to the recorded PageCursor on the first call (not page 1)")

			gotState := existingCursorStates(context.Background(), gotCursor)["myworkspace/repoA"]
			if testCase.expectClearAfter {
				assert.Empty(t, gotState.PageCursor,
					"PageCursor must be cleared after a clean iteration so the next run starts fresh at page 1")
				assert.True(t, gotState.Since.IsZero(),
					"Since must be cleared after a clean iteration")
			}
			assert.False(t, gotState.Timestamp.IsZero(),
				"Timestamp must hold the iteration's high-water mark on success")
			if !testCase.expectTimestampAtLeast.IsZero() {
				assert.False(t, gotState.Timestamp.Before(testCase.expectTimestampAtLeast),
					"returned Timestamp %s must not regress below the prior watermark %s — resumed iterations walk older pages and the cursor would re-process already-ingested PRs on the next run",
					gotState.Timestamp.UTC().Format(time.RFC3339), testCase.expectTimestampAtLeast.UTC().Format(time.RFC3339))
			}
		})
	}
}

// --- TestFetchPRsOp_Execute_PerPageCheckpoint ---

// Pins the per-page checkpoint write. After page 1
// completes, an onPage write must land before fetchPRsForRepo returns; the
// per-repo Checkpoint then fires for the final cursor state.
//
// On a single-page repo this gives Checkpoint two writes: one with PageCursor
// non-empty if there were more pages (or empty if the response said hasNext=
// false), and one final write with PageCursor empty and Timestamp set.
//
// The page-1-max-as-final-watermark hazard is pinned here too: across a complete iteration, the FINAL persisted state
// must have PageCursor cleared even if the per-page checkpoint had set one.
func TestFetchPRsOp_Execute_PerPageCheckpoint(t *testing.T) {
	t.Parallel()

	canonicalTimestamp := time.Now().UTC().Truncate(time.Second)
	earlierTimestamp := canonicalTimestamp.Add(-time.Hour)

	singlePR := bbPullRequest{
		ID: 1, State: "OPEN",
		CreatedOn: earlierTimestamp,
		UpdatedOn: canonicalTimestamp,
	}

	ctrl := gomock.NewController(t)
	gateway := expectGatewayDispatch(ctrl, func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.BitbucketListPullRequestsRequest:
			return bbPRsResponse([]bbPullRequest{singlePR}, false), nil
		case *connectorpb.BitbucketGetPRDiffstatRequest:
			return bbDiffstatResponse(0, 0), nil
		case *connectorpb.BitbucketListPRCommitsRequest:
			return bbPRCommitsResponse(nil, false), nil
		default:
			return nil, fmt.Errorf("unexpected request type: %T", req)
		}
	})
	sink := testkit.NewChannelSink()

	recorder := &checkpointRecorder{}
	params := testParamsMultiRepo(sink, gateway, nil,
		[]string{"myworkspace/repoA"}, recorder.fn)

	op := &FetchPRsOp{}
	gotCursor, gotErr := op.Execute(context.Background(), params)
	require.NoError(t, gotErr)
	require.NotNil(t, gotCursor)

	require.Equal(t, 2, recorder.totalCalls(),
		"expected exactly two Checkpoint writes per successful single-page repo: onPage (after page 1) + per-repo (after fetchPRsForRepo returns)")

	// First write is the per-page Checkpoint. With hasNext=false, the
	// per-page write carries an EMPTY nextPageCursor (loop is finishing).
	firstCheckpointStates := existingCursorStates(context.Background(), recorder.calls[0])
	firstWrite, hasFirstRepo := firstCheckpointStates["myworkspace/repoA"]
	require.True(t, hasFirstRepo, "first checkpoint must include the repo's state")
	assert.Empty(t, firstWrite.PageCursor,
		"single-page iteration with no next page must write an empty PageCursor")

	// Second write is the per-repo Checkpoint after the iteration completes.
	// It must persist PageCursor="" and Since=zero so the next run starts at
	// page 1 with `since = firstSyncSince(Timestamp)`.
	finalCheckpointStates := existingCursorStates(context.Background(), recorder.calls[1])
	finalWrite, hasFinalRepo := finalCheckpointStates["myworkspace/repoA"]
	require.True(t, hasFinalRepo, "final checkpoint must include the repo's state")
	assert.Empty(t, finalWrite.PageCursor,
		"final per-repo write must clear PageCursor")
	assert.True(t, finalWrite.Since.IsZero(),
		"final per-repo write must clear Since")
	assert.Equal(t, canonicalTimestamp.UTC(), finalWrite.Timestamp.UTC(),
		"final Timestamp must be the page-max updated_on")

	// The returned cursor must match the final per-repo write.
	finalReturnedState := existingCursorStates(context.Background(), gotCursor)["myworkspace/repoA"]
	assert.Equal(t, finalWrite, finalReturnedState)
}

// --- TestFetchPRsOp_Execute_MultiPagePagination ---

// Walks three pages of PRs end-to-end to pin the per-page checkpoint's
// page-arithmetic. Each page returns a distinct PR sorted newest→oldest
// (Bitbucket's `?sort=-updated_on` semantic) AND a non-empty NextPageUrl
// except the last; the loop must therefore:
//
//   - request pages 1, 2, 3 in that order;
//   - fire onPage three times — the first with nextPageCursor="2", the second
//     with "3", and the third (last page) with "";
//   - advance latestUpdated to the page-1 max (newest seen) and not regress
//     it on subsequent older pages;
//   - clear PageCursor + Since on the final per-repo write.
//
// Single-page coverage already exists in TestFetchPRsOp_Execute_PerPageCheckpoint;
// this case locks the page-number transitions the single-page mock can't
// exercise, so a future change to the next-page arithmetic (e.g. off-by-one
// on the increment, or missing the "clear PageCursor when finished" branch)
// fails loudly here.
func TestFetchPRsOp_Execute_MultiPagePagination(t *testing.T) {
	t.Parallel()

	// Sorted newest→oldest as the API would emit; page 1 carries the iteration's
	// high-water mark, pages 2/3 walk older PRs and must NOT regress latestUpdated.
	page1PR := bbPullRequest{
		ID: 101, State: "OPEN",
		CreatedOn: time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC),
		UpdatedOn: time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC),
	}
	page2PR := bbPullRequest{
		ID: 102, State: "MERGED",
		CreatedOn: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
		UpdatedOn: time.Date(2026, 5, 15, 9, 0, 0, 0, time.UTC),
	}
	page3PR := bbPullRequest{
		ID: 103, State: "MERGED",
		CreatedOn: time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC),
		UpdatedOn: time.Date(2026, 5, 7, 18, 0, 0, 0, time.UTC),
	}

	ctrl := gomock.NewController(t)
	pageHandler := func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch typedRequest := req.(type) {
		case *connectorpb.BitbucketListPullRequestsRequest:
			switch typedRequest.GetPage() {
			case 1:
				return bbPRsResponse([]bbPullRequest{page1PR}, true), nil // hasNext=true
			case 2:
				return bbPRsResponse([]bbPullRequest{page2PR}, true), nil
			case 3:
				return bbPRsResponse([]bbPullRequest{page3PR}, false), nil // last page
			default:
				return nil, fmt.Errorf("unexpected page number requested: %d", typedRequest.GetPage())
			}
		case *connectorpb.BitbucketGetPRDiffstatRequest:
			return bbDiffstatResponse(0, 0), nil
		case *connectorpb.BitbucketListPRCommitsRequest:
			return bbPRCommitsResponse(nil, false), nil
		default:
			return nil, fmt.Errorf("unexpected request type: %T", req)
		}
	}
	gateway := expectGatewayDispatch(ctrl, pageHandler)
	sink := testkit.NewChannelSink()

	recorder := &checkpointRecorder{}
	params := testParamsMultiRepo(sink, gateway, nil,
		[]string{"myworkspace/repoA"}, recorder.fn)

	op := &FetchPRsOp{}
	gotCursor, gotErr := op.Execute(context.Background(), params)
	require.NoError(t, gotErr)
	require.NotNil(t, gotCursor)

	// Three pages produce three per-page checkpoints; Execute's per-repo
	// checkpoint then fires once more = 4 total writes.
	require.Equal(t, 4, recorder.totalCalls(),
		"expected three per-page checkpoints (pages 1/2/3) + one per-repo checkpoint after fetchPRsForRepo returns")

	// Per-page writes 1, 2, 3 must carry the NEXT page identifier.
	type expectedCheckpoint struct {
		writeIndex     int
		wantPageCursor string
	}
	expectedCheckpoints := []expectedCheckpoint{
		{writeIndex: 0, wantPageCursor: "2"}, // after page 1 finishes
		{writeIndex: 1, wantPageCursor: "3"}, // after page 2 finishes
		{writeIndex: 2, wantPageCursor: ""},  // after page 3 (last page) — empty signals "iteration done"
	}
	for _, expected := range expectedCheckpoints {
		states := existingCursorStates(context.Background(), recorder.calls[expected.writeIndex])
		write, hasRepo := states["myworkspace/repoA"]
		require.True(t, hasRepo, "checkpoint #%d must include repoA state", expected.writeIndex+1)
		assert.Equal(t, expected.wantPageCursor, write.PageCursor,
			"checkpoint #%d must carry nextPageCursor=%q so a future resume jumps to that page",
			expected.writeIndex+1, expected.wantPageCursor)
		// Timestamp is page-1's UpdatedOn from the first onPage onwards —
		// subsequent older pages must NOT regress it because pr.UpdatedOn.After
		// stays false for older PRs.
		assert.Equal(t, page1PR.UpdatedOn.UTC(), write.Timestamp.UTC(),
			"checkpoint #%d Timestamp must remain at the page-1 max — older pages cannot regress the watermark",
			expected.writeIndex+1)
	}

	// Final per-repo write (4th total) clears PageCursor + Since so the next
	// run starts fresh at page 1 with `since = firstSyncSince(Timestamp)`.
	finalState := existingCursorStates(context.Background(), recorder.calls[3])["myworkspace/repoA"]
	assert.Empty(t, finalState.PageCursor, "final per-repo write must clear PageCursor")
	assert.True(t, finalState.Since.IsZero(), "final per-repo write must clear Since")
	assert.Equal(t, page1PR.UpdatedOn.UTC(), finalState.Timestamp.UTC(),
		"final Timestamp must be the iteration's high-water mark = page-1 max")

	// Returned cursor matches the final write.
	returnedState := existingCursorStates(context.Background(), gotCursor)["myworkspace/repoA"]
	assert.Equal(t, finalState, returnedState)
}
