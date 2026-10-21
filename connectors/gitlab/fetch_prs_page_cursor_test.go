// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// --- shared test scaffolding ---

// checkpointRecorder captures every framework Checkpoint write so a test can
// assert the exact cursor persisted after each page.
type checkpointRecorder struct {
	mu    sync.Mutex
	calls []*sdk.Cursor
}

func (r *checkpointRecorder) fn(_ context.Context, c *sdk.Cursor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, c)
	return nil
}

func (r *checkpointRecorder) totalCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *checkpointRecorder) lastCursor() *sdk.Cursor {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return nil
	}
	return r.calls[len(r.calls)-1]
}

// glMRsPageResponse marshals MRs into a merge-requests response with an explicit
// NextPageUrl so multi-page pagination can be exercised.
func glMRsPageResponse(mrs []glMergeRequest, nextPageURL string) *connectorpb.GitLabListProjectMergeRequestsResponse {
	mrBytes := make([][]byte, len(mrs))
	for i, mr := range mrs {
		mrBytes[i], _ = json.Marshal(mr)
	}
	return &connectorpb.GitLabListProjectMergeRequestsResponse{
		MergeRequests: mrBytes,
		NextPageUrl:   nextPageURL,
	}
}

func glMR(projectID, iid int, updatedAt time.Time) glMergeRequest {
	return glMergeRequest{
		ID:        iid,
		IID:       iid,
		ProjectID: projectID,
		State:     "opened",
		CreatedAt: updatedAt.Add(-time.Hour),
		UpdatedAt: updatedAt,
	}
}

const cursorTestProjectID = 100

func cursorTestProjectsResponse() *connectorpb.GitLabListProjectsResponse {
	return glProjectsResponse([]glProject{
		{ID: cursorTestProjectID, PathWithNamespace: "mygroup/repoA", Name: "repoA"},
	})
}

// --- TestRepoCursorStateRoundTrip ---

// The cursor codec must round-trip a repoCursorState into the in-memory state
// the Execute loop expects, and drop any other shape, a bare timestamp
// included, like a corrupt entry.
func TestRepoCursorStateRoundTrip(t *testing.T) {
	t.Parallel()

	canonical := time.Date(2026, 5, 21, 8, 30, 0, 0, time.UTC)

	mustMarshal := func(v any) json.RawMessage {
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		return raw
	}

	testCases := []struct {
		name        string
		parts       map[string]json.RawMessage
		projectKey  string
		wantState   repoCursorState
		wantPresent bool
	}{
		{
			name:        "a bare timestamp is not an entry and is dropped like a corrupt one",
			parts:       map[string]json.RawMessage{"100": mustMarshal(canonical)},
			projectKey:  "100",
			wantPresent: false,
		},
		{
			name:        "struct format round-trips intact",
			parts:       map[string]json.RawMessage{"100": mustMarshal(repoCursorState{Timestamp: canonical})},
			projectKey:  "100",
			wantState:   repoCursorState{Timestamp: canonical},
			wantPresent: true,
		},
		{
			name:        "missing project key returns empty state",
			parts:       map[string]json.RawMessage{"100": mustMarshal(canonical)},
			projectKey:  "200",
			wantState:   repoCursorState{},
			wantPresent: false,
		},
		{
			name:        "corrupt per-project blob treated as empty (no panic)",
			parts:       map[string]json.RawMessage{"100": json.RawMessage(`"not-a-timestamp-or-state"`)},
			projectKey:  "100",
			wantState:   repoCursorState{},
			wantPresent: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cursor := sdk.NewCompositeCursor(tc.parts)

			gotSingle := getRepoCursorState(context.Background(), cursor, tc.projectKey)
			assert.Equal(t, tc.wantState, gotSingle, "getRepoCursorState")

			all := existingCursorStates(context.Background(), cursor)
			_, present := all[tc.projectKey]
			assert.Equal(t, tc.wantPresent, present, "existingCursorStates presence")
		})
	}

	t.Run("buildStateCursor -> existingCursorStates round-trips a populated map", func(t *testing.T) {
		t.Parallel()
		input := map[string]repoCursorState{
			"100": {Timestamp: canonical},
			"200": {Timestamp: canonical.Add(-24 * time.Hour)},
		}
		got := existingCursorStates(context.Background(), buildStateCursor(context.Background(), input))
		assert.Equal(t, input, got, "round-trip must preserve every field for every project")
	})
}

// --- TestFetchPRsOp_KillAfterPage1_ResumesAtPage1FromHighWaterMark ---

// A resume after a kill starts again from page 1, because the vendor's ordering
// can shift between runs; repoCursorState's doc comment (operations.go) has the
// mechanism.
//
// Shown in two runs of the real FetchPRsOp.Execute:
//
//	First run (interrupted): the gateway serves page 1 (hasNext=true) then ERRORS
//	  on page 2 — simulating a SIGKILL/OOM after the page-1 checkpoint landed.
//	Second run (resume, reordered): a fresh gateway simulating the vendor-side
//	  re-sort — the MR that was on page 1 now sorts onto page 2, and a
//	  not-yet-fetched MR now sits on page 1. A page-NUMBER resume (asking for
//	  page 2 first) would never re-request page 1 and would permanently miss
//	  the shifted MR. The resume must request page 1 first and capture both MRs.
func TestFetchPRsOp_KillAfterPage1_ResumesAtPage1FromHighWaterMark(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	page1MR := glMR(cursorTestProjectID, 1, now) // newest — page-1 high-water

	// enrichMR + fetchEarliestMRCommit fire per MR; answer them with empties.
	answerChained := func(req proto.Message) (proto.Message, bool) {
		switch req.(type) {
		case *connectorpb.GitLabListMRDiffsRequest:
			return glDiffsResponse(nil), true
		case *connectorpb.GitLabListMRCommitsRequest:
			return &connectorpb.GitLabListMRCommitsResponse{}, true
		default:
			return nil, false
		}
	}

	// --- First run: interrupted after page 1 ---
	recorder := &checkpointRecorder{}
	firstGW := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if resp, ok := answerChained(req); ok {
				return resp, nil
			}
			switch r := req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return cursorTestProjectsResponse(), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				switch r.GetPage() {
				case 1:
					return glMRsPageResponse([]glMergeRequest{page1MR}, "https://gitlab.com/next?page=2"), nil
				default:
					return nil, fmt.Errorf("simulated kill: connection lost fetching page %d", r.GetPage())
				}
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	firstSink := testkit.NewChannelSink()
	firstParams := testParams(firstSink, firstGW, nil)
	firstParams.Checkpoint = recorder.fn

	_, firstErr := (&FetchPRsOp{}).Execute(context.Background(), firstParams)
	require.Error(t, firstErr, "the first run must fail on the simulated page-2 kill")

	require.GreaterOrEqual(t, recorder.totalCalls(), 1,
		"the per-page checkpoint must have fired after page 1 before the kill")
	killCursor := recorder.lastCursor()
	require.NotNil(t, killCursor)
	killState := getRepoCursorState(context.Background(), killCursor, fmt.Sprint(cursorTestProjectID))
	assert.Equal(t, page1MR.UpdatedAt.UTC(), killState.Timestamp.UTC(),
		"the durable cursor after page 1 must record the page-1 high-water mark")

	// --- Second run: resume, simulating a vendor-side re-sort. The MR that was
	// on page 1 (still `since`-eligible: its updated_at equals the checkpoint,
	// not before it) now returns on page 2; a new MR now sits on
	// page 1. ---
	shiftedNewMR := glMR(cursorTestProjectID, 2, now.Add(time.Second))
	var (
		mu          sync.Mutex
		mrPagesSeen []int32
	)
	secondGW := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if resp, ok := answerChained(req); ok {
				return resp, nil
			}
			switch r := req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return cursorTestProjectsResponse(), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				mu.Lock()
				mrPagesSeen = append(mrPagesSeen, r.GetPage())
				mu.Unlock()
				switch r.GetPage() {
				case 1:
					return glMRsPageResponse([]glMergeRequest{shiftedNewMR}, "https://gitlab.com/next?page=2"), nil
				case 2:
					return glMRsPageResponse([]glMergeRequest{page1MR}, ""), nil
				default:
					return nil, fmt.Errorf("unexpected page %d in resume", r.GetPage())
				}
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	secondSink := testkit.NewChannelSink()
	secondParams := testParams(secondSink, secondGW, killCursor)

	_, secondErr := (&FetchPRsOp{}).Execute(context.Background(), secondParams)
	require.NoError(t, secondErr, "resume run must succeed")

	mu.Lock()
	pages := append([]int32(nil), mrPagesSeen...)
	mu.Unlock()

	require.NotEmpty(t, pages, "resume must issue at least one merge-requests call")
	assert.Equal(t, int32(1), pages[0], "resume must restart at page 1, not a saved page number")

	// Both the shifted MR (now on page 1) and the original page-1 MR (now
	// shifted to page 2) must be captured — a page-NUMBER resume starting at
	// page 2 would have missed the shifted MR entirely.
	gotIIDs := map[int]bool{}
	for _, rec := range secondSink.Records() {
		if strings.HasSuffix(rec.ExternalID, "#opened") {
			var mrID int
			_, err := fmt.Sscanf(rec.ExternalID, fmt.Sprintf("mr-%d!%%d#opened", cursorTestProjectID), &mrID)
			require.NoError(t, err)
			gotIIDs[mrID] = true
		}
	}
	assert.True(t, gotIIDs[1] && gotIIDs[2],
		"resume must capture both the shifted MR and the original page-1 MR; got %v", gotIIDs)
}

// --- TestFetchPRsOp_PerPageCheckpoint ---

// Pins the per-page checkpoint contract on a clean multi-page run: three
// pages produce three per-page checkpoints, each pinned at the page-1
// high-water mark (older pages cannot regress it). No separate per-repo
// checkpoint call — the cleaned end state is carried out via the returned
// cursor, which must show the same high-water mark.
func TestFetchPRsOp_PerPageCheckpoint(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	page1MR := glMR(cursorTestProjectID, 101, now)                    // page-1 high-water
	page2MR := glMR(cursorTestProjectID, 102, now.Add(-72*time.Hour)) // older
	page3MR := glMR(cursorTestProjectID, 103, now.Add(-96*time.Hour)) // older still

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return cursorTestProjectsResponse(), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				switch r.GetPage() {
				case 1:
					return glMRsPageResponse([]glMergeRequest{page1MR}, "https://gitlab.com/next?page=2"), nil
				case 2:
					return glMRsPageResponse([]glMergeRequest{page2MR}, "https://gitlab.com/next?page=3"), nil
				case 3:
					return glMRsPageResponse([]glMergeRequest{page3MR}, ""), nil
				default:
					return nil, fmt.Errorf("unexpected page %d", r.GetPage())
				}
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				return &connectorpb.GitLabListMRCommitsResponse{}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	recorder := &checkpointRecorder{}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.Checkpoint = recorder.fn

	gotCursor, err := (&FetchPRsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, gotCursor)

	// Mirrors the GitHub sibling: one per-page checkpoint per page, no separate
	// per-repo Checkpoint call (the cleaned state is carried out via the return
	// value's buildStateCursor). Three pages → three checkpoint writes.
	require.Equal(t, 3, recorder.totalCalls(),
		"one per-page checkpoint per page (pages 1/2/3); the cleaned cursor is returned, not re-checkpointed")

	projectKey := fmt.Sprint(cursorTestProjectID)
	for i, calls := range recorder.calls {
		st := getRepoCursorState(context.Background(), calls, projectKey)
		assert.Equal(t, page1MR.UpdatedAt.UTC(), st.Timestamp.UTC(),
			"per-page checkpoint #%d: Timestamp must stay at the page-1 high-water; older pages cannot regress it", i+1)
	}

	returnedState := getRepoCursorState(context.Background(), gotCursor, projectKey)
	assert.Equal(t, page1MR.UpdatedAt.UTC(), returnedState.Timestamp.UTC(),
		"returned Timestamp must be the iteration high-water mark")
}
