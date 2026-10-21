// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

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

// --- Test helpers (worklogs/comments cursor resume) ---

// recordedCheckpoint captures every CheckpointFunc invocation so tests can
// assert mid-iter cursor advancement. Optionally fails the Nth call to exercise
// the log-and-continue path.
type recordedCheckpoint struct {
	mu        sync.Mutex
	calls     []*sdk.Cursor
	failNth   int
	failErr   error
	callsSeen int
}

func (rc *recordedCheckpoint) fn() sdk.CheckpointFunc {
	return func(ctx context.Context, cursor *sdk.Cursor) error {
		rc.mu.Lock()
		defer rc.mu.Unlock()
		rc.callsSeen++
		if rc.failNth > 0 && rc.callsSeen == rc.failNth {
			return rc.failErr
		}
		// Deep-copy the cursor so the recorder isn't observing later mutations.
		cursorCopy := &sdk.Cursor{Data: append([]byte(nil), cursor.Data...), UpdatedAt: cursor.UpdatedAt}
		rc.calls = append(rc.calls, cursorCopy)
		return nil
	}
}

func (rc *recordedCheckpoint) snapshot() []*sdk.Cursor {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	out := make([]*sdk.Cursor, len(rc.calls))
	copy(out, rc.calls)
	return out
}

// buildWorklogsCursor builds a one-project cursor for fetch_worklogs / fetch_comments tests.
func buildWorklogsCursor(t *testing.T, projectKey string, state projectCursorState) *sdk.Cursor {
	t.Helper()
	parts := map[string]json.RawMessage{projectKey: mustJSON(state)}
	return sdk.NewCompositeCursor(parts)
}

// projectStateFromCursor reads back a per-project state for assertion.
func projectStateFromCursor(t *testing.T, cursor *sdk.Cursor, projectKey string) projectCursorState {
	t.Helper()
	parts, err := sdk.CompositeFromCursor(cursor)
	require.NoError(t, err, "cursor must be well-formed composite")
	raw, ok := parts[projectKey]
	require.Truef(t, ok, "project %s must be present in cursor; keys: %v", projectKey, mapKeys(parts))
	var state projectCursorState
	require.NoError(t, json.Unmarshal(raw, &state))
	return state
}

func mapKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// worklogsRefs builds N issueRef-like issues with monotonic IDs and timestamps
// spaced by `step`. It feeds the JiraSearchIssuesResponse mock.
func makeIssues(t *testing.T, count int, baseTime time.Time, step time.Duration, idStart int) []jiraIssue {
	t.Helper()
	out := make([]jiraIssue, count)
	for index := 0; index < count; index++ {
		updated := baseTime.Add(time.Duration(index) * step)
		out[index] = jiraIssue{
			ID:  fmt.Sprintf("%d", idStart+index),
			Key: fmt.Sprintf("PROJ-%d", index+1),
			Fields: jiraIssueFields{
				Updated: jiraTime{updated},
			},
		}
	}
	return out
}

// worklogsExecutor builds an executeFunc that:
//   - Always returns a single-project list.
//   - Captures the JQL passed to JiraSearchIssuesRequest into capturedJQL.
//   - For worklog requests, returns the per-issue map (issueKey -> response builder).
//   - Tracks which issues were called.
type worklogsExecutor struct {
	t                 *testing.T
	projectKey        string
	issues            []jiraIssue
	capturedJQL       string
	worklogsByIssue   map[string]func() (*connectorpb.JiraListIssueWorklogsResponse, error)
	worklogsCalledFor []string
	mu                sync.Mutex
}

func (we *worklogsExecutor) executeFunc() func(ctx context.Context, req proto.Message) (proto.Message, error) {
	return func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch typed := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "10000", Key: we.projectKey}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			we.mu.Lock()
			we.capturedJQL = typed.GetJql()
			we.mu.Unlock()
			return searchIssuesResponse(we.issues, ""), nil
		case *connectorpb.JiraListIssueWorklogsRequest:
			we.mu.Lock()
			we.worklogsCalledFor = append(we.worklogsCalledFor, typed.GetIssueIdOrKey())
			we.mu.Unlock()
			builder, ok := we.worklogsByIssue[typed.GetIssueIdOrKey()]
			if !ok {
				return worklogsResponse(nil, 0), nil
			}
			resp, err := builder()
			if err != nil {
				return nil, err
			}
			return resp, nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
}

func defaultWorklogResponse() func() (*connectorpb.JiraListIssueWorklogsResponse, error) {
	return func() (*connectorpb.JiraListIssueWorklogsResponse, error) {
		return worklogsResponse([]jiraWorklog{
			{ID: "10001", Created: jiraTime{time.Now().UTC()}},
		}, 1), nil
	}
}

// --- FetchWorklogsOp cursor-resume tests ---

func TestFetchWorklogsOp_FirstSync_ProcessesAllIssues(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 3, baseTime, time.Minute, 10001)

	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		worklogsByIssue: map[string]func() (*connectorpb.JiraListIssueWorklogsResponse, error){
			"PROJ-1": defaultWorklogResponse(),
			"PROJ-2": defaultWorklogResponse(),
			"PROJ-3": defaultWorklogResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	checkpoints := &recordedCheckpoint{}
	params := testParams(sink, gw, nil)
	params.Checkpoint = checkpoints.fn()

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, cursor)

	assert.ElementsMatch(t, []string{"PROJ-1", "PROJ-2", "PROJ-3"}, executor.worklogsCalledFor,
		"first sync must process every issue")
	assert.Len(t, sink.Records(), 3, "one worklog record per issue")

	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.True(t, finalState.Timestamp.Equal(issues[2].Fields.Updated.Time),
		"final Timestamp must be highest UpdatedOn")
	assert.Equal(t, "10003", finalState.LastIssueID,
		"final LastIssueID must be last processed issue's numeric ID")

	calls := checkpoints.snapshot()
	assert.Len(t, calls, 3, "per-issue checkpoint fires once per processed issue")
}

func TestFetchWorklogsOp_Resume_SkipsAlreadyProcessed(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 5, baseTime, time.Minute, 10001)
	// Cursor: already processed up to issue 2 (id "10002").
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   issues[1].Fields.Updated.Time,
		LastIssueID: "10002",
	})

	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		worklogsByIssue: map[string]func() (*connectorpb.JiraListIssueWorklogsResponse, error){
			"PROJ-3": defaultWorklogResponse(),
			"PROJ-4": defaultWorklogResponse(),
			"PROJ-5": defaultWorklogResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"PROJ-3", "PROJ-4", "PROJ-5"}, executor.worklogsCalledFor,
		"already-processed issues must be skipped on resume")

	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.Equal(t, "10005", finalState.LastIssueID)
}

func TestFetchWorklogsOp_SameTimestampTie_NoMiss(t *testing.T) {
	atSameTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	// All 5 issues share the same UpdatedOn; only IDs differ.
	issues := makeIssues(t, 5, atSameTime, 0, 10001)
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   atSameTime,
		LastIssueID: "10002",
	})

	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		worklogsByIssue: map[string]func() (*connectorpb.JiraListIssueWorklogsResponse, error){
			"PROJ-3": defaultWorklogResponse(),
			"PROJ-4": defaultWorklogResponse(),
			"PROJ-5": defaultWorklogResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"PROJ-3", "PROJ-4", "PROJ-5"}, executor.worklogsCalledFor,
		"same-Timestamp tiebreaker must skip ids <= cursor.LastIssueID")

	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.Equal(t, "10005", finalState.LastIssueID)
}

// TestFetchWorklogsOp_SameTimestamp_NewIssueWithLowerKey_NotMissed pins the
// LastIssueID (vs LastIssueKey) correctness fix. A new issue created between
// runs at the same UpdatedOn but with a lexicographically lower key — yet a
// higher numeric ID since IDs are monotonic — must be processed on resume.
// Using LastIssueKey instead would incorrectly skip it.
func TestFetchWorklogsOp_SameTimestamp_NewIssueWithLowerKey_NotMissed(t *testing.T) {
	atSameTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := []jiraIssue{
		// Sorted Go-side by (UpdatedOn, ID). Existing issue at lower id.
		{ID: "10500", Key: "PROJ-500", Fields: jiraIssueFields{Updated: jiraTime{atSameTime}}},
		// New issue created later: alphabetically LOWER key but numerically HIGHER id.
		{ID: "10999", Key: "PROJ-499", Fields: jiraIssueFields{Updated: jiraTime{atSameTime}}},
	}
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   atSameTime,
		LastIssueID: "10500",
	})

	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		worklogsByIssue: map[string]func() (*connectorpb.JiraListIssueWorklogsResponse, error){
			"PROJ-499": defaultWorklogResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, []string{"PROJ-499"}, executor.worklogsCalledFor,
		"alphabetic key is irrelevant; numeric ID 10999 > 10500 means new issue must be processed")
	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.Equal(t, "10999", finalState.LastIssueID)
}

func TestFetchWorklogsOp_OverlapWindow_AppliedOnResume(t *testing.T) {
	cursorTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   cursorTime,
		LastIssueID: "10500",
	})

	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     nil,
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	// JQL must use (cursorTime - 5min) as the lower bound. The format
	// is minute-precision (jiraTimeFormat), so we check the exact rendered
	// timestamp falls in the overlap-shifted minute.
	expectedSince := cursorTime.Add(-defaultCursorOverlapWindow - maxJQLZoneOffset).UTC().Format(jiraTimeFormat)
	assert.Contains(t, executor.capturedJQL, expectedSince,
		"JQL must apply cursorOverlapWindow (5-min subtract) for defense-in-depth")
}

func TestFetchWorklogsOp_OverlapWindow_FirstSyncBypass(t *testing.T) {
	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     nil,
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil) // no prior cursor — first sync

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	// On first sync, since must be ~12 months ago (firstSyncSince), NOT
	// 5 minutes ago.
	assert.Contains(t, executor.capturedJQL, "updated >=",
		"first sync still applies an updated-since filter via firstSyncSince")
	// Parse the actual since-timestamp out of the JQL and assert it is older
	// than 11 months. A whole-string lexicographic compare against a literal
	// template silently passes the moment the JQL format shifts (e.g. quoting
	// the project key) because the divergence lands before the date — compare
	// the parsed date directly so the guard cannot be defanged that way.
	sinceInJQL := jqlSinceTimestamp(t, executor.capturedJQL)
	cutoff := time.Now().UTC().Add(-11 * 30 * 24 * time.Hour)
	assert.True(t, sinceInJQL.Before(cutoff),
		"first sync must NOT use cursorOverlapWindow (would produce a recent date)")
}

// jqlSinceTimestamp extracts the `updated >= "<ts>"` bound from a JQL string
// and parses it with jiraTimeFormat, failing the test if the filter is absent
// or malformed. Comparing the parsed time (not the raw JQL string) keeps the
// assertion robust to unrelated format changes elsewhere in the query.
func jqlSinceTimestamp(t *testing.T, jql string) time.Time {
	t.Helper()
	const marker = `updated >= "`
	i := strings.Index(jql, marker)
	require.GreaterOrEqual(t, i, 0, "JQL must carry an updated-since filter: %q", jql)
	rest := jql[i+len(marker):]
	end := strings.IndexByte(rest, '"')
	require.GreaterOrEqual(t, end, 0, "updated-since timestamp must be quote-terminated: %q", jql)
	ts, err := time.Parse(jiraTimeFormat, rest[:end])
	require.NoError(t, err)
	return ts
}

func TestFetchWorklogsOp_CtxCancellationMidIssueLoop(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 5, baseTime, time.Minute, 10001)

	ctx, cancel := context.WithCancel(context.Background())
	// Mock cancels ctx after the second worklog request fires.
	worklogCallCount := 0
	var mu sync.Mutex

	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "10000", Key: "PROJ"}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			return searchIssuesResponse(issues, ""), nil
		case *connectorpb.JiraListIssueWorklogsRequest:
			mu.Lock()
			worklogCallCount++
			cnt := worklogCallCount
			mu.Unlock()
			if cnt == 2 {
				cancel()
			}
			return worklogsResponse([]jiraWorklog{{ID: fmt.Sprintf("%d", 10000+cnt), Created: jiraTime{time.Now().UTC()}}}, 1), nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(ctx, params)
	require.Error(t, err, "ctx cancellation must produce a wrapped error")
	assert.Contains(t, err.Error(), "ctx exhausted")
	require.ErrorIs(t, err, context.Canceled, "underlying error must be context.Canceled")

	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.Equal(t, "10002", finalState.LastIssueID,
		"cursor must reflect last SUCCESSFULLY processed issue (10002), not advanced past")
}

// TestFetchWorklogsOp_FirstIssueFailure_PreservesPriorCursor pins the edge
// case where the very first issue fails on a fresh project: cursorStates
// must not be written for this project, so any prior cursor entry survives
// untouched. Without this guarantee a transient failure on the first issue
// of a long-stale project would silently reset its cursor.
func TestFetchWorklogsOp_FirstIssueFailure_PreservesPriorCursor(t *testing.T) {
	priorTime := time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC)
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   priorTime,
		LastIssueID: "9000",
	})

	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 3, baseTime, time.Minute, 10001)

	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		worklogsByIssue: map[string]func() (*connectorpb.JiraListIssueWorklogsResponse, error){
			"PROJ-1": func() (*connectorpb.JiraListIssueWorklogsResponse, error) {
				return nil, fmt.Errorf("simulated 500 on PROJ-1")
			},
			// PROJ-2 and PROJ-3 must NOT be invoked.
			"PROJ-2": defaultWorklogResponse(),
			"PROJ-3": defaultWorklogResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PROJ-1")

	assert.Equal(t, []string{"PROJ-1"}, executor.worklogsCalledFor,
		"only the first failing issue must have been called")

	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.True(t, finalState.Timestamp.Equal(priorTime),
		"prior cursor Timestamp must survive a first-issue failure (no in-run advance)")
	assert.Equal(t, "9000", finalState.LastIssueID,
		"prior LastIssueID must survive a first-issue failure")
}

// TestFetchWorklogsOp_PerIssueError_StopsIteration pins the data-loss
// prevention: when an issue fails to fetch worklogs, iteration MUST stop
// before any later success can advance the cursor past the failed issue's
// timestamp. Otherwise `isAlreadyProcessed` (UpdatedOn < cursorTime) would
// permanently skip the failed issue on retry.
func TestFetchWorklogsOp_PerIssueError_StopsIteration(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 4, baseTime, time.Minute, 10001)

	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		worklogsByIssue: map[string]func() (*connectorpb.JiraListIssueWorklogsResponse, error){
			"PROJ-1": defaultWorklogResponse(),
			"PROJ-2": func() (*connectorpb.JiraListIssueWorklogsResponse, error) {
				return nil, fmt.Errorf("simulated 500 on PROJ-2")
			},
			// PROJ-3 and PROJ-4 builders are present but must NOT be invoked —
			// iteration must stop at PROJ-2's failure.
			"PROJ-3": defaultWorklogResponse(),
			"PROJ-4": defaultWorklogResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.Error(t, err, "PROJ-2 failure must surface in returned error")
	assert.Contains(t, err.Error(), "PROJ-2")

	assert.Equal(t, []string{"PROJ-1", "PROJ-2"}, executor.worklogsCalledFor,
		"PROJ-3 and PROJ-4 must NOT be processed: advancing cursor past PROJ-2 "+
			"would cause isAlreadyProcessed to permanently skip it on retry")

	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.Equal(t, "10001", finalState.LastIssueID,
		"cursor stays at last successful issue BEFORE the failure (PROJ-1, id 10001)")
	assert.True(t, finalState.Timestamp.Equal(issues[0].Fields.Updated.Time),
		"Timestamp must reflect PROJ-1, not PROJ-2 or later")
}

func TestFetchWorklogsOp_PartialFailureAcrossProjects(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	projectAIssues := makeIssues(t, 2, baseTime, time.Minute, 10001)

	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch typed := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{
				{ID: "1", Key: "PROJA"},
				{ID: "2", Key: "PROJB"},
			}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			if typed.GetJql() != "" && contains(typed.GetJql(), "PROJB") {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: "fetch_worklogs",
					Kind:        sdk.ErrKindRateLimit,
					Cause:       fmt.Errorf("PROJB issue keys failed"),
				}
			}
			return searchIssuesResponse(projectAIssues, ""), nil
		case *connectorpb.JiraListIssueWorklogsRequest:
			return worklogsResponse([]jiraWorklog{{ID: "10001", Created: jiraTime{time.Now().UTC()}}}, 1), nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err, "one failed project is skipped; PROJA's progress stands")

	finalA := projectStateFromCursor(t, cursor, "PROJA")
	assert.Equal(t, "10002", finalA.LastIssueID, "PROJA succeeded — its cursor must advance")

	// PROJB has no entry (issue-keys fetch failed before any per-issue progress).
	parts, err := sdk.CompositeFromCursor(cursor)
	require.NoError(t, err)
	_, hasPROJB := parts["PROJB"]
	assert.False(t, hasPROJB, "PROJB had no progress — must not appear in cursor")
}

func TestFetchWorklogsOp_AllProjectsFail(t *testing.T) {
	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{
				{ID: "1", Key: "PROJA"},
				{ID: "2", Key: "PROJB"},
			}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			return nil, fmt.Errorf("simulated REST 500")
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all 2 attempted projects failed")
}

func TestFetchWorklogsOp_EmptyIssueList_CursorUnchanged(t *testing.T) {
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC),
		LastIssueID: "10500",
	})

	executor := &worklogsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     nil,
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Empty(t, executor.worklogsCalledFor, "no issues — no worklog calls")

	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.Equal(t, "10500", finalState.LastIssueID,
		"empty fetch must not erase prior LastIssueID")
}

// TestFetchWorklogsOp_MultiRunResume simulates the production loop-forever
// scenario at correct cardinality: three sequential runs, two cancellations,
// one clean completion. After three runs every issue is processed exactly
// once and the cursor ends at the highest (T, ID).
func TestFetchWorklogsOp_MultiRunResume(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 9, baseTime, time.Minute, 10001)

	type runResult struct {
		processedKeys []string
		cursor        *sdk.Cursor
		err           error
	}

	// run() executes the operation with a ctx that cancels after `cancelAfterN`
	// worklog calls. cancelAfterN <= 0 means never cancel.
	run := func(t *testing.T, priorCursor *sdk.Cursor, cancelAfterN int) runResult {
		t.Helper()
		var processed []string
		var mu sync.Mutex
		worklogCalls := 0

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch typed := req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "1", Key: "PROJ"}}, true), nil
			case *connectorpb.JiraSearchIssuesRequest:
				return searchIssuesResponse(issues, ""), nil
			case *connectorpb.JiraListIssueWorklogsRequest:
				mu.Lock()
				worklogCalls++
				currentCount := worklogCalls
				processed = append(processed, typed.GetIssueIdOrKey())
				mu.Unlock()
				resp := worklogsResponse([]jiraWorklog{{ID: fmt.Sprintf("%d", 10000+currentCount), Created: jiraTime{time.Now().UTC()}}}, 1)
				if cancelAfterN > 0 && currentCount == cancelAfterN {
					cancel()
				}
				return resp, nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		}

		gw := &mockVendorGateway{executeFunc: executeFunc}
		sink := testkit.NewChannelSink()
		params := testParams(sink, gw, priorCursor)

		op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
		cursor, err := op.Execute(ctx, params)
		return runResult{processedKeys: processed, cursor: cursor, err: err}
	}

	// Run 1: cancel after 3 issues processed.
	run1 := run(t, nil, 3)
	require.Error(t, run1.err)
	assert.Equal(t, []string{"PROJ-1", "PROJ-2", "PROJ-3"}, run1.processedKeys)
	state1 := projectStateFromCursor(t, run1.cursor, "PROJ")
	assert.Equal(t, "10003", state1.LastIssueID)

	// Run 2: resume from cursor, cancel after another 3.
	run2 := run(t, run1.cursor, 3)
	require.Error(t, run2.err)
	assert.Equal(t, []string{"PROJ-4", "PROJ-5", "PROJ-6"}, run2.processedKeys,
		"must resume right after run1's stop point")
	state2 := projectStateFromCursor(t, run2.cursor, "PROJ")
	assert.Equal(t, "10006", state2.LastIssueID)

	// Run 3: resume from cursor, no cancel.
	run3 := run(t, run2.cursor, -1)
	require.NoError(t, run3.err)
	assert.Equal(t, []string{"PROJ-7", "PROJ-8", "PROJ-9"}, run3.processedKeys,
		"final run finishes the remaining tail")
	state3 := projectStateFromCursor(t, run3.cursor, "PROJ")
	assert.Equal(t, "10009", state3.LastIssueID)

	// End-to-end invariant: every issue processed exactly once across the three runs.
	combined := append(append([]string{}, run1.processedKeys...), run2.processedKeys...)
	combined = append(combined, run3.processedKeys...)
	assert.Equal(t,
		[]string{"PROJ-1", "PROJ-2", "PROJ-3", "PROJ-4", "PROJ-5", "PROJ-6", "PROJ-7", "PROJ-8", "PROJ-9"},
		combined,
		"three runs combined must process every issue exactly once in order")
}

// contains is a small helper so we don't need strings imported.
func contains(haystack, needle string) bool {
	for index := 0; index+len(needle) <= len(haystack); index++ {
		if haystack[index:index+len(needle)] == needle {
			return true
		}
	}
	return false
}
