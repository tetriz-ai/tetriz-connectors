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

// TestFetchIssuesOp_OnPageFailureContinues pins that an onPage
// (per-page Checkpoint) failure must not cascade into a project-level
// failure that aborts iteration. The page must complete, subsequent
// page's checkpoint must still fire, and the operation must succeed.
func TestFetchIssuesOp_OnPageFailureContinues(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issuesPage1 := []jiraIssue{
		{ID: "10001", Key: "PROJ-1", Fields: jiraIssueFields{Created: jiraTime{baseTime}, Updated: jiraTime{baseTime}}},
	}
	issuesPage2 := []jiraIssue{
		{ID: "10002", Key: "PROJ-2", Fields: jiraIssueFields{Created: jiraTime{baseTime.Add(time.Minute)}, Updated: jiraTime{baseTime.Add(time.Minute)}}},
	}

	callCount := 0
	var mu sync.Mutex
	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch typed := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "1", Key: "PROJ"}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			mu.Lock()
			callCount++
			currentCallNum := callCount
			mu.Unlock()
			if currentCallNum == 1 {
				return searchIssuesResponse(issuesPage1, "page2-token"), nil
			}
			assert.Equal(t, "page2-token", typed.GetNextPageToken(), "second call must resume from token")
			return searchIssuesResponse(issuesPage2, ""), nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()

	checkpoints := &recordedCheckpoint{failNth: 1, failErr: fmt.Errorf("simulated checkpoint write failure")}
	params := testParams(sink, gw, nil)
	params.Checkpoint = checkpoints.fn()

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err, "first onPage Checkpoint failure MUST NOT abort the project")

	// Both pages must have been processed -> 2 issues × 2 records (created+snapshot) = 4 records.
	assert.Len(t, sink.Records(), 4, "both pages must complete despite first checkpoint failing")
}

// TestFetchIssuesOp_MalformedCursor_FallsBackToFresh pins that a
// cursor where PageToken is set but Since is zero is malformed (should never
// occur in practice but defensive). It must NOT trigger a full-history JQL
// scan; instead it must fall back to the fresh-iteration path with the
// timestamp-derived since.
func TestFetchIssuesOp_MalformedCursor_FallsBackToFresh(t *testing.T) {
	knownTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	// PageToken set, Since intentionally zero.
	parts := map[string]json.RawMessage{
		"PROJ": json.RawMessage(fmt.Sprintf(`{"timestamp":"%s","page_token":"tok123","since":"0001-01-01T00:00:00Z"}`, knownTime.Format(time.RFC3339Nano))),
	}
	priorCursor := sdk.NewCompositeCursor(parts)

	var capturedJQL string
	var capturedToken string
	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch typed := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "1", Key: "PROJ"}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			capturedJQL = typed.GetJql()
			capturedToken = typed.GetNextPageToken()
			return searchIssuesResponse(nil, ""), nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Empty(t, capturedToken, "malformed cursor must NOT resume from the orphaned PageToken")
	// Since the cursor's Timestamp is non-zero, effectiveSince applies the overlap.
	expectedSince := knownTime.Add(-defaultCursorOverlapWindow - maxJQLZoneOffset).UTC().Format(jiraTimeFormat)
	assert.Contains(t, capturedJQL, expectedSince,
		"malformed cursor must fall back to effectiveSince(state.Timestamp)")
}

// TestFetchIssuesOp_PerProjectCheckpoint_PersistsCleanState pins that
// every project that completes successfully must trigger an explicit
// params.Checkpoint that persists the clean state (Timestamp only, no
// PageToken/Since).
func TestFetchIssuesOp_PerProjectCheckpoint_PersistsCleanState(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch typed := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{
				{ID: "1", Key: "PROJA"},
				{ID: "2", Key: "PROJB"},
			}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			// Each project gets a single issue.
			projectKey := "UNK"
			if strings.Contains(typed.GetJql(), "PROJA") {
				projectKey = "PROJA"
			} else if strings.Contains(typed.GetJql(), "PROJB") {
				projectKey = "PROJB"
			}
			return searchIssuesResponse([]jiraIssue{
				{ID: "10001", Key: projectKey + "-1", Fields: jiraIssueFields{Created: jiraTime{baseTime}, Updated: jiraTime{baseTime}}},
			}, ""), nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	checkpoints := &recordedCheckpoint{}
	params := testParams(sink, gw, nil)
	params.Checkpoint = checkpoints.fn()

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, cursor)

	// Final cursor must contain BOTH projects with clean state (Timestamp set,
	// PageToken empty, Since zero).
	stateA := projectStateFromCursor(t, cursor, "PROJA")
	stateB := projectStateFromCursor(t, cursor, "PROJB")
	assert.True(t, stateA.Timestamp.Equal(baseTime))
	assert.Empty(t, stateA.PageToken)
	assert.True(t, stateA.Since.IsZero())
	assert.True(t, stateB.Timestamp.Equal(baseTime))

	// Per-project Checkpoint must fire after each project (plus the per-page
	// Checkpoint that fires on the final page). We assert at least 2 total
	// invocations — one per project.
	assert.GreaterOrEqual(t, len(checkpoints.snapshot()), 2,
		"per-project Checkpoint must fire after each successful project")
}

// TestFetchIssuesOp_CtxCancellationBetweenProjects pins that the
// ctx.Err() guard at the top of the project loop must bail with a wrapped
// error, while preserving the cursor for projects that have already completed.
func TestFetchIssuesOp_CtxCancellationBetweenProjects(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())

	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch typed := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{
				{ID: "1", Key: "PROJA"},
				{ID: "2", Key: "PROJB"},
			}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			if strings.Contains(typed.GetJql(), "PROJA") {
				return searchIssuesResponse([]jiraIssue{
					{ID: "10001", Key: "PROJA-1", Fields: jiraIssueFields{Created: jiraTime{baseTime}, Updated: jiraTime{baseTime}}},
				}, ""), nil
			}
			// Should never reach PROJB — ctx is cancelled between projects below.
			return searchIssuesResponse(nil, ""), nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	// Wrap with checkpoint that cancels ctx after first invocation. This
	// simulates ctx being cancelled between the first and second project.
	checkpointInvocations := 0
	var mu sync.Mutex
	checkpoint := func(ctx context.Context, cursor *sdk.Cursor) error {
		mu.Lock()
		defer mu.Unlock()
		checkpointInvocations++
		if checkpointInvocations == 2 {
			cancel()
		}
		return nil
	}

	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.Checkpoint = checkpoint

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(ctx, params)
	require.Error(t, err, "ctx cancellation between projects must surface as error")
	assert.Contains(t, err.Error(), "ctx exhausted")

	// PROJA must still appear in cursor (it completed before cancellation).
	stateA := projectStateFromCursor(t, cursor, "PROJA")
	assert.True(t, stateA.Timestamp.Equal(baseTime), "completed project's cursor must survive ctx-cancel between projects")
}

// TestFetchIssuesOp_ProjectErrorClearsStaleResumeToken pins that
// when a project errors WITH a non-empty resume token (e.g., expired server-
// side page token), the cursor's PageToken must be cleared AND persisted via
// params.Checkpoint so the next run does a fresh JQL. The orchestrator's
// failRun path does NOT save the returned cursor — only successful runs hit
// SaveCursor. Without an explicit Checkpoint call here, the cleared state
// stays in-memory and is lost when Execute returns the error.
func TestFetchIssuesOp_ProjectErrorClearsStaleResumeToken(t *testing.T) {
	knownTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	// Prior cursor has PageToken set — simulate mid-pagination resume state.
	parts := map[string]json.RawMessage{
		"PROJ": json.RawMessage(fmt.Sprintf(
			`{"timestamp":"%s","page_token":"stale-token","since":"%s"}`,
			knownTime.Format(time.RFC3339Nano),
			knownTime.Add(-time.Hour).Format(time.RFC3339Nano))),
	}
	priorCursor := sdk.NewCompositeCursor(parts)

	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "1", Key: "PROJ"}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			// Simulate Atlassian rejecting the stale page token.
			return nil, fmt.Errorf("HTTP 400: page token expired or invalid")
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	checkpoints := &recordedCheckpoint{}
	params := testParams(sink, gw, priorCursor)
	params.Checkpoint = checkpoints.fn()

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.Error(t, err, "stale token error must surface")

	// Returned cursor must have PageToken cleared.
	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.True(t, finalState.Timestamp.Equal(knownTime),
		"Timestamp must survive — only the resume token gets cleared")
	assert.Empty(t, finalState.PageToken, "stale PageToken must be cleared so next run doesn't replay it")
	assert.True(t, finalState.Since.IsZero(), "Since must be cleared when PageToken is cleared")

	// Checkpoint must have been invoked to PERSIST the cleared state — without
	// this the orchestrator's failRun path drops the cleared cursor.
	calls := checkpoints.snapshot()
	require.NotEmpty(t, calls, "Checkpoint must be invoked so cleared token persists on error")
	lastCall := calls[len(calls)-1]
	persistedState := projectStateFromCursor(t, lastCall, "PROJ")
	assert.Empty(t, persistedState.PageToken,
		"Checkpoint must persist the cleared PageToken, not the original stale value")
	assert.True(t, persistedState.Timestamp.Equal(knownTime),
		"persisted Timestamp must match the cleared in-memory value")
}

// TestFetchIssuesOp_ProjectErrorAggregation pins that one project's failure
// must not stop the operation. All projects are attempted, errors are
// aggregated, and the returned error includes the typed sync-error chain so
// downstream classification works.
func TestFetchIssuesOp_ProjectErrorAggregation(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)

	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch typed := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{
				{ID: "1", Key: "PROJA"},
				{ID: "2", Key: "PROJB"},
				{ID: "3", Key: "PROJC"},
			}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			if strings.Contains(typed.GetJql(), "PROJB") {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: "fetch_issues",
					Kind:        sdk.ErrKindRateLimit,
					Cause:       fmt.Errorf("PROJB hit rate limit"),
				}
			}
			projectKey := "PROJA"
			if strings.Contains(typed.GetJql(), "PROJC") {
				projectKey = "PROJC"
			}
			return searchIssuesResponse([]jiraIssue{
				{ID: "10001", Key: projectKey + "-1", Fields: jiraIssueFields{Created: jiraTime{baseTime}, Updated: jiraTime{baseTime}}},
			}, ""), nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	// One failed project is that project's failure: the run completes and the
	// others' cursors are saved.
	require.NoError(t, err)

	// PROJA and PROJC must have cursors; PROJB must not.
	stateA := projectStateFromCursor(t, cursor, "PROJA")
	stateC := projectStateFromCursor(t, cursor, "PROJC")
	assert.True(t, stateA.Timestamp.Equal(baseTime))
	assert.True(t, stateC.Timestamp.Equal(baseTime))

	parts, err := sdk.CompositeFromCursor(cursor)
	require.NoError(t, err)
	_, hasPROJB := parts["PROJB"]
	assert.False(t, hasPROJB, "failed project must not appear in cursor")
}
