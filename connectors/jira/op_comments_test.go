// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// commentsExecutor is the comments-flavour twin of worklogsExecutor.
type commentsExecutor struct {
	t                 *testing.T
	projectKey        string
	issues            []jiraIssue
	capturedJQL       string
	commentsByIssue   map[string]func() (*connectorpb.JiraListIssueCommentsResponse, error)
	commentsCalledFor []string
	mu                sync.Mutex
}

func (ce *commentsExecutor) executeFunc() func(ctx context.Context, req proto.Message) (proto.Message, error) {
	return func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch typed := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "10000", Key: ce.projectKey}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			ce.mu.Lock()
			ce.capturedJQL = typed.GetJql()
			ce.mu.Unlock()
			return searchIssuesResponse(ce.issues, ""), nil
		case *connectorpb.JiraListIssueCommentsRequest:
			ce.mu.Lock()
			ce.commentsCalledFor = append(ce.commentsCalledFor, typed.GetIssueIdOrKey())
			ce.mu.Unlock()
			builder, ok := ce.commentsByIssue[typed.GetIssueIdOrKey()]
			if !ok {
				return commentsResponse(nil, 0), nil
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

func defaultCommentResponse() func() (*connectorpb.JiraListIssueCommentsResponse, error) {
	return func() (*connectorpb.JiraListIssueCommentsResponse, error) {
		return commentsResponse([]jiraComment{
			{ID: "c-1", Created: jiraTime{time.Now().UTC()}},
		}, 1), nil
	}
}

// --- FetchCommentsOp cursor-resume tests (mirror of worklogs) ---

func TestFetchCommentsOp_FirstSync_ProcessesAllIssues(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 3, baseTime, time.Minute, 10001)

	executor := &commentsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		commentsByIssue: map[string]func() (*connectorpb.JiraListIssueCommentsResponse, error){
			"PROJ-1": defaultCommentResponse(),
			"PROJ-2": defaultCommentResponse(),
			"PROJ-3": defaultCommentResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	checkpoints := &recordedCheckpoint{}
	params := testParams(sink, gw, nil)
	params.Checkpoint = checkpoints.fn()

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, cursor)

	assert.ElementsMatch(t, []string{"PROJ-1", "PROJ-2", "PROJ-3"}, executor.commentsCalledFor)
	assert.Len(t, sink.Records(), 3)

	finalState := projectStateFromCursor(t, cursor, "PROJ")
	assert.Equal(t, "10003", finalState.LastIssueID)
	assert.Len(t, checkpoints.snapshot(), 3, "per-issue checkpoint fires once per processed issue")
}

func TestFetchCommentsOp_Resume_SkipsAlreadyProcessed(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 5, baseTime, time.Minute, 10001)
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   issues[1].Fields.Updated.Time,
		LastIssueID: "10002",
	})

	executor := &commentsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		commentsByIssue: map[string]func() (*connectorpb.JiraListIssueCommentsResponse, error){
			"PROJ-3": defaultCommentResponse(),
			"PROJ-4": defaultCommentResponse(),
			"PROJ-5": defaultCommentResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"PROJ-3", "PROJ-4", "PROJ-5"}, executor.commentsCalledFor)
	assert.Equal(t, "10005", projectStateFromCursor(t, cursor, "PROJ").LastIssueID)
}

func TestFetchCommentsOp_SameTimestampTie_NoMiss(t *testing.T) {
	atSameTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 5, atSameTime, 0, 10001)
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   atSameTime,
		LastIssueID: "10002",
	})

	executor := &commentsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		commentsByIssue: map[string]func() (*connectorpb.JiraListIssueCommentsResponse, error){
			"PROJ-3": defaultCommentResponse(),
			"PROJ-4": defaultCommentResponse(),
			"PROJ-5": defaultCommentResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"PROJ-3", "PROJ-4", "PROJ-5"}, executor.commentsCalledFor)
	assert.Equal(t, "10005", projectStateFromCursor(t, cursor, "PROJ").LastIssueID)
}

func TestFetchCommentsOp_SameTimestamp_NewIssueWithLowerKey_NotMissed(t *testing.T) {
	atSameTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := []jiraIssue{
		{ID: "10500", Key: "PROJ-500", Fields: jiraIssueFields{Updated: jiraTime{atSameTime}}},
		{ID: "10999", Key: "PROJ-499", Fields: jiraIssueFields{Updated: jiraTime{atSameTime}}},
	}
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   atSameTime,
		LastIssueID: "10500",
	})

	executor := &commentsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		commentsByIssue: map[string]func() (*connectorpb.JiraListIssueCommentsResponse, error){
			"PROJ-499": defaultCommentResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, []string{"PROJ-499"}, executor.commentsCalledFor)
	assert.Equal(t, "10999", projectStateFromCursor(t, cursor, "PROJ").LastIssueID)
}

func TestFetchCommentsOp_OverlapWindow_AppliedOnResume(t *testing.T) {
	cursorTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   cursorTime,
		LastIssueID: "10500",
	})

	executor := &commentsExecutor{t: t, projectKey: "PROJ"}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	expectedSince := cursorTime.Add(-defaultCursorOverlapWindow - maxJQLZoneOffset).UTC().Format(jiraTimeFormat)
	assert.Contains(t, executor.capturedJQL, expectedSince,
		"JQL must apply cursorOverlapWindow on resume")
}

func TestFetchCommentsOp_CtxCancellationMidIssueLoop(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 5, baseTime, time.Minute, 10001)

	ctx, cancel := context.WithCancel(context.Background())
	commentCalls := 0
	var mu sync.Mutex

	executeFunc := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "1", Key: "PROJ"}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			return searchIssuesResponse(issues, ""), nil
		case *connectorpb.JiraListIssueCommentsRequest:
			mu.Lock()
			commentCalls++
			cnt := commentCalls
			mu.Unlock()
			if cnt == 2 {
				cancel()
			}
			return commentsResponse([]jiraComment{{ID: fmt.Sprintf("c-%d", cnt), Created: jiraTime{time.Now().UTC()}}}, 1), nil
		default:
			return nil, fmt.Errorf("unexpected: %T", req)
		}
	}
	gw := &mockVendorGateway{executeFunc: executeFunc}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(ctx, params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ctx exhausted")
	require.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, "10002", projectStateFromCursor(t, cursor, "PROJ").LastIssueID)
}

// TestFetchCommentsOp_PerIssueError_StopsIteration: mirror of the worklogs
// data-loss-prevention test. After PROJ-2 fails, no later issues must be
// processed; cursor must stay at PROJ-1.
func TestFetchCommentsOp_PerIssueError_StopsIteration(t *testing.T) {
	baseTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	issues := makeIssues(t, 4, baseTime, time.Minute, 10001)

	executor := &commentsExecutor{
		t:          t,
		projectKey: "PROJ",
		issues:     issues,
		commentsByIssue: map[string]func() (*connectorpb.JiraListIssueCommentsResponse, error){
			"PROJ-1": defaultCommentResponse(),
			"PROJ-2": func() (*connectorpb.JiraListIssueCommentsResponse, error) {
				return nil, fmt.Errorf("simulated 500 on PROJ-2")
			},
			// PROJ-3, PROJ-4 must NOT be invoked.
			"PROJ-3": defaultCommentResponse(),
			"PROJ-4": defaultCommentResponse(),
		},
	}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PROJ-2")

	assert.Equal(t, []string{"PROJ-1", "PROJ-2"}, executor.commentsCalledFor,
		"PROJ-3 and PROJ-4 must NOT be processed: advancing cursor past PROJ-2 "+
			"would cause isAlreadyProcessed to permanently skip it on retry")
	assert.Equal(t, "10001", projectStateFromCursor(t, cursor, "PROJ").LastIssueID,
		"cursor stays at last successful issue BEFORE the failure")
}

func TestFetchCommentsOp_EmptyIssueList_CursorUnchanged(t *testing.T) {
	priorCursor := buildWorklogsCursor(t, "PROJ", projectCursorState{
		Timestamp:   time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC),
		LastIssueID: "10500",
	})

	executor := &commentsExecutor{t: t, projectKey: "PROJ"}
	gw := &mockVendorGateway{executeFunc: executor.executeFunc()}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, priorCursor)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Empty(t, executor.commentsCalledFor)
	assert.Equal(t, "10500", projectStateFromCursor(t, cursor, "PROJ").LastIssueID)
}
