// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

type jiraRecordedWrite struct {
	Key     string
	Content []byte
}

func newJiraRecordingS3(t *testing.T) (*sdkmocks.MockBlobStore, *[]jiraRecordedWrite) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mock := sdkmocks.NewMockBlobStore(ctrl)
	writes := &[]jiraRecordedWrite{}
	mock.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, dst string, content []byte, _ string) error {
			cp := make([]byte, len(content))
			copy(cp, content)
			*writes = append(*writes, jiraRecordedWrite{Key: dst, Content: cp})
			return nil
		}).AnyTimes()
	return mock, writes
}

// configWithProjectKeys returns a test config that pins a known
// project_key set so fetch_issues_data doesn't need to roundtrip
// through fetchAllProjectKeys' /projects listing.
func configWithProjectKeys(projectKeys ...string) *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID:    "jira",
		OrganizationID: "org-test",
		Credentials: mustJSON(JiraCredentials{
			OAuthRefreshToken: "test-refresh-token",
		}),
		Settings: mustJSON(JiraSettings{
			CloudID:     "abc123-def456",
			ProjectKeys: projectKeys,
		}),
		Enabled: true,
	}
}

// jiraIssueDataResponseRaw builds a JiraSearchIssuesResponse whose
// issues field carries one issue with the narrow projection
// FetchIssuesDataOp consumes (id, key, fields.{updated, description,
// project}).
func jiraIssueDataResponseRaw(t *testing.T, issues []jiraIssueDescription, nextPageToken string) *connectorpb.JiraSearchIssuesResponse {
	t.Helper()
	out := make([][]byte, len(issues))
	for i, iss := range issues {
		raw, err := json.Marshal(iss)
		require.NoError(t, err)
		out[i] = raw
	}
	return &connectorpb.JiraSearchIssuesResponse{
		Issues:        out,
		NextPageToken: nextPageToken,
	}
}

// TestFetchIssuesDataOp_Execute exercises the ADF-source variant of
// the canonical pipeline + S3 PUT + RawRecord emit flow. Each row
// asserts (a) the canonical text the connector hashes equals the
// bytes uploaded to S3, (b) payload.issue_key is the Jira issue
// key verbatim (matches a consumer's issue key for jira-sourced rows),
// (c) S3 path uses project.id (rename-proof) not project.key.
func TestFetchIssuesDataOp_Execute(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	const (
		issueKey  = "PROJ-100"
		projectID = "10042"
	)

	mkIssue := func(adf json.RawMessage) jiraIssueDescription {
		iss := jiraIssueDescription{ID: "10000", Key: issueKey}
		iss.Fields.Updated = jiraTime{Time: now}
		iss.Fields.Description = adf
		iss.Fields.Project = &struct {
			ID  string `json:"id"`
			Key string `json:"key"`
		}{ID: projectID, Key: "PROJ"}
		return iss
	}

	tests := []struct {
		name      string
		adf       json.RawMessage
		wantS3Key string
		wantEmit  bool
	}{
		{
			name:      "real ADF body uploads raw JSON + emits ref with sha",
			adf:       json.RawMessage(`{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"Bug: fixes PROJ-99"}]}]}`),
			wantS3Key: fmt.Sprintf("descriptions/org-test/%s/issues/%s/%s", connectorID, projectID, issueKey),
			wantEmit:  true,
		},
		{
			name:     "nil description: no S3 PUT, no emit",
			adf:      nil,
			wantEmit: false,
		},
		{
			name:     "JSON null description: no S3 PUT, no emit",
			adf:      json.RawMessage("null"),
			wantEmit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
					return jiraIssueDataResponseRaw(t,
						[]jiraIssueDescription{mkIssue(tt.adf)}, ""), nil
				},
			}

			sink := testkit.NewChannelSink()
			s3, writes := newJiraRecordingS3(t)
			params := testParams(sink, gw, nil)
			params.Config = configWithProjectKeys("PROJ")
			params.BlobStore = s3

			op := &FetchIssuesDataOp{}
			cursor, err := op.Execute(context.Background(), params)
			require.NoError(t, err)
			require.NotNil(t, cursor)

			records := sink.Drain()
			if !tt.wantEmit {
				assert.Empty(t, records, "no-blob outcome must not emit")
				assert.Empty(t, *writes, "no-blob outcome must not touch S3")
				return
			}
			require.Len(t, records, 1)

			rec := records[0]
			assert.Equal(t, "fetch_issues_data", rec.OperationID)
			assert.Equal(t, "10000", rec.ExternalID, "ExternalID is the numeric issue id, which survives a move")
			assert.True(t, rec.DataDate.IsZero(), "a description pointer is current state: no DataDate")

			var payload map[string]any
			require.NoError(t, json.Unmarshal(rec.Payload, &payload))
			assert.Equal(t, tt.wantS3Key, payload["s3_key"])
			assert.Equal(t, issueKey, payload["issue_key"], "issue_key must equal Jira issue.key verbatim")
			_, hasNaturalID := payload["parent_natural_id"]
			assert.False(t, hasNaturalID, "parent_natural_id dropped from payload")
			assert.Equal(t, projectID, payload["platform_project_id"])
			_, hasRepo := payload["repo_full_name"]
			assert.False(t, hasRepo, "Jira payload omits repo_full_name (omitempty)")

			digest := sha256.Sum256([]byte(tt.adf))
			wantSha := "sha256:" + hex.EncodeToString(digest[:])
			assert.Equal(t, wantSha, rec.Metadata["description_sha"], "sha must equal sha256(raw ADF bytes)")

			require.Len(t, *writes, 1)
			assert.Equal(t, tt.wantS3Key, (*writes)[0].Key)
			assert.Equal(t, []byte(tt.adf), (*writes)[0].Content, "S3 bytes must equal the raw ADF bytes")
		})
	}
}

// TestFetchIssuesDataOp_ErrorPaths pins the fetch_issues_data error-handling
// contract across the three failure shapes that share one Execute loop:
//   - a dead resume PageToken is cleared AND persisted via params.Checkpoint, so
//     the next run does a fresh JQL instead of replaying the same token forever;
//   - total failure returns a real error (never nil / green-on-total-failure);
//   - a project that succeeds is checkpointed mid-run even when a sibling later
//     fails, so its progress survives the orchestrator dropping the returned
//     cursor on error (failRun does NOT SaveCursor).
//
// Each row carries a verify closure because the assertions differ per shape
// (cleared-token state vs error message vs persisted sibling cursor); the shared
// harness owns the identical Execute wiring.
func TestFetchIssuesDataOp_ErrorPaths(t *testing.T) {
	t.Parallel()

	knownTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	now := time.Now().UTC().Truncate(time.Second)

	okIssue := jiraIssueDescription{ID: "10000", Key: "PROJA-1"}
	okIssue.Fields.Updated = jiraTime{Time: now}
	okIssue.Fields.Description = json.RawMessage(`{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"hi"}]}]}`)
	okIssue.Fields.Project = &struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	}{ID: "10042", Key: "PROJA"}

	// staleTokenCursor seeds a mid-pagination resume state for "PROJ".
	staleTokenCursor := sdk.NewCompositeCursor(map[string]json.RawMessage{
		"PROJ": json.RawMessage(fmt.Sprintf(
			`{"timestamp":"%s","page_token":"stale-token","since":"%s"}`,
			knownTime.Format(time.RFC3339Nano),
			knownTime.Add(-time.Hour).Format(time.RFC3339Nano))),
	})

	tests := []struct {
		name        string
		projectKeys []string
		priorCursor *sdk.Cursor
		executeFunc func(ctx context.Context, req proto.Message) (proto.Message, error)
		wantErr     string
		wantNoErr   bool
		verify      func(t *testing.T, cursor *sdk.Cursor, checkpoints *recordedCheckpoint)
	}{
		{
			name:        "dead resume token is cleared and persisted via checkpoint",
			projectKeys: []string{"PROJ"},
			priorCursor: staleTokenCursor,
			executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
				// Simulate Atlassian rejecting the stale page token.
				return nil, fmt.Errorf("HTTP 400: page token expired or invalid")
			},
			verify: func(t *testing.T, cursor *sdk.Cursor, checkpoints *recordedCheckpoint) {
				// Returned cursor keeps the high-water mark but drops the token.
				finalState := projectStateFromCursor(t, cursor, "PROJ")
				assert.True(t, finalState.Timestamp.Equal(knownTime),
					"Timestamp must survive — only the resume token gets cleared")
				assert.Empty(t, finalState.PageToken, "stale PageToken must be cleared so next run doesn't replay it")
				assert.True(t, finalState.Since.IsZero(), "Since must be cleared when PageToken is cleared")

				// Checkpoint must have PERSISTED the cleared state — without this
				// the orchestrator's failRun path drops the cleared cursor.
				calls := checkpoints.snapshot()
				require.NotEmpty(t, calls, "Checkpoint must be invoked so the cleared token persists on error")
				persistedState := projectStateFromCursor(t, calls[len(calls)-1], "PROJ")
				assert.Empty(t, persistedState.PageToken,
					"Checkpoint must persist the cleared PageToken, not the original stale value")
				assert.True(t, persistedState.Timestamp.Equal(knownTime),
					"persisted Timestamp must match the cleared in-memory value")
			},
		},
		{
			name:        "total failure returns a real error, not green",
			projectKeys: []string{"PROJA", "PROJB"},
			executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
				return nil, fmt.Errorf("HTTP 500: upstream unavailable")
			},
			wantErr: "all 2 attempted projects failed",
		},
		{
			name:        "successful project is checkpointed before a sibling fails",
			projectKeys: []string{"PROJA", "PROJB"},
			executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				search, ok := req.(*connectorpb.JiraSearchIssuesRequest)
				require.True(t, ok, "only search requests expected (project keys are pinned)")
				// PROJA succeeds in a single page; PROJB fails on a fresh attempt
				// (no resume token), so the error-branch token-clear is skipped
				// and the ONLY checkpoint that can fire is PROJA's success one.
				if strings.Contains(search.GetJql(), "PROJB") {
					return nil, fmt.Errorf("HTTP 500: upstream unavailable")
				}
				return jiraIssueDataResponseRaw(t, []jiraIssueDescription{okIssue}, ""), nil
			},
			// One failed project is skipped, so the run completes.
			wantNoErr: true,
			verify: func(t *testing.T, _ *sdk.Cursor, checkpoints *recordedCheckpoint) {
				calls := checkpoints.snapshot()
				require.NotEmpty(t, calls, "successful project must be checkpointed before the run errors out")
				stateA := projectStateFromCursor(t, calls[len(calls)-1], "PROJA")
				assert.True(t, stateA.Timestamp.Equal(now), "PROJA's advanced high-water mark must be persisted")
				assert.Empty(t, stateA.PageToken, "a single-page success persists a clean cursor")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gw := &mockVendorGateway{executeFunc: tt.executeFunc}
			sink := testkit.NewChannelSink()
			s3, _ := newJiraRecordingS3(t)
			checkpoints := &recordedCheckpoint{}
			params := testParams(sink, gw, tt.priorCursor)
			params.Config = configWithProjectKeys(tt.projectKeys...)
			params.BlobStore = s3
			params.Checkpoint = checkpoints.fn()

			op := &FetchIssuesDataOp{}
			cursor, err := op.Execute(context.Background(), params)
			if tt.wantNoErr {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "an error path must surface a real error, not be swallowed")
			}
			if tt.wantErr != "" {
				assert.Contains(t, err.Error(), tt.wantErr)
			}
			if tt.verify != nil {
				tt.verify(t, cursor, checkpoints)
			}
		})
	}
}

// TestFetchIssuesDataOp_SkipsWithoutProjectID asserts the defensive
// branch: an issue payload missing fields.project.id can't yield a
// rename-proof S3 key (project.key must not appear in the path, since a project can be renamed), so
// the op skips that issue rather than emitting a malformed ref.
func TestFetchIssuesDataOp_SkipsWithoutProjectID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			iss := jiraIssueDescription{ID: "10000", Key: "PROJ-1"}
			iss.Fields.Updated = jiraTime{Time: now}
			iss.Fields.Description = json.RawMessage(`{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"hi"}]}]}`)
			// Intentionally no Project block.
			return jiraIssueDataResponseRaw(t, []jiraIssueDescription{iss}, ""), nil
		},
	}

	sink := testkit.NewChannelSink()
	s3, writes := newJiraRecordingS3(t)
	params := testParams(sink, gw, nil)
	params.Config = configWithProjectKeys("PROJ")
	params.BlobStore = s3

	op := &FetchIssuesDataOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Empty(t, sink.Drain())
	assert.Empty(t, *writes)
}

// A run whose context has ended stops before the next project and reports the
// context error, not a project failure.
func TestFetchIssuesDataOp_StopsWhenTheContextEnds(t *testing.T) {
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
		return nil, fmt.Errorf("must not be called")
	}}
	params := testParams(testkit.NewChannelSink(), gw, nil)
	params.Config = configWithProjectKeys("PROJA", "PROJB")
	params.BlobStore, _ = newJiraRecordingS3(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&FetchIssuesDataOp{}).Execute(ctx, params)
	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "ctx exhausted after 0/2 projects")
}
