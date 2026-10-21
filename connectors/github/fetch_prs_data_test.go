// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

type recordedWrite struct {
	Key     string
	Content []byte
}

func newRecordingS3(t *testing.T) (*sdkmocks.MockBlobStore, *[]recordedWrite) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mock := sdkmocks.NewMockBlobStore(ctrl)
	writes := &[]recordedWrite{}
	mock.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, dst string, content []byte, _ string) error {
			cp := make([]byte, len(content))
			copy(cp, content)
			*writes = append(*writes, recordedWrite{Key: dst, Content: cp})
			return nil
		}).AnyTimes()
	return mock, writes
}

func fetchPRsDataResponseRaw(databaseID int64, nodes []ghPRDescription, hasNext bool, endCursor string) *connectorpb.GitHubGraphQLResponse {
	resp := fetchPRsDataResponse{}
	resp.Repository.DatabaseID = databaseID
	resp.Repository.PullRequests = &struct {
		Nodes    []ghPRDescription `json:"nodes"`
		PageInfo ghPageInfo        `json:"pageInfo"`
	}{
		Nodes:    nodes,
		PageInfo: ghPageInfo{HasNextPage: hasNext, EndCursor: endCursor},
	}
	raw, _ := json.Marshal(resp)
	return &connectorpb.GitHubGraphQLResponse{Data: raw}
}

func TestFetchPRsDataOp_Execute(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	const (
		platformRepoID = "999"
		repoFullName   = "acme/widgets"
	)

	tests := []struct {
		name      string
		body      string
		wantS3Key string
		wantEmit  bool
	}{
		{
			name:      "real markdown body uploads raw bytes + emits ref with sha",
			body:      "Fixes PROJ-123\n\n**Details**: see [docs](https://example.com)",
			wantS3Key: fmt.Sprintf("descriptions/org-test/%s/pull_requests/%s/42", connectorID, platformRepoID),
			wantEmit:  true,
		},
		{
			name:     "empty body: no S3 PUT, no emit",
			body:     "",
			wantEmit: false,
		},
		{
			name:     "whitespace-only body: no S3 PUT, no emit",
			body:     "   \n\n\t  \n  ",
			wantEmit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
					return fetchPRsDataResponseRaw(999, []ghPRDescription{
						{Number: 42, UpdatedAt: now, Body: tt.body},
					}, false, ""), nil
				},
			}

			sink := testkit.NewChannelSink()
			s3, writes := newRecordingS3(t)
			params := testParams(sink, gw, nil)
			params.BlobStore = s3

			op := &FetchPRsDataOp{}
			cursor, err := op.Execute(context.Background(), params)
			require.NoError(t, err)
			require.NotNil(t, cursor)

			records := sink.Drain()
			if !tt.wantEmit {
				assert.Empty(t, records, "no-blob outcome must not emit")
				assert.Empty(t, *writes, "no-blob outcome must not touch S3")
				return
			}
			require.Len(t, records, 1, "expected exactly one emit")

			rec := records[0]
			assert.Equal(t, "fetch_prs_data", rec.OperationID)
			assert.Equal(t, fmt.Sprintf("%s#PR-42", repoFullName), rec.ExternalID)
			assert.Equal(t, "org-test", rec.OrganizationID)
			assert.True(t, rec.DataDate.IsZero(), "a description pointer is current state: no DataDate, got %v", rec.DataDate)

			var payload map[string]any
			require.NoError(t, json.Unmarshal(rec.Payload, &payload))
			assert.Equal(t, tt.wantS3Key, payload["s3_key"], "payload s3_key")
			assert.EqualValues(t, 42, payload["pr_number"])
			assert.Equal(t, platformRepoID, payload["platform_repo_id"])
			assert.Equal(t, repoFullName, payload["repo_full_name"])

			digest := sha256.Sum256([]byte(tt.body))
			wantSha := "sha256:" + hex.EncodeToString(digest[:])
			assert.Equal(t, wantSha, rec.Metadata["description_sha"], "sha must equal sha256(raw body bytes)")

			require.Len(t, *writes, 1, "expected one S3 write for real ref")
			assert.Equal(t, tt.wantS3Key, (*writes)[0].Key, "S3 key must equal payload.s3_key")
			assert.Equal(t, tt.body, string((*writes)[0].Content), "S3 bytes must equal the raw body bytes the sha was computed over")
		})
	}
}

func TestFetchPRsDataOp_EarlyExitOnCursor(t *testing.T) {
	t.Parallel()

	cursorTime := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	before := cursorTime.Add(-24 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return fetchPRsDataResponseRaw(999, []ghPRDescription{
				{Number: 1, UpdatedAt: before, Body: "stale"},
				{Number: 2, UpdatedAt: before.Add(-time.Hour), Body: "also stale"},
			}, false, ""), nil
		},
	}

	composite := map[string]json.RawMessage{}
	stateBytes, _ := json.Marshal(repoCursorState{Timestamp: cursorTime})
	composite["acme/widgets"] = stateBytes
	cursor := sdk.NewCompositeCursor(composite)

	sink := testkit.NewChannelSink()
	s3, writes := newRecordingS3(t)
	params := testParams(sink, gw, cursor)
	params.BlobStore = s3

	op := &FetchPRsDataOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Empty(t, sink.Drain(), "early-exit must not emit any records")
	assert.Empty(t, *writes, "early-exit must not touch S3")
}

func TestFetchPRsDataOp_NoS3ClientSkipsTick(t *testing.T) {
	t.Parallel()

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			t.Fatal("vendor gateway should not be hit when S3Client is missing")
			return nil, nil
		},
	}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.BlobStore = nil

	op := &FetchPRsDataOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Nil(t, cursor, "skipped tick returns the input cursor unchanged")
	assert.Empty(t, sink.Drain())
}

// When every repository fails, the run fails with the first repository's
// error, so a terminal auth failure is recorded as auth and pauses the
// connector.
func TestFetchPRsDataOp_EveryRepositoryFailingFailsTheRun(t *testing.T) {
	t.Parallel()
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return nil, &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401, Terminal: true, Cause: errors.New("HTTP 401")}
		},
	}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	s3, _ := newRecordingS3(t)
	params.BlobStore = s3

	_, err := (&FetchPRsDataOp{}).Execute(context.Background(), params)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.True(t, syncErr.Terminal)
	assert.Empty(t, sink.Drain())
}
