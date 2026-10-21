// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

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
	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

type bbRecordedWrite struct {
	Key     string
	Content []byte
}

func newBBRecordingS3(t *testing.T) (*sdkmocks.MockBlobStore, *[]bbRecordedWrite) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mock := sdkmocks.NewMockBlobStore(ctrl)
	writes := &[]bbRecordedWrite{}
	mock.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, dst string, content []byte, _ string) error {
			cp := make([]byte, len(content))
			copy(cp, content)
			*writes = append(*writes, bbRecordedWrite{Key: dst, Content: cp})
			return nil
		}).AnyTimes()
	return mock, writes
}

func bbPRListResponseRaw(nodes []bbPRDescription, hasNext bool) *connectorpb.BitbucketListPullRequestsResponse {
	prsRaw := make([][]byte, len(nodes))
	for i, n := range nodes {
		raw, _ := json.Marshal(n)
		prsRaw[i] = raw
	}
	resp := &connectorpb.BitbucketListPullRequestsResponse{PullRequests: prsRaw}
	if hasNext {
		resp.NextPageUrl = "https://api.bitbucket.org/2.0/repositories/myworkspace/myrepo/pullrequests?page=2"
	}
	return resp
}

func TestFetchPRsDataOp_BB_Execute(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	const (
		repoUUID     = "{abcd-1234}"
		repoFullName = "myworkspace/myrepo"
	)

	mkNode := func(id int, desc string) bbPRDescription {
		dest := &struct {
			Repository struct {
				UUID string `json:"uuid"`
			} `json:"repository"`
		}{}
		dest.Repository.UUID = repoUUID
		return bbPRDescription{
			ID:          id,
			UpdatedOn:   now,
			Description: desc,
			Destination: dest,
		}
	}

	tests := []struct {
		name      string
		body      string
		wantS3Key string
		wantEmit  bool
	}{
		{
			name:      "real markdown body uploads raw bytes + emits ref with sha",
			body:      "Fixes PROJ-321\n\n**details** [docs](https://example.com)",
			wantS3Key: fmt.Sprintf("descriptions/org-test/%s/pull_requests/%s/42", connectorID, repoUUID),
			wantEmit:  true,
		},
		{
			name:     "empty body: no S3 PUT, no emit",
			body:     "",
			wantEmit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
					return bbPRListResponseRaw([]bbPRDescription{mkNode(42, tt.body)}, false), nil
				},
			}

			sink := testkit.NewChannelSink()
			s3, writes := newBBRecordingS3(t)
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
			assert.True(t, rec.DataDate.IsZero(), "a description pointer is current state: no DataDate (updated_on would fork it per edit)")

			var payload map[string]any
			require.NoError(t, json.Unmarshal(rec.Payload, &payload))
			assert.Equal(t, tt.wantS3Key, payload["s3_key"])
			assert.EqualValues(t, 42, payload["pr_number"])
			assert.Equal(t, repoUUID, payload["platform_repo_id"])
			assert.Equal(t, repoFullName, payload["repo_full_name"])

			digest := sha256.Sum256([]byte(tt.body))
			wantSha := "sha256:" + hex.EncodeToString(digest[:])
			assert.Equal(t, wantSha, rec.Metadata["description_sha"], "sha must equal sha256(raw body bytes)")

			require.Len(t, *writes, 1, "expected one S3 write for real ref")
			assert.Equal(t, tt.wantS3Key, (*writes)[0].Key)
			assert.Equal(t, tt.body, string((*writes)[0].Content))
		})
	}
}

// TestFetchPRsDataOp_BB_PerPageCheckpoint pins the per-page checkpoint to fire
// on EVERY page including the terminating one, with an empty cursor on the
// final page (the "finished cleanly, clear PageCursor" signal). Asserted by the
// exact checkpoint count plus the per-page cursor sequence.
func TestFetchPRsDataOp_BB_PerPageCheckpoint(t *testing.T) {
	t.Parallel()

	const (
		repoUUID = "{abcd-1234}"
		// testParams seeds Settings.Repositories with this single repo, so
		// fetchAllRepos returns it directly (no list-repos gateway call) and
		// the composite cursor is keyed by this full name.
		dataRepoFullName = "myworkspace/myrepo"
	)

	// Sorted newest→oldest as the Bitbucket `?sort=-updated_on` list emits;
	// page 1 carries the high-water mark, later pages walk older PRs.
	mkNode := func(id int, updated time.Time) bbPRDescription {
		dest := &struct {
			Repository struct {
				UUID string `json:"uuid"`
			} `json:"repository"`
		}{}
		dest.Repository.UUID = repoUUID
		return bbPRDescription{
			ID:          id,
			UpdatedOn:   updated,
			Description: fmt.Sprintf("body for PR %d", id),
			Destination: dest,
		}
	}

	page1Updated := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	page2Updated := time.Date(2026, 5, 15, 9, 0, 0, 0, time.UTC)
	page3Updated := time.Date(2026, 5, 7, 18, 0, 0, 0, time.UTC)

	testCases := []struct {
		name string
		// pageHandler maps a requested page number to its (nodes, hasNext)
		// response, modeling Bitbucket's page-numbered REST pagination.
		response func(page int32) *connectorpb.BitbucketListPullRequestsResponse
		// wantCheckpointCursors lists the PageCursor recorded by each onPage
		// call, in order. Length == expected checkpoint count; the trailing ""
		// is the final-page "finished cleanly" signal.
		wantCheckpointCursors []string
	}{
		{
			name: "three pages: checkpoint fires per page incl. last, cursors 2,3,\"\"",
			response: func(page int32) *connectorpb.BitbucketListPullRequestsResponse {
				switch page {
				case 1:
					return bbPRListResponseRaw([]bbPRDescription{mkNode(101, page1Updated)}, true)
				case 2:
					return bbPRListResponseRaw([]bbPRDescription{mkNode(102, page2Updated)}, true)
				case 3:
					return bbPRListResponseRaw([]bbPRDescription{mkNode(103, page3Updated)}, false)
				default:
					return nil
				}
			},
			wantCheckpointCursors: []string{"2", "3", ""},
		},
		{
			name: "single page (hasNext=false): exactly one checkpoint with empty cursor",
			response: func(page int32) *connectorpb.BitbucketListPullRequestsResponse {
				if page == 1 {
					return bbPRListResponseRaw([]bbPRDescription{mkNode(201, page1Updated)}, false)
				}
				return nil
			},
			wantCheckpointCursors: []string{""},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					listReq, ok := req.(*connectorpb.BitbucketListPullRequestsRequest)
					if !ok {
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
					resp := tc.response(listReq.GetPage())
					if resp == nil {
						return nil, fmt.Errorf("unexpected page requested: %d", listReq.GetPage())
					}
					return resp, nil
				},
			}

			sink := testkit.NewChannelSink()
			s3, _ := newBBRecordingS3(t)
			recorder := &checkpointRecorder{}
			params := testParams(sink, gw, nil)
			params.BlobStore = s3
			// testParams leaves Checkpoint nil; the per-page checkpoint only
			// wires up when a run-level Checkpoint exists, so set it explicitly.
			params.Checkpoint = recorder.fn

			op := &FetchPRsDataOp{}
			_, err := op.Execute(context.Background(), params)
			require.NoError(t, err)

			require.Equal(t, len(tc.wantCheckpointCursors), recorder.totalCalls(),
				"onPage must fire once per page INCLUDING the final page (breaking before onPage would drop the last checkpoint)")

			for i, wantCursor := range tc.wantCheckpointCursors {
				states := existingCursorStates(context.Background(), recorder.calls[i])
				write, hasRepo := states[dataRepoFullName]
				require.True(t, hasRepo, "checkpoint #%d must include the repo state", i+1)
				assert.Equal(t, wantCursor, write.PageCursor,
					"checkpoint #%d must record PageCursor=%q (empty on the final page = finished cleanly)", i+1, wantCursor)
			}
		})
	}
}

func TestFetchPRsDataOp_BB_SkipsWithoutDestinationUUID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return bbPRListResponseRaw([]bbPRDescription{
				// No Destination block — Description present.
				{ID: 99, UpdatedOn: now, Description: "real content"},
			}, false), nil
		},
	}

	sink := testkit.NewChannelSink()
	s3, writes := newBBRecordingS3(t)
	params := testParams(sink, gw, nil)
	params.BlobStore = s3

	op := &FetchPRsDataOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Empty(t, sink.Drain(), "no destination.repository.uuid → no emit")
	assert.Empty(t, *writes, "no destination.repository.uuid → no S3 PUT")
}

func TestFetchPRsDataOp_BB_NoS3ClientSkipsTick(t *testing.T) {
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
	assert.Nil(t, cursor)
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
	params := testParamsMultiRepo(sink, gw, nil, []string{"myworkspace/a", "myworkspace/b"}, nil)
	s3 := sdkmocks.NewMockBlobStore(gomock.NewController(t))
	params.BlobStore = s3

	_, err := (&FetchPRsDataOp{}).Execute(context.Background(), params)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.True(t, syncErr.Terminal)
	assert.Empty(t, sink.Drain())
}
