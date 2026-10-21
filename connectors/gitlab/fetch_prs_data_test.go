// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
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

// glMRDescResponse marshals MR list entries carrying an inline description so
// fetch_prs_data's glPRDescription unmarshal sees iid / updated_at / description.
func glMRDescResponse(mrs []glPRDescription, nextPageURL string) *connectorpb.GitLabListProjectMergeRequestsResponse {
	mrBytes := make([][]byte, len(mrs))
	for i, mr := range mrs {
		mrBytes[i], _ = json.Marshal(mr)
	}
	return &connectorpb.GitLabListProjectMergeRequestsResponse{
		MergeRequests: mrBytes,
		NextPageUrl:   nextPageURL,
	}
}

func TestFetchPRsDataOp_Execute(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	const (
		projectID      = 100
		platformRepoID = "100"
		repoFullName   = "mygroup/myproject"
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.GitLabListProjectsRequest:
						return glProjectsResponse([]glProject{
							{ID: projectID, PathWithNamespace: repoFullName, Name: "myproject"},
						}), nil
					case *connectorpb.GitLabListProjectMergeRequestsRequest:
						return glMRDescResponse([]glPRDescription{
							{IID: 42, UpdatedAt: now, Description: tt.body},
						}, ""), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
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

			records := sink.Records()
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
			assert.Equal(t, platformRepoID, rec.Metadata["platform_repo_id"])

			require.Len(t, *writes, 1, "expected one S3 write for real ref")
			assert.Equal(t, tt.wantS3Key, (*writes)[0].Key, "S3 key must equal payload.s3_key")
			assert.Equal(t, tt.body, string((*writes)[0].Content), "S3 bytes must equal the raw body bytes the sha was computed over")
		})
	}
}

// TestFetchPRsDataOp_MalformedEntryRetriedNotSkippedOrAborted guards against a single
// malformed MR entry either aborting the whole project fetch (blocking every
// other MR forever) or being silently skipped-and-forgotten (permanent data
// loss, since the high-water mark would otherwise advance past it via a later
// entry's success). Neither is acceptable, so this pins the high-water mark at
// the failure point instead: the valid MR after it is still emitted now (best
// effort), but the persisted cursor does not advance past the malformed entry,
// so the next tick's `since` window includes it again — retried indefinitely,
// without blocking anything else in the project.
func TestFetchPRsDataOp_MalformedEntryRetriedNotSkippedOrAborted(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	const (
		projectID      = 100
		platformRepoID = "100"
		repoFullName   = "mygroup/myproject"
	)

	validMR, err := json.Marshal(glPRDescription{IID: 42, UpdatedAt: now, Description: "a real description"})
	require.NoError(t, err)

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: projectID, PathWithNamespace: repoFullName, Name: "myproject"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return &connectorpb.GitLabListProjectMergeRequestsResponse{
					MergeRequests: [][]byte{[]byte("{not valid json"), validMR},
					NextPageUrl:   "",
				}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	s3, _ := newRecordingS3(t)
	params := testParams(sink, gw, nil)
	params.BlobStore = s3

	op := &FetchPRsDataOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err, "a malformed entry must not abort the project fetch")
	require.NotNil(t, cursor)

	records := sink.Records()
	require.Len(t, records, 1, "the valid MR after the malformed entry must still be emitted this run")
	assert.Equal(t, fmt.Sprintf("%s#PR-42", repoFullName), records[0].ExternalID)

	state := getRepoCursorState(context.Background(), cursor, fmt.Sprint(projectID))
	assert.True(t, state.Timestamp.Before(now),
		"the persisted high-water mark must NOT advance to the valid MR's timestamp (%v) — "+
			"it must stay before the malformed entry so the next tick's `since` filter retries it; got %v", now, state.Timestamp)
}

// TestFetchPRsDataOp_KillAfterPage1_ResumesAtPage1FromHighWaterMark shows that a
// resume after a kill starts again from page 1, because the vendor's ordering can
// shift between runs; repoCursorState's doc comment in operations.go has the
// mechanism.
//
// Run 1: page 1 has a valid MR (no malformed entry, so stuckAt/stuckPage do not
// apply and this exercises the resume path on its own), page 2 then fails
// (simulated kill). Resume simulates the
// vendor-side re-sort: the MR that was on page 1 now returns on page 2, and a
// new MR now sits on page 1. Non-vacuous by construction: a
// PageCursor="2" resume would request page 2 first and never re-request
// page 1, permanently missing the shifted MR.
func TestFetchPRsDataOp_KillAfterPage1_ResumesAtPage1FromHighWaterMark(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	const (
		projectID    = 100
		repoFullName = "mygroup/myproject"
	)

	page1MR, err := json.Marshal(glPRDescription{IID: 1, UpdatedAt: now, Description: "original page-1 MR"})
	require.NoError(t, err)

	// --- Run 1: valid MR on page 1, then a simulated kill on page 2. ---
	recorder := &checkpointRecorder{}
	run1GW := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: projectID, PathWithNamespace: repoFullName, Name: "myproject"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				switch r.GetPage() {
				case 1:
					return &connectorpb.GitLabListProjectMergeRequestsResponse{
						MergeRequests: [][]byte{page1MR},
						NextPageUrl:   "https://gitlab.com/next?page=2",
					}, nil
				default:
					return nil, fmt.Errorf("simulated kill fetching page %d", r.GetPage())
				}
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	s3, _ := newRecordingS3(t)
	params := testParams(sink, run1GW, nil)
	params.BlobStore = s3
	params.Checkpoint = recorder.fn

	op := &FetchPRsDataOp{}
	_, err = op.Execute(context.Background(), params)
	require.Error(t, err, "the only project failed, so the run fails; its checkpoint is kept")

	require.Positive(t, recorder.totalCalls(), "the per-page checkpoint must have fired after page 1, before the page-2 kill")
	killCursor := recorder.lastCursor()
	require.NotNil(t, killCursor)

	// --- Resume, simulating a vendor-side re-sort: the MR that was on page 1
	// now returns on page 2; a new MR now sits on page 1. ---
	shiftedNewMR, err := json.Marshal(glPRDescription{IID: 2, UpdatedAt: now.Add(time.Second), Description: "shifted-in MR"})
	require.NoError(t, err)

	var page1Requested bool
	resumeGW := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: projectID, PathWithNamespace: repoFullName, Name: "myproject"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				switch r.GetPage() {
				case 1:
					page1Requested = true
					return &connectorpb.GitLabListProjectMergeRequestsResponse{
						MergeRequests: [][]byte{shiftedNewMR},
						NextPageUrl:   "https://gitlab.com/next?page=2",
					}, nil
				case 2:
					return &connectorpb.GitLabListProjectMergeRequestsResponse{
						MergeRequests: [][]byte{page1MR},
						NextPageUrl:   "",
					}, nil
				default:
					return nil, fmt.Errorf("unexpected page %d in resume", r.GetPage())
				}
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	resumeSink := testkit.NewChannelSink()
	resumeParams := testParams(resumeSink, resumeGW, killCursor)
	resumeParams.BlobStore = s3

	_, err = op.Execute(context.Background(), resumeParams)
	require.NoError(t, err)

	assert.True(t, page1Requested, "resume must restart at page 1, not a saved page number — "+
		"a page-NUMBER resume would never re-request page 1 and would permanently miss the shifted MR")

	gotPRNumbers := map[float64]bool{}
	for _, rec := range resumeSink.Records() {
		var payload map[string]any
		require.NoError(t, json.Unmarshal(rec.Payload, &payload))
		gotPRNumbers[payload["pr_number"].(float64)] = true
	}
	assert.True(t, gotPRNumbers[1] && gotPRNumbers[2],
		"resume must capture both the shifted-in MR and the original page-1 MR (now on page 2); got %v", gotPRNumbers)
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
	assert.Empty(t, sink.Records())
}
