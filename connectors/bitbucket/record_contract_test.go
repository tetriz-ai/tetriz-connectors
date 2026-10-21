// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// Two reviewers without account ids requesting changes on one pull request
// are two records: like approvals, the ExternalID falls back to the user
// UUID. A participant with neither is skipped rather than keyed on "".
func TestEmitChangesRequestedReviews_ReviewersWithoutAccountIDStayDistinct(t *testing.T) {
	created := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	pr := bbPullRequest{
		ID:        42,
		CreatedOn: created,
		Participants: []bbPRParticipant{
			{User: &bbUser{UUID: "{aaaa}", DisplayName: "Former user"}, Role: "REVIEWER", State: "changes_requested"},
			{User: &bbUser{UUID: "{bbbb}", DisplayName: "Former user"}, Role: "REVIEWER", State: "changes_requested"},
			{User: &bbUser{DisplayName: "No identity"}, Role: "REVIEWER", State: "changes_requested"},
		},
	}
	sink := testkit.NewChannelSink()
	require.NoError(t, emitChangesRequestedReviews(context.Background(), sink, "fetch_reviews", "org-test", "ws/repo",
		bbPRInfo{ID: 42, CreatedOn: created}, pr))

	recs := sink.Records()
	require.Len(t, recs, 2, "the participant with no account id and no uuid is skipped")
	assert.Equal(t, "ws/repo#PR-42/changes-requested-{aaaa}", recs[0].ExternalID)
	assert.Equal(t, "ws/repo#PR-42/changes-requested-{bbbb}", recs[1].ExternalID)
}

// An approval or activity comment without a date is skipped: dating it at
// the fetch time would give it a new DataDate (a new row
// in a date-keyed sink) on every run.
func TestFetchPRActivity_UndatedActivityIsSkipped(t *testing.T) {
	gw := activityGateway(
		`{"approval":{"user":{"account_id":"acc-1","uuid":"{u1}"}}}`,
		`{"approval":{"date":"not a date","user":{"account_id":"acc-2","uuid":"{u2}"}}}`,
		`{"comment":{"id":5,"user":{"account_id":"acc-3"}}}`,
		`{"approval":{"date":"2024-05-01T10:00:00+00:00","user":{"account_id":"acc-4"}}}`,
	)
	sink := testkit.NewChannelSink()
	latest, err := fetchPRActivity(context.Background(), gw, sink, "fetch_reviews", "org-test", "ws", "repo", "ws/repo",
		bbPRInfo{ID: 7}, time.Time{}, &pacing.PacingBudget{})
	require.NoError(t, err)

	recs := sink.Records()
	require.Len(t, recs, 1, "only the dated approval is emitted")
	want := time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC)
	assert.Equal(t, "ws/repo#PR-7/approval-acc-4", recs[0].ExternalID)
	assert.True(t, recs[0].DataDate.Equal(want))
	assert.True(t, latest.Equal(want), "the watermark comes from vendor dates only, never from now")
}

// An approval with neither account id nor uuid is skipped rather than keyed
// on an empty identity every such approval would share.
func TestFetchPRActivity_ApprovalWithNoIdentityIsSkipped(t *testing.T) {
	gw := activityGateway(
		`{"approval":{"date":"2024-05-01T10:00:00+00:00","user":{"display_name":"ghost"}}}`,
		`{"approval":{"date":"2024-05-01T11:00:00+00:00"}}`,
	)
	sink := testkit.NewChannelSink()
	_, err := fetchPRActivity(context.Background(), gw, sink, "fetch_reviews", "org-test", "ws", "repo", "ws/repo",
		bbPRInfo{ID: 7}, time.Time{}, &pacing.PacingBudget{})
	require.NoError(t, err)
	assert.Empty(t, sink.Records())
}

// files_changed counts distinct paths. A diffstat can list one path in more
// than one entry; the repeat adds its lines to the path's single entry in
// files_line_stats. A rename counts once, under its new path.
func TestEnrichPR_FilesChangedCountsDistinctPaths(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return bbPRDiffstatResponse([]bbDiffstatEntry{
				{LinesAdded: 3, LinesRemoved: 1, New: &bbDiffstatFile{Path: "a.go"}},
				{LinesAdded: 2, LinesRemoved: 0, New: &bbDiffstatFile{Path: "a.go"}},
				{LinesAdded: 1, LinesRemoved: 1, Old: &bbDiffstatFile{Path: "old.go"}, New: &bbDiffstatFile{Path: "new.go"}},
				{LinesAdded: 0, LinesRemoved: 4, Old: &bbDiffstatFile{Path: "gone.go"}},
			}, ""), nil
		},
	}
	got, err := enrichPR(context.Background(), gw, nil, "ws", "repo", 1, json.RawMessage(`{"id":1,"state":"OPEN"}`),
		"org-test", "fetch_prs", &pacing.PacingBudget{})
	require.NoError(t, err)
	assert.JSONEq(t, `["a.go","new.go","gone.go"]`, got.FilesStr)
	assert.Equal(t, 3, countFilePathsInJSON(got.FilesStr))
	assert.JSONEq(t, `[{"path":"a.go","additions":5,"deletions":1},{"path":"new.go","additions":1,"deletions":1},{"path":"gone.go","additions":0,"deletions":4}]`, got.FilesStatStr)
	assert.Equal(t, 6, got.Additions)
	assert.Equal(t, 6, got.Deletions)
}

// A cached file list may repeat a path. The count read back from it is of
// distinct paths.
func TestCountFilePathsInJSON_CountsDistinctPaths(t *testing.T) {
	assert.Equal(t, 2, countFilePathsInJSON(`["a.go","b.go","a.go"]`))
	assert.Equal(t, 0, countFilePathsInJSON(""))
}

func TestEnrichCommit_FilesChangedCountsDistinctPaths(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return bbCommitDiffstatResponse([]bbDiffstatEntry{
				{LinesAdded: 3, New: &bbDiffstatFile{Path: "a.go"}},
				{LinesAdded: 2, New: &bbDiffstatFile{Path: "a.go"}},
				{LinesRemoved: 4, Old: &bbDiffstatFile{Path: "gone.go"}},
			}, ""), nil
		},
	}
	got, err := enrichCommit(context.Background(), gw, nil, "ws", "repo", "sha", json.RawMessage(`{"hash":"sha"}`), nil, "fetch_commits", "org-test")
	require.NoError(t, err)
	assert.Equal(t, 2, got.FilesChanged)
	assert.Equal(t, 5, got.Additions)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(got.Payload, &payload))
	assert.EqualValues(t, 2, payload["files_changed"])
}

// The no-change record shares the ExternalID of the record that uploaded the
// tree, so it carries the same treeS3Key: a sink that overwrites on
// ExternalID still has the key after the second run.
func TestFetchRepoTreeOp_NoChangeKeepsTreeS3Key(t *testing.T) {
	const headSHA = "sha-unchanged"
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	var written string
	s3.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		DoAndReturn(func(_ context.Context, dst string, _ []byte, _ string) error {
			written = dst
			return nil
		}).Times(1)
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{
		bbBranchHandler(headSHA),
		bbSrcHandler([]bbSrcEntry{{Type: "commit_file", Path: "README.md", Size: 1}}),
	}}

	first := testkit.NewChannelSink()
	cursor, err := (&FetchRepoTreeOp{}).Execute(context.Background(), bbTreeTestParams(t, first, gw, s3, nil))
	require.NoError(t, err)
	second := testkit.NewChannelSink()
	_, err = (&FetchRepoTreeOp{}).Execute(context.Background(), bbTreeTestParams(t, second, gw, s3, cursor))
	require.NoError(t, err)

	require.Len(t, first.Records(), 1)
	require.Len(t, second.Records(), 1)
	uploaded, unchanged := first.Records()[0], second.Records()[0]
	assert.Equal(t, uploaded.ExternalID, unchanged.ExternalID)

	rec := decodeBBTreeRecord(t, unchanged.Payload)
	assert.True(t, rec.NoChange)
	assert.Equal(t, written, rec.TreeS3Key, "the no-change record names the blob the earlier run wrote")
	assert.Equal(t, decodeBBTreeRecord(t, uploaded.Payload).TreeS3Key, rec.TreeS3Key)
}
