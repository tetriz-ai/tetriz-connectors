// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/vcs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// --- Mock VendorGateway ---

type mockVendorGateway struct {
	executeFunc func(ctx context.Context, req proto.Message) (proto.Message, error)
}

func (m *mockVendorGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not expected in new architecture")
}

func (m *mockVendorGateway) DoJSON(ctx context.Context, method, url string, reqBody, respBody any) error {
	return fmt.Errorf("DoJSON not expected in new architecture")
}

func (m *mockVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	return m
}

func (m *mockVendorGateway) ForGroup(group string) sdk.VendorGateway {
	return m
}

func (m *mockVendorGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, req)
	}
	return nil, fmt.Errorf("unexpected Execute call: %T", req)
}

// --- Test helpers ---

func testParams(sink sdk.DataSink, gw sdk.VendorGateway, cursor *sdk.Cursor) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: "org-test",
			Credentials: mustJSON(GitLabCredentials{
				OAuthRefreshToken: "test-refresh-token",
			}),
			Settings: mustJSON(GitLabSettings{
				GroupID:   42,
				GroupPath: "mygroup",
				GroupName: "My Group",
			}),
			Enabled: true,
		},
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gw,
	}
}

func glProjectsResponse(projects []glProject) *connectorpb.GitLabListProjectsResponse {
	projBytes := make([][]byte, len(projects))
	for i, p := range projects {
		projBytes[i], _ = json.Marshal(p)
	}
	return &connectorpb.GitLabListProjectsResponse{
		Projects: projBytes,
	}
}

func glMRsResponse(mrs []glMergeRequest) *connectorpb.GitLabListProjectMergeRequestsResponse {
	mrBytes := make([][]byte, len(mrs))
	for i, mr := range mrs {
		mrBytes[i], _ = json.Marshal(mr)
	}
	return &connectorpb.GitLabListProjectMergeRequestsResponse{
		MergeRequests: mrBytes,
	}
}

func glDiffsResponse(diffs []glMRDiffEntry) *connectorpb.GitLabListMRDiffsResponse {
	diffBytes := make([][]byte, len(diffs))
	for i, d := range diffs {
		diffBytes[i], _ = json.Marshal(d)
	}
	return &connectorpb.GitLabListMRDiffsResponse{
		Diffs: diffBytes,
	}
}

// --- FetchPRsOp tests ---

func TestFetchPRsOp_MetadataIncludesAuthorBranchAndFiles(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 100, PathWithNamespace: "mygroup/myproject", Name: "myproject"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID:           501,
						IID:          7,
						ProjectID:    100,
						CreatedAt:    now.Add(-time.Hour),
						UpdatedAt:    now,
						State:        "opened",
						SourceBranch: "feat/gl-feature",
						Author: struct {
							ID       int64  `json:"id"`
							Username string `json:"username"`
							Email    string `json:"email"`
							Bot      bool   `json:"bot"`
						}{ID: 99001, Username: "gluser", Email: "gluser@example.com"},
						Reviewers: []struct {
							ID int64 `json:"id"`
						}{{ID: 1}, {ID: 2}},
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse([]glMRDiffEntry{
					{
						OldPath: "src/main.go",
						NewPath: "src/main.go",
						Diff:    "@@ -1,3 +1,5 @@\n context\n+added line 1\n+added line 2\n context\n-removed line\n",
					},
					{
						OldPath: "",
						NewPath: "src/new_file.go",
						NewFile: true,
						Diff:    "@@ -0,0 +1,4 @@\n+package main\n+\n+func New() {}\n+\n",
					},
				}), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	meta := records[0].Metadata
	assert.Equal(t, "99001", meta["pr_author_id"])
	assert.Equal(t, "feat/gl-feature", meta["head_branch"])
	assert.NotEmpty(t, meta["files_changed"])
	assert.NotEmpty(t, meta["files_line_stats"])
	assert.Equal(t, "2", meta["reviews_requested"], "connector stamps the assigned-reviewer count")

	// Verify files_changed is a valid JSON array with expected paths.
	var filePaths []string
	require.NoError(t, json.Unmarshal([]byte(meta["files_changed"]), &filePaths))
	assert.Equal(t, []string{"src/main.go", "src/new_file.go"}, filePaths)

	// Verify files_line_stats is a valid JSON array with per-file stats.
	var lineStats []glMRFileLineStat
	require.NoError(t, json.Unmarshal([]byte(meta["files_line_stats"]), &lineStats))
	require.Len(t, lineStats, 2)
	assert.Equal(t, "src/main.go", lineStats[0].Path)
	assert.Equal(t, 2, lineStats[0].Additions)
	assert.Equal(t, 1, lineStats[0].Deletions)
	assert.Equal(t, "src/new_file.go", lineStats[1].Path)
	assert.Equal(t, 4, lineStats[1].Additions)
	assert.Equal(t, 0, lineStats[1].Deletions)
}

func TestFetchPRsOp_EnrichMRFailureUsesEmptyDefaults(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 200, PathWithNamespace: "mygroup/diffless", Name: "diffless"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID:           601,
						IID:          1,
						ProjectID:    200,
						CreatedAt:    now.Add(-time.Hour),
						UpdatedAt:    now,
						State:        "opened",
						SourceBranch: "feat/no-diffs",
						Author: struct {
							ID       int64  `json:"id"`
							Username string `json:"username"`
							Email    string `json:"email"`
							Bot      bool   `json:"bot"`
						}{ID: 88001, Username: "diffuser"},
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				// Diffs endpoint fails.
				return nil, fmt.Errorf("HTTP 500: internal server error")
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	meta := records[0].Metadata
	assert.Equal(t, "[]", meta["files_changed"], "enrichMR failure should yield empty files_changed")
	// files_line_stats MUST be omitted on enrichment failure — an "[]" here is
	// indistinguishable from a real empty diff and the DAO COALESCEs it over the
	// stored stats, so a transient diff-fetch error would erase real path coverage.
	_, present := meta["files_line_stats"]
	assert.False(t, present, "enrichMR failure must omit files_line_stats so the stored value is preserved, not clobbered with []")
}

func TestFetchPRsOp_DegradedEnrichmentOmitsFilesLineStats(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 200, PathWithNamespace: "mygroup/degraded", Name: "degraded"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID:           603,
						IID:          3,
						ProjectID:    200,
						CreatedAt:    now.Add(-time.Hour),
						UpdatedAt:    now,
						State:        "opened",
						SourceBranch: "feat/degraded",
						Author: struct {
							ID       int64  `json:"id"`
							Username string `json:"username"`
							Email    string `json:"email"`
							Bot      bool   `json:"bot"`
						}{ID: 88003, Username: "degradeduser"},
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				// One valid entry + one unparseable entry → enrichMR skips the bad
				// entry, returns a nil error, and marks the aggregate Degraded.
				valid, _ := json.Marshal(glMRDiffEntry{NewPath: "a.go", Diff: "@@ -1 +1 @@\n+x\n"})
				return &connectorpb.GitLabListMRDiffsResponse{
					Diffs: [][]byte{valid, []byte("{not json")},
				}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	// A degraded (incomplete) file list is not trustworthy enough to persist as
	// authoritative — omit the key, mirroring the Bitbucket guard, so a partial list
	// cannot COALESCE over complete stored stats.
	_, present := records[0].Metadata["files_line_stats"]
	assert.False(t, present, "a degraded enrichment must omit files_line_stats so a partial list cannot overwrite stored stats")
}

func TestFetchPRsOp_TruncatedDiffBodiesOmitFilesLineStats(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Enough MODIFIED files with an empty diff body (GitLab dropped the content —
	// collapsed / too_large) to clear the shared truncation thresholds (floor 10,
	// ratio 0.10). All 12 are modified candidates with no diff body → ratio 1.0.
	diffs := make([]glMRDiffEntry, 0, 12)
	for i := 0; i < 12; i++ {
		diffs = append(diffs, glMRDiffEntry{NewPath: fmt.Sprintf("pkg/file%d.go", i), Diff: ""})
	}

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 200, PathWithNamespace: "mygroup/truncated", Name: "truncated"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID:           604,
						IID:          4,
						ProjectID:    200,
						CreatedAt:    now.Add(-time.Hour),
						UpdatedAt:    now,
						State:        "opened",
						SourceBranch: "feat/truncated",
						Author: struct {
							ID       int64  `json:"id"`
							Username string `json:"username"`
							Email    string `json:"email"`
							Bot      bool   `json:"bot"`
						}{ID: 88004, Username: "truncateduser"},
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(diffs), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	// Materially truncated diff bodies make the per-file stats incomplete even though
	// every page fetched cleanly — the key must be omitted, matching the collapsed-diff
	// guard fetch_mr_diffs.go applies to its line counts.
	_, present := records[0].Metadata["files_line_stats"]
	assert.False(t, present, "materially truncated diff bodies must omit files_line_stats so the partial list cannot clobber stored stats")
}

func TestFetchPRsOp_EmptyDiffStampsFilesLineStats(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 200, PathWithNamespace: "mygroup/emptydiff", Name: "emptydiff"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID:           602,
						IID:          2,
						ProjectID:    200,
						CreatedAt:    now.Add(-time.Hour),
						UpdatedAt:    now,
						State:        "opened",
						SourceBranch: "feat/empty-diff",
						Author: struct {
							ID       int64  `json:"id"`
							Username string `json:"username"`
							Email    string `json:"email"`
							Bot      bool   `json:"bot"`
						}{ID: 88002, Username: "emptyuser"},
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				// Diffs endpoint succeeds with no files — a genuinely empty diff.
				return glDiffsResponse([]glMRDiffEntry{}), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	meta := records[0].Metadata
	// A nil-error empty diff IS an authoritative empty answer — the key is stamped as
	// "[]" (measured, not unmeasurable), unlike the omit-on-failure case above.
	v, present := meta["files_line_stats"]
	require.True(t, present, "a successful empty diff must stamp files_line_stats so the PR is measured, not unmeasurable")
	assert.Equal(t, "[]", v, "an empty successful diff stamps an empty array, not a populated one")
}

func TestFormatAuthorID(t *testing.T) {
	tests := []struct {
		name  string
		input int64
		want  string
	}{
		{name: "nonzero", input: 99001, want: "99001"},
		{name: "zero returns empty", input: 0, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, formatAuthorID(tt.input))
		})
	}
}

func TestDetectIsBot(t *testing.T) {
	tests := []struct {
		name string
		in   bool
		want string
	}{
		{name: "human reviewer", in: false, want: "false"},
		{name: "bot reviewer", in: true, want: "true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, detectIsBot(tt.in))
		})
	}
}

// glNotesResponse builds a fake GitLabListNotesResponse from a slice of glNote.
func glNotesResponse(notes []glNote) *connectorpb.GitLabListNotesResponse {
	noteBytes := make([][]byte, len(notes))
	for i, n := range notes {
		noteBytes[i], _ = json.Marshal(n)
	}
	return &connectorpb.GitLabListNotesResponse{Notes: noteBytes}
}

// glApprovalsResponseBytes builds a fake GitLabListMRApprovalsResponse from a payload struct.
func glApprovalsResponseBytes(body glApprovalsResponse) *connectorpb.GitLabListMRApprovalsResponse {
	bodyJSON, _ := json.Marshal(body)
	return &connectorpb.GitLabListMRApprovalsResponse{Approvals: bodyJSON}
}

// reviewFixture is the common shape for FetchReviewsOp / FetchPRCommentsOp
// table-driven tests — defines the project, the MR, the note(s), and the
// approver(s) that the mock VendorGateway should return.
type reviewFixture struct {
	project   glProject
	mr        glMergeRequest
	notes     []glNote
	approvals []glApproval
}

func (f reviewFixture) executeFunc() func(context.Context, proto.Message) (proto.Message, error) {
	return func(ctx context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.GitLabListProjectsRequest:
			return glProjectsResponse([]glProject{f.project}), nil
		case *connectorpb.GitLabListProjectMergeRequestsRequest:
			return glMRsResponse([]glMergeRequest{f.mr}), nil
		case *connectorpb.GitLabListNotesRequest:
			return glNotesResponse(f.notes), nil
		case *connectorpb.GitLabListMRApprovalsRequest:
			return glApprovalsResponseBytes(glApprovalsResponse{
				Approved:   len(f.approvals) > 0,
				ApprovedBy: f.approvals,
			}), nil
		case *connectorpb.GitLabListMRDiffsRequest:
			return glDiffsResponse(nil), nil
		default:
			return nil, fmt.Errorf("unexpected request type: %T", req)
		}
	}
}

// recordsByOp partitions sink records by operation id.
func recordsByOp(records []*sdk.RawRecord, opID string) []*sdk.RawRecord {
	var out []*sdk.RawRecord
	for _, r := range records {
		if r.OperationID == opID {
			out = append(out, r)
		}
	}
	return out
}

func mrAuthor(id int64, username, email string, bot bool) struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	Bot      bool   `json:"bot"`
} {
	return struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Email    string `json:"email"`
		Bot      bool   `json:"bot"`
	}{ID: id, Username: username, Email: email, Bot: bot}
}

func noteAuthor(id int64, username string, bot bool) struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Bot      bool   `json:"bot"`
} {
	return struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	}{ID: id, Username: username, Bot: bot}
}

func approvalUser(id int64, username string, bot bool) struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Bot      bool   `json:"bot"`
} {
	return struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	}{ID: id, Username: username, Bot: bot}
}

// TestFetchReviewsOp_EmitsReviewerLoadMetadata covers the four reviewer-load
// metadata fields (`pr_author_login`, `pr_author_id`, `platform_repo_id`,
// `reviewer_is_bot`) on every emitted fetch_reviews record — both the note
// (state=COMMENTED) path and the approval (review_type=approval) path —
// across human and bot reviewers.
func TestFetchReviewsOp_EmitsReviewerLoadMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	baseProject := glProject{ID: 100, PathWithNamespace: "mygroup/myproject", Name: "myproject"}
	baseMR := glMergeRequest{
		ID:        501,
		IID:       7,
		ProjectID: 100,
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now,
		State:     "opened",
		Author:    mrAuthor(99001, "gluser", "gluser@example.com", false),
	}

	tests := []struct {
		name         string
		fixture      reviewFixture
		assertNote   func(t *testing.T, meta map[string]string)
		assertApprov func(t *testing.T, meta map[string]string)
	}{
		{
			name: "human reviewer — note + approval emitted with reviewer_is_bot=false",
			fixture: reviewFixture{
				project: baseProject,
				mr:      baseMR,
				notes: []glNote{{
					ID: 1, ProjectID: 100, CreatedAt: now, UpdatedAt: now, System: false,
					Author: noteAuthor(7001, "alice", false),
				}},
				approvals: []glApproval{{
					User: approvalUser(7002, "bob", false), CreatedAt: now,
				}},
			},
			assertNote: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "gluser", meta["pr_author_login"])
				assert.Equal(t, "99001", meta["pr_author_id"])
				assert.Equal(t, "100", meta["platform_repo_id"])
				assert.Equal(t, "false", meta["reviewer_is_bot"])
				assert.Equal(t, "mygroup/myproject", meta["repo"])
				assert.Equal(t, "mr-100!7", meta["pr_key"])
				// pr_created_at = parent MR opened_at (date anchor).
				assert.Equal(t, now.Add(-time.Hour).Format(time.RFC3339), meta["pr_created_at"])
			},
			assertApprov: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "gluser", meta["pr_author_login"])
				assert.Equal(t, "99001", meta["pr_author_id"])
				assert.Equal(t, "100", meta["platform_repo_id"])
				assert.Equal(t, "false", meta["reviewer_is_bot"])
				assert.Equal(t, "approval", meta["review_type"])
				assert.Equal(t, now.Add(-time.Hour).Format(time.RFC3339), meta["pr_created_at"])
			},
		},
		{
			name: "bot reviewer — reviewer_is_bot=true on both note and approval",
			fixture: reviewFixture{
				project: baseProject,
				mr:      baseMR,
				notes: []glNote{{
					ID: 2, ProjectID: 100, CreatedAt: now, UpdatedAt: now, System: false,
					Author: noteAuthor(8001, "review-bot", true),
				}},
				approvals: []glApproval{{
					User: approvalUser(8002, "renovate-bot", true), CreatedAt: now,
				}},
			},
			assertNote: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "true", meta["reviewer_is_bot"])
				assert.Equal(t, "gluser", meta["pr_author_login"])
				assert.Equal(t, "100", meta["platform_repo_id"])
			},
			assertApprov: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "true", meta["reviewer_is_bot"])
				assert.Equal(t, "gluser", meta["pr_author_login"])
				assert.Equal(t, "100", meta["platform_repo_id"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &mockVendorGateway{executeFunc: tt.fixture.executeFunc()}
			sink := testkit.NewChannelSink()
			params := testParams(sink, gw, nil)

			op := &FetchReviewsOp{}
			_, err := op.Execute(context.Background(), params)
			require.NoError(t, err)

			reviewRecs := recordsByOp(sink.Records(), "fetch_reviews")
			require.Len(t, reviewRecs, 2, "expected one note record and one approval record")

			// First fetch_reviews record is the note (emitted before approvals in the loop).
			tt.assertNote(t, reviewRecs[0].Metadata)
			// Second is the approval — has review_type metadata.
			tt.assertApprov(t, reviewRecs[1].Metadata)
		})
	}
}

// TestFetchPRCommentsOp_EmitsReviewerLoadMetadata covers the four reviewer-load
// metadata fields on every emitted fetch_pr_comments record across human and
// bot commenters.
func TestFetchPRCommentsOp_EmitsReviewerLoadMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	baseProject := glProject{ID: 100, PathWithNamespace: "mygroup/myproject", Name: "myproject"}
	baseMR := glMergeRequest{
		ID:        501,
		IID:       7,
		ProjectID: 100,
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now,
		State:     "opened",
		Author:    mrAuthor(99001, "gluser", "gluser@example.com", false),
	}

	tests := []struct {
		name       string
		notes      []glNote
		assertMeta func(t *testing.T, meta map[string]string)
	}{
		{
			name: "human commenter — reviewer_is_bot=false",
			notes: []glNote{{
				ID: 11, ProjectID: 100, CreatedAt: now, UpdatedAt: now, System: false,
				Author: noteAuthor(7001, "alice", false),
			}},
			assertMeta: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "gluser", meta["pr_author_login"])
				assert.Equal(t, "99001", meta["pr_author_id"])
				assert.Equal(t, "100", meta["platform_repo_id"])
				assert.Equal(t, "false", meta["reviewer_is_bot"])
				// Existing fields stay intact.
				assert.Equal(t, "mygroup/myproject", meta["repo"])
				assert.Equal(t, "7", meta["mr_iid"])
				assert.Equal(t, "gluser", meta["mr_author_username"])
				assert.Equal(t, "99001", meta["mr_author_id"])
				// pr_created_at = parent MR opened_at (date anchor).
				assert.Equal(t, now.Add(-time.Hour).Format(time.RFC3339), meta["pr_created_at"])
			},
		},
		{
			name: "bot commenter — reviewer_is_bot=true",
			notes: []glNote{{
				ID: 12, ProjectID: 100, CreatedAt: now, UpdatedAt: now, System: false,
				Author: noteAuthor(8001, "review-bot", true),
			}},
			assertMeta: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "true", meta["reviewer_is_bot"])
				assert.Equal(t, "100", meta["platform_repo_id"])
				assert.Equal(t, "gluser", meta["pr_author_login"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := reviewFixture{
				project: baseProject,
				mr:      baseMR,
				notes:   tt.notes,
			}
			gw := &mockVendorGateway{executeFunc: fixture.executeFunc()}
			sink := testkit.NewChannelSink()
			params := testParams(sink, gw, nil)

			op := &FetchPRCommentsOp{}
			_, err := op.Execute(context.Background(), params)
			require.NoError(t, err)

			recs := recordsByOp(sink.Records(), "fetch_pr_comments")
			require.Len(t, recs, 1)
			tt.assertMeta(t, recs[0].Metadata)
		})
	}
}

// glMRCommitsResponse builds a GitLabListMRCommitsResponse from a slice of
// raw commit structs. Used by first_commit_at chained-call tests.
func glMRCommitsResponse(commits []glMRCommit) *connectorpb.GitLabListMRCommitsResponse {
	commitBytes := make([][]byte, len(commits))
	for i, c := range commits {
		commitBytes[i], _ = json.Marshal(c)
	}
	return &connectorpb.GitLabListMRCommitsResponse{
		Commits: commitBytes,
	}
}

// glMRCommit mirrors the fields extracted by fetchEarliestMRCommit.
type glMRCommit struct {
	ID           string    `json:"id"`
	AuthoredDate time.Time `json:"authored_date"`
}

// TestFetchPRsOp_FirstCommitAtStampedInMetadata verifies the first_commit_at
// happy path: the chained commits call returns commits in arbitrary order
// (the GitLab REST endpoint does not document order_by/sort) and the MR
// record's metadata carries first_commit_at = MIN(authored_date) — proves the
// page-MIN logic doesn't blindly trust commits[0].
func TestFetchPRsOp_FirstCommitAtStampedInMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	firstCommit := now.Add(-3 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 300, PathWithNamespace: "mygroup/firstcommitrepo", Name: "firstcommitrepo"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID:           701,
						IID:          11,
						ProjectID:    300,
						CreatedAt:    now.Add(-time.Hour),
						UpdatedAt:    now,
						State:        "opened",
						SourceBranch: "feat/first-commit",
						Author:       mrAuthor(55001, "devuser", "", false),
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				// firstCommit is the EARLIEST authored_date but listed second
				// in the response — proves fetchEarliestMRCommit takes MIN
				// across the page rather than commits[0].
				return glMRCommitsResponse([]glMRCommit{
					{ID: "later", AuthoredDate: firstCommit.Add(2 * time.Hour)},
					{ID: "earliest", AuthoredDate: firstCommit},
					{ID: "middle", AuthoredDate: firstCommit.Add(time.Hour)},
				}), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	// All records for each MR share the same meta, so first_commit_at is present on all.
	assert.Equal(t, firstCommit.UTC().Format(time.RFC3339), records[0].Metadata["first_commit_at"],
		"opened record should carry the EARLIEST authored_date across the commits page (not commits[0])")

	// The same page also carries the commit-to-merge-request link. GitLab's commit payload
	// names no merge request, so this record is the only place the link appears at ingest,
	// and it costs no request beyond the one already made for the date above.
	assert.Equal(t, "later,earliest,middle", records[0].Metadata[vcs.MetaKeyPRCommitSHAs],
		"the commits page's ids are kept as the link, in page order")
}

// TestFetchPRsOp_CommitsFetchFailureDoesNotDropMR verifies the first_commit_at
// failure tolerance: a 500 from the chained commits call logs a warning and the
// MR is still emitted without first_commit_at — the record is not lost.
func TestFetchPRsOp_CommitsFetchFailureDoesNotDropMR(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 400, PathWithNamespace: "mygroup/failrepo", Name: "failrepo"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID:           801,
						IID:          22,
						ProjectID:    400,
						CreatedAt:    now.Add(-time.Hour),
						UpdatedAt:    now,
						State:        "opened",
						SourceBranch: "feat/fail-commit",
						Author:       mrAuthor(66001, "failuser", "", false),
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				// Simulates a server-side error on the chained commits call.
				return nil, fmt.Errorf("HTTP 500: internal server error")
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err, "chained call failure must not abort the sync")

	records := sink.Records()
	require.NotEmpty(t, records, "MR must still be emitted despite commit fetch failure")
	assert.Empty(t, records[0].Metadata["first_commit_at"],
		"first_commit_at must be absent when commit fetch fails")
}

// TestFetchPRsOp_ClosesIssuesStampedInMetadata verifies the native PR-to-issue linkage happy path: the
// chained closes_issues call's raw array body is stamped verbatim onto the MR
// record metadata under "closes_issues" for a consumer's extractor.
func TestFetchPRsOp_ClosesIssuesStampedInMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const closesBody = `[{"iid":17,"references":{"full":"mygroup/closesrepo#17"}}]`

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 500, PathWithNamespace: "mygroup/closesrepo", Name: "closesrepo"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID: 901, IID: 33, ProjectID: 500, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
						State: "opened", SourceBranch: "feat/closes", Author: mrAuthor(77001, "closeuser", "", false),
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				return glMRCommitsResponse(nil), nil
			case *connectorpb.GitLabListMRClosesIssuesRequest:
				return &connectorpb.GitLabListMRClosesIssuesResponse{Issues: []byte(closesBody)}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	op := &FetchPRsOp{}
	params := testParams(sink, gw, nil)
	params.Config.Settings = mustJSON(GitLabSettings{
		GroupID: 42, GroupPath: "mygroup", GroupName: "My Group", FetchNativeIssueLinks: true,
	})
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)
	assert.JSONEq(t, closesBody, records[0].Metadata["closes_issues"],
		"the closes_issues raw array must be stamped onto the record metadata")
}

// TestFetchPRsOp_ClosesIssuesFetchFailureDoesNotDropMR verifies the native PR-to-issue linkage failure
// path: a 500 from the chained closes_issues call logs a warning, the MR is
// still emitted, and the metadata key is omitted so a consumer preserves any
// prior linkage rather than clobbering it with an empty set.
func TestFetchPRsOp_ClosesIssuesFetchFailureDoesNotDropMR(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 600, PathWithNamespace: "mygroup/closesfail", Name: "closesfail"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID: 902, IID: 44, ProjectID: 600, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
						State: "opened", SourceBranch: "feat/closes-fail", Author: mrAuthor(88001, "failuser", "", false),
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				return glMRCommitsResponse(nil), nil
			case *connectorpb.GitLabListMRClosesIssuesRequest:
				return nil, fmt.Errorf("HTTP 500: internal server error")
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	op := &FetchPRsOp{}
	params := testParams(sink, gw, nil)
	params.Config.Settings = mustJSON(GitLabSettings{
		GroupID: 42, GroupPath: "mygroup", GroupName: "My Group", FetchNativeIssueLinks: true,
	})
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err, "closes_issues fetch failure must not abort the sync")

	records := sink.Records()
	require.NotEmpty(t, records, "MR must still be emitted despite closes_issues fetch failure")
	_, present := records[0].Metadata["closes_issues"]
	assert.False(t, present, "closes_issues key must be absent when the fetch fails")
}

// TestFetchPRsOp_ClosesIssuesEmptyArrayStampedWhenGateOn verifies the stamp-vs-omit
// boundary at the connector layer: with the gate on, an endpoint response of "[]"
// (an authoritative "checked, no closing issues") is stamped verbatim as "[]",
// NOT omitted. This is distinct from the failure/empty-body path (key omitted so
// a consumer preserves prior linkage) — a consumer's extracted flag relies
// on the "[]" vs absent distinction.
func TestFetchPRsOp_ClosesIssuesEmptyArrayStampedWhenGateOn(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 800, PathWithNamespace: "mygroup/emptycloses", Name: "emptycloses"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID: 904, IID: 66, ProjectID: 800, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
						State: "opened", SourceBranch: "feat/empty-closes", Author: mrAuthor(11001, "emptyuser", "", false),
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				return glMRCommitsResponse(nil), nil
			case *connectorpb.GitLabListMRClosesIssuesRequest:
				return &connectorpb.GitLabListMRClosesIssuesResponse{Issues: []byte(`[]`)}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	op := &FetchPRsOp{}
	params := testParams(sink, gw, nil)
	params.Config.Settings = mustJSON(GitLabSettings{
		GroupID: 42, GroupPath: "mygroup", GroupName: "My Group", FetchNativeIssueLinks: true,
	})
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)
	got, present := records[0].Metadata["closes_issues"]
	require.True(t, present, "an authoritative empty-array response must be stamped, not omitted")
	assert.JSONEq(t, `[]`, got, "empty array must be stamped verbatim as []")
}

// TestFetchPRsOp_ClosesIssuesSkippedWhenGateOff verifies the native PR-to-issue linkage rate-gate: with
// FetchNativeIssueLinks unset (the default), fetch_prs must NOT issue the extra
// per-MR closes_issues REST call, and the metadata key stays absent so native
// linkage is left unset (a consumer preserves prior linkage). This is the
// boundary that keeps GitLab's tightest-of-three rate budget at the two-call
// baseline for orgs not yet opted in.
func TestFetchPRsOp_ClosesIssuesSkippedWhenGateOff(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var closesCalled bool

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 700, PathWithNamespace: "mygroup/gateoff", Name: "gateoff"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID: 903, IID: 55, ProjectID: 700, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
						State: "opened", SourceBranch: "feat/gate-off", Author: mrAuthor(99001, "gateuser", "", false),
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				return glMRCommitsResponse(nil), nil
			case *connectorpb.GitLabListMRClosesIssuesRequest:
				closesCalled = true
				return &connectorpb.GitLabListMRClosesIssuesResponse{Issues: []byte(`[]`)}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	op := &FetchPRsOp{}
	// testParams builds settings with FetchNativeIssueLinks unset (the default).
	_, err := op.Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)

	assert.False(t, closesCalled,
		"closes_issues endpoint must NOT be called when FetchNativeIssueLinks is off")
	records := sink.Records()
	require.NotEmpty(t, records)
	_, present := records[0].Metadata["closes_issues"]
	assert.False(t, present, "closes_issues metadata key must be absent when the gate is off")
}

// TestFetchCommitsOp_AlwaysDefaultBranch verifies fetch_commits always passes
// the project's default branch as RefName, regardless of the tracked-branch /
// all-branches setting (which governs only the PR read-filter). The mock
// captures the RefName from the GitLabListCommitsRequest.
func TestFetchCommitsOp_AlwaysDefaultBranch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const projectPath = "mygroup/myrepo"
	project := glProject{ID: 7, Path: "myrepo", PathWithNamespace: projectPath, DefaultBranch: "main"}

	tests := []struct {
		name     string
		settings GitLabSettings
	}{
		{
			name:     "tracked branch develop ignored",
			settings: GitLabSettings{GroupID: 42, TrackedBranch: "develop"},
		},
		{
			name:     "track all branches ignored",
			settings: GitLabSettings{GroupID: 42, TrackAllBranches: true},
		},
		{
			name:     "per-repo override develop ignored",
			settings: GitLabSettings{GroupID: 42, RepoBranchOverrides: map[string]string{projectPath: "develop"}},
		},
		{
			name:     "per-repo all-branches ignored",
			settings: GitLabSettings{GroupID: 42, RepoAllBranches: []string{projectPath}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedRefName string
			commit := glCommit{ID: "c1", CommittedDate: now}
			commitJSON, _ := json.Marshal(commit)

			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch r := req.(type) {
					case *connectorpb.GitLabListProjectsRequest:
						return glProjectsResponse([]glProject{project}), nil
					case *connectorpb.GitLabListCommitsRequest:
						capturedRefName = r.GetRefName()
						return &connectorpb.GitLabListCommitsResponse{Commits: [][]byte{commitJSON}}, nil
					case *connectorpb.GitLabGetCommitDetailRequest:
						return &connectorpb.GitLabGetCommitDetailResponse{}, nil
					default:
						return nil, fmt.Errorf("unexpected request %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			params := &sdk.ExecutionParams{
				Config: &sdk.ConnectorConfig{
					ConnectorID:    "gitlab",
					OrganizationID: "org-test",
					Credentials:    mustJSON(GitLabCredentials{OAuthRefreshToken: "t"}),
					Settings:       mustJSON(tt.settings),
					Enabled:        true,
				},
				Sink:          sink,
				VendorGateway: gw,
			}

			_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
			require.NoError(t, err)
			assert.Equal(t, "main", capturedRefName,
				"commit fetch must use the default branch, not the tracked/override branch")
			require.Len(t, sink.Records(), 1)
		})
	}
}

// TestGitLab_MRForkDetection verifies that is_cross_repository is set to "true"
// in metadata when source_project_id differs from target_project_id on a MR.
func TestGitLab_MRForkDetection(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 1, PathWithNamespace: "mygroup/myproject", Name: "myproject"},
				}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				return glMRsResponse([]glMergeRequest{
					{
						ID:              10,
						IID:             7,
						ProjectID:       1,
						SourceProjectID: 2, // fork — differs from ProjectID
						TargetProjectID: 1,
						CreatedAt:       now.Add(-time.Hour),
						UpdatedAt:       now,
						State:           "opened",
						SourceBranch:    "feat/fork-feature",
						Author: struct {
							ID       int64  `json:"id"`
							Username string `json:"username"`
							Email    string `json:"email"`
							Bot      bool   `json:"bot"`
						}{ID: 99, Username: "forkuser"},
					},
				}), nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				return &connectorpb.GitLabListMRCommitsResponse{}, nil
			default:
				return nil, fmt.Errorf("unexpected request type in fork detection test: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records, "at least one MR record must be emitted")
	assert.Equal(t, "true", records[0].Metadata["is_cross_repository"],
		"is_cross_repository must be true when source_project_id != target_project_id")
}

// TestGitLab_CommitsRequestUsesFirstParent verifies that the FetchCommitsOp sets
// FirstParent=true on the GitLabListCommitsRequest sent to the vendor gateway.
func TestGitLab_CommitsRequestUsesFirstParent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	commitJSON, _ := json.Marshal(glCommit{
		ID:            "abc123",
		CommittedDate: now,
		AuthorEmail:   "dev@example.com",
	})

	var capturedCommitReq *connectorpb.GitLabListCommitsRequest
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{
					{ID: 1, PathWithNamespace: "mygroup/myproject", Name: "myproject", DefaultBranch: "main"},
				}), nil
			case *connectorpb.GitLabListCommitsRequest:
				capturedCommitReq = r
				return &connectorpb.GitLabListCommitsResponse{Commits: [][]byte{commitJSON}}, nil
			case *connectorpb.GitLabGetCommitDetailRequest:
				return &connectorpb.GitLabGetCommitDetailResponse{}, nil
			default:
				return nil, fmt.Errorf("unexpected request type in first_parent test: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchCommitsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	require.NotNil(t, capturedCommitReq, "a GitLabListCommitsRequest must have been sent")
	assert.True(t, capturedCommitReq.GetFirstParent(),
		"fetch_commits must set first_parent=true to restrict to trunk history")
}
