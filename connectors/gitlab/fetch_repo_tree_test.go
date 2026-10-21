// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	connectorv1 "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/vcs/treewalker"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// glCompositeGateway handles both Execute (used by fetchAllProjects) and DoJSON
// (used by getGitLabBranchSHA + gitlabTreeAPI.GetTree) within a single gateway.
// It implements the full sdk.VendorGateway interface.
type glCompositeGateway struct {
	executeFunc    func(ctx context.Context, req proto.Message) (proto.Message, error)
	doJSONHandlers []glDoJSONHandler
}

type glDoJSONHandler struct {
	match string
	fn    func(urlStr string, respBody any) error
}

func (g *glCompositeGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if g.executeFunc != nil {
		return g.executeFunc(ctx, req)
	}
	return nil, fmt.Errorf("Execute not configured in glCompositeGateway")
}

func (g *glCompositeGateway) DoJSON(_ context.Context, _, urlStr string, _, respBody any) error {
	for _, h := range g.doJSONHandlers {
		if strings.Contains(urlStr, h.match) {
			return h.fn(urlStr, respBody)
		}
	}
	return fmt.Errorf("unregistered DoJSON call: %s", urlStr)
}

func (g *glCompositeGateway) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not used in fetch_repo_tree tests")
}

func (g *glCompositeGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}

func (g *glCompositeGateway) ForGroup(_ string) sdk.VendorGateway { return g }

// newGLTreeTestGW builds a glCompositeGateway backed by the given project list
// for Execute calls and the provided handlers for DoJSON calls.
func newGLTreeTestGW(projects []glProject, doJSONHandlers []glDoJSONHandler) *glCompositeGateway {
	raw := make([][]byte, len(projects))
	for i, p := range projects {
		raw[i], _ = json.Marshal(p)
	}
	return &glCompositeGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return &connectorpb.GitLabListProjectsResponse{
				Projects: raw,
				// NextPageUrl empty → single page.
			}, nil
		},
		doJSONHandlers: doJSONHandlers,
	}
}

// glBranchHandlerFor returns a handler that answers /repository/branches/ calls.
func glBranchHandlerFor(sha string) glDoJSONHandler {
	return glDoJSONHandler{
		match: "/repository/branches/",
		fn: func(_ string, respBody any) error {
			r := respBody.(*glBranchResponse)
			r.Name = "main"
			r.Commit.ID = sha
			return nil
		},
	}
}

// glTreeHandlerFor returns a handler that answers /repository/tree calls with
// the given entries in a single page (len < 100, so no second page fetch).
func glTreeHandlerFor(entries []treewalker.TreeEntry) glDoJSONHandler {
	return glDoJSONHandler{
		match: "/repository/tree",
		fn: func(_ string, respBody any) error {
			r := respBody.(*[]glTreeEntry)
			for _, e := range entries {
				*r = append(*r, glTreeEntry{
					ID:   e.SHA,
					Name: e.Path,
					Type: e.Type,
					Path: e.Path,
					Mode: e.Mode,
				})
			}
			return nil
		},
	}
}

// testGLProject returns a minimal glProject suitable for tree tests.
func testGLProject(id int, pathWithNamespace string) glProject {
	return glProject{
		ID:                id,
		Path:              pathWithNamespace,
		PathWithNamespace: pathWithNamespace,
		DefaultBranch:     "main",
		Visibility:        "private",
	}
}

// treeParamsGL builds an ExecutionParams for fetch_repo_tree tests.
func treeParamsGL(t *testing.T, sink sdk.DataSink, gw sdk.VendorGateway, s3 *sdkmocks.MockBlobStore, cursor *sdk.Cursor) *sdk.ExecutionParams {
	t.Helper()
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: "org-tree-gl-test",
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
		BlobStore:     s3,
	}
}

// decodeGLTreeRecord JSON-unmarshals a RepoTreeRecord from raw payload bytes.
func decodeGLTreeRecord(t *testing.T, payload []byte) *connectorv1.RepoTreeRecord {
	t.Helper()
	var rec connectorv1.RepoTreeRecord
	require.NoError(t, protojson.Unmarshal(payload, &rec))
	return &rec
}

// TestFetchRepoTreeOp_GL_SHAMatch verifies the short-circuit path: when the
// HEAD SHA equals the last-known cursor value, a no_change record is emitted
// and S3 is not touched.
func TestFetchRepoTreeOp_GL_SHAMatch(t *testing.T) {
	const (
		headSHA   = "abc123glsha"
		projectID = 101
	)

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	// No S3 Write expected — gomock controller enforces this at test end.

	proj := testGLProject(projectID, "mygroup/myrepo")
	gw := newGLTreeTestGW([]glProject{proj}, []glDoJSONHandler{
		glBranchHandlerFor(headSHA),
	})
	sink := testkit.NewChannelSink()

	// Pre-load cursor with the current SHA so the op detects no change.
	projectIDStr := fmt.Sprintf("%d", projectID)
	shaBytes, _ := json.Marshal(headSHA)
	cursor := sdk.NewCompositeCursor(map[string]json.RawMessage{
		projectIDStr: shaBytes,
	})

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeParamsGL(t, sink, gw, s3, cursor))
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "expected exactly one no_change record")
	rec := decodeGLTreeRecord(t, records[0].Payload)
	assert.True(t, rec.NoChange, "expected NoChange=true for SHA-match path")
	assert.Equal(t, headSHA, rec.TreeSha)
	assert.Equal(t, projectIDStr, rec.PlatformRepoId)
	// It shares the ExternalID of the record that uploaded the tree, so it
	// names the same blob: a sink that overwrites on ExternalID keeps the key.
	assert.True(t, strings.HasSuffix(rec.TreeS3Key, "/"+projectIDStr+"/"+headSHA+".json"), "no-change record names the tree blob, got %q", rec.TreeS3Key)
}

// TestFetchRepoTreeOp_GL_SHAMismatch verifies that a changed SHA triggers a
// full tree walk, S3 upload, and a populated RepoTreeRecord.
func TestFetchRepoTreeOp_GL_SHAMismatch(t *testing.T) {
	const (
		headSHA   = "new-sha-gl-999"
		projectID = 202
	)

	entries := []treewalker.TreeEntry{
		{Path: "README.md", Type: "blob", SHA: "b1"},
		{Path: "src", Type: "tree", SHA: "sub1"},
		{Path: "src/main.go", Type: "blob", SHA: "b2"},
	}

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)

	var capturedKey string
	s3.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		DoAndReturn(func(_ context.Context, dst string, content []byte, _ string) error {
			capturedKey = dst
			return nil
		})

	proj := testGLProject(projectID, "mygroup/myrepo")
	gw := newGLTreeTestGW([]glProject{proj}, []glDoJSONHandler{
		glBranchHandlerFor(headSHA),
		glTreeHandlerFor(entries),
	})
	sink := testkit.NewChannelSink()

	newCursor, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeParamsGL(t, sink, gw, s3, nil))
	require.NoError(t, err)

	require.NotEmpty(t, capturedKey, "expected S3 write")
	assert.Contains(t, capturedKey, "repo-trees/")
	assert.Contains(t, capturedKey, headSHA)

	records := sink.Records()
	require.Len(t, records, 1)
	rec := decodeGLTreeRecord(t, records[0].Payload)
	assert.False(t, rec.NoChange)
	assert.Equal(t, headSHA, rec.TreeSha)
	assert.Equal(t, capturedKey, rec.TreeS3Key)
	assert.False(t, rec.TreeTruncated)

	// Cursor must advance with the new SHA keyed by numeric project ID.
	projectIDStr := fmt.Sprintf("%d", projectID)
	cursorMap, err := sdk.CompositeFromCursor(newCursor)
	require.NoError(t, err)
	var stored string
	require.NoError(t, json.Unmarshal(cursorMap[projectIDStr], &stored))
	assert.Equal(t, headSHA, stored)
}

// TestFetchRepoTreeOp_GL_WalkError verifies that a tree walk error causes the
// project to be skipped with no S3 write and no record emitted.
func TestFetchRepoTreeOp_GL_WalkError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	// No Write expected — gomock enforces.

	proj := testGLProject(303, "mygroup/myrepo")
	gw := newGLTreeTestGW([]glProject{proj}, []glDoJSONHandler{
		glBranchHandlerFor("err-sha"),
		{
			match: "/repository/tree",
			fn: func(_ string, _ any) error {
				return errors.New("network timeout")
			},
		},
	})
	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeParamsGL(t, sink, gw, s3, nil))
	require.Error(t, err, "the only repository failed, so the run fails")
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_GL_S3UploadError verifies that an S3 upload failure
// causes the project to be skipped without emitting a record.
func TestFetchRepoTreeOp_GL_S3UploadError(t *testing.T) {
	entries := []treewalker.TreeEntry{{Path: "go.mod", Type: "blob", SHA: "bX"}}

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	s3.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		Return(errors.New("s3: simulated upload failure"))

	proj := testGLProject(404, "mygroup/myrepo")
	gw := newGLTreeTestGW([]glProject{proj}, []glDoJSONHandler{
		glBranchHandlerFor("s3fail-sha"),
		glTreeHandlerFor(entries),
	})
	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeParamsGL(t, sink, gw, s3, nil))
	require.Error(t, err, "the only repository failed, so the run fails")
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_GL_NilS3Client verifies that a nil S3 client returns an error
// so asynq retries the task rather than silently marking it successful.
func TestFetchRepoTreeOp_GL_NilS3Client(t *testing.T) {
	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: "org-tree-gl-test",
			Credentials:    mustJSON(GitLabCredentials{OAuthRefreshToken: "tok"}),
			Settings:       mustJSON(GitLabSettings{GroupID: 42}),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: newGLTreeTestGW(nil, nil),
		BlobStore:     nil,
	}

	cursor, err := (&FetchRepoTreeOp{}).Execute(context.Background(), params)
	require.Error(t, err, "nil S3Client must return an error so asynq retries")
	assert.Contains(t, err.Error(), "BlobStore is nil")
	assert.Nil(t, cursor)
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_GL_PaginatedFetch verifies that the gitlabTreeAPI
// correctly loops through multiple pages of tree entries. GitLab signals the
// last page by returning fewer entries than per_page (100). We simulate two
// pages: the first returns 100 entries and the second returns 2 (triggering
// the stop condition).
func TestFetchRepoTreeOp_GL_PaginatedFetch(t *testing.T) {
	const (
		headSHA   = "paginated-sha"
		projectID = 505
	)

	// Build 100 entries for page 1 and 2 entries for page 2.
	page1Entries := make([]glTreeEntry, 100)
	for i := range page1Entries {
		page1Entries[i] = glTreeEntry{
			ID:   fmt.Sprintf("sha-%d", i),
			Name: fmt.Sprintf("file%d.go", i),
			Type: "blob",
			Path: fmt.Sprintf("file%d.go", i),
			Mode: "100644",
		}
	}
	page2Entries := []glTreeEntry{
		{ID: "sha-last-a", Name: "final_a.go", Type: "blob", Path: "final_a.go", Mode: "100644"},
		{ID: "sha-last-b", Name: "final_b.go", Type: "blob", Path: "final_b.go", Mode: "100644"},
	}

	// Route by the page= query parameter embedded in the URL.
	// page=1 → 100 entries (full page, triggers fetch of page 2).
	// page=2 → 2 entries (partial page, stops iteration).
	treeHandler := glDoJSONHandler{
		match: "/repository/tree",
		fn: func(urlStr string, respBody any) error {
			r := respBody.(*[]glTreeEntry)
			if strings.Contains(urlStr, "page=2") {
				*r = append(*r, page2Entries...)
			} else {
				*r = append(*r, page1Entries...)
			}
			return nil
		},
	}

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	var capturedBody []byte
	s3.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		DoAndReturn(func(_ context.Context, _ string, content []byte, _ string) error {
			capturedBody = content
			return nil
		})

	proj := testGLProject(projectID, "mygroup/big-repo")
	gw := newGLTreeTestGW([]glProject{proj}, []glDoJSONHandler{
		glBranchHandlerFor(headSHA),
		treeHandler,
	})
	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeParamsGL(t, sink, gw, s3, nil))
	require.NoError(t, err)

	require.NotNil(t, capturedBody, "expected S3 write with combined entries")
	var blob gitlabRepoTreeBlob
	require.NoError(t, json.Unmarshal(capturedBody, &blob))
	assert.Equal(t, headSHA, blob.TreeSHA)
	assert.False(t, blob.Truncated, "GitLab should never mark a tree as truncated")
	// Total entries = 100 (page 1) + 2 (page 2).
	assert.Len(t, blob.Entries, 102)
}

// TestFetchRepoTreeOp_GL_AlwaysDefaultBranch verifies the repo tree always
// resolves the project's default branch, regardless of the tracked-branch /
// all-branches setting (which governs only the PR read-filter). The branch-SHA
// lookup URL must reference the default branch ("main"), not develop/all.
func TestFetchRepoTreeOp_GL_AlwaysDefaultBranch(t *testing.T) {
	const projectPath = "mygroup/myrepo"
	project := testGLProject(7, projectPath) // DefaultBranch == "main"

	tests := []struct {
		name     string
		settings GitLabSettings
	}{
		{name: "tracked branch develop ignored", settings: GitLabSettings{GroupID: 42, TrackedBranch: "develop"}},
		{name: "track all branches ignored", settings: GitLabSettings{GroupID: 42, TrackAllBranches: true}},
		{name: "per-repo override develop ignored", settings: GitLabSettings{GroupID: 42, RepoBranchOverrides: map[string]string{projectPath: "develop"}}},
		{name: "per-repo all-branches ignored", settings: GitLabSettings{GroupID: 42, RepoAllBranches: []string{projectPath}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var branchURLs []string
			branchHandler := glDoJSONHandler{
				match: "/repository/branches/",
				fn: func(urlStr string, respBody any) error {
					branchURLs = append(branchURLs, urlStr)
					r := respBody.(*glBranchResponse)
					r.Name = "main"
					r.Commit.ID = "headsha1"
					return nil
				},
			}
			gw := newGLTreeTestGW([]glProject{project}, []glDoJSONHandler{
				branchHandler,
				glTreeHandlerFor([]treewalker.TreeEntry{{Path: "README.md", Type: "blob", SHA: "b1"}}),
			})

			ctrl := gomock.NewController(t)
			s3 := sdkmocks.NewMockBlobStore(ctrl)
			s3.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), "").Return(nil)
			sink := testkit.NewChannelSink()

			params := &sdk.ExecutionParams{
				Config: &sdk.ConnectorConfig{
					ConnectorID:    "gitlab",
					OrganizationID: "org-tree-gl-test",
					Credentials:    mustJSON(GitLabCredentials{OAuthRefreshToken: "t"}),
					Settings:       mustJSON(tt.settings),
					Enabled:        true,
				},
				Sink:          sink,
				VendorGateway: gw,
				BlobStore:     s3,
			}

			_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), params)
			require.NoError(t, err)

			require.NotEmpty(t, branchURLs, "expected a branch-SHA lookup")
			for _, u := range branchURLs {
				assert.Contains(t, u, "/repository/branches/main", "tree must resolve the default branch")
				assert.NotContains(t, u, "develop", "tree must not resolve the tracked/override branch")
			}
			records := sink.Records()
			require.Len(t, records, 1)
			assert.Equal(t, "main", decodeGLTreeRecord(t, records[0].Payload).TreeBranch)
		})
	}
}

// A run whose context has ended keeps the trees already walked: the stored
// per-project SHAs are checkpointed and returned with the context error.
func TestFetchRepoTreeOp_KeepsProgressWhenTheContextEnds(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	gw := newGLTreeTestGW([]glProject{testGLProject(101, "mygroup/myrepo")}, []glDoJSONHandler{glBranchHandlerFor("abc123")})
	shaBytes, _ := json.Marshal("old-sha")
	prior := sdk.NewCompositeCursor(map[string]json.RawMessage{"999": shaBytes})
	params := treeParamsGL(t, testkit.NewChannelSink(), gw, s3, prior)
	var saved *sdk.Cursor
	params.Checkpoint = func(_ context.Context, c *sdk.Cursor) error { saved = c; return nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&FetchRepoTreeOp{}).Execute(ctx, params)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, saved, "the progress is checkpointed")
	assert.Contains(t, string(saved.Data), `"999"`, "the stored SHAs are kept")
}

// When the context has ended and the checkpoint that saves the progress
// fails, the run returns that failure joined with the context error.
func TestFetchRepoTreeOp_ReturnsAFailedCheckpointWithTheCause(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	gw := newGLTreeTestGW([]glProject{testGLProject(101, "mygroup/myrepo")}, []glDoJSONHandler{glBranchHandlerFor("abc123")})
	shaBytes, _ := json.Marshal("old-sha")
	prior := sdk.NewCompositeCursor(map[string]json.RawMessage{"999": shaBytes})
	params := treeParamsGL(t, testkit.NewChannelSink(), gw, s3, prior)
	checkpointErr := errors.New("store unavailable")
	params.Checkpoint = func(context.Context, *sdk.Cursor) error { return checkpointErr }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&FetchRepoTreeOp{}).Execute(ctx, params)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, checkpointErr, "the failed checkpoint is returned with the cause")
}
