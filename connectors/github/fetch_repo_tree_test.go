// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

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

// treeGateway is a test VendorGateway that routes DoJSON calls to registered handlers.
type treeGateway struct {
	handlers []treeGatewayHandler
}

type treeGatewayHandler struct {
	match string
	fn    func(url string, respBody any) error
}

func (m *treeGateway) DoJSON(_ context.Context, _, urlStr string, _, respBody any) error {
	for _, h := range m.handlers {
		if strings.Contains(urlStr, h.match) {
			return h.fn(urlStr, respBody)
		}
	}
	return fmt.Errorf("unregistered DoJSON call: %s", urlStr)
}

func (m *treeGateway) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not used in fetch_repo_tree tests")
}

func (m *treeGateway) Execute(_ context.Context, _ proto.Message) (proto.Message, error) {
	return nil, fmt.Errorf("Execute not used in fetch_repo_tree tests")
}

func (m *treeGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway { return m }
func (m *treeGateway) ForGroup(_ string) sdk.VendorGateway                      { return m }

// branchHandlerFor returns a handler that answers /branches/ requests with sha.
func branchHandlerFor(sha string) treeGatewayHandler {
	return treeGatewayHandler{
		match: "/branches/",
		fn: func(_ string, respBody any) error {
			r := respBody.(*ghBranchResponse)
			r.Name = "main"
			r.Commit.SHA = sha
			return nil
		},
	}
}

// gitTreeHandlerFor returns a handler that answers /git/trees/ requests.
func gitTreeHandlerFor(entries []treewalker.TreeEntry, truncated bool) treeGatewayHandler {
	return treeGatewayHandler{
		match: "/git/trees/",
		fn: func(_ string, respBody any) error {
			r := respBody.(*ghTreeResponse)
			r.Truncated = truncated
			for _, e := range entries {
				te := ghTreeEntry{Path: e.Path, Type: e.Type, Mode: e.Mode, SHA: e.SHA}
				if e.Size != nil {
					te.Size = *e.Size
				}
				r.Tree = append(r.Tree, te)
			}
			return nil
		},
	}
}

// treeTestParams builds an ExecutionParams for fetch_repo_tree tests.
func treeTestParams(t *testing.T, sink sdk.DataSink, gw sdk.VendorGateway, s3 *sdkmocks.MockBlobStore, cursor *sdk.Cursor) *sdk.ExecutionParams {
	t.Helper()
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-tree-test",
			Credentials: mustJSON(GitHubCredentials{
				InstallationID: "99999",
			}),
			Settings: mustJSON(GitHubSettings{
				Organization: "testorg",
				Repositories: []string{"testorg/myrepo"},
			}),
			Enabled: true,
		},
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gw,
		BlobStore:     s3,
	}
}

// decodeTreeRecord JSON-unmarshals a RepoTreeRecord from raw payload bytes.
func decodeTreeRecord(t *testing.T, payload []byte) *connectorv1.RepoTreeRecord {
	t.Helper()
	var rec connectorv1.RepoTreeRecord
	require.NoError(t, protojson.Unmarshal(payload, &rec))
	return &rec
}

// discoveryTreeGateway answers the repo-listing Execute call (auto-discovery)
// and routes DoJSON to the registered tree handlers. It captures every branch-SHA
// URL so a test can assert which branch the tree op resolved.
type discoveryTreeGateway struct {
	treeGateway
	repos       [][]byte
	branchURLs  []string
	branchSHA   string
	treeEntries []treewalker.TreeEntry
}

func (g *discoveryTreeGateway) DoJSON(_ context.Context, _, urlStr string, _, respBody any) error {
	if strings.Contains(urlStr, "/branches/") {
		g.branchURLs = append(g.branchURLs, urlStr)
		r := respBody.(*ghBranchResponse)
		r.Commit.SHA = g.branchSHA
		return nil
	}
	if strings.Contains(urlStr, "/git/trees/") {
		r := respBody.(*ghTreeResponse)
		for _, e := range g.treeEntries {
			r.Tree = append(r.Tree, ghTreeEntry{Path: e.Path, Type: e.Type, Mode: e.Mode, SHA: e.SHA})
		}
		return nil
	}
	return fmt.Errorf("unregistered DoJSON call: %s", urlStr)
}

func (g *discoveryTreeGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	if _, ok := req.(*connectorpb.ListOrgReposRequest); ok {
		return &connectorpb.ListOrgReposResponse{Repos: g.repos, HasNextPage: false}, nil
	}
	return nil, fmt.Errorf("unexpected Execute call: %T", req)
}

func (g *discoveryTreeGateway) ForGroup(_ string) sdk.VendorGateway { return g }

// TestFetchRepoTreeOp_AlwaysDefaultBranch verifies the repo tree always resolves
// the repo's default branch, regardless of the tracked-branch / all-branches
// setting (which governs only the PR read-filter). The auto-discovered repo's
// default branch is "main"; the branch-SHA lookup URL must reference main, not
// the configured develop/all settings.
func TestFetchRepoTreeOp_AlwaysDefaultBranch(t *testing.T) {
	tests := []struct {
		name     string
		settings GitHubSettings
	}{
		{
			name:     "tracked branch develop ignored",
			settings: GitHubSettings{Organization: "testorg", TrackedBranch: "develop"},
		},
		{
			name:     "track all branches ignored",
			settings: GitHubSettings{Organization: "testorg", TrackAllBranches: true},
		},
		{
			name:     "per-repo override develop ignored",
			settings: GitHubSettings{Organization: "testorg", RepoBranchOverrides: map[string]string{"testorg/myrepo": "develop"}},
		},
		{
			name:     "per-repo all-branches ignored",
			settings: GitHubSettings{Organization: "testorg", RepoAllBranches: []string{"testorg/myrepo"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3 := sdkmocks.NewMockBlobStore(ctrl)
			s3.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), "").Return(nil)

			gw := &discoveryTreeGateway{
				repos:       [][]byte{mustJSON(ghRepository{ID: 7, FullName: "testorg/myrepo", DefaultBranch: "main"})},
				branchSHA:   "headsha1",
				treeEntries: []treewalker.TreeEntry{{Path: "README.md", Type: "blob", SHA: "b1"}},
			}
			sink := testkit.NewChannelSink()

			params := &sdk.ExecutionParams{
				Config: &sdk.ConnectorConfig{
					ConnectorID:    "github",
					OrganizationID: "org-tree-test",
					Credentials:    mustJSON(GitHubCredentials{InstallationID: "99999"}),
					Settings:       mustJSON(tt.settings),
					Enabled:        true,
				},
				Sink:          sink,
				VendorGateway: gw,
				BlobStore:     s3,
			}

			_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), params)
			require.NoError(t, err)

			require.NotEmpty(t, gw.branchURLs, "expected a branch-SHA lookup")
			for _, u := range gw.branchURLs {
				assert.Contains(t, u, "/branches/main", "tree must resolve the default branch")
				assert.NotContains(t, u, "develop", "tree must not resolve the tracked/override branch")
			}
			records := sink.Records()
			require.Len(t, records, 1)
			assert.Equal(t, "main", decodeTreeRecord(t, records[0].Payload).TreeBranch)
		})
	}
}

// TestFetchRepoTreeOp_SHAMatch verifies the short-circuit path: when the HEAD SHA
// equals the last-known cursor value, a no_change record is emitted and S3 is
// not touched.
func TestFetchRepoTreeOp_SHAMatch(t *testing.T) {
	const headSHA = "abc123"

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	// No S3 Write expected — gomock controller enforces this.

	gw := &treeGateway{handlers: []treeGatewayHandler{branchHandlerFor(headSHA)}}
	sink := testkit.NewChannelSink()

	shaBytes, _ := json.Marshal(headSHA)
	cursor := sdk.NewCompositeCursor(map[string]json.RawMessage{
		"testorg/myrepo": shaBytes,
	})

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeTestParams(t, sink, gw, s3, cursor))
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "expected exactly one no_change record")
	rec := decodeTreeRecord(t, records[0].Payload)
	assert.True(t, rec.NoChange, "expected NoChange=true for SHA-match path")
	assert.Equal(t, headSHA, rec.TreeSha)
	// It shares the ExternalID of the record that uploaded the tree, so it
	// names the same blob: a sink that overwrites on ExternalID keeps the key.
	assert.True(t, strings.HasSuffix(rec.TreeS3Key, "/"+headSHA+".json"), "no-change record names the tree blob, got %q", rec.TreeS3Key)
}

// TestFetchRepoTreeOp_SHAMismatch verifies that a changed SHA triggers a full tree
// walk, S3 upload, and a populated RepoTreeRecord.
func TestFetchRepoTreeOp_SHAMismatch(t *testing.T) {
	const headSHA = "new-sha-999"

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

	gw := &treeGateway{handlers: []treeGatewayHandler{
		branchHandlerFor(headSHA),
		gitTreeHandlerFor(entries, false),
	}}
	sink := testkit.NewChannelSink()

	newCursor, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeTestParams(t, sink, gw, s3, nil))
	require.NoError(t, err)

	require.NotEmpty(t, capturedKey, "expected S3 write")
	assert.Contains(t, capturedKey, "repo-trees/")
	assert.Contains(t, capturedKey, headSHA)

	records := sink.Records()
	require.Len(t, records, 1)
	rec := decodeTreeRecord(t, records[0].Payload)
	assert.False(t, rec.NoChange)
	assert.Equal(t, headSHA, rec.TreeSha)
	assert.Equal(t, capturedKey, rec.TreeS3Key)
	assert.False(t, rec.TreeTruncated)

	// Cursor must advance with the new SHA.
	cursorMap, err := sdk.CompositeFromCursor(newCursor)
	require.NoError(t, err)
	var stored string
	require.NoError(t, json.Unmarshal(cursorMap["testorg/myrepo"], &stored))
	assert.Equal(t, headSHA, stored)
}

// TestFetchRepoTreeOp_WalkError verifies that a tree walk error causes the repo to be
// skipped with no S3 write and no record emitted.
func TestFetchRepoTreeOp_WalkError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	// No Write expected — gomock enforces.

	gw := &treeGateway{handlers: []treeGatewayHandler{
		branchHandlerFor("err-sha"),
		{match: "/git/trees/", fn: func(_ string, _ any) error {
			return errors.New("network timeout")
		}},
	}}
	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeTestParams(t, sink, gw, s3, nil))
	require.Error(t, err, "the only repository failed, so the run fails")
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_S3UploadError verifies that an S3 upload failure causes the repo
// to be skipped without emitting a record.
func TestFetchRepoTreeOp_S3UploadError(t *testing.T) {
	entries := []treewalker.TreeEntry{{Path: "go.mod", Type: "blob", SHA: "bX"}}

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	s3.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		Return(errors.New("s3: simulated upload failure"))

	gw := &treeGateway{handlers: []treeGatewayHandler{
		branchHandlerFor("s3fail-sha"),
		gitTreeHandlerFor(entries, false),
	}}
	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeTestParams(t, sink, gw, s3, nil))
	require.Error(t, err, "the only repository failed, so the run fails")
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_NilS3Client verifies that a nil S3 client returns an error
// so asynq retries the task rather than silently marking it successful.
func TestFetchRepoTreeOp_NilS3Client(t *testing.T) {
	sink := testkit.NewChannelSink()
	// Build params manually to allow nil S3Client.
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-tree-test",
			Credentials:    mustJSON(GitHubCredentials{InstallationID: "99999"}),
			Settings: mustJSON(GitHubSettings{
				Organization: "testorg",
				Repositories: []string{"testorg/myrepo"},
			}),
			Enabled: true,
		},
		Sink:          sink,
		VendorGateway: &treeGateway{},
		BlobStore:     nil,
	}

	cursor, err := (&FetchRepoTreeOp{}).Execute(context.Background(), params)
	require.Error(t, err, "nil S3Client must return an error so asynq retries")
	assert.Contains(t, err.Error(), "BlobStore is nil")
	assert.Nil(t, cursor)
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_BFSFallback verifies that when the recursive tree call returns
// truncated=true, the walker falls back to BFS and the full tree is assembled
// across multiple non-recursive GetTree calls.
func TestFetchRepoTreeOp_BFSFallback(t *testing.T) {
	const headSHA = "bfs-sha"

	// The recursive call returns truncated=true — this triggers the BFS fallback.
	// The BFS then walks root non-recursively, which returns a blob and a subtree.
	// The subtree is then walked non-recursively to collect its child blob.
	callCount := 0
	gw := &treeGateway{handlers: []treeGatewayHandler{
		branchHandlerFor(headSHA),
		{
			match: "/git/trees/",
			fn: func(urlStr string, respBody any) error {
				r := respBody.(*ghTreeResponse)
				callCount++
				switch {
				case strings.Contains(urlStr, "bfs-sha?recursive=1"):
					// First recursive call — signal truncation to trigger BFS.
					r.Truncated = true
					r.Tree = []ghTreeEntry{
						{Path: "README.md", Type: "blob", SHA: "b1"},
						{Path: "src", Type: "tree", SHA: "sub1"},
					}
				case strings.Contains(urlStr, "bfs-sha") && !strings.Contains(urlStr, "recursive"):
					// BFS non-recursive fetch of the root.
					r.Tree = []ghTreeEntry{
						{Path: "README.md", Type: "blob", SHA: "b1"},
						{Path: "src", Type: "tree", SHA: "sub1"},
					}
				case strings.Contains(urlStr, "sub1"):
					// BFS non-recursive fetch of the src/ subtree.
					r.Tree = []ghTreeEntry{
						{Path: "main.go", Type: "blob", SHA: "b2"},
					}
				default:
					return fmt.Errorf("unexpected tree URL: %s", urlStr)
				}
				return nil
			},
		},
	}}

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	var capturedBody []byte
	s3.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		DoAndReturn(func(_ context.Context, _ string, content []byte, _ string) error {
			capturedBody = content
			return nil
		})

	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeTestParams(t, sink, gw, s3, nil))
	require.NoError(t, err)

	// S3 blob must contain entries assembled by BFS (README.md + src + src/main.go).
	var blob repoTreeBlob
	require.NoError(t, json.Unmarshal(capturedBody, &blob))
	assert.Equal(t, headSHA, blob.TreeSHA)

	paths := make(map[string]bool, len(blob.Entries))
	for _, e := range blob.Entries {
		paths[e.Path] = true
	}
	assert.True(t, paths["README.md"], "BFS result must include README.md")
	assert.True(t, paths["src"], "BFS result must include src/")
	assert.True(t, paths["src/main.go"], "BFS result must include src/main.go")

	// Record is emitted with no_change=false and the correct SHA.
	records := sink.Records()
	require.Len(t, records, 1)
	rec := decodeTreeRecord(t, records[0].Payload)
	assert.False(t, rec.NoChange)
	assert.Equal(t, headSHA, rec.TreeSha)
}

// TestFetchRepoTreeOp_TruncatedDoesNotAdvanceCursor proves the cursor guard: a
// walk that ends truncated (BFS could not complete the tree) still emits + uploads the
// partial tree with tree_truncated=true, but does NOT advance the per-repo SHA cursor —
// so the next run re-walks this repo and self-heals once the tree fits. Its non-truncated
// counterpart (cursor DOES advance) is TestFetchRepoTreeOp_SHAMismatch.
func TestFetchRepoTreeOp_TruncatedDoesNotAdvanceCursor(t *testing.T) {
	const headSHA = "trunc-sha"

	// Every /git/trees/ call reports truncated=true: the recursive call triggers BFS,
	// and the BFS root non-recursive fetch is itself truncated with no subtree, so Walk
	// surfaces truncated=true overall.
	gw := &treeGateway{handlers: []treeGatewayHandler{
		branchHandlerFor(headSHA),
		{
			match: "/git/trees/",
			fn: func(_ string, respBody any) error {
				r := respBody.(*ghTreeResponse)
				r.Truncated = true
				r.Tree = []ghTreeEntry{{Path: "README.md", Type: "blob", SHA: "b1"}}
				return nil
			},
		},
	}}

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	var wrote bool
	s3.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		DoAndReturn(func(_ context.Context, _ string, _ []byte, _ string) error {
			wrote = true
			return nil
		})

	sink := testkit.NewChannelSink()

	newCursor, err := (&FetchRepoTreeOp{}).Execute(context.Background(), treeTestParams(t, sink, gw, s3, nil))
	require.NoError(t, err)

	require.True(t, wrote, "the partial tree is still uploaded")
	records := sink.Records()
	require.Len(t, records, 1)
	rec := decodeTreeRecord(t, records[0].Payload)
	assert.True(t, rec.TreeTruncated, "a truncated walk emits tree_truncated=true so a consumer skips it")
	assert.Equal(t, headSHA, rec.TreeSha)

	// Cursor must NOT advance for a truncated walk — the repo re-walks next run.
	cursorMap, err := sdk.CompositeFromCursor(newCursor)
	require.NoError(t, err)
	_, advanced := cursorMap["testorg/myrepo"]
	assert.False(t, advanced, "a truncated walk must NOT advance the per-repo SHA cursor")
}

// A run whose context has ended keeps the trees already walked: the stored
// per-repository SHAs are checkpointed and returned with the context error.
func TestFetchRepoTreeOp_KeepsProgressWhenTheContextEnds(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	gw := &treeGateway{handlers: []treeGatewayHandler{branchHandlerFor("abc123")}}
	shaBytes, _ := json.Marshal("old-sha")
	prior := sdk.NewCompositeCursor(map[string]json.RawMessage{"other/repo": shaBytes})
	params := treeTestParams(t, testkit.NewChannelSink(), gw, s3, prior)
	var saved *sdk.Cursor
	params.Checkpoint = func(_ context.Context, c *sdk.Cursor) error { saved = c; return nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cur, err := (&FetchRepoTreeOp{}).Execute(ctx, params)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, cur)
	require.NotNil(t, saved, "the progress is checkpointed")
	assert.Contains(t, string(saved.Data), "other/repo", "the stored SHAs are kept")
}

// When the context has ended and the checkpoint that saves the progress
// fails, the run returns that failure joined with the context error.
func TestFetchRepoTreeOp_ReturnsAFailedCheckpointWithTheCause(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	gw := &treeGateway{handlers: []treeGatewayHandler{branchHandlerFor("abc123")}}
	shaBytes, _ := json.Marshal("old-sha")
	prior := sdk.NewCompositeCursor(map[string]json.RawMessage{"other/repo": shaBytes})
	params := treeTestParams(t, testkit.NewChannelSink(), gw, s3, prior)
	checkpointErr := errors.New("store unavailable")
	params.Checkpoint = func(context.Context, *sdk.Cursor) error { return checkpointErr }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&FetchRepoTreeOp{}).Execute(ctx, params)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, checkpointErr, "the failed checkpoint is returned with the cause")
}
