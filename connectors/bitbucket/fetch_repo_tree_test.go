// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	connectorv1 "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// bbTreeGateway is a test VendorGateway that routes DoJSON calls to registered handlers.
// Execute calls are forwarded to executeFunc for repo listing.
type bbTreeGateway struct {
	doJSONHandlers []bbTreeHandler
	executeFunc    func(ctx context.Context, req proto.Message) (proto.Message, error)
}

type bbTreeHandler struct {
	match string
	fn    func(url string, respBody any) error
}

func (m *bbTreeGateway) DoJSON(_ context.Context, _, urlStr string, _, respBody any) error {
	for _, h := range m.doJSONHandlers {
		if strings.Contains(urlStr, h.match) {
			return h.fn(urlStr, respBody)
		}
	}
	return fmt.Errorf("unregistered DoJSON call: %s", urlStr)
}

func (m *bbTreeGateway) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not used in fetch_repo_tree tests")
}

func (m *bbTreeGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, req)
	}
	return nil, fmt.Errorf("unexpected Execute call: %T", req)
}

func (m *bbTreeGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return m
}
func (m *bbTreeGateway) ForGroup(_ string) sdk.VendorGateway { return m }

// bbBranchHandler returns a DoJSON handler that answers refs/branches/ requests.
func bbBranchHandler(sha string) bbTreeHandler {
	return bbTreeHandler{
		match: "/refs/branches/",
		fn: func(_ string, respBody any) error {
			r := respBody.(*bbBranchSHAResponse)
			r.Target.Hash = sha
			return nil
		},
	}
}

// bbSrcHandler returns a DoJSON handler for /src/ listing requests.
// entries is the flat list of bbSrcEntry items to return on the first page.
// A second page is not needed for these tests.
func bbSrcHandler(entries []bbSrcEntry) bbTreeHandler {
	return bbTreeHandler{
		match: "/src/",
		fn: func(_ string, respBody any) error {
			r := respBody.(*bbSrcResponse)
			r.Values = entries
			r.Next = ""
			return nil
		},
	}
}

// bbTreeTestParams builds an ExecutionParams for fetch_repo_tree tests.
func bbTreeTestParams(t *testing.T, sink sdk.DataSink, gw sdk.VendorGateway, s3 *sdkmocks.MockBlobStore, cursor *sdk.Cursor) *sdk.ExecutionParams {
	t.Helper()
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-bb-tree-test",
			Credentials: mustJSON(BitbucketCredentials{
				OAuthAccessToken: "test-token",
			}),
			Settings: mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{"myworkspace/myrepo"},
			}),
			Enabled: true,
		},
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gw,
		BlobStore:     s3,
	}
}

// decodeBBTreeRecord JSON-unmarshals a RepoTreeRecord from raw payload bytes.
func decodeBBTreeRecord(t *testing.T, payload []byte) *connectorv1.RepoTreeRecord {
	t.Helper()
	var rec connectorv1.RepoTreeRecord
	require.NoError(t, protojson.Unmarshal(payload, &rec))
	return &rec
}

// TestFetchRepoTreeOp_OperationID verifies the operation ID is registered correctly.
func TestFetchRepoTreeOp_OperationID(t *testing.T) {
	op := &FetchRepoTreeOp{}
	assert.Equal(t, sdk.OperationID("fetch_repo_tree"), op.OperationID())
}

// TestFetchRepoTreeOp_NilS3Client verifies that a nil S3 client returns an error
// so asynq retries the task rather than silently marking it successful.
func TestFetchRepoTreeOp_NilS3Client(t *testing.T) {
	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-bb-tree-test",
			Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "tok"}),
			Settings: mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{"myworkspace/myrepo"},
			}),
			Enabled: true,
		},
		Sink:          sink,
		VendorGateway: &bbTreeGateway{},
		BlobStore:     nil,
	}

	cursor, err := (&FetchRepoTreeOp{}).Execute(context.Background(), params)
	require.Error(t, err, "nil S3Client must return an error so asynq retries")
	assert.Contains(t, err.Error(), "BlobStore is nil")
	assert.Nil(t, cursor)
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_SHAMatch verifies the short-circuit path: when the HEAD SHA
// equals the last-known cursor value a no_change record is emitted and S3 is
// not written.
func TestFetchRepoTreeOp_SHAMatch(t *testing.T) {
	const headSHA = "aabbcc112233"

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	// No Write call expected — gomock controller enforces this.

	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{bbBranchHandler(headSHA)}}
	sink := testkit.NewChannelSink()

	shaBytes, _ := json.Marshal(headSHA)
	cursor := sdk.NewCompositeCursor(map[string]json.RawMessage{
		"myworkspace/myrepo": shaBytes,
	})

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), bbTreeTestParams(t, sink, gw, s3, cursor))
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "expected exactly one no_change record")
	rec := decodeBBTreeRecord(t, records[0].Payload)
	assert.True(t, rec.NoChange, "NoChange must be true on SHA-match path")
	assert.Equal(t, headSHA, rec.TreeSha)
	assert.Equal(t, "myworkspace/myrepo", rec.PlatformRepoId)
}

// TestFetchRepoTreeOp_SHAMismatch verifies that a changed SHA triggers a full tree
// walk using Bitbucket-specific BFS, S3 upload, and a populated RepoTreeRecord.
// The tree contains a blob at the root, a subdirectory, and a blob inside it.
func TestFetchRepoTreeOp_SHAMismatch(t *testing.T) {
	const headSHA = "newsha9999"

	// Root level: one blob + one directory.
	rootEntries := []bbSrcEntry{
		{Type: "commit_file", Path: "README.md", Size: 128},
		{Type: "commit_directory", Path: "src"},
	}
	// src/ level: one blob.
	srcEntries := []bbSrcEntry{
		{Type: "commit_file", Path: "src/main.go", Size: 512},
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

	gw := &bbTreeGateway{
		doJSONHandlers: []bbTreeHandler{
			bbBranchHandler(headSHA),
			{
				match: "/src/",
				fn: func(urlStr string, respBody any) error {
					r := respBody.(*bbSrcResponse)
					r.Next = ""
					// Route based on path segment after the commit hash.
					if strings.Contains(urlStr, "/src/"+headSHA+"/src") {
						r.Values = srcEntries
					} else {
						r.Values = rootEntries
					}
					return nil
				},
			},
		},
	}
	sink := testkit.NewChannelSink()

	newCursor, err := (&FetchRepoTreeOp{}).Execute(context.Background(), bbTreeTestParams(t, sink, gw, s3, nil))
	require.NoError(t, err)

	require.NotEmpty(t, capturedKey, "expected an S3 write")
	assert.Contains(t, capturedKey, "repo-trees/")
	assert.Contains(t, capturedKey, headSHA)

	records := sink.Records()
	require.Len(t, records, 1)
	rec := decodeBBTreeRecord(t, records[0].Payload)
	assert.False(t, rec.NoChange)
	assert.Equal(t, headSHA, rec.TreeSha)
	assert.Equal(t, capturedKey, rec.TreeS3Key)
	assert.False(t, rec.TreeTruncated, "Bitbucket never truncates — always false")

	// Cursor must advance with the new SHA. Bitbucket never truncates, so the cursor
	// guard is a no-op here — this asserts a complete walk DOES advance the
	// cursor (the guard does not block the normal path).
	cursorMap, cursorErr := sdk.CompositeFromCursor(newCursor)
	require.NoError(t, cursorErr)
	var stored string
	require.NoError(t, json.Unmarshal(cursorMap["myworkspace/myrepo"], &stored))
	assert.Equal(t, headSHA, stored)
}

// TestFetchRepoTreeOp_WalkError verifies that a src/ API error causes the repo to be
// skipped: no S3 write, no record emitted, no propagated error.
func TestFetchRepoTreeOp_WalkError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	// No Write expected — gomock enforces this.

	gw := &bbTreeGateway{
		doJSONHandlers: []bbTreeHandler{
			bbBranchHandler("err-sha"),
			{
				match: "/src/",
				fn: func(_ string, _ any) error {
					return errors.New("simulated API error")
				},
			},
		},
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), bbTreeTestParams(t, sink, gw, s3, nil))
	require.Error(t, err, "the only repository failed, so the run fails")
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_S3UploadError verifies that an S3 write failure causes the repo
// to be skipped without emitting a record.
func TestFetchRepoTreeOp_S3UploadError(t *testing.T) {
	const headSHA = "s3failsha"

	entries := []bbSrcEntry{
		{Type: "commit_file", Path: "go.mod", Size: 256},
	}

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	s3.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		Return(errors.New("s3: simulated upload failure"))

	gw := &bbTreeGateway{
		doJSONHandlers: []bbTreeHandler{
			bbBranchHandler(headSHA),
			bbSrcHandler(entries),
		},
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), bbTreeTestParams(t, sink, gw, s3, nil))
	require.Error(t, err, "the only repository failed, so the run fails")
	assert.Empty(t, sink.Records())
}

// TestFetchRepoTreeOp_SHAMismatch_SubtreeRecursion verifies BFS recursion across
// multiple directory levels: root → lib/ → lib/util/ → lib/util/helper.go.
func TestFetchRepoTreeOp_SHAMismatch_SubtreeRecursion(t *testing.T) {
	const headSHA = "deepsha000"

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)

	var capturedBody []byte
	s3.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), "").
		DoAndReturn(func(_ context.Context, _ string, content []byte, _ string) error {
			capturedBody = content
			return nil
		})

	gw := &bbTreeGateway{
		doJSONHandlers: []bbTreeHandler{
			bbBranchHandler(headSHA),
			(&bbFakeSrcTree{entries: []bbSrcEntry{
				{Type: "commit_file", Path: "main.go", Size: 256},
				{Type: "commit_directory", Path: "lib"},
				{Type: "commit_directory", Path: "lib/util"},
				{Type: "commit_file", Path: "lib/util/helper.go", Size: 64},
			}}).handler(t, headSHA),
		},
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), bbTreeTestParams(t, sink, gw, s3, nil))
	require.NoError(t, err)

	require.NotNil(t, capturedBody, "expected S3 write")
	var blob bbRepoTreeBlob
	require.NoError(t, json.Unmarshal(capturedBody, &blob))

	paths := make(map[string]bool, len(blob.Entries))
	for _, e := range blob.Entries {
		paths[e.Path] = true
	}

	assert.True(t, paths["main.go"], "must include root blob main.go")
	assert.True(t, paths["lib"], "must include lib/ directory entry")
	assert.True(t, paths["lib/util"], "must include lib/util/ directory entry")
	assert.True(t, paths["lib/util/helper.go"], "must include lib/util/helper.go blob")

	records := sink.Records()
	require.Len(t, records, 1)
	rec := decodeBBTreeRecord(t, records[0].Payload)
	assert.Equal(t, headSHA, rec.TreeSha)
	assert.False(t, rec.TreeTruncated)
}

// TestBbSrcTypeToTreeType verifies the type conversion helper.
func TestBbSrcTypeToTreeType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"commit_file", "blob"},
		{"commit_directory", "tree"},
		{"", "blob"},
		{"unknown_type", "blob"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := bbSrcTypeToTreeType(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestGetBitbucketBranchSHA_EmptyHash verifies that an empty hash from the API
// returns an appropriate error.
func TestGetBitbucketBranchSHA_EmptyHash(t *testing.T) {
	gw := &bbTreeGateway{
		doJSONHandlers: []bbTreeHandler{
			{
				match: "/refs/branches/",
				fn: func(_ string, respBody any) error {
					// Return empty target hash.
					r := respBody.(*bbBranchSHAResponse)
					r.Target.Hash = ""
					return nil
				},
			},
		},
	}

	_, err := getBitbucketBranchSHA(context.Background(), gw, "non-sim-org", "ws", "repo", "main", make(chan struct{}, 1), time.Now().Add(time.Minute))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty commit SHA")
}

// TestBuildSrcURL pins the src/ listing URL contract: no `format` param
// (format=meta is a file-only selector that 400s on directory/root paths),
// pagelen for pagination, and a trailing slash before the query string so the
// bare-commit root returns the listing rather than a 302. The Execute-level
// tests stub the gateway on the "/src/" substring and ignore query params, so
// the URL shape must be guarded here independently.
func TestBuildSrcURL(t *testing.T) {
	t.Parallel()
	const commit = "abc123"
	tests := []struct {
		name      string
		dirPath   string
		afterPath string
		wantPath  string // path portion before the query string
		wantAfter bool
	}{
		{name: "root", dirPath: "", afterPath: "", wantPath: "/src/abc123/"},
		{name: "subdir", dirPath: "src/main", afterPath: "", wantPath: "/src/abc123/src/main/"},
		{name: "with after cursor", dirPath: "pkg", afterPath: "pkg/z.go", wantPath: "/src/abc123/pkg/", wantAfter: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := buildSrcURL("non-sim-org", "myworkspace", "myrepo", commit, tt.dirPath, tt.afterPath, 1)
			assert.NotContains(t, got, "format=meta", "format=meta must never be sent — it 400s on directory/root paths")
			assert.NotContains(t, got, "format=", "no format param belongs on a directory listing")
			assert.Contains(t, got, "pagelen=100")
			assert.Contains(t, got, tt.wantPath+"?", "directory path must end in a slash before the query string")
			if tt.wantAfter {
				assert.Contains(t, got, "&after=")
			} else {
				assert.NotContains(t, got, "after=")
			}
		})
	}
}

// TestWalkBitbucketTree_PopulatesSHAFromCommitHash pins the per-entry SHA fix:
// walkBitbucketTree copies the src listing's commit.hash into TreeEntry.SHA (the
// change-detection baseline the harness content flow keys on). A nil commit leaves
// the SHA empty without panicking.
func TestWalkBitbucketTree_PopulatesSHAFromCommitHash(t *testing.T) {
	t.Parallel()
	const headSHA = "walkshadeadbeef"

	gw := &bbTreeGateway{
		doJSONHandlers: []bbTreeHandler{
			{
				match: "/src/",
				fn: func(_ string, respBody any) error {
					r := respBody.(*bbSrcResponse)
					r.Next = ""
					r.Values = []bbSrcEntry{
						// commit.hash present → SHA populated.
						{Type: "commit_file", Path: "README.md", Size: 12, Commit: &struct {
							Hash string `json:"hash"`
						}{Hash: "filecommit111"}},
						// commit absent → SHA stays empty, no panic.
						{Type: "commit_file", Path: "go.mod", Size: 34},
					}
					return nil
				},
			},
		},
	}

	entries, err := walkBitbucketTree(context.Background(), gw, "non-sim-org", "acme", "widgets", headSHA, "", bbTreeWalkerParallelism, make(chan struct{}, bbTreeWalkerParallelism), time.Now().Add(time.Minute), bbTreeMaxDepth)
	require.NoError(t, err)

	shaByPath := make(map[string]string, len(entries))
	for _, e := range entries {
		shaByPath[e.Path] = e.SHA
	}
	assert.Equal(t, "filecommit111", shaByPath["README.md"], "SHA must come from the src listing's commit.hash")
	assert.Empty(t, shaByPath["go.mod"], "a nil commit leaves SHA empty without panicking")
}

// TestWalkBitbucketTree_BoundsConcurrencyAcrossDepth pins
// walkBitbucketTree's walk-wide bound (see its doc): peak concurrency must
// not multiply with depth. The tree is deep enough that a per-level limit
// would visibly exceed the bound.
func TestWalkBitbucketTree_BoundsConcurrencyAcrossDepth(t *testing.T) {
	const (
		limit         = 2
		depth         = 5
		dirsPerLevel  = 3
		headSHAForest = "abc123"
	)

	var (
		mu       sync.Mutex
		inFlight int
		peak     int
	)

	// Every directory below `depth` reports dirsPerLevel subdirectories, so the
	// walk recurses dirsPerLevel^depth ways — far more than `limit` if the
	// bound were per level.
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{{
		match: "/src/",
		fn: func(urlStr string, respBody any) error {
			mu.Lock()
			inFlight++
			if inFlight > peak {
				peak = inFlight
			}
			mu.Unlock()

			// Hold the slot briefly so genuinely-concurrent calls overlap.
			time.Sleep(2 * time.Millisecond)

			// Path depth drives the recursion; stop expanding at `depth`.
			level := strings.Count(strings.TrimSuffix(pathAfterSrc(urlStr), "/"), "/")
			r := respBody.(*bbSrcResponse)
			if level < depth {
				for i := range dirsPerLevel {
					r.Values = append(r.Values, bbSrcEntry{
						Path: fmt.Sprintf("%sd%d", pathPrefix(urlStr), i),
						Type: "commit_directory",
					})
				}
			}
			r.Values = append(r.Values, bbSrcEntry{Path: pathPrefix(urlStr) + "f", Type: "commit_file"})
			r.Next = ""

			mu.Lock()
			inFlight--
			mu.Unlock()
			return nil
		},
	}}}

	sem := make(chan struct{}, limit)
	_, err := walkBitbucketTree(context.Background(), gw, "non-sim-org", "acme", "widgets", headSHAForest, "", limit, sem, time.Now().Add(time.Minute), bbTreeMaxDepth)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, peak, limit,
		"peak in-flight src/ calls was %d for a walk-wide limit of %d — the bound is multiplying with depth", peak, limit)
}

// pathAfterSrc extracts the directory path a src/ URL addresses, i.e. whatever
// follows "/src/<sha>/".
func pathAfterSrc(urlStr string) string {
	i := strings.Index(urlStr, "/src/")
	if i < 0 {
		return ""
	}
	rest := urlStr[i+len("/src/"):]
	// Drop the commit hash segment and any query string.
	if q := strings.IndexByte(rest, '?'); q >= 0 {
		rest = rest[:q]
	}
	if s := strings.IndexByte(rest, '/'); s >= 0 {
		return rest[s+1:]
	}
	return ""
}

// pathPrefix returns the child-path prefix for entries listed under a src/ URL.
func pathPrefix(urlStr string) string {
	p := pathAfterSrc(urlStr)
	if p == "" {
		return ""
	}
	return strings.TrimSuffix(p, "/") + "/"
}

// TestFetchRepoTreeOp_AlwaysDefaultBranch verifies the repo tree always resolves
// the repo's default branch (mainbranch), regardless of the tracked-branch /
// all-branches setting (which governs only the PR read-filter). The branch-SHA
// lookup URL must reference the default branch ("main"), not develop/all.
func TestFetchRepoTreeOp_AlwaysDefaultBranch(t *testing.T) {
	const repoFullName = "myworkspace/myrepo"
	repo := bbRepository{UUID: "{u-1}", FullName: repoFullName, Mainbranch: &bbBranchRef{Name: "main"}}

	tests := []struct {
		name     string
		settings BitbucketSettings
	}{
		{name: "tracked branch develop ignored", settings: BitbucketSettings{Workspace: "myworkspace", TrackedBranch: "develop"}},
		{name: "track all branches ignored", settings: BitbucketSettings{Workspace: "myworkspace", TrackAllBranches: true}},
		{name: "per-repo override develop ignored", settings: BitbucketSettings{Workspace: "myworkspace", RepoBranchOverrides: map[string]string{repoFullName: "develop"}}},
		{name: "per-repo all-branches ignored", settings: BitbucketSettings{Workspace: "myworkspace", RepoAllBranches: []string{repoFullName}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var branchURLs []string
			gw := &bbTreeGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					if _, ok := req.(*connectorpb.BitbucketListReposRequest); ok {
						return bbReposResponse([]bbRepository{repo}, false), nil
					}
					return nil, fmt.Errorf("unexpected Execute call: %T", req)
				},
				doJSONHandlers: []bbTreeHandler{
					{
						match: "/refs/branches/",
						fn: func(urlStr string, respBody any) error {
							branchURLs = append(branchURLs, urlStr)
							r := respBody.(*bbBranchSHAResponse)
							r.Target.Hash = "headsha1"
							return nil
						},
					},
					bbSrcHandler([]bbSrcEntry{{Type: "commit_file", Path: "README.md", Size: 10}}),
				},
			}

			ctrl := gomock.NewController(t)
			s3 := sdkmocks.NewMockBlobStore(ctrl)
			s3.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), "").Return(nil)
			sink := testkit.NewChannelSink()

			params := &sdk.ExecutionParams{
				Config: &sdk.ConnectorConfig{
					ConnectorID:    "bitbucket",
					OrganizationID: "org-bb-tree-test",
					Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "t"}),
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
				assert.Contains(t, u, "/refs/branches/main", "tree must resolve the default branch")
				assert.NotContains(t, u, "develop", "tree must not resolve the tracked/override branch")
			}
			records := sink.Records()
			require.Len(t, records, 1)
			assert.Equal(t, "main", decodeBBTreeRecord(t, records[0].Payload).TreeBranch)
		})
	}
}

// TestGetJSONWaitsOutLocalRateLimit pins that the walk waits out the
// per-minute limiter window instead of failing.
//
// The walk issues one call per directory, so it routinely exhausts a window the
// fail-fast ops never approach. Because walkBitbucketTree must not write a
// partial tree and Execute skips a failed repo without advancing its cursor, an
// abort here strands that repo's tree forever. Local capacity must therefore be
// waited out, not treated as a failure.
func TestGetJSONWaitsOutLocalRateLimit(t *testing.T) {
	// Shrink the sleeps — this test asserts the retry behaviour, not the wall
	// clock. Not parallel, since it reassigns package state.
	defer withShortCapacityWaits(t)()

	calls := 0
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{{
		match: "/src/",
		fn: func(_ string, respBody any) error {
			calls++
			if calls <= 2 {
				// Shaped exactly like rateLimitedDoer's local-limiter rejection,
				// including the DoJSON "%w" wrap it travels through.
				return fmt.Errorf("request failed: %w", &sdk.SyncError{
					Kind:              sdk.ErrKindRateLimit,
					Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
					Retryable:         true,
					RetryAfterSeconds: 30,
				})
			}
			r := respBody.(*bbSrcResponse)
			r.Values = []bbSrcEntry{{Type: "commit_file", Path: "README.md"}}
			return nil
		},
	}}}

	var out bbSrcResponse
	err := getJSONWaitingForCapacity(context.Background(), gw, "https://api.bitbucket.org/2.0/src/sha/", &out, make(chan struct{}, 1), time.Now().Add(time.Minute))
	require.NoError(t, err, "a local-limiter rejection must be waited out, not returned")
	assert.Equal(t, 3, calls, "should retry until capacity frees")
	require.Len(t, out.Values, 1, "the successful attempt's body must reach the caller")
	assert.Equal(t, "README.md", out.Values[0].Path)
}

// TestGetJSONAbortsOnVendorRateLimit pins the other half of the contract: a 429
// or an open circuit means the vendor wants us to stop, which is the opposite
// of slowing down. Waiting those out would keep pressure on a vendor already
// throttling us.
func TestGetJSONAbortsOnVendorRateLimit(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "vendor 429 carries no local-limiter sentinel",
			err: &sdk.SyncError{
				Kind:              sdk.ErrKindRateLimit,
				Cause:             errors.New("bitbucket: 429 Too Many Requests"),
				HTTPStatus:        429,
				RetryAfterSeconds: 60,
			},
		},
		{
			name: "open circuit is a stop, not a queue",
			err: &sdk.SyncError{
				Kind:  sdk.ErrKindRateLimit,
				Cause: errors.New("circuit open for bitbucket-rest (consecutive 429 storm)"),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{{
				match: "/src/",
				fn: func(_ string, _ any) error {
					calls++
					return fmt.Errorf("request failed: %w", tt.err)
				},
			}}}

			var out bbSrcResponse
			err := getJSONWaitingForCapacity(context.Background(), gw, "https://api.bitbucket.org/2.0/src/sha/", &out, make(chan struct{}, 1), time.Now().Add(time.Minute))
			require.Error(t, err)
			assert.Equal(t, 1, calls, "must abort on the first attempt, not retry")
		})
	}
}

// TestGetJSONStopsWaitingWhenContextDone pins the bound: a limiter that never
// frees must fail the walk rather than pin a worker until the task deadline.
func TestGetJSONStopsWaitingWhenContextDone(t *testing.T) {
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{{
		match: "/src/",
		fn: func(_ string, _ any) error {
			return fmt.Errorf("request failed: %w", &sdk.SyncError{
				Kind:  sdk.ErrKindRateLimit,
				Cause: fmt.Errorf("rate limit exceeded: %w", sdk.ErrLocalRateLimit),
			})
		},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var out bbSrcResponse
	err := getJSONWaitingForCapacity(ctx, gw, "https://api.bitbucket.org/2.0/src/sha/", &out, make(chan struct{}, 1), time.Now().Add(time.Minute))
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// withShortCapacityWaits shrinks the capacity-wait tuning for a test and
// returns the restore func.
func withShortCapacityWaits(t *testing.T) func() {
	t.Helper()
	origWait, origJitter := treeWalkCapacityWait, treeWalkCapacityWaitJitter
	treeWalkCapacityWait, treeWalkCapacityWaitJitter = time.Millisecond, time.Millisecond
	return func() {
		treeWalkCapacityWait, treeWalkCapacityWaitJitter = origWait, origJitter
	}
}

// TestGetJSONWaitsPastFormerRetryCap: the repo's deadline, not a fixed retry
// cap, bounds the wait for capacity.
//
// Capacity frees in bursts and acquisition is an unfair race, so a count of
// rejections can run out while most of the budget remains. Patience is bounded
// by the repo's remaining budget, not by a loss count.
func TestGetJSONWaitsPastFormerRetryCap(t *testing.T) {
	defer withShortCapacityWaits(t)()

	const rejections = 60 // comfortably past a 45-wait cap

	calls := 0
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{{
		match: "/src/",
		fn: func(_ string, respBody any) error {
			calls++
			if calls <= rejections {
				return fmt.Errorf("request failed: %w", &sdk.SyncError{
					Kind:      sdk.ErrKindRateLimit,
					Cause:     fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
					Retryable: true,
				})
			}
			r := respBody.(*bbSrcResponse)
			r.Values = []bbSrcEntry{{Type: "commit_file", Path: "README.md"}}
			return nil
		},
	}}}

	var out bbSrcResponse
	err := getJSONWaitingForCapacity(context.Background(), gw, "https://api.bitbucket.org/2.0/src/sha/", &out,
		make(chan struct{}, 1), time.Now().Add(time.Minute))
	require.NoError(t, err, "budget remained, so the walk must keep waiting rather than abandon the repo")
	assert.Equal(t, rejections+1, calls, "every rejection inside the budget must be retried")
}

// TestGetJSONStopsAtRepoBudget pins the other half: once this repo's share of
// the run budget is gone the listing fails, so one pathological repo cannot
// consume the whole run and starve the repos queued behind it.
func TestGetJSONStopsAtRepoBudget(t *testing.T) {
	defer withShortCapacityWaits(t)()

	calls := 0
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{{
		match: "/src/",
		fn: func(_ string, _ any) error {
			calls++
			return fmt.Errorf("request failed: %w", &sdk.SyncError{
				Kind:      sdk.ErrKindRateLimit,
				Cause:     fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
				Retryable: true,
			})
		},
	}}}

	var out bbSrcResponse
	err := getJSONWaitingForCapacity(context.Background(), gw, "https://api.bitbucket.org/2.0/src/sha/", &out,
		make(chan struct{}, 1), time.Now().Add(20*time.Millisecond))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "share of the run budget")
	assert.ErrorIs(t, err, sdk.ErrLocalRateLimit, "the limiter cause must survive for classification")
}

// TestRepoWalkDeadlineSharesRunBudget pins the allocation rule: an even share of
// the time still left, so time the repos ahead did not spend rolls forward.
func TestRepoWalkDeadlineSharesRunBudget(t *testing.T) {
	tests := []struct {
		name           string
		runBudget      time.Duration
		reposRemaining int
		wantShare      time.Duration
	}{
		{name: "first of ten gets a tenth", runBudget: 30 * time.Minute, reposRemaining: 10, wantShare: 3 * time.Minute},
		{name: "last repo may use all that is left", runBudget: 30 * time.Minute, reposRemaining: 1, wantShare: 30 * time.Minute},
		{name: "zero remaining is clamped, never a divide by zero", runBudget: 10 * time.Minute, reposRemaining: 0, wantShare: 10 * time.Minute},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), tt.runBudget)
			defer cancel()

			got := time.Until(repoWalkDeadline(ctx, tt.reposRemaining))
			assert.InDelta(t, tt.wantShare.Seconds(), got.Seconds(), 1.0)
		})
	}
}

// TestRepoWalkDeadlineWithoutRunDeadline covers the local/test path: an
// un-deadlined context must still bound the walk rather than wait forever.
func TestRepoWalkDeadlineWithoutRunDeadline(t *testing.T) {
	t.Parallel()
	got := time.Until(repoWalkDeadline(context.Background(), 1))
	assert.InDelta(t, treeWalkFallbackRunBudget.Seconds(), got.Seconds(), 1.0)
}

// bbFakeSrcTree serves src/ listings the way Bitbucket Cloud actually does: a
// listing rooted at some path returns every descendant down to max_depth levels
// below that root, and withholds the contents of directories sitting exactly at
// the cap.
//
// A fake that returns only one level per call — which is what these tests used
// before the walk asked the vendor to recurse — cannot tell a correct frontier
// rule from a wrong one: it would keep passing for a walk that re-listed
// directories the vendor had already expanded (duplicating entries) or that
// stopped short of the cap (dropping subtrees). Mirroring the vendor is what
// makes the frontier assertion meaningful.
type bbFakeSrcTree struct {
	entries []bbSrcEntry
	mu      sync.Mutex
	urls    []string
}

// srcRootAndDepth extracts the listing root path and the requested max_depth
// from a src/ URL. Absent max_depth means the API default of 1.
//
// A URL the walk emits must always parse and carry an integer depth, so both
// error paths fail the test rather than being absorbed into a default — a fake
// that quietly fell back to depth 1 would report a green walk while the walk was
// in fact emitting malformed requests.
func srcRootAndDepth(t *testing.T, urlStr, headSHA string) (string, int) {
	t.Helper()
	u, err := url.Parse(urlStr)
	require.NoError(t, err, "walk emitted an unparseable src/ URL: %s", urlStr)
	depth := 1
	if v := u.Query().Get("max_depth"); v != "" {
		n, convErr := strconv.Atoi(v)
		require.NoError(t, convErr, "max_depth must be an integer, got %q", v)
		depth = n
	}
	path := urlStr
	if idx := strings.Index(path, "/src/"+headSHA); idx >= 0 {
		path = path[idx+len("/src/"+headSHA):]
	}
	if q := strings.Index(path, "?"); q >= 0 {
		path = path[:q]
	}
	return strings.Trim(path, "/"), depth
}

func (f *bbFakeSrcTree) handler(t *testing.T, headSHA string) bbTreeHandler {
	t.Helper()
	return bbTreeHandler{
		match: "/src/",
		fn: func(urlStr string, respBody any) error {
			root, depth := srcRootAndDepth(t, urlStr, headSHA)
			f.mu.Lock()
			f.urls = append(f.urls, urlStr)
			f.mu.Unlock()

			r := respBody.(*bbSrcResponse)
			r.Next = ""
			for _, e := range f.entries {
				if root != "" && !strings.HasPrefix(e.Path, root+"/") {
					continue
				}
				rel := pathDepth(e.Path) - pathDepth(root)
				if rel >= 1 && rel <= depth {
					r.Values = append(r.Values, e)
				}
			}
			return nil
		},
	}
}

func (f *bbFakeSrcTree) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.urls...)
}

// TestWalkBitbucketTree_VendorRecursion pins the frontier rule the walk depends
// on: the vendor expands everything above the depth cap, so the only
// directories still owing a call are those sitting exactly at it. Re-listing a
// shallower directory would duplicate entries the vendor already returned.
func TestWalkBitbucketTree_VendorRecursion(t *testing.T) {
	t.Parallel()
	const headSHA = "deepsha0001"

	// A chain a/b/c/... deeper than the cap, so exactly one frontier call is owed.
	var entries []bbSrcEntry
	path := ""
	for d := 1; d <= bbTreeMaxDepth+2; d++ {
		if path == "" {
			path = "d1"
		} else {
			path = fmt.Sprintf("%s/d%d", path, d)
		}
		entries = append(entries, bbSrcEntry{Type: "commit_directory", Path: path})
	}
	entries = append(entries, bbSrcEntry{Type: "commit_file", Path: path + "/leaf.go", Size: 7})

	fake := &bbFakeSrcTree{entries: entries}
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{fake.handler(t, headSHA)}}

	got, err := walkBitbucketTree(context.Background(), gw, "non-sim-org", "acme", "widgets", headSHA, "",
		bbTreeWalkerParallelism, make(chan struct{}, bbTreeWalkerParallelism), time.Now().Add(time.Minute), bbTreeMaxDepth)
	require.NoError(t, err)

	seen := map[string]int{}
	for _, e := range got {
		seen[e.Path]++
	}
	for _, e := range entries {
		assert.Equal(t, 1, seen[e.Path], "every path must appear exactly once: %s", e.Path)
	}
	assert.Len(t, got, len(entries), "no entry may be duplicated by re-listing an already-expanded directory")

	calls := fake.calls()
	assert.Len(t, calls, 2, "one root listing plus one frontier call for the directory at the depth cap")
	for _, u := range calls {
		assert.NotContains(t, u, "format=", "walk must not send a format param — it 400s on directory listings: %s", u)
		assert.Contains(t, u, "pagelen=100", "walk must paginate the listing: %s", u)
		assert.Contains(t, u, "max_depth=", "walk must ask the vendor to recurse: %s", u)
	}
}

// TestWalkBitbucketTree_RetriesShallowerOn555 pins the documented 555 contract:
// "recursion depth too large" is the vendor asking for a smaller ask, not a
// failure, so the listing steps down instead of losing the repo.
func TestWalkBitbucketTree_RetriesShallowerOn555(t *testing.T) {
	t.Parallel()
	const headSHA = "timeoutsha1"

	var depths []int
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{{
		match: "/src/",
		fn: func(urlStr string, respBody any) error {
			_, d := srcRootAndDepth(t, urlStr, headSHA)
			depths = append(depths, d)
			if d > 2 {
				return fmt.Errorf("request failed: %w", &sdk.SyncError{
					Kind:       sdk.ErrKindVendorError,
					HTTPStatus: bbSrcDepthTimeoutStatus,
					Cause:      errors.New("bitbucket: 555 timeout"),
					Retryable:  true,
				})
			}
			r := respBody.(*bbSrcResponse)
			r.Values = []bbSrcEntry{{Type: "commit_file", Path: "README.md", Size: 3}}
			return nil
		},
	}}}

	got, err := walkBitbucketTree(context.Background(), gw, "non-sim-org", "acme", "widgets", headSHA, "",
		bbTreeWalkerParallelism, make(chan struct{}, bbTreeWalkerParallelism), time.Now().Add(time.Minute), 10)
	require.NoError(t, err, "a 555 must be retried shallower, not surfaced as a walk failure")
	require.Len(t, got, 1)
	assert.Equal(t, []int{10, 5, 2}, depths, "depth must halve until the vendor can finish")
}

// TestWalkBitbucketTree_555AtFloorIsRealFailure pins the other edge: once the
// request is already as small as the API allows, a 555 is a genuine error and
// must not be swallowed into a silently-partial tree.
func TestWalkBitbucketTree_555AtFloorIsRealFailure(t *testing.T) {
	t.Parallel()
	const headSHA = "flooredsha1"

	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{{
		match: "/src/",
		fn: func(_ string, _ any) error {
			return fmt.Errorf("request failed: %w", &sdk.SyncError{
				Kind:       sdk.ErrKindVendorError,
				HTTPStatus: bbSrcDepthTimeoutStatus,
				Cause:      errors.New("bitbucket: 555 timeout"),
			})
		},
	}}}

	_, err := walkBitbucketTree(context.Background(), gw, "non-sim-org", "acme", "widgets", headSHA, "",
		bbTreeWalkerParallelism, make(chan struct{}, bbTreeWalkerParallelism), time.Now().Add(time.Minute), 1)
	require.Error(t, err, "a partial tree must never be written")
}

// A run whose context has ended keeps the trees already walked: the stored
// per-repository SHAs are checkpointed and returned with the context error.
func TestFetchRepoTreeOp_KeepsProgressWhenTheContextEnds(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{bbBranchHandler("abc123")}}
	shaBytes, _ := json.Marshal("old-sha")
	prior := sdk.NewCompositeCursor(map[string]json.RawMessage{"other/repo": shaBytes})
	params := bbTreeTestParams(t, testkit.NewChannelSink(), gw, s3, prior)
	var saved *sdk.Cursor
	params.Checkpoint = func(_ context.Context, c *sdk.Cursor) error { saved = c; return nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&FetchRepoTreeOp{}).Execute(ctx, params)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, saved, "the progress is checkpointed")
	assert.Contains(t, string(saved.Data), "other/repo", "the stored SHAs are kept")
}

// When the context has ended and the checkpoint that saves the progress
// fails, the run returns that failure joined with the context error.
func TestFetchRepoTreeOp_ReturnsAFailedCheckpointWithTheCause(t *testing.T) {
	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{bbBranchHandler("abc123")}}
	shaBytes, _ := json.Marshal("old-sha")
	prior := sdk.NewCompositeCursor(map[string]json.RawMessage{"other/repo": shaBytes})
	params := bbTreeTestParams(t, testkit.NewChannelSink(), gw, s3, prior)
	checkpointErr := errors.New("store unavailable")
	params.Checkpoint = func(context.Context, *sdk.Cursor) error { return checkpointErr }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&FetchRepoTreeOp{}).Execute(ctx, params)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, checkpointErr, "the failed checkpoint is returned with the cause")
}
