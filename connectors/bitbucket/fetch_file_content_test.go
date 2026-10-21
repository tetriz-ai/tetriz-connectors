// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	"github.com/alicebob/miniredis/v2"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

const bbfcTestOrgID = "org-bb-1"

// bbfcMockGateway proxies Do to an httptest server, rewriting the api.bitbucket.org
// URL to the test server while preserving the escaped path+query. It records the
// last raw request URI so a test can assert the src-URL encoding.
type bbfcMockGateway struct {
	serverURL  string
	client     *http.Client
	lastRawURI string
}

func newBBFCMockGateway(serverURL string) *bbfcMockGateway {
	return &bbfcMockGateway{serverURL: serverURL, client: &http.Client{Timeout: 5 * time.Second}}
}

func (m *bbfcMockGateway) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	m.lastRawURI = req.URL.RequestURI()
	rewritten, err := http.NewRequest(req.Method, m.serverURL+req.URL.RequestURI(), req.Body)
	if err != nil {
		return nil, err
	}
	rewritten.Header = req.Header.Clone()
	return m.client.Do(rewritten)
}

func (m *bbfcMockGateway) DoJSON(context.Context, string, string, any, any) error {
	return fmt.Errorf("DoJSON not used in fetch_file_content tests")
}

func (m *bbfcMockGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return m
}
func (m *bbfcMockGateway) ForGroup(string) sdk.VendorGateway { return m }
func (m *bbfcMockGateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, fmt.Errorf("Execute not used in fetch_file_content tests")
}

// bbfcCountingGateway records how many Do calls it forwards.
type bbfcCountingGateway struct {
	inner *bbfcMockGateway
	calls int
}

func (g *bbfcCountingGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	g.calls++
	return g.inner.Do(ctx, req)
}

func (g *bbfcCountingGateway) DoJSON(ctx context.Context, m, u string, rq, rs any) error {
	return g.inner.DoJSON(ctx, m, u, rq, rs)
}

func (g *bbfcCountingGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *bbfcCountingGateway) ForGroup(string) sdk.VendorGateway { return g }
func (g *bbfcCountingGateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, nil
}

// bbfcContentServer serves RAW file bytes keyed by the file path in the src/ URL
// (the segments after "/src/{sha}/"; slashes stay literal, unlike GitLab). A path
// in notFound returns 404. Content-Length is set explicitly so a >1 MiB body still
// reports its full size to the truncation path.
func bbfcContentServer(t *testing.T, bodyByPath map[string][]byte, notFound map[string]bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "900")
		w.Header().Set("X-RateLimit-Limit", "1000")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))

		idx := strings.Index(r.URL.Path, "/src/")
		if idx < 0 {
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusInternalServerError)
			return
		}
		rest := r.URL.Path[idx+len("/src/"):] // {sha}/{path...}
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 {
			http.Error(w, "no path after sha", http.StatusInternalServerError)
			return
		}
		repoPath := parts[1]
		if notFound[repoPath] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, ok := bodyByPath[repoPath]
		if !ok {
			http.Error(w, "no fixture for "+repoPath, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

type bbfcStagingWrite struct {
	key     string
	content []byte
	opts    *sdk.BlobWriteOpts
}

func bbfcCaptureStagingWrites(t *testing.T, ctrl *gomock.Controller) (*sdkmocks.MockBlobStore, *[]bbfcStagingWrite, *[]string) {
	t.Helper()
	writes := &[]bbfcStagingWrite{}
	deletes := &[]string{}
	mock := sdkmocks.NewMockBlobStore(ctrl)
	mock.EXPECT().WriteWithOpts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, dst string, content []byte, opts *sdk.BlobWriteOpts) error {
			cp := append([]byte(nil), content...)
			*writes = append(*writes, bbfcStagingWrite{key: dst, content: cp, opts: opts})
			return nil
		}).AnyTimes()
	mock.EXPECT().Delete(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, dst string) error {
			*deletes = append(*deletes, dst)
			return nil
		}).AnyTimes()
	return mock, writes, deletes
}

// bbfcOneFileRecord builds a single-file changed artifact. Bitbucket addresses files
// by workspace/repo_slug (split from repo_full_name).
func bbfcOneFileRecord() *workqueuev1.FileContentRecord {
	return &workqueuev1.FileContentRecord{
		RepositoryId:   "repo-1",
		RepoFullName:   "acme/widgets",
		PlatformRepoId: "{uuid-1}",
		SourcePath:     ".claude/skills/deploy/SKILL.md",
		IdeType:        commonv1.IdeType_IDE_TYPE_CLAUDE_CODE,
		Name:           "deploy",
		Branch:         "main",
		Ref:            "commit-sha-abc",
		Generation:     42,
		Files: []*workqueuev1.FileContentRecord_FetchedFile{
			{RelativePath: "SKILL.md", BlobSha: "blob-1", IsPrimary: true},
		},
	}
}

func bbfcNewParams(gw sdk.VendorGateway, s3 sdk.BlobStore, budget sdk.BudgetObserver, sink sdk.DataSink) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config:        &sdk.ConnectorConfig{ConnectorID: "bitbucket", OrganizationID: bbfcTestOrgID},
		Cursor:        &sdk.Cursor{},
		Sink:          sink,
		VendorGateway: gw,
		Host:          sdk.HostServices{}, // WorkQueue set per test
		BlobStore:     s3,
		BudgetLimiter: budget,
	}
}

func bbfcNewTestBudget(t *testing.T) *ratelimiter.BudgetLimiter {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return ratelimiter.NewBudgetLimiter(rdb, ratelimiter.BitbucketAdapter{})
}

// TestBitbucketFileContentURL_SegmentEncodesKeepsSlashes pins the Bitbucket src-URL
// contract: the commit and each path segment are individually escaped, but the "/"
// separators stay literal (mirroring buildSrcURL, unlike GitLab's %2F encoding).
func TestBitbucketFileContentURL_SegmentEncodesKeepsSlashes(t *testing.T) {
	got := bitbucketFileContentURL("non-sim-org", "acme", "widgets", "commit-sha-abc", ".claude/skills/a b/deploy.md")
	assert.Contains(t, got, "/repositories/acme/widgets/src/commit-sha-abc/")
	assert.Contains(t, got, "/.claude/skills/", "path separators stay literal")
	assert.Contains(t, got, "a%20b", "a space in a segment is percent-encoded")
	assert.NotContains(t, got, "%2F", "slashes must NOT be percent-encoded for Bitbucket")
}

func TestFetchFileContentOp_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	body := []byte("# Deploy skill\nrun the thing\n")
	srv := bbfcContentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": body}, nil)
	gw := newBBFCMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{bbfcOneFileRecord()}}, nil)

	s3mock, writes, _ := bbfcCaptureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, bbfcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	emittedBefore := artifactOutcomeCount("emitted")
	fetchedBefore := filesFetchedCount()

	cursor, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Same(t, params.Cursor, cursor, "cursor must be pass-through")

	// A successful emit + one staged file bump the observability counters exactly once.
	assert.Equal(t, 1.0, artifactOutcomeCount("emitted")-emittedBefore, "one artifact emitted")
	assert.Equal(t, 1.0, filesFetchedCount()-fetchedBefore, "one file fetched and staged")

	// The connector produced a workspace/repo_slug src URL with literal slashes.
	assert.Contains(t, gw.lastRawURI, "/repositories/acme/widgets/src/commit-sha-abc/.claude/skills/deploy/SKILL.md")

	require.Len(t, *writes, 1)
	w := (*writes)[0]
	assert.True(t, strings.HasPrefix(w.key, "harness-staging/org-bb-1/bitbucket/repo-1/42/"), "key=%s", w.key)
	assert.Contains(t, w.key, ".claude/skills/deploy/SKILL.md")
	assert.NotContains(t, w.key, "harness/org-bb-1/", "must not be a canonical namespace")

	digest := sha256.Sum256(body)
	require.NotNil(t, w.opts)
	assert.Equal(t, base64.StdEncoding.EncodeToString(digest[:]), w.opts.ChecksumSHA256)

	recs := sink.Records()
	require.Len(t, recs, 1)
	assert.Equal(t, "repo-1#1#.claude/skills/deploy/SKILL.md#42", recs[0].ExternalID)

	emitted := &workqueuev1.FileContentRecord{}
	require.NoError(t, protojson.Unmarshal(recs[0].Payload, emitted))
	assert.Equal(t, "deploy", emitted.GetName())
	assert.Equal(t, "main", emitted.GetBranch())
	require.Len(t, emitted.GetFiles(), 1)
	assert.Equal(t, hex.EncodeToString(digest[:]), emitted.GetFiles()[0].GetContentHash())
	assert.Equal(t, w.key, emitted.GetFiles()[0].GetS3Key())
	assert.False(t, emitted.GetFiles()[0].GetTruncated())
	assert.Equal(t, int64(len(body)), emitted.GetFiles()[0].GetOriginalBytes())
}

func TestFetchFileContentOp_404DefersWholeArtifact(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := bbfcOneFileRecord()
	rec.Files = append(rec.Files, &workqueuev1.FileContentRecord_FetchedFile{RelativePath: "scripts/run.sh", BlobSha: "blob-2"})
	srv := bbfcContentServer(t,
		map[string][]byte{".claude/skills/deploy/SKILL.md": []byte("ok")},
		map[string]bool{".claude/skills/deploy/scripts/run.sh": true})
	gw := newBBFCMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock, _, deletes := bbfcCaptureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, bbfcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err, "a per-item 404 never fails the run")
	assert.Empty(t, sink.Records(), "no record emitted for a deferred artifact")
	// The first file staged before the sibling 404 is cleaned up on abort — no orphaned
	// staging object with no record to promote/delete it.
	require.Len(t, *deletes, 1, "the already-staged sibling body is deleted on abort")
	assert.Contains(t, (*deletes)[0], ".claude/skills/deploy/SKILL.md")
}

func TestFetchFileContentOp_BudgetReservationDefersWithZeroCalls(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := bbfcOneFileRecord()
	for i := 0; i < 9; i++ {
		rec.Files = append(rec.Files, &workqueuev1.FileContentRecord_FetchedFile{RelativePath: fmt.Sprintf("f%d.md", i), BlobSha: "b"})
	}

	budget := bbfcNewTestBudget(t)
	headers := http.Header{}
	headers.Set("X-RateLimit-Remaining", strconv.Itoa(ratelimiter.BitbucketCoreFloor+3))
	headers.Set("X-RateLimit-Limit", "1000")
	headers.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	require.NoError(t, budget.UpdateFromResponse(context.Background(), bbfcTestOrgID, headers))

	gw := &bbfcCountingGateway{inner: newBBFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, budget, sink)
	params.Host.WorkQueue = analytics

	deferredBefore := artifactOutcomeCount("deferred_budget")

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "a budget-deferred artifact must issue zero vendor calls")
	assert.Empty(t, sink.Records())
	assert.Equal(t, 1.0, artifactOutcomeCount("deferred_budget")-deferredBefore, "one deferred_budget outcome")
}

// TestFetchFileContentOp_RemovalOnlyBypassesBudget: a removal-only record (helper paths
// deleted, no files to fetch) makes zero vendor requests, so it must emit even when the
// core budget is fully depleted below the floor — otherwise the helper deletion is
// stranded indefinitely behind a budget it never needs.
func TestFetchFileContentOp_RemovalOnlyBypassesBudget(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := bbfcOneFileRecord()
	rec.Files = nil                               // nothing to fetch
	rec.RemovedPaths = []string{"scripts/old.sh"} // only a helper removal to apply

	// Deplete the bucket to zero: a record WITH files would defer here.
	budget := bbfcNewTestBudget(t)
	headers := http.Header{}
	headers.Set("X-RateLimit-Remaining", "0")
	headers.Set("X-RateLimit-Limit", "1000")
	headers.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	require.NoError(t, budget.UpdateFromResponse(context.Background(), bbfcTestOrgID, headers))

	gw := &bbfcCountingGateway{inner: newBBFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	// No WriteWithOpts / Delete expectation — a removal-only record stages nothing.
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, budget, sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "a removal-only record issues zero vendor calls")

	recs := sink.Records()
	require.Len(t, recs, 1, "the removal-only record emits despite the depleted budget")
	emitted := &workqueuev1.FileContentRecord{}
	require.NoError(t, protojson.Unmarshal(recs[0].Payload, emitted))
	assert.Empty(t, emitted.GetFiles())
	assert.Equal(t, []string{"scripts/old.sh"}, emitted.GetRemovedPaths())
}

// artifactOutcomeCount / filesFetchedCount read the current value of the fetch_file_content
// observability counters for this connector, so a test can assert a per-run delta.
func artifactOutcomeCount(outcome string) float64 {
	return prom_testutil.ToFloat64(obs.HarnessFileContentArtifactsTotal.WithLabelValues(string(connectorID), outcome))
}

func filesFetchedCount() float64 {
	return prom_testutil.ToFloat64(obs.HarnessFileContentFilesFetchedTotal.WithLabelValues(string(connectorID)))
}

// failingSink is a DataSink whose Emit always errors, to drive the emit-failure terminal
// branch (outcome=deferred_emit_error).
type failingSink struct{}

func (failingSink) Emit(context.Context, *sdk.RawRecord) error {
	return fmt.Errorf("injected emit failure")
}

func (failingSink) EmitBatch(context.Context, []*sdk.RawRecord) error {
	return fmt.Errorf("injected emit failure")
}
func (failingSink) Flush(context.Context) error { return nil }

// TestFetchFileContentOp_EmitFailureRecordsDeferredEmitErr: the file fetches and stages fine,
// but the sink emit fails — the artifact is deferred (never fails the run) and the failure is
// recorded exactly once as deferred_emit_error, so an emit failure is visible on the outcome
// counter rather than silently dropped.
func TestFetchFileContentOp_EmitFailureRecordsDeferredEmitErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	body := []byte("# Deploy skill\nrun the thing\n")
	srv := bbfcContentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": body}, nil)
	gw := newBBFCMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{bbfcOneFileRecord()}}, nil)

	s3mock, _, deletes := bbfcCaptureStagingWrites(t, ctrl)
	params := bbfcNewParams(gw, s3mock, bbfcNewTestBudget(t), failingSink{})
	params.Host.WorkQueue = analytics

	emitErrBefore := artifactOutcomeCount("deferred_emit_error")

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err, "an emit failure defers the artifact, never fails the run")

	assert.Equal(t, 1.0, artifactOutcomeCount("deferred_emit_error")-emitErrBefore, "one deferred_emit_error outcome")
	// The staged body is cleaned up when emit fails — nothing was emitted to promote it,
	// so it would otherwise be an orphaned staging object.
	require.Len(t, *deletes, 1, "the staged body is deleted when emit fails")
	assert.Contains(t, (*deletes)[0], ".claude/skills/deploy/SKILL.md")
}

func TestFetchFileContentOp_OversizedFileTruncated(t *testing.T) {
	ctrl := gomock.NewController(t)
	big := make([]byte, maxFileContentBytes+2048)
	for i := range big {
		big[i] = 'a'
	}
	srv := bbfcContentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": big}, nil)
	gw := newBBFCMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{bbfcOneFileRecord()}}, nil)

	s3mock, writes, _ := bbfcCaptureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, bbfcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	require.Len(t, *writes, 1)
	assert.Len(t, (*writes)[0].content, maxFileContentBytes, "stored body capped at the 1 MiB cap")

	recs := sink.Records()
	require.Len(t, recs, 1)
	emitted := &workqueuev1.FileContentRecord{}
	require.NoError(t, protojson.Unmarshal(recs[0].Payload, emitted))
	assert.True(t, emitted.GetFiles()[0].GetTruncated())
	assert.Equal(t, int64(len(big)), emitted.GetFiles()[0].GetOriginalBytes(), "original size from Content-Length")
}

func TestFetchFileContentOp_TombstoneReEmittedNoFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := bbfcOneFileRecord()
	rec.IsTombstone = true
	rec.Files = nil

	gw := &bbfcCountingGateway{inner: newBBFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, bbfcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "a tombstone issues no vendor calls")

	recs := sink.Records()
	require.Len(t, recs, 1, "the tombstone is re-emitted so the delete travels the durable write leg")
	emitted := &workqueuev1.FileContentRecord{}
	require.NoError(t, protojson.Unmarshal(recs[0].Payload, emitted))
	assert.True(t, emitted.GetIsTombstone())
}

// Without the host's work queue the op fails, terminal, naming it; with the
// queue but no blob store or budget limiter it fails before claiming work.
func TestFetchFileContentOp_NilCollaboratorsFail(t *testing.T) {
	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{ConnectorID: "bitbucket", OrganizationID: bbfcTestOrgID},
		Cursor: &sdk.Cursor{},
		Sink:   sink,
	}
	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.True(t, se.Terminal)
	assert.Equal(t, sdk.ErrKindConfigInvalid, se.Kind)
	assert.ErrorContains(t, err, "needs host services: work_queue")

	params.Host.WorkQueue = sdkmocks.NewMockWorkQueue(gomock.NewController(t)) // no expectations: a claim fails the test
	_, err = (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.ErrorAs(t, err, &se)
	assert.True(t, se.Terminal)
	assert.ErrorContains(t, err, "needs a blob store and a budget limiter")
	assert.Empty(t, sink.Records())
}

func TestFetchFileContentOp_WorkListRPCErrorIsHardFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("host down"))

	gw := &bbfcCountingGateway{inner: newBBFCMockGateway("http://unused")}
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, bbfcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	cursor, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.Error(t, err, "a work-list gRPC error is a hard failure so asynq retries")
	assert.Nil(t, cursor, "hard failure returns a nil cursor, not the pass-through cursor")
	assert.Zero(t, gw.calls, "no vendor calls on a work-list failure")
	assert.Empty(t, sink.Records())
}

func TestFetchFileContentOp_InvalidRepoFullNameDefers(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := bbfcOneFileRecord()
	rec.RepoFullName = "no-slash" // splitRepoFullName returns ok=false

	gw := &bbfcCountingGateway{inner: newBBFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, bbfcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "a malformed repo full name defers before any fetch")
	assert.Empty(t, sink.Records())
}

func TestFetchFileContentOp_PathEscapeDefers(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := &workqueuev1.FileContentRecord{
		RepositoryId:   "repo-1",
		RepoFullName:   "acme/widgets",
		PlatformRepoId: "{uuid-1}",
		SourcePath:     "CLAUDE.md",
		Name:           "claude-md",
		Branch:         "main",
		Ref:            "commit-sha-abc",
		Generation:     7,
		Files: []*workqueuev1.FileContentRecord_FetchedFile{
			{RelativePath: "../escape.md", BlobSha: "blob-x", IsPrimary: true},
		},
	}

	gw := &bbfcCountingGateway{inner: newBBFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(gw, s3mock, bbfcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "the escape guard fires before any fetch")
	assert.Empty(t, sink.Records())
}

// TestBoundFileContent pins the truncation accounting, including the absent /
// non-credible Content-Length floor (original_bytes must stay > 0 for the proto's
// truncated⟹original_bytes>0 invariant).
func TestBoundFileContent(t *testing.T) {
	tests := []struct {
		name          string
		rawLen        int
		contentLength int64
		wantStored    int
		wantTruncated bool
		wantOriginal  int64
	}{
		{name: "whole file under cap", rawLen: 100, contentLength: 100, wantStored: 100, wantTruncated: false, wantOriginal: 100},
		{name: "exactly at cap", rawLen: maxFileContentBytes, contentLength: maxFileContentBytes, wantStored: maxFileContentBytes, wantTruncated: false, wantOriginal: maxFileContentBytes},
		{name: "truncated, content-length carries full size", rawLen: maxFileContentBytes + 1, contentLength: 5 << 20, wantStored: maxFileContentBytes, wantTruncated: true, wantOriginal: 5 << 20},
		{name: "truncated, content-length absent → floored to bytes read", rawLen: maxFileContentBytes + 1, contentLength: -1, wantStored: maxFileContentBytes, wantTruncated: true, wantOriginal: maxFileContentBytes + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := make([]byte, tt.rawLen)
			stored, truncated, original := boundFileContent(context.Background(), raw, tt.contentLength, "x")
			assert.Len(t, stored, tt.wantStored)
			assert.Equal(t, tt.wantTruncated, truncated)
			assert.Equal(t, tt.wantOriginal, original)
			if truncated {
				assert.Positive(t, original, "truncated⟹original_bytes>0 (proto CEL)")
			}
		})
	}
}
