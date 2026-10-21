// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

const fcTestOrgID = "org-gl-1"

// fcMockGateway proxies Do to an httptest server, rewriting the hardcoded
// gitlab.com URL to the test server while preserving the escaped path+query
// (so the %2F-encoded file path survives the round-trip). It records the last
// raw request URI so a test can assert the encoding the connector produced.
type fcMockGateway struct {
	serverURL  string
	client     *http.Client
	lastRawURI string
}

func newFCMockGateway(serverURL string) *fcMockGateway {
	return &fcMockGateway{serverURL: serverURL, client: &http.Client{Timeout: 5 * time.Second}}
}

func (m *fcMockGateway) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	m.lastRawURI = req.URL.RequestURI()
	rewritten, err := http.NewRequest(req.Method, m.serverURL+req.URL.RequestURI(), req.Body)
	if err != nil {
		return nil, err
	}
	rewritten.Header = req.Header.Clone()
	return m.client.Do(rewritten)
}

func (m *fcMockGateway) DoJSON(context.Context, string, string, any, any) error {
	return fmt.Errorf("DoJSON not used in fetch_file_content tests")
}

func (m *fcMockGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return m
}
func (m *fcMockGateway) ForGroup(string) sdk.VendorGateway { return m }
func (m *fcMockGateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, fmt.Errorf("Execute not used in fetch_file_content tests")
}

// fcCountingGateway records how many Do calls it forwards, so a test can assert a
// budget-deferred / tombstone artifact issues ZERO vendor calls.
type fcCountingGateway struct {
	inner *fcMockGateway
	calls int
}

func (g *fcCountingGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	g.calls++
	return g.inner.Do(ctx, req)
}

func (g *fcCountingGateway) DoJSON(ctx context.Context, m, u string, rq, rs any) error {
	return g.inner.DoJSON(ctx, m, u, rq, rs)
}

func (g *fcCountingGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *fcCountingGateway) ForGroup(string) sdk.VendorGateway { return g }
func (g *fcCountingGateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, nil
}

// glContentServer serves RAW file bytes keyed by the DECODED file path (the
// segment between "/files/" and "/raw", %2F-decoded). A path in notFound returns
// 404. When sizeByPath has an entry it is set as X-Gitlab-Size (the full size the
// truncation path reads); otherwise no size header is sent.
func glContentServer(t *testing.T, bodyByPath map[string][]byte, notFound map[string]bool, sizeByPath map[string]int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// GitLab RFC-draft rate-limit headers (no X- prefix) so the budget refresh
		// has something to read.
		w.Header().Set("RateLimit-Remaining", "9000")
		w.Header().Set("RateLimit-Limit", "10000")
		w.Header().Set("RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))

		escaped := r.URL.EscapedPath()
		start := strings.Index(escaped, "/files/")
		end := strings.LastIndex(escaped, "/raw")
		if start < 0 || end < 0 || end <= start {
			http.Error(w, "unexpected path "+escaped, http.StatusInternalServerError)
			return
		}
		encodedPath := escaped[start+len("/files/") : end]
		repoPath, err := decodeGLPath(encodedPath)
		if err != nil {
			http.Error(w, "bad path: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if notFound[repoPath] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, ok := bodyByPath[repoPath]
		if !ok {
			http.Error(w, "no fixture for "+repoPath, http.StatusInternalServerError)
			return
		}
		if size, ok := sizeByPath[repoPath]; ok {
			w.Header().Set("X-Gitlab-Size", strconv.Itoa(size))
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// decodeGLPath reverses the %2F-slash encoding the connector applies. The %2F
// separators become "/" first, then any remaining escapes (e.g. %20) decode.
func decodeGLPath(encoded string) (string, error) {
	return url.PathUnescape(strings.ReplaceAll(encoded, "%2F", "/"))
}

func fcCaptureStagingWrites(t *testing.T, ctrl *gomock.Controller) (*sdkmocks.MockBlobStore, *[]fcStagingWrite, *[]string) {
	t.Helper()
	writes := &[]fcStagingWrite{}
	deletes := &[]string{}
	mock := sdkmocks.NewMockBlobStore(ctrl)
	mock.EXPECT().WriteWithOpts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, dst string, content []byte, opts *sdk.BlobWriteOpts) error {
			cp := append([]byte(nil), content...)
			*writes = append(*writes, fcStagingWrite{key: dst, content: cp, opts: opts})
			return nil
		}).AnyTimes()
	mock.EXPECT().Delete(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, dst string) error {
			*deletes = append(*deletes, dst)
			return nil
		}).AnyTimes()
	return mock, writes, deletes
}

type fcStagingWrite struct {
	key     string
	content []byte
	opts    *sdk.BlobWriteOpts
}

// fcOneFileRecord builds a single-file changed artifact whose one file needs a
// fetch. platform_repo_id carries the numeric GitLab project ID (fetch_repo_tree
// stamps it) — GitLab addresses files by project ID, not owner/repo.
func fcOneFileRecord() *workqueuev1.FileContentRecord {
	return &workqueuev1.FileContentRecord{
		RepositoryId:   "repo-1",
		RepoFullName:   "acme/widgets",
		PlatformRepoId: "12345",
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

func fcNewParams(gw sdk.VendorGateway, s3 sdk.BlobStore, budget sdk.BudgetObserver, sink sdk.DataSink) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config:        &sdk.ConnectorConfig{ConnectorID: "gitlab", OrganizationID: fcTestOrgID},
		Cursor:        &sdk.Cursor{},
		Sink:          sink,
		VendorGateway: gw,
		Host:          sdk.HostServices{}, // WorkQueue set per test
		BlobStore:     s3,
		BudgetLimiter: budget,
	}
}

func fcNewTestBudget(t *testing.T) *ratelimiter.BudgetLimiter {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return ratelimiter.NewBudgetLimiter(rdb, ratelimiter.GitLabAdapter{})
}

// TestGitlabFileContentURL_FullyEncodesPath pins the GitLab quirk: the file path
// is FULLY url-encoded (slashes → %2F, unlike GitHub's per-segment encoding), the
// ref is query-encoded, and the numeric project ID sits literally in the path.
func TestGitlabFileContentURL_FullyEncodesPath(t *testing.T) {
	got := gitlabFileContentURL(fcTestOrgID, "12345", ".claude/skills/a b/we#ird?.md", "sha#1")
	assert.Contains(t, got, "/projects/12345/repository/files/")
	assert.Contains(t, got, ".claude%2Fskills%2F", "slashes must be percent-encoded as %2F")
	assert.NotContains(t, got, "files/.claude/skills", "no literal slash may remain in the file path")
	// The ref survives in the query, uncorrupted by a '#'/'?' in the path.
	req, err := http.NewRequest(http.MethodGet, got, nil)
	require.NoError(t, err)
	assert.Equal(t, "sha#1", req.URL.Query().Get("ref"))
	assert.Empty(t, req.URL.Fragment, "no fragment leaked from a '#' in the path")
}

func TestFetchFileContentOp_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	body := []byte("# Deploy skill\nrun the thing\n")
	srv := glContentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": body}, nil, nil)
	gw := newFCMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{fcOneFileRecord()}}, nil)

	s3mock, writes, _ := fcCaptureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, fcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	emittedBefore := artifactOutcomeCount("emitted")
	fetchedBefore := filesFetchedCount()

	cursor, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Same(t, params.Cursor, cursor, "cursor must be pass-through")

	// A successful emit + one staged file bump the observability counters exactly once.
	assert.Equal(t, 1.0, artifactOutcomeCount("emitted")-emittedBefore, "one artifact emitted")
	assert.Equal(t, 1.0, filesFetchedCount()-fetchedBefore, "one file fetched and staged")

	// The connector produced a %2F-encoded file path with the project ID in the path.
	assert.Contains(t, gw.lastRawURI, "/projects/12345/repository/files/")
	assert.Contains(t, gw.lastRawURI, "%2F", "file path slashes must be %2F-encoded")

	// One staging write under harness-staging/, NOT a canonical namespace.
	require.Len(t, *writes, 1)
	w := (*writes)[0]
	assert.True(t, strings.HasPrefix(w.key, "harness-staging/org-gl-1/gitlab/repo-1/42/"), "key=%s", w.key)
	assert.Contains(t, w.key, ".claude/skills/deploy/SKILL.md")
	assert.NotContains(t, w.key, "harness/org-gl-1/", "must not be a canonical namespace")

	digest := sha256.Sum256(body)
	require.NotNil(t, w.opts)
	assert.Equal(t, base64.StdEncoding.EncodeToString(digest[:]), w.opts.ChecksumSHA256)

	recs := sink.Records()
	require.Len(t, recs, 1)
	// externalID spans repository_id # ide_type (CLAUDE_CODE=1) # source_path # generation.
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
	rec := fcOneFileRecord()
	rec.Files = append(rec.Files, &workqueuev1.FileContentRecord_FetchedFile{RelativePath: "scripts/run.sh", BlobSha: "blob-2"})
	srv := glContentServer(t,
		map[string][]byte{".claude/skills/deploy/SKILL.md": []byte("ok")},
		map[string]bool{".claude/skills/deploy/scripts/run.sh": true}, nil)
	gw := newFCMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock, _, deletes := fcCaptureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, fcNewTestBudget(t), sink)
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
	rec := fcOneFileRecord()
	for i := 0; i < 9; i++ {
		rec.Files = append(rec.Files, &workqueuev1.FileContentRecord_FetchedFile{RelativePath: fmt.Sprintf("f%d.md", i), BlobSha: "b"})
	}

	// Seed remaining just above the base floor but below floor+fileCount, so a small
	// artifact would admit but this whole skill is deferred.
	budget := fcNewTestBudget(t)
	headers := http.Header{}
	headers.Set("RateLimit-Remaining", strconv.Itoa(ratelimiter.GitLabCoreFloor+3))
	headers.Set("RateLimit-Limit", "10000")
	headers.Set("RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	require.NoError(t, budget.UpdateFromResponse(context.Background(), fcTestOrgID, headers))

	gw := &fcCountingGateway{inner: newFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, budget, sink)
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
	rec := fcOneFileRecord()
	rec.Files = nil                               // nothing to fetch
	rec.RemovedPaths = []string{"scripts/old.sh"} // only a helper removal to apply

	// Deplete the bucket to zero: a record WITH files would defer here.
	budget := fcNewTestBudget(t)
	headers := http.Header{}
	headers.Set("RateLimit-Remaining", "0")
	headers.Set("RateLimit-Limit", "10000")
	headers.Set("RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	require.NoError(t, budget.UpdateFromResponse(context.Background(), fcTestOrgID, headers))

	gw := &fcCountingGateway{inner: newFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	// No WriteWithOpts / Delete expectation — a removal-only record stages nothing.
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, budget, sink)
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
	srv := glContentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": body}, nil, nil)
	gw := newFCMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{fcOneFileRecord()}}, nil)

	s3mock, _, deletes := fcCaptureStagingWrites(t, ctrl)
	params := fcNewParams(gw, s3mock, fcNewTestBudget(t), failingSink{})
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
	// X-Gitlab-Size carries the full untruncated size.
	srv := glContentServer(t,
		map[string][]byte{".claude/skills/deploy/SKILL.md": big}, nil,
		map[string]int{".claude/skills/deploy/SKILL.md": len(big)})
	gw := newFCMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{fcOneFileRecord()}}, nil)

	s3mock, writes, _ := fcCaptureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, fcNewTestBudget(t), sink)
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
	assert.Equal(t, int64(len(big)), emitted.GetFiles()[0].GetOriginalBytes(), "original size from X-Gitlab-Size")
}

func TestFetchFileContentOp_TombstoneReEmittedNoFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := fcOneFileRecord()
	rec.IsTombstone = true
	rec.Files = nil

	gw := &fcCountingGateway{inner: newFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, fcNewTestBudget(t), sink)
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
		Config: &sdk.ConnectorConfig{ConnectorID: "gitlab", OrganizationID: fcTestOrgID},
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

	gw := &fcCountingGateway{inner: newFCMockGateway("http://unused")}
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, fcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	cursor, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.Error(t, err, "a work-list gRPC error is a hard failure so asynq retries")
	assert.Nil(t, cursor, "hard failure returns a nil cursor, not the pass-through cursor")
	assert.Zero(t, gw.calls, "no vendor calls on a work-list failure")
	assert.Empty(t, sink.Records())
}

func TestFetchFileContentOp_EmptyPlatformRepoIDDefers(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := fcOneFileRecord()
	rec.PlatformRepoId = "" // no project ID resolved — defer rather than build a bad URL

	gw := &fcCountingGateway{inner: newFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, fcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "empty project ID defers before any fetch")
	assert.Empty(t, sink.Records())
}

func TestFetchFileContentOp_PathEscapeDefers(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := &workqueuev1.FileContentRecord{
		RepositoryId:   "repo-1",
		RepoFullName:   "acme/widgets",
		PlatformRepoId: "12345",
		SourcePath:     "CLAUDE.md",
		Name:           "claude-md",
		Branch:         "main",
		Ref:            "commit-sha-abc",
		Generation:     7,
		Files: []*workqueuev1.FileContentRecord_FetchedFile{
			{RelativePath: "../escape.md", BlobSha: "blob-x", IsPrimary: true},
		},
	}

	gw := &fcCountingGateway{inner: newFCMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(gw, s3mock, fcNewTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "the escape guard fires before any fetch")
	assert.Empty(t, sink.Records())
}

// TestBoundFileContent pins the truncation accounting, including the absent /
// non-credible size floor (original_bytes must stay > 0 for the proto's
// truncated⟹original_bytes>0 invariant).
func TestBoundFileContent(t *testing.T) {
	tests := []struct {
		name          string
		rawLen        int
		reportedSize  int64
		wantStored    int
		wantTruncated bool
		wantOriginal  int64
	}{
		{name: "whole file under cap", rawLen: 100, reportedSize: 100, wantStored: 100, wantTruncated: false, wantOriginal: 100},
		{name: "exactly at cap", rawLen: maxFileContentBytes, reportedSize: maxFileContentBytes, wantStored: maxFileContentBytes, wantTruncated: false, wantOriginal: maxFileContentBytes},
		{name: "truncated, size header carries full size", rawLen: maxFileContentBytes + 1, reportedSize: 5 << 20, wantStored: maxFileContentBytes, wantTruncated: true, wantOriginal: 5 << 20},
		{name: "truncated, size header absent → floored to bytes read", rawLen: maxFileContentBytes + 1, reportedSize: -1, wantStored: maxFileContentBytes, wantTruncated: true, wantOriginal: maxFileContentBytes + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := make([]byte, tt.rawLen)
			stored, truncated, original := boundFileContent(context.Background(), raw, tt.reportedSize, "x")
			assert.Len(t, stored, tt.wantStored)
			assert.Equal(t, tt.wantTruncated, truncated)
			assert.Equal(t, tt.wantOriginal, original)
			if truncated {
				assert.Positive(t, original, "truncated⟹original_bytes>0 (proto CEL)")
			}
		})
	}
}
