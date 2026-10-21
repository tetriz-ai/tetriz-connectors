// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

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

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
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

const testOrgID = "org-1"

// contentServer serves RAW file bytes (the Accept: application/vnd.github.raw route)
// keyed by the decoded file path in the URL. A path in notFound returns 404 (the
// whole-artifact defer trigger). net/http auto-sets Content-Length from the written
// body, which the fetch reads as the original (untruncated) size.
func contentServer(t *testing.T, bodyByPath map[string][]byte, notFound map[string]bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// GitHub rate-limit headers so the budget refresh has something to read.
		w.Header().Set("X-RateLimit-Remaining", "4000")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
		w.Header().Set("X-RateLimit-Resource", "core")

		idx := strings.Index(r.URL.Path, "/contents/")
		if idx < 0 {
			http.Error(w, "unexpected path", http.StatusInternalServerError)
			return
		}
		repoPath := r.URL.Path[idx+len("/contents/"):]
		if notFound[repoPath] {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Not Found"}`))
			return
		}
		body, ok := bodyByPath[repoPath]
		if !ok {
			http.Error(w, "no fixture for "+repoPath, http.StatusInternalServerError)
			return
		}
		// Set Content-Length explicitly (as GitHub does) so it survives as the full size
		// even for a >1 MiB body — otherwise net/http streams large bodies chunked with no
		// Content-Length, and the fetch's original_bytes would fall back to the floor.
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// countingGateway records how many Do calls it forwards, so a test can assert a
// budget-deferred artifact issues ZERO vendor calls.
type countingGateway struct {
	inner *diffMockGateway
	calls int
}

func (g *countingGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	g.calls++
	return g.inner.Do(ctx, req)
}

func (g *countingGateway) DoJSON(ctx context.Context, m, u string, rq, rs any) error {
	return g.inner.DoJSON(ctx, m, u, rq, rs)
}

func (g *countingGateway) WithRateLimits(c []sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *countingGateway) ForGroup(s string) sdk.VendorGateway { return g }
func (g *countingGateway) Execute(ctx context.Context, m proto.Message) (proto.Message, error) {
	return nil, nil
}

// stagingWrite records one WriteWithOpts so a test can assert the staging key and
// its checksum.
type stagingWrite struct {
	key     string
	content []byte
	opts    *sdk.BlobWriteOpts
}

// captureStagingWrites returns a generated s3 mock that records every WriteWithOpts and
// every Delete, so a test can assert both the staged keys and the on-abort staging cleanup.
func captureStagingWrites(t *testing.T, ctrl *gomock.Controller) (*sdkmocks.MockBlobStore, *[]stagingWrite, *[]string) {
	t.Helper()
	writes := &[]stagingWrite{}
	deletes := &[]string{}
	mock := sdkmocks.NewMockBlobStore(ctrl)
	mock.EXPECT().WriteWithOpts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, dst string, content []byte, opts *sdk.BlobWriteOpts) error {
			cp := append([]byte(nil), content...)
			*writes = append(*writes, stagingWrite{key: dst, content: cp, opts: opts})
			return nil
		}).AnyTimes()
	mock.EXPECT().Delete(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, dst string) error {
			*deletes = append(*deletes, dst)
			return nil
		}).AnyTimes()
	return mock, writes, deletes
}

// oneFileRecord builds a single-file changed artifact whose one file needs a fetch.
func oneFileRecord() *workqueuev1.FileContentRecord {
	return &workqueuev1.FileContentRecord{
		RepositoryId: "repo-1",
		RepoFullName: "acme/widgets",
		SourcePath:   ".claude/skills/deploy/SKILL.md",
		IdeType:      commonv1.IdeType_IDE_TYPE_CLAUDE_CODE,
		Name:         "deploy",
		Branch:       "main",
		Ref:          "commit-sha-abc",
		Generation:   42,
		Files: []*workqueuev1.FileContentRecord_FetchedFile{
			{RelativePath: "SKILL.md", BlobSha: "blob-1", IsPrimary: true},
		},
	}
}

func newParams(gw sdk.VendorGateway, s3 sdk.BlobStore, budget sdk.BudgetObserver, sink sdk.DataSink) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config:        &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID},
		Cursor:        &sdk.Cursor{},
		Sink:          sink,
		VendorGateway: gw,
		Host:          sdk.HostServices{}, // WorkQueue set per test
		BlobStore:     s3,
		BudgetLimiter: budget,
	}
}

func TestFetchFileContentOp_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	body := []byte("# Deploy skill\nrun the thing\n")
	srv := contentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": body}, nil)
	gw := newDiffMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{oneFileRecord()}}, nil)

	s3mock, writes, _ := captureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	emittedBefore := artifactOutcomeCount("emitted")
	fetchedBefore := filesFetchedCount()

	cursor, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Same(t, params.Cursor, cursor, "cursor must be pass-through")

	// A successful emit + one staged file bump the observability counters exactly once.
	assert.Equal(t, 1.0, artifactOutcomeCount("emitted")-emittedBefore, "one artifact emitted")
	assert.Equal(t, 1.0, filesFetchedCount()-fetchedBefore, "one file fetched and staged")

	// One staging write under harness-staging/, NOT the canonical S3KeyFor namespace.
	require.Len(t, *writes, 1)
	w := (*writes)[0]
	assert.True(t, strings.HasPrefix(w.key, "harness-staging/org-1/github/repo-1/42/"), "key=%s", w.key)
	assert.Contains(t, w.key, ".claude/skills/deploy/SKILL.md")
	assert.NotContains(t, w.key, "harness/org-1/", "must not be the canonical S3KeyFor namespace")

	// Checksum is base64 of the raw sha256 of the STORED bytes.
	digest := sha256.Sum256(body)
	require.NotNil(t, w.opts)
	assert.Equal(t, base64.StdEncoding.EncodeToString(digest[:]), w.opts.ChecksumSHA256)

	// Exactly one record emitted, files filled, RPC-supplied name/branch re-emitted.
	recs := sink.Records()
	require.Len(t, recs, 1)
	// externalID spans repository_id # ide_type (CLAUDE_CODE=1) # source_path # generation —
	// ide_type is part of the harness identity, so it must be in the dedup key.
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
	rec := oneFileRecord()
	rec.Files = append(rec.Files, &workqueuev1.FileContentRecord_FetchedFile{RelativePath: "scripts/run.sh", BlobSha: "blob-2"})
	// First file present, sibling 404s → whole artifact deferred (no record at all).
	srv := contentServer(t,
		map[string][]byte{".claude/skills/deploy/SKILL.md": []byte("ok")},
		map[string]bool{".claude/skills/deploy/scripts/run.sh": true})
	gw := newDiffMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock, _, deletes := captureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
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
	rec := oneFileRecord()
	// A ten-file skill: floor = GitHubCoreFloor + 10.
	for i := 0; i < 9; i++ {
		rec.Files = append(rec.Files, &workqueuev1.FileContentRecord_FetchedFile{RelativePath: fmt.Sprintf("f%d.md", i), BlobSha: "b"})
	}

	// Seed remaining just above the base floor but below floor+fileCount, so a small
	// artifact would admit but this whole skill is deferred.
	budget := newTestBudget(t)
	headers := http.Header{}
	headers.Set("X-RateLimit-Remaining", strconv.Itoa(ratelimiter.GitHubCoreFloor+3))
	headers.Set("X-RateLimit-Limit", "5000")
	headers.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	headers.Set("X-RateLimit-Resource", "core")
	require.NoError(t, budget.UpdateFromResponse(context.Background(), testOrgID, headers))

	gw := &countingGateway{inner: newDiffMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	// No WriteWithOpts expectation set — gomock fails on any unexpected staging write.
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, budget, sink)
	params.Host.WorkQueue = analytics

	deferredBefore := artifactOutcomeCount("deferred_budget")

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "a budget-deferred artifact must issue zero vendor calls")
	assert.Empty(t, sink.Records())
	// The budget-floor defer counts as a deferred_budget outcome.
	assert.Equal(t, 1.0, artifactOutcomeCount("deferred_budget")-deferredBefore, "one deferred_budget outcome")
}

// TestFetchFileContentOp_RemovalOnlyBypassesBudget: a removal-only record (helper paths
// deleted, no files to fetch) makes zero vendor requests, so it must emit even when the
// core budget is fully depleted below the floor — otherwise the helper deletion is
// stranded indefinitely behind a budget it never needs.
func TestFetchFileContentOp_RemovalOnlyBypassesBudget(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := oneFileRecord()
	rec.Files = nil                               // nothing to fetch
	rec.RemovedPaths = []string{"scripts/old.sh"} // only a helper removal to apply

	// Deplete the bucket to zero: a record WITH files would defer here.
	budget := newTestBudget(t)
	headers := http.Header{}
	headers.Set("X-RateLimit-Remaining", "0")
	headers.Set("X-RateLimit-Limit", "5000")
	headers.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	headers.Set("X-RateLimit-Resource", "core")
	require.NoError(t, budget.UpdateFromResponse(context.Background(), testOrgID, headers))

	gw := &countingGateway{inner: newDiffMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	// No WriteWithOpts / Delete expectation — a removal-only record stages nothing.
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, budget, sink)
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
	srv := contentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": body}, nil)
	gw := newDiffMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{oneFileRecord()}}, nil)

	s3mock, _, deletes := captureStagingWrites(t, ctrl)
	params := newParams(gw, s3mock, newTestBudget(t), failingSink{})
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
	srv := contentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": big}, nil)
	gw := newDiffMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{oneFileRecord()}}, nil)

	s3mock, writes, _ := captureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
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
	assert.Equal(t, int64(len(big)), emitted.GetFiles()[0].GetOriginalBytes(), "original (untruncated) size reported")
}

func TestFetchFileContentOp_TombstoneReEmittedNoFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := oneFileRecord()
	rec.IsTombstone = true
	rec.Files = nil

	gw := &countingGateway{inner: newDiffMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	// No WriteWithOpts expectation set — gomock fails if a tombstone stages a body.
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
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
		Config: &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID},
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

// TestGitHubContentsURL_EscapesPathAndRef pins FIX B: a path/ref with '#', '?', or a
// space must be escaped so http.NewRequest cannot reparse the tail as fragment/query and
// drop the ref.
func TestGitHubContentsURL_EscapesPathAndRef(t *testing.T) {
	got := githubContentsURL("org-1", "acme", "widgets", ".claude/skills/a b/we#ird?.md", "sha#1")
	req, err := http.NewRequest(http.MethodGet, got, nil)
	require.NoError(t, err)
	// The '#'/'?'/space survive as literal path bytes (no fragment, correct segments)…
	assert.Equal(t, "/repos/acme/widgets/contents/.claude/skills/a b/we#ird?.md", req.URL.Path)
	assert.Empty(t, req.URL.Fragment, "no fragment leaked from a '#' in the path")
	// …and the ref is intact in the query, not corrupted by the path's '?'.
	assert.Equal(t, "sha#1", req.URL.Query().Get("ref"))
}

// TestFetchFileContentOp_SpecialCharPathPreservesRef drives Execute end-to-end with a
// path containing '#'/'?'/space and asserts the server received the decoded path and the
// intact ref — the escaping fix seen through a real request.
func TestFetchFileContentOp_SpecialCharPathPreservesRef(t *testing.T) {
	ctrl := gomock.NewController(t)
	rec := oneFileRecord()
	rec.SourcePath = "CLAUDE.md"
	rec.Files = []*workqueuev1.FileContentRecord_FetchedFile{{RelativePath: "we#ird? name.md", BlobSha: "b", IsPrimary: true}}

	var gotPath, gotRef string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotRef = r.URL.Path, r.URL.Query().Get("ref")
		_, _ = w.Write([]byte("body"))
	}))
	t.Cleanup(srv.Close)
	gw := newDiffMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	s3mock, _, _ := captureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Equal(t, "/repos/acme/widgets/contents/we#ird? name.md", gotPath, "server saw the exact decoded path")
	assert.Equal(t, "commit-sha-abc", gotRef, "ref survived a '#'/'?' in the path")
	require.Len(t, sink.Records(), 1, "the file was fetched and emitted")
}

// TestFetchFileContentOp_UnexpectedStatusDefers pins that a non-2xx, non-404 status
// (e.g. 403) defers the whole artifact — no staging write, no record, run succeeds. The
// raw-media-type route has no blob fallback; there is one bounded call per file.
func TestFetchFileContentOp_UnexpectedStatusDefers(t *testing.T) {
	ctrl := gomock.NewController(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"forbidden"}`))
	}))
	t.Cleanup(srv.Close)
	gw := newDiffMockGateway(srv.URL)

	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{oneFileRecord()}}, nil)

	// No WriteWithOpts expectation — gomock fails if a body is staged.
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err, "an unexpected status defers the artifact, never fails the run")
	assert.Empty(t, sink.Records())
}

// TestBoundFileContent pins FIX C's truncation accounting, including the absent /
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

// TestFetchFileContentOp_ExternalIDIncludesIdeType pins that two artifacts identical
// but for ide_type must not share a downstream dedup key.
func TestFetchFileContentOp_ExternalIDIncludesIdeType(t *testing.T) {
	ctrl := gomock.NewController(t)
	claude := oneFileRecord()
	cursor := oneFileRecord()
	cursor.IdeType = commonv1.IdeType_IDE_TYPE_CURSOR

	srv := contentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": []byte("x")}, nil)
	gw := newDiffMockGateway(srv.URL)
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{claude, cursor}}, nil)

	s3mock, _, _ := captureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	recs := sink.Records()
	require.Len(t, recs, 2)
	assert.NotEqual(t, recs[0].ExternalID, recs[1].ExternalID, "ide_type differentiates the dedup key")
	assert.Equal(t, "repo-1#1#.claude/skills/deploy/SKILL.md#42", recs[0].ExternalID)
	assert.Equal(t, "repo-1#2#.claude/skills/deploy/SKILL.md#42", recs[1].ExternalID)
}

func TestFetchFileContentOp_WorkListRPCErrorIsHardFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("host down"))

	gw := &countingGateway{inner: newDiffMockGateway("http://unused")}
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	cursor, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.Error(t, err, "a work-list gRPC error is a hard failure so asynq retries")
	assert.Nil(t, cursor, "hard failure returns a nil cursor, not the pass-through cursor")
	assert.Zero(t, gw.calls, "no vendor calls on a work-list failure")
	assert.Empty(t, sink.Records())
}

func TestFetchFileContentOp_PathEscapeDefers(t *testing.T) {
	ctrl := gomock.NewController(t)
	// source_path in the repo root (sourceDir="."), a relative_path climbing above it —
	// path.Join keeps the ".." so the escape guard fires.
	rec := &workqueuev1.FileContentRecord{
		RepositoryId: "repo-1",
		RepoFullName: "acme/widgets",
		SourcePath:   "CLAUDE.md",
		Name:         "claude-md",
		Branch:       "main",
		Ref:          "commit-sha-abc",
		Generation:   7,
		Files: []*workqueuev1.FileContentRecord_FetchedFile{
			{RelativePath: "../escape.md", BlobSha: "blob-x", IsPrimary: true},
		},
	}

	gw := &countingGateway{inner: newDiffMockGateway("http://unused")}
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	analytics.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{rec}}, nil)

	// No WriteWithOpts expectation — gomock fails if a traversal key is written.
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(gw, s3mock, newTestBudget(t), sink)
	params.Host.WorkQueue = analytics

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Zero(t, gw.calls, "the escape guard fires before any fetch")
	assert.Empty(t, sink.Records(), "no record emitted for a traversal-path artifact")
}
