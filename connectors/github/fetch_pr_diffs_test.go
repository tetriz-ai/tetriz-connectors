// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/gatewaytest"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// startMiniRedis starts an in-memory Redis for the lifetime of t and returns
// the address. Cleaned up automatically via miniredis.RunT.
func startMiniRedis(t *testing.T) string {
	t.Helper()
	mr := miniredis.RunT(t)
	return mr.Addr()
}

// --- Mock VendorGateway that proxies Do to an httptest server ---

type diffMockGateway struct {
	serverURL string
	client    *http.Client
}

func newDiffMockGateway(serverURL string) *diffMockGateway {
	return &diffMockGateway{serverURL: serverURL, client: &http.Client{Timeout: 5 * time.Second}}
}

func (m *diffMockGateway) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	// Rewrite the api.github.com URL to point at the httptest server. Use
	// RequestURI() (path + query) so paginated callers can pass ?page= etc.
	rewritten, err := http.NewRequest(req.Method, m.serverURL+req.URL.RequestURI(), req.Body)
	if err != nil {
		return nil, err
	}
	rewritten.Header = req.Header.Clone()
	return m.client.Do(rewritten)
}

func (m *diffMockGateway) DoJSON(_ context.Context, _, _ string, _, _ any) error {
	return fmt.Errorf("DoJSON not used in fetch_pr_diffs tests")
}

func (m *diffMockGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return m
}

func (m *diffMockGateway) ForGroup(_ string) sdk.VendorGateway { return m }

func (m *diffMockGateway) Execute(_ context.Context, _ proto.Message) (proto.Message, error) {
	return nil, fmt.Errorf("Execute not used in fetch_pr_diffs tests")
}

// --- Helpers ---

// miniredis would be overkill; use an in-memory redis.Cmdable stub via
// miniredis package. If not available, skip budget exhaustion with a nil
// client path. We fall back to a lightweight fake Cmdable below.

// diffHandlerOpts configures the test HTTP server response per PR.
//
// filesHandler is optional; when set, requests to /repos/{owner}/{repo}/pulls/{n}/files
// dispatch there with the page number parsed from the `page` query arg. When
// unset, /files paths return 404. The primary path (/repos/{owner}/{repo}/pulls/{n})
// always uses statusByPR / bodyByPR.
type diffHandlerOpts struct {
	statusByPR   map[int]int
	bodyByPR     map[int][]byte
	headers      http.Header
	filesHandler func(prNum, page int) (status int, body []byte, headers http.Header)
}

func newDiffServer(t *testing.T, opts diffHandlerOpts) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// path: /repos/{owner}/{repo}/pulls/{n}            (primary diff)
		//   or  /repos/{owner}/{repo}/pulls/{n}/files      (per-file fallback)
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		// assert (not require) in an http handler goroutine; guard the parts[4]
		// index so a short path fails the assertion instead of panicking into a 500.
		if !assert.GreaterOrEqual(t, len(parts), 5, "unexpected path %q", r.URL.Path) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		prNum, _ := strconv.Atoi(parts[4])

		// /files sub-path → per-file fallback handler
		if len(parts) >= 6 && parts[5] == "files" {
			if opts.filesHandler == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			page := 1
			if p := r.URL.Query().Get("page"); p != "" {
				page, _ = strconv.Atoi(p)
			}
			status, body, hdr := opts.filesHandler(prNum, page)
			for k, vs := range hdr {
				for _, v := range vs {
					w.Header().Add(k, v)
				}
			}
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			if len(body) > 0 {
				_, _ = w.Write(body)
			}
			return
		}

		// Primary diff path.
		assert.Len(t, parts, 5, "unexpected path %q", r.URL.Path)
		status := opts.statusByPR[prNum]
		if status == 0 {
			status = http.StatusOK
		}
		for k, vs := range opts.headers {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(status)
		if body, ok := opts.bodyByPR[prNum]; ok {
			_, _ = w.Write(body)
		}
	}))
}

func defaultRateHeaders(remaining int) http.Header {
	h := http.Header{}
	h.Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	h.Set("X-RateLimit-Limit", "5000")
	h.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	return h
}

// newTestBudget returns a BudgetLimiter backed by a brand-new miniredis.
// Falls back to nil if miniredis isn't available (build tag).
func newTestBudget(t *testing.T) *ratelimiter.BudgetLimiter {
	t.Helper()
	// We use the real redis.NewClient against a local miniredis if set.
	// For unit tests, use a fake in-memory Cmdable via go-redis's miniredis path.
	mr := startMiniRedis(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr})
	t.Cleanup(func() { _ = rdb.Close() })
	return ratelimiter.NewBudgetLimiter(rdb, ratelimiter.GitHubAdapter{})
}

// --- Tests ---

func TestFetchPRDiffsOp_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	s3 := sdkmocks.NewMockBlobStore(ctrl)

	sample := []byte("diff --git a/foo b/foo\n+x\n")
	srv := newDiffServer(t, diffHandlerOpts{
		bodyByPR: map[int][]byte{42: sample},
		headers:  defaultRateHeaders(4999),
	})
	defer srv.Close()

	gw := newDiffMockGateway(srv.URL)
	budget := newTestBudget(t)

	analytics.EXPECT().
		ListPRsPendingDiffFetch(gomock.Any(), gomock.Any()).
		Return(&workqueuev1.ListPRsPendingDiffFetchResponse{
			Prs: []*workqueuev1.PRDiffFetchCandidate{
				{
					PullRequestId:  "pr-uuid-1",
					PlatformRepoId: "gh-repo-999",
					RepoFullName:   "acme/widgets",
					PrNumber:       42,
				},
			},
		}, nil)

	wantKey := "pr-diffs/org-1/github/gh-repo-999/42.patch"
	s3.EXPECT().
		Write(gomock.Any(), wantKey, sample, "").
		Return(nil)

	var recorded *workqueuev1.UpdatePRDiffStatusRequest
	analytics.EXPECT().
		UpdatePRDiffStatus(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workqueuev1.UpdatePRDiffStatusRequest) (*workqueuev1.UpdatePRDiffStatusResponse, error) {
			recorded = req
			return &workqueuev1.UpdatePRDiffStatusResponse{}, nil
		})

	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-1",
		},
		Sink:          testkit.NewChannelSink(),
		VendorGateway: gw,
		Host:          sdk.HostServices{WorkQueue: analytics},
		BlobStore:     s3,
		BudgetLimiter: budget,
	}

	op := &FetchPRDiffsOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Nil(t, cursor)

	require.NotNil(t, recorded)
	assert.Equal(t, "pr-uuid-1", recorded.PullRequestId)
	assert.Equal(t, wantKey, recorded.DiffS3Key)
}

func TestFetchPRDiffsOp_NotFoundSentinel(t *testing.T) {
	ctrl := gomock.NewController(t)
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	s3 := sdkmocks.NewMockBlobStore(ctrl)

	srv := newDiffServer(t, diffHandlerOpts{
		statusByPR: map[int]int{7: http.StatusNotFound},
		headers:    defaultRateHeaders(4999),
	})
	defer srv.Close()

	analytics.EXPECT().
		ListPRsPendingDiffFetch(gomock.Any(), gomock.Any()).
		Return(&workqueuev1.ListPRsPendingDiffFetchResponse{
			Prs: []*workqueuev1.PRDiffFetchCandidate{
				{PullRequestId: "pr-2", PlatformRepoId: "gh-repo-7", RepoFullName: "w/r", PrNumber: 7},
			},
		}, nil)

	// S3 must NOT be written on a sentinel — no EXPECT on Write; ctrl.Finish
	// will fail if any unexpected calls happen.
	analytics.EXPECT().
		UpdatePRDiffStatus(gomock.Any(), matchSentinel("pr-2", sentinelNotAvailable)).
		Return(&workqueuev1.UpdatePRDiffStatusResponse{}, nil)

	params := &sdk.ExecutionParams{
		Config:        &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "o"},
		Sink:          testkit.NewChannelSink(),
		VendorGateway: newDiffMockGateway(srv.URL),
		Host:          sdk.HostServices{WorkQueue: analytics},
		BlobStore:     s3,
		BudgetLimiter: newTestBudget(t),
	}

	_, err := (&FetchPRDiffsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
}

func TestFetchPRDiffsOp_TooLargeSentinel_422(t *testing.T) {
	ctrl := gomock.NewController(t)
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	s3 := sdkmocks.NewMockBlobStore(ctrl)

	srv := newDiffServer(t, diffHandlerOpts{
		statusByPR: map[int]int{7: http.StatusUnprocessableEntity},
		headers:    defaultRateHeaders(4999),
	})
	defer srv.Close()

	analytics.EXPECT().ListPRsPendingDiffFetch(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPRsPendingDiffFetchResponse{
			Prs: []*workqueuev1.PRDiffFetchCandidate{{PullRequestId: "pr-3", PlatformRepoId: "r3", RepoFullName: "w/r", PrNumber: 7}},
		}, nil)
	analytics.EXPECT().UpdatePRDiffStatus(gomock.Any(), matchSentinel("pr-3", sentinelTooLarge)).
		Return(&workqueuev1.UpdatePRDiffStatusResponse{}, nil)

	params := &sdk.ExecutionParams{
		Config:        &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "o"},
		Sink:          testkit.NewChannelSink(),
		VendorGateway: newDiffMockGateway(srv.URL),
		Host:          sdk.HostServices{WorkQueue: analytics},
		BlobStore:     s3,
		BudgetLimiter: newTestBudget(t),
	}
	_, err := (&FetchPRDiffsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
}

func TestFetchPRDiffsOp_OversizeBodySentinel(t *testing.T) {
	ctrl := gomock.NewController(t)
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	s3 := sdkmocks.NewMockBlobStore(ctrl)

	huge := make([]byte, maxDiffSizeBytes+10)
	for i := range huge {
		huge[i] = 'x'
	}
	srv := newDiffServer(t, diffHandlerOpts{
		bodyByPR: map[int][]byte{9: huge},
		headers:  defaultRateHeaders(4999),
	})
	defer srv.Close()

	analytics.EXPECT().ListPRsPendingDiffFetch(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPRsPendingDiffFetchResponse{
			Prs: []*workqueuev1.PRDiffFetchCandidate{{PullRequestId: "pr-4", PlatformRepoId: "r4", RepoFullName: "w/r", PrNumber: 9}},
		}, nil)
	analytics.EXPECT().UpdatePRDiffStatus(gomock.Any(), matchSentinel("pr-4", sentinelTooLarge)).
		Return(&workqueuev1.UpdatePRDiffStatusResponse{}, nil)

	params := &sdk.ExecutionParams{
		Config:        &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "o"},
		Sink:          testkit.NewChannelSink(),
		VendorGateway: newDiffMockGateway(srv.URL),
		Host:          sdk.HostServices{WorkQueue: analytics},
		BlobStore:     s3,
		BudgetLimiter: newTestBudget(t),
	}
	_, err := (&FetchPRDiffsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
}

func TestFetchPRDiffsOp_BudgetExhausted_EarlyReturn(t *testing.T) {
	ctrl := gomock.NewController(t)
	analytics := sdkmocks.NewMockWorkQueue(ctrl)
	s3 := sdkmocks.NewMockBlobStore(ctrl)

	// Seed the budget below the core floor so Allow returns false.
	budget := newTestBudget(t)
	exhaustedHeaders := http.Header{}
	exhaustedHeaders.Set("X-RateLimit-Remaining", "10")
	exhaustedHeaders.Set("X-RateLimit-Limit", "5000")
	exhaustedHeaders.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10))
	exhaustedHeaders.Set("X-RateLimit-Resource", "core")
	require.NoError(t, budget.UpdateFromResponse(context.Background(), "o", exhaustedHeaders))

	analytics.EXPECT().ListPRsPendingDiffFetch(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPRsPendingDiffFetchResponse{
			Prs: []*workqueuev1.PRDiffFetchCandidate{{PullRequestId: "pr-5", PlatformRepoId: "r5", RepoFullName: "w/r", PrNumber: 1}},
		}, nil)
	// No UpdatePRDiffStatus / S3 Write expected — budget blocks before the HTTP call.

	params := &sdk.ExecutionParams{
		Config:        &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "o"},
		Sink:          testkit.NewChannelSink(),
		VendorGateway: newDiffMockGateway("http://unused"),
		Host:          sdk.HostServices{WorkQueue: analytics},
		BlobStore:     s3,
		BudgetLimiter: budget,
	}
	_, err := (&FetchPRDiffsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
}

// TestFetchPRDiffsOp_PerFileFallback exercises the 406 → /files fallback flow
// in processOne and fetchGitHubPRDiffViaFiles. Each case stubs the primary
// /pulls/{n} endpoint to return 406 (forcing the fallback) and varies the
// /files endpoint's behavior to cover every outcome the fallback can produce.
func TestFetchPRDiffsOp_PerFileFallback(t *testing.T) {
	// Single page of /files JSON — 2 files with non-empty patches.
	singlePageBody := []byte(`[
		{"filename":"src/big_file.go","status":"modified","patch":"@@ -1,3 +1,5 @@\n+added1\n+added2"},
		{"filename":"src/another.go","status":"added","patch":"@@ -0,0 +1,2 @@\n+new1\n+new2"}
	]`)

	// Two single-file pages for the pagination case.
	paginatedPage1 := []byte(`[
		{"filename":"a.go","status":"modified","patch":"@@ -1 +1 @@\n+page1_file1"}
	]`)
	paginatedPage2 := []byte(`[
		{"filename":"b.go","status":"modified","patch":"@@ -1 +1 @@\n+page2_file1"}
	]`)

	// 100 text files; 11 with empty patch (>10% threshold, >=10 absolute floor).
	// Should sentinel TOO_LARGE.
	var truncSB strings.Builder
	truncSB.WriteString("[")
	for i := range 100 {
		if i > 0 {
			truncSB.WriteString(",")
		}
		// First 11 files: empty patch WITH non-zero additions — simulates
		// GitHub's per-file >300 KB truncation (real content loss). The rest
		// have a valid patch.
		patch := `"@@ -1 +1 @@\n+line"`
		additions := 1
		if i < 11 {
			patch = `""`
			additions = 500 // non-zero — content was lost to GitHub-side truncation
		}
		fmt.Fprintf(&truncSB, `{"filename":"src/file%d.go","status":"modified","patch":%s,"additions":%d,"deletions":0}`, i, patch, additions)
	}
	truncSB.WriteString("]")
	truncatedPageBody := []byte(truncSB.String())

	// 100 text files where 30 have empty patch but ZERO additions+deletions —
	// renames / mode-only changes / submodule pointer updates. None of these
	// represent content loss; the threshold should NOT fire.
	var noContentSB strings.Builder
	noContentSB.WriteString("[")
	for i := range 100 {
		if i > 0 {
			noContentSB.WriteString(",")
		}
		patch := `"@@ -1 +1 @@\n+line"`
		additions := 1
		if i < 30 {
			patch = `""`
			additions = 0 // rename / mode-only / submodule — nothing to diff
		}
		fmt.Fprintf(&noContentSB, `{"filename":"src/file%d.go","status":"modified","patch":%s,"additions":%d,"deletions":0}`, i, patch, additions)
	}
	noContentSB.WriteString("]")
	noContentDeltaPageBody := []byte(noContentSB.String())

	// 20 binary files (PNG, JPG) and 5 text files. GitHub returns empty
	// `patch` with `additions=0, deletions=0` for binaries; the no-content-
	// delta guard excludes them from the truncation count without needing
	// an extension list.
	binaryPageBody := []byte(`[
		{"filename":"assets/a.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/b.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/c.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/d.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/e.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/f.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/g.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/h.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/i.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/j.png","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/k.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/l.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/m.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/n.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/o.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/p.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/q.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/r.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/s.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"assets/t.jpg","status":"added","patch":"","additions":0,"deletions":0},
		{"filename":"src/a.go","status":"added","patch":"@@ -0,0 +1 @@\n+a","additions":1,"deletions":0},
		{"filename":"src/b.go","status":"added","patch":"@@ -0,0 +1 @@\n+b","additions":1,"deletions":0},
		{"filename":"src/c.go","status":"added","patch":"@@ -0,0 +1 @@\n+c","additions":1,"deletions":0},
		{"filename":"src/d.go","status":"added","patch":"@@ -0,0 +1 @@\n+d","additions":1,"deletions":0},
		{"filename":"src/e.go","status":"added","patch":"@@ -0,0 +1 @@\n+e","additions":1,"deletions":0}
	]`)

	// 20 files × ~600 KB each ≈ 12 MB reconstructed — well above the primary
	// 10 MB cap, well under the 50 MB fallback cap. Each `+x\n` in the parsed
	// patch is 3 bytes; the JSON wire form escapes the newline as the
	// two-byte literal `\n`, and the parser collapses that back to a real
	// newline.
	patchBody := strings.Repeat(`+x\n`, 200000)
	var largeSB strings.Builder
	largeSB.WriteString("[")
	for i := range 20 {
		if i > 0 {
			largeSB.WriteString(",")
		}
		fmt.Fprintf(&largeSB, `{"filename":"src/file%d.go","status":"modified","patch":"@@ -1 +1 @@\n%s"}`, i, patchBody)
	}
	largeSB.WriteString("]")
	largePageBody := []byte(largeSB.String())

	tests := []struct {
		name           string
		orgID          string
		platformRepoID string
		repoFullName   string
		pullRequestID  string
		prNumber       int32
		filesHandler   func(prNum, page int) (int, []byte, http.Header)
		// Outcome — exactly one of these three branches applies per case:
		// (1) wantSentinel != "" → expect UpdatePRDiffStatus(sentinel), no S3 write.
		// (2) wantNoUpdate       → expect neither UpdatePRDiffStatus nor S3 write (NULL preserved for retry).
		// (3) otherwise          → expect S3 Write at the canonical key + UpdatePRDiffStatus with that key.
		wantSentinel  string
		wantNoUpdate  bool
		assertWritten func(t *testing.T, content []byte)
	}{
		{
			name:           "single page success",
			orgID:          "org-1",
			platformRepoID: "gh-repo-19",
			repoFullName:   "acme/poc",
			pullRequestID:  "pr-19",
			prNumber:       19,
			filesHandler: func(prNum, page int) (int, []byte, http.Header) {
				return http.StatusOK, singlePageBody, defaultRateHeaders(4997)
			},
			assertWritten: func(t *testing.T, content []byte) {
				s := string(content)
				assert.Contains(t, s, "diff --git a/src/big_file.go b/src/big_file.go")
				assert.Contains(t, s, "diff --git a/src/another.go b/src/another.go")
				// `--- a/X` and `+++ b/X` are load-bearing: a unified-diff
				// parser finds zero files without them.
				assert.Contains(t, s, "--- a/src/big_file.go")
				assert.Contains(t, s, "+++ b/src/big_file.go")
				assert.Contains(t, s, "--- a/src/another.go")
				assert.Contains(t, s, "+++ b/src/another.go")
				assert.Contains(t, s, "+added1")
				assert.Contains(t, s, "+new1")
			},
		},
		{
			name:           "paginated — Link rel=next followed across pages",
			orgID:          "org-1",
			platformRepoID: "r42",
			repoFullName:   "o/r",
			pullRequestID:  "pr-42",
			prNumber:       42,
			filesHandler: func(prNum, page int) (int, []byte, http.Header) {
				h := defaultRateHeaders(4997)
				switch page {
				case 1:
					h.Set("Link", `<https://api.github.com/repos/o/r/pulls/42/files?per_page=100&page=2>; rel="next", <https://api.github.com/repos/o/r/pulls/42/files?per_page=100&page=2>; rel="last"`)
					return http.StatusOK, paginatedPage1, h
				case 2:
					// Terminal page — no Link rel="next".
					return http.StatusOK, paginatedPage2, h
				}
				return http.StatusInternalServerError, nil, h
			},
			assertWritten: func(t *testing.T, content []byte) {
				s := string(content)
				assert.Contains(t, s, "+page1_file1")
				assert.Contains(t, s, "+page2_file1")
				assert.Contains(t, s, "diff --git a/a.go")
				assert.Contains(t, s, "diff --git a/b.go")
				assert.Contains(t, s, "+++ b/a.go")
				assert.Contains(t, s, "+++ b/b.go")
			},
		},
		{
			name:           "/files endpoint own 406 → TOO_LARGE sentinel",
			orgID:          "org-1",
			platformRepoID: "r99",
			repoFullName:   "o/r",
			pullRequestID:  "pr-99",
			prNumber:       99,
			filesHandler: func(prNum, page int) (int, []byte, http.Header) {
				return http.StatusNotAcceptable, []byte(`{"message":"too large"}`), defaultRateHeaders(4997)
			},
			wantSentinel: sentinelTooLarge,
		},
		{
			name:           "truncated-text threshold → TOO_LARGE sentinel",
			orgID:          "org-1",
			platformRepoID: "r55",
			repoFullName:   "o/r",
			pullRequestID:  "pr-55",
			prNumber:       55,
			filesHandler: func(prNum, page int) (int, []byte, http.Header) {
				return http.StatusOK, truncatedPageBody, defaultRateHeaders(4997)
			},
			wantSentinel: sentinelTooLarge,
		},
		{
			// 30 of 100 files have empty patch but zero additions+deletions
			// (renames, mode-only, submodule updates). Pure ratio would be
			// 30% — well past the 10% threshold — but the new
			// additions+deletions guard means none of those 30 count as
			// "truncated", so the PR proceeds and the 70 real files get
			// synthesized into the diff.
			name:           "no-content-delta empty patches do not trip threshold",
			orgID:          "org-1",
			platformRepoID: "r56",
			repoFullName:   "o/r",
			pullRequestID:  "pr-56",
			prNumber:       56,
			filesHandler: func(prNum, page int) (int, []byte, http.Header) {
				return http.StatusOK, noContentDeltaPageBody, defaultRateHeaders(4997)
			},
			assertWritten: func(t *testing.T, content []byte) {
				s := string(content)
				// The 30 no-content-delta files must NOT appear (empty patch → skipped).
				assert.NotContains(t, s, "diff --git a/src/file0.go")
				assert.NotContains(t, s, "diff --git a/src/file29.go")
				// The 70 real-content files must appear.
				assert.Contains(t, s, "diff --git a/src/file30.go")
				assert.Contains(t, s, "diff --git a/src/file99.go")
			},
		},
		{
			name:           "binary files do not trip truncation threshold",
			orgID:          "org-1",
			platformRepoID: "r77",
			repoFullName:   "o/r",
			pullRequestID:  "pr-77",
			prNumber:       77,
			filesHandler: func(prNum, page int) (int, []byte, http.Header) {
				return http.StatusOK, binaryPageBody, defaultRateHeaders(4997)
			},
			assertWritten: func(t *testing.T, content []byte) {
				s := string(content)
				// Binary files must NOT appear in the synthesized diff.
				assert.NotContains(t, s, "diff --git a/assets/")
				// All 5 text files must appear.
				for _, name := range []string{"a", "b", "c", "d", "e"} {
					assert.Contains(t, s, fmt.Sprintf("diff --git a/src/%s.go", name))
				}
			},
		},
		{
			name:           "reconstructed >10MB succeeds under 50MB fallback cap",
			orgID:          "org-1",
			platformRepoID: "r88",
			repoFullName:   "o/r",
			pullRequestID:  "pr-88",
			prNumber:       88,
			filesHandler: func(prNum, page int) (int, []byte, http.Header) {
				return http.StatusOK, largePageBody, defaultRateHeaders(4997)
			},
			assertWritten: func(t *testing.T, content []byte) {
				// A diff in the (10 MB, 50 MB] band must
				// NOT be sentinel'd — that's the recovery case the fallback
				// exists for.
				assert.Greater(t, len(content), maxDiffSizeBytes,
					"reconstructed diff should exceed the primary 10 MB cap — that's what this case guards")
				assert.LessOrEqual(t, len(content), maxFallbackDiffSizeBytes,
					"reconstructed diff should not exceed the fallback 50 MB cap")
			},
		},
		{
			name:           "transient /files error → NULL preserved, no sentinel",
			orgID:          "org-1",
			platformRepoID: "r33",
			repoFullName:   "o/r",
			pullRequestID:  "pr-33",
			prNumber:       33,
			filesHandler: func(prNum, page int) (int, []byte, http.Header) {
				return http.StatusInternalServerError, []byte(`{"message":"oops"}`), defaultRateHeaders(4997)
			},
			wantNoUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			analytics := sdkmocks.NewMockWorkQueue(ctrl)
			s3 := sdkmocks.NewMockBlobStore(ctrl)

			srv := newDiffServer(t, diffHandlerOpts{
				statusByPR:   map[int]int{int(tt.prNumber): http.StatusNotAcceptable},
				bodyByPR:     map[int][]byte{int(tt.prNumber): []byte(`{"message":"too large","errors":[{"code":"too_large"}]}`)},
				headers:      defaultRateHeaders(4998),
				filesHandler: tt.filesHandler,
			})
			defer srv.Close()

			analytics.EXPECT().ListPRsPendingDiffFetch(gomock.Any(), gomock.Any()).Return(
				&workqueuev1.ListPRsPendingDiffFetchResponse{
					Prs: []*workqueuev1.PRDiffFetchCandidate{
						{
							PullRequestId:  tt.pullRequestID,
							PlatformRepoId: tt.platformRepoID,
							RepoFullName:   tt.repoFullName,
							PrNumber:       tt.prNumber,
						},
					},
				}, nil)

			var written []byte
			switch {
			case tt.wantNoUpdate:
				// gomock controller asserts no unexpected calls in t.Cleanup.
			case tt.wantSentinel != "":
				analytics.EXPECT().UpdatePRDiffStatus(gomock.Any(),
					matchSentinel(tt.pullRequestID, tt.wantSentinel)).
					Return(&workqueuev1.UpdatePRDiffStatusResponse{}, nil)
			default:
				wantKey := fmt.Sprintf("pr-diffs/%s/github/%s/%d.patch",
					tt.orgID, tt.platformRepoID, tt.prNumber)
				s3.EXPECT().Write(gomock.Any(), wantKey, gomock.Any(), "").
					DoAndReturn(func(_ context.Context, _ string, content []byte, _ string) error {
						written = content
						return nil
					})
				analytics.EXPECT().UpdatePRDiffStatus(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *workqueuev1.UpdatePRDiffStatusRequest) (*workqueuev1.UpdatePRDiffStatusResponse, error) {
						assert.Equal(t, wantKey, req.DiffS3Key, "must be real S3 key, never a sentinel")
						return &workqueuev1.UpdatePRDiffStatusResponse{}, nil
					})
			}

			params := &sdk.ExecutionParams{
				Config:        &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: tt.orgID},
				Sink:          testkit.NewChannelSink(),
				VendorGateway: newDiffMockGateway(srv.URL),
				Host:          sdk.HostServices{WorkQueue: analytics},
				BlobStore:     s3,
				BudgetLimiter: newTestBudget(t),
			}
			_, err := (&FetchPRDiffsOp{}).Execute(context.Background(), params)
			require.NoError(t, err)

			if tt.assertWritten != nil {
				require.NotNil(t, written, "expected S3 Write to capture content")
				tt.assertWritten(t, written)
			}
		})
	}
}

// capturingGateway records the outgoing request's full URL (scheme + host +
// path) instead of forwarding it anywhere, so a test can assert exactly which
// base githubRESTBase routed to without standing up a listener at that host.
type capturingGateway struct {
	capturedURL string
	status      int
	body        []byte
}

func (m *capturingGateway) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	m.capturedURL = req.URL.String()
	return &http.Response{
		StatusCode: m.status,
		Header:     defaultRateHeaders(4999),
		Body:       io.NopCloser(bytes.NewReader(m.body)),
	}, nil
}

func (m *capturingGateway) DoJSON(_ context.Context, _, _ string, _, _ any) error {
	return fmt.Errorf("DoJSON not used in fetch_pr_diffs tests")
}

func (m *capturingGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return m
}

func (m *capturingGateway) ForGroup(_ string) sdk.VendorGateway { return m }

func (m *capturingGateway) Execute(_ context.Context, _ proto.Message) (proto.Message, error) {
	return nil, fmt.Errorf("Execute not used in fetch_pr_diffs tests")
}

// TestFetchGitHubPRDiff_RoutesThroughGitHubRESTBase guards that both diff
// fetches use githubRESTBase(orgID), so a simulator-routed organization's diff
// request never reaches api.github.com. The test config (gatewaytest) routes one
// org to the simulator, so this exercises the real routing decision, not a
// stubbed one.
func TestFetchGitHubPRDiff_RoutesThroughGitHubRESTBase(t *testing.T) {
	const simulatorOrgID = gatewaytest.SimulatorOrgID
	const nonSimulatorOrgID = "org-not-in-simulator-list"

	tests := []struct {
		name     string
		orgID    string
		wantBase string
	}{
		{"simulator org routes to the simulator base", simulatorOrgID, "http://localhost:8080/github"},
		{"non-simulator org routes to the real GitHub API", nonSimulatorOrgID, "https://api.github.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &capturingGateway{status: http.StatusOK, body: []byte("diff --git a/x b/x\n+x\n")}
			_, _, err := fetchGitHubPRDiff(context.Background(), gw, tt.orgID, "owner", "repo", 1)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(gw.capturedURL, tt.wantBase+"/repos/owner/repo/pulls/1"),
				"fetchGitHubPRDiff: got URL %q, want prefix %q", gw.capturedURL, tt.wantBase)

			filesGW := &capturingGateway{status: http.StatusOK, body: []byte(`[]`)}
			_, _, err = fetchGitHubPRDiffViaFiles(context.Background(), filesGW, tt.orgID, "owner", "repo", 1)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(filesGW.capturedURL, tt.wantBase+"/repos/owner/repo/pulls/1/files"),
				"fetchGitHubPRDiffViaFiles: got URL %q, want prefix %q", filesGW.capturedURL, tt.wantBase)
		})
	}
}

// matchSentinel returns a gomock matcher for UpdatePRDiffStatusRequest with
// the expected PullRequestId and DiffS3Key sentinel value.
func matchSentinel(prID, sentinel string) gomock.Matcher {
	return sentinelMatcher{prID: prID, sentinel: sentinel}
}

type sentinelMatcher struct {
	prID     string
	sentinel string
}

func (m sentinelMatcher) Matches(x any) bool {
	req, ok := x.(*workqueuev1.UpdatePRDiffStatusRequest)
	if !ok {
		return false
	}
	return req.PullRequestId == m.prID && req.DiffS3Key == m.sentinel
}

func (m sentinelMatcher) String() string {
	return fmt.Sprintf("UpdatePRDiffStatus{pr=%s, sentinel=%s}", m.prID, m.sentinel)
}
