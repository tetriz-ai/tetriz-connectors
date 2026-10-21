// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// --- fallback-only mock gateway (Execute path) ---

// mrDiffFallbackGateway serves only the paginated /diffs Execute calls the
// fallback issues; Do is never used by fetchGitLabMRDiffViaPages.
type mrDiffFallbackGateway struct {
	executeFunc func(page int32) (proto.Message, error)
}

func (g *mrDiffFallbackGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not expected on fallback gateway")
}

func (g *mrDiffFallbackGateway) DoJSON(context.Context, string, string, any, any) error {
	return fmt.Errorf("DoJSON not expected")
}

func (g *mrDiffFallbackGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *mrDiffFallbackGateway) ForGroup(string) sdk.VendorGateway { return g }
func (g *mrDiffFallbackGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	r, ok := req.(*connectorpb.GitLabListMRDiffsRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected request type %T", req)
	}
	return g.executeFunc(r.GetPage())
}

func glDiffsPageResponse(diffs []glMRDiffEntry, nextPageURL string) *connectorpb.GitLabListMRDiffsResponse {
	resp := glDiffsResponse(diffs)
	resp.NextPageUrl = nextPageURL
	return resp
}

// TestFetchGitLabMRDiffViaPages exercises the fallback reconstruction directly.
func TestFetchGitLabMRDiffViaPages(t *testing.T) {
	t.Parallel()

	t.Run("multi-page reconstruction wraps each hunk in a unified-diff envelope", func(t *testing.T) {
		t.Parallel()
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				switch page {
				case 1:
					return glDiffsPageResponse([]glMRDiffEntry{
						{OldPath: "a.go", NewPath: "a.go", Diff: "@@ -1 +1 @@\n-old\n+new\n"},
					}, "https://gitlab.com/next?page=2"), nil
				case 2:
					return glDiffsPageResponse([]glMRDiffEntry{
						{OldPath: "", NewPath: "b.go", NewFile: true, Diff: "@@ -0,0 +1 @@\n+brand new\n"},
					}, ""), nil
				default:
					return nil, fmt.Errorf("unexpected page %d", page)
				}
			},
		}

		got, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.NoError(t, err)

		out := string(got)
		assert.Contains(t, out, "diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1 @@")
		assert.Contains(t, out, "diff --git a/b.go b/b.go\n--- a/b.go\n+++ b/b.go\n@@ -0,0 +1 @@")
		assert.Contains(t, out, "+new")
		assert.Contains(t, out, "+brand new")
	})

	t.Run("byte cap overflow sentinels TOO_LARGE", func(t *testing.T) {
		t.Parallel()
		// One page whose single entry already exceeds the fallback byte cap.
		huge := "@@ -1 +1 @@\n" + strings.Repeat("+x\n", (mrDiffFallbackMaxSizeBytes/3)+10)
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				return glDiffsPageResponse([]glMRDiffEntry{
					{OldPath: "big.go", NewPath: "big.go", Diff: huge},
				}, ""), nil
			},
		}
		_, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.ErrorIs(t, err, errMRDiffTooLarge)
	})

	t.Run("page cap exhausted with pages pending sentinels TOO_LARGE", func(t *testing.T) {
		t.Parallel()
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				// Every page reports another page follows — never terminates.
				return glDiffsPageResponse([]glMRDiffEntry{
					{OldPath: "f.go", NewPath: "f.go", Diff: "@@ -1 +1 @@\n+x\n"},
				}, "https://gitlab.com/next"), nil
			},
		}
		_, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.ErrorIs(t, err, errMRDiffTooLarge)
	})

	t.Run("too many modified files missing diff bodies sentinels TOO_LARGE", func(t *testing.T) {
		t.Parallel()
		// 100 modified files, 20 with empty diff (>10% and >=10 floor) → truncated.
		entries := make([]glMRDiffEntry, 0, 100)
		for i := range 100 {
			e := glMRDiffEntry{OldPath: fmt.Sprintf("f%d.go", i), NewPath: fmt.Sprintf("f%d.go", i)}
			if i < 20 {
				e.Diff = "" // modified file, GitLab dropped the body
			} else {
				e.Diff = "@@ -1 +1 @@\n+x\n"
			}
			entries = append(entries, e)
		}
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				return glDiffsPageResponse(entries, ""), nil
			},
		}
		_, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.ErrorIs(t, err, errMRDiffTooLarge)
	})

	t.Run("deleted file with content emits +++ /dev/null, not +++ b/<path>", func(t *testing.T) {
		t.Parallel()
		// A deletion whose diff body is non-empty (GitLab can return the removed
		// lines) must be marked with the unified-diff /dev/null convention for
		// deletions — otherwise it is misread as a still-existing
		// modified file with its removed lines misread as content.
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				return glDiffsPageResponse([]glMRDiffEntry{
					{OldPath: "gone.go", NewPath: "", DeletedFile: true, Diff: "@@ -1,2 +0,0 @@\n-line1\n-line2\n"},
				}, ""), nil
			},
		}
		got, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.NoError(t, err)
		out := string(got)
		assert.Contains(t, out, "+++ /dev/null", "a deleted file's diff must use the /dev/null convention")
		assert.NotContains(t, out, "+++ b/gone.go", "must not emit the deleted path on the +++ side")
	})

	t.Run("truncation ratio denominator excludes structural files diluting the count", func(t *testing.T) {
		t.Parallel()
		// 15 modified files (10 with dropped diff bodies, 5 with real content) —
		// 10/15 = 66.7%, well over the 10% threshold and the 10-file floor — but
		// padded with 100 structural (new/rename/delete) empty-diff entries that
		// legitimately never count as truncated. Dividing by all 115 entries
		// would dilute the ratio to 10/115 = 8.7%, silently under the
		// threshold, so the denominator is the modified candidates only (15).
		entries := make([]glMRDiffEntry, 0, 115)
		for i := range 100 {
			entries = append(entries, glMRDiffEntry{OldPath: fmt.Sprintf("new%d.go", i), NewPath: fmt.Sprintf("new%d.go", i), NewFile: true})
		}
		for i := range 15 {
			e := glMRDiffEntry{OldPath: fmt.Sprintf("m%d.go", i), NewPath: fmt.Sprintf("m%d.go", i)}
			if i < 10 {
				e.Diff = ""
			} else {
				e.Diff = "@@ -1 +1 @@\n+x\n"
			}
			entries = append(entries, e)
		}
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				return glDiffsPageResponse(entries, ""), nil
			},
		}
		_, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.ErrorIs(t, err, errMRDiffTooLarge,
			"structural files must not dilute the truncation ratio's denominator below the real modified-candidate count")
	})

	t.Run("terminal 413 from the paginated Execute call maps to errMRDiffTooLarge", func(t *testing.T) {
		t.Parallel()
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				return nil, &sdk.SyncError{HTTPStatus: http.StatusRequestEntityTooLarge, Cause: fmt.Errorf("413")}
			},
		}
		_, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.ErrorIs(t, err, errMRDiffTooLarge,
			"a terminal 413 mid-pagination must map to the TOO_LARGE sentinel, not a generic retryable error")
	})

	t.Run("terminal 404 from the paginated Execute call maps to errMRDiffNotAvailable", func(t *testing.T) {
		t.Parallel()
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				return nil, &sdk.SyncError{HTTPStatus: http.StatusNotFound, Cause: fmt.Errorf("404")}
			},
		}
		_, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.ErrorIs(t, err, errMRDiffNotAvailable,
			"a terminal 404 (MR disappeared mid-fetch) must map to NOT_AVAILABLE, not be retried forever")
	})

	t.Run("binary/rename/delete empty-diff entries do NOT count as truncation", func(t *testing.T) {
		t.Parallel()
		// 100 entries, 30 empty-diff but all structural (new binary / rename /
		// delete) — none is a MODIFIED text file, so the guard must NOT fire.
		entries := make([]glMRDiffEntry, 0, 100)
		for i := range 100 {
			e := glMRDiffEntry{OldPath: fmt.Sprintf("f%d", i), NewPath: fmt.Sprintf("f%d", i)}
			switch {
			case i < 10:
				e.NewFile = true // new binary — empty diff legitimately
			case i < 20:
				e.RenamedFile = true // rename-only — empty diff legitimately
			case i < 30:
				e.DeletedFile = true // delete — empty diff legitimately
			default:
				e.Diff = "@@ -1 +1 @@\n+x\n"
			}
			entries = append(entries, e)
		}
		gw := &mrDiffFallbackGateway{
			executeFunc: func(page int32) (proto.Message, error) {
				return glDiffsPageResponse(entries, ""), nil
			},
		}
		got, err := fetchGitLabMRDiffViaPages(context.Background(), gw, "100", 7)
		require.NoError(t, err, "structural empty-diff entries must not trip the truncation guard")
		assert.NotEmpty(t, got)
	})
}

// --- processOne end-to-end: primary 413 → fallback ---

// mrDiffE2EGateway serves the primary /diffs via Do (a synthetic *http.Response)
// and the paginated fallback via Execute.
type mrDiffE2EGateway struct {
	primaryStatus int
	primaryBody   []byte
	fallback      func(page int32) (proto.Message, error)
}

func (g *mrDiffE2EGateway) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: g.primaryStatus,
		Body:       io.NopCloser(strings.NewReader(string(g.primaryBody))),
		Header:     http.Header{},
	}, nil
}

func (g *mrDiffE2EGateway) DoJSON(context.Context, string, string, any, any) error {
	return fmt.Errorf("DoJSON not expected")
}

func (g *mrDiffE2EGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *mrDiffE2EGateway) ForGroup(string) sdk.VendorGateway { return g }
func (g *mrDiffE2EGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	r, ok := req.(*connectorpb.GitLabListMRDiffsRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected request type %T", req)
	}
	return g.fallback(r.GetPage())
}

func newMRDiffTestBudget(t *testing.T) *ratelimiter.BudgetLimiter {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return ratelimiter.NewBudgetLimiter(rdb, ratelimiter.GitLabAdapter{})
}

func TestFetchMRDiffsOp_ProcessOne_OversizedFallback(t *testing.T) {
	t.Parallel()

	const (
		orgID          = "org-test"
		platformRepoID = "100"
		prID           = "pr-uuid-1"
		prNumber       = 7
	)

	t.Run("primary 413 → fallback reconstructs → writes .patch key + UpdatePRDiffStatus", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)

		gw := &mrDiffE2EGateway{
			primaryStatus: http.StatusRequestEntityTooLarge,
			fallback: func(page int32) (proto.Message, error) {
				if page == 1 {
					return glDiffsPageResponse([]glMRDiffEntry{
						{OldPath: "a.go", NewPath: "a.go", Diff: "@@ -1 +1 @@\n-old\n+new\n"},
					}, ""), nil
				}
				return nil, fmt.Errorf("unexpected page %d", page)
			},
		}

		var wroteKey string
		var wroteContent []byte
		s3 := sdkmocks.NewMockBlobStore(ctrl)
		s3.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, dst string, content []byte, _ string) error {
				wroteKey = dst
				wroteContent = append([]byte(nil), content...)
				return nil
			})

		analytics := sdkmocks.NewMockWorkQueue(ctrl)
		var statusKey string
		analytics.EXPECT().UpdatePRDiffStatus(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, in *workqueuev1.UpdatePRDiffStatusRequest) (*workqueuev1.UpdatePRDiffStatusResponse, error) {
				statusKey = in.GetDiffS3Key()
				return &workqueuev1.UpdatePRDiffStatusResponse{}, nil
			})

		op := &FetchMRDiffsOp{}
		op.processOne(context.Background(), gw, s3, analytics, newMRDiffTestBudget(t), orgID, &workqueuev1.PRDiffFetchCandidate{
			PullRequestId:  prID,
			PlatformRepoId: platformRepoID,
			PrNumber:       prNumber,
			RepoFullName:   "mygroup/repoA",
		})

		wantKey := fmt.Sprintf("pr-diffs/%s/%s/%s/%d.patch", orgID, connectorID, platformRepoID, prNumber)
		assert.Equal(t, wantKey, wroteKey, "fallback must write the unified diff under a .patch key")
		assert.Equal(t, wantKey, statusKey, "UpdatePRDiffStatus must record the .patch key")
		assert.Contains(t, string(wroteContent), "diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n")
		assert.Contains(t, string(wroteContent), "+new")
	})

	t.Run("primary 413 → oversized fallback → sentinel TOO_LARGE, no S3 write", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)

		gw := &mrDiffE2EGateway{
			primaryStatus: http.StatusRequestEntityTooLarge,
			fallback: func(page int32) (proto.Message, error) {
				// Never terminates → page cap exhausted → errMRDiffTooLarge.
				return glDiffsPageResponse([]glMRDiffEntry{
					{OldPath: "f.go", NewPath: "f.go", Diff: "@@ -1 +1 @@\n+x\n"},
				}, "https://gitlab.com/next"), nil
			},
		}

		s3 := sdkmocks.NewMockBlobStore(ctrl) // no Write expected

		analytics := sdkmocks.NewMockWorkQueue(ctrl)
		var statusKey string
		analytics.EXPECT().UpdatePRDiffStatus(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, in *workqueuev1.UpdatePRDiffStatusRequest) (*workqueuev1.UpdatePRDiffStatusResponse, error) {
				statusKey = in.GetDiffS3Key()
				return &workqueuev1.UpdatePRDiffStatusResponse{}, nil
			})

		op := &FetchMRDiffsOp{}
		op.processOne(context.Background(), gw, s3, analytics, newMRDiffTestBudget(t), orgID, &workqueuev1.PRDiffFetchCandidate{
			PullRequestId:  prID,
			PlatformRepoId: platformRepoID,
			PrNumber:       prNumber,
			RepoFullName:   "mygroup/repoA",
		})

		assert.Equal(t, mrDiffSentinelTooLarge, statusKey,
			"an oversized fallback must sentinel TOO_LARGE")
	})
}
