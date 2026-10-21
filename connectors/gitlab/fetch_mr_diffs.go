// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Endpoint: GET /api/v4/projects/{id}/merge_requests/{iid}/diffs
// Returns: JSON array of diff objects.
// S3 key (primary): pr-diffs/{orgID}/gitlab/{platform_repo_id}/{pr_number}.json
// S3 key (fallback): pr-diffs/{orgID}/gitlab/{platform_repo_id}/{pr_number}.patch
// Sentinels: 404 → NOT_AVAILABLE. An oversized primary diff (413, or a body
// over the 10MB cap) is NOT sentineled immediately — it first tries the
// paginated /diffs fallback, mirroring GitHub's /files fallback; only a
// fallback that also overflows (byte cap, page cap, or a materially truncated
// reconstruction) sentinels TOO_LARGE.
//
// Format note: the primary writes GitLab's raw JSON-array diff under a `.json`
// key; the fallback synthesizes a unified diff under a `.patch` key. A reader
// can route by suffix (`.json` → GitLab parser, else → unified parser), so
// each shape is readable from its own key.

const (
	mrDiffPerPRTimeout   = 30 * time.Second
	mrDiffMaxSizeBytes   = 10 * 1024 * 1024
	mrDiffFetchBatchSize = 50

	// mrDiffFallbackMaxSizeBytes caps the reconstructed unified diff from the
	// paginated fallback. Larger than the primary cap because the fallback
	// exists precisely to handle MRs the primary refuses, and pagination avoids
	// the monolithic single-response memory pressure that motivated the 10MB
	// primary cap. Mirrors GitHub's maxFallbackDiffSizeBytes.
	mrDiffFallbackMaxSizeBytes = 50 * 1024 * 1024
	// mrDiffFallbackMaxPages bounds the fallback pagination. Matches enrichMR's
	// maxDiffPages; exhausting it with pages still pending sentinels TOO_LARGE.
	mrDiffFallbackMaxPages = 50
	// mrDiffTruncatedRatioThreshold + mrDiffTruncatedAbsoluteFloor: if too many
	// modified text files come back with an empty `diff` body (GitLab dropped
	// content a diff reader would have used), the reconstruction is materially
	// incomplete and we sentinel TOO_LARGE rather than persist a misleading
	// line count. Mirrors GitHub's truncated-text guard.
	mrDiffTruncatedRatioThreshold = 0.10
	mrDiffTruncatedAbsoluteFloor  = 10

	mrDiffSentinelNotAvailable = "NOT_AVAILABLE"
	mrDiffSentinelTooLarge     = "TOO_LARGE"

	opFetchMRDiffs = "fetch_mr_diffs"
)

// mrDiffsMateriallyTruncated reports whether GitLab dropped enough modified-file
// diff bodies that the reconstruction is materially incomplete. Both GitLab diff
// paths — the diff fallback here and enrichMR's per-file line stats —
// call this so they draw the same line from the same thresholds. truncatedFiles is
// the modified candidates that came back with an empty diff; modifiedCandidates the
// population (excludes new/renamed/deleted/pathless entries).
func mrDiffsMateriallyTruncated(truncatedFiles, modifiedCandidates int) bool {
	return truncatedFiles >= mrDiffTruncatedAbsoluteFloor &&
		modifiedCandidates > 0 &&
		float64(truncatedFiles)/float64(modifiedCandidates) > mrDiffTruncatedRatioThreshold
}

var (
	errMRDiffNotAvailable = errors.New("mr diff not available")
	errMRDiffTooLarge     = errors.New("mr diff too large")
)

// fetchMRDiffsRequiresHost lists the host services the operation cannot run without.
var fetchMRDiffsRequiresHost = []sdk.HostService{sdk.HostWorkQueue}

// FetchMRDiffsOp fetches the JSON diff representation for merged GitLab
// MRs and records the resulting S3 key (or a sentinel) via host's gRPC WorkQueue.
type FetchMRDiffsOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchMRDiffsOp) OperationID() sdk.OperationID { return opFetchMRDiffs }

// Execute fetches one batch of pending MR diffs and returns the current
// cursor; the next tick resumes via ListPRsPendingDiffFetch. It fails,
// terminal, without the host's work queue, a blob store or a budget limiter.
func (op *FetchMRDiffsOp) Execute(parentCtx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID

	if missing := params.Host.Missing(fetchMRDiffsRequiresHost); len(missing) > 0 {
		return nil, sdk.MissingHostServicesError(connectorID, op.OperationID(), orgID, missing)
	}
	if params.BlobStore == nil || params.BudgetLimiter == nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid, Terminal: true,
			Cause: fmt.Errorf("fetch_mr_diffs needs a blob store and a budget limiter (blob store set: %t, budget limiter set: %t)",
				params.BlobStore != nil, params.BudgetLimiter != nil),
		}
	}

	resp, err := params.Host.WorkQueue.ListPRsPendingDiffFetch(parentCtx, &workqueuev1.ListPRsPendingDiffFetchRequest{
		OrganizationId: orgID,
		ConnectorId:    string(connectorID),
		Limit:          mrDiffFetchBatchSize,
	})
	if err != nil {
		logger.Error(parentCtx, "listing MRs pending diff fetch",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.ConnectorID, string(connectorID)),
			zap.Error(err),
		)
		return nil, fmt.Errorf("listing MRs pending diff fetch: %w", err)
	}

	prs := resp.GetPrs()
	if len(prs) == 0 {
		return params.Cursor, nil
	}

	gw := params.VendorGateway.ForGroup(constants.GroupGitLabREST)

	for _, pr := range prs {
		// GitLab scopes rate limits per user token; pass orgID as userID scope
		// because we store the token per-org.
		ok, wait, err := params.BudgetLimiter.Allow(parentCtx, orgID, ratelimiter.BucketDefault, ratelimiter.GitLabCoreFloor)
		if err != nil {
			logger.Error(parentCtx, "fetch_mr_diffs: budget check failed — deferring batch",
				zap.String(logger.OrgID, orgID),
				zap.Error(err),
			)
			return params.Cursor, nil
		}
		if !ok {
			logger.Info(parentCtx, "fetch_mr_diffs: budget floor hit — deferring remaining MRs to next tick",
				zap.String(logger.OrgID, orgID),
				zap.Duration("reset_in", wait),
			)
			return params.Cursor, nil
		}

		ctx, cancel := context.WithTimeout(parentCtx, mrDiffPerPRTimeout)
		op.processOne(ctx, gw, params.BlobStore, params.Host.WorkQueue, params.BudgetLimiter, orgID, pr)
		cancel()
	}

	return params.Cursor, nil
}

// processOne fetches a single MR diff. Per-MR errors are logged and swallowed.
func (op *FetchMRDiffsOp) processOne(
	ctx context.Context,
	gw sdk.VendorGateway,
	s3Client mrS3Writer,
	analytics sdk.WorkQueue,
	budget sdk.BudgetObserver,
	orgID string,
	pr *workqueuev1.PRDiffFetchCandidate,
) {
	diffBytes, respHeader, err := fetchGitLabMRDiff(ctx, gw, orgID, pr.GetPlatformRepoId(), int(pr.GetPrNumber()))
	if respHeader != nil {
		if refreshErr := budget.UpdateFromResponse(ctx, orgID, respHeader); refreshErr != nil {
			logger.Error(ctx, "fetch_mr_diffs: budget refresh from response headers failed",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.PullRequestID, pr.GetPullRequestId()),
				zap.Error(refreshErr),
			)
		}
	}

	// An oversized primary diff is either the 413 sentinel error or a 200 whose
	// body overran the primary cap — both mean "too big to store in one shot"
	// and both are recoverable by paginating /diffs.
	primaryTooLarge := errors.Is(err, errMRDiffTooLarge) || (err == nil && len(diffBytes) > mrDiffMaxSizeBytes)

	// sizeCap + s3Suffix track which path produced diffBytes: the primary JSON
	// diff (10MB cap, `.json`) or the paginated unified-diff fallback (50MB cap,
	// `.patch`). The fallback branch overrides both, then falls through to the
	// shared size guard + S3 write.
	sizeCap := mrDiffMaxSizeBytes
	s3Suffix := "json"

	switch {
	case errors.Is(err, errMRDiffNotAvailable):
		op.markSentinel(ctx, analytics, pr.GetPullRequestId(), mrDiffSentinelNotAvailable, orgID)
		return
	case primaryTooLarge:
		logger.Info(ctx, "fetch_mr_diffs: primary diff too large — falling back to paginated /diffs",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.String("repo", pr.GetRepoFullName()),
			zap.Int32("pr_number", pr.GetPrNumber()),
		)
		sizeCap = mrDiffFallbackMaxSizeBytes
		s3Suffix = "patch"
		diffBytes, err = fetchGitLabMRDiffViaPages(ctx, gw, pr.GetPlatformRepoId(), int(pr.GetPrNumber()))
		switch {
		case errors.Is(err, errMRDiffTooLarge):
			op.markSentinel(ctx, analytics, pr.GetPullRequestId(), mrDiffSentinelTooLarge, orgID)
			return
		case errors.Is(err, errMRDiffNotAvailable):
			op.markSentinel(ctx, analytics, pr.GetPullRequestId(), mrDiffSentinelNotAvailable, orgID)
			return
		case err != nil:
			logger.Error(ctx, "fetch_mr_diffs: paginated fallback failed — leaving NULL for retry",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.PullRequestID, pr.GetPullRequestId()),
				zap.String("repo", pr.GetRepoFullName()),
				zap.Int32("pr_number", pr.GetPrNumber()),
				zap.Error(err),
			)
			return
		}
		// Fallback succeeded; diffBytes carries the reconstructed unified diff.
		// Fall through to the size guard + S3 upload below.
	case err != nil:
		logger.Error(ctx, "fetch_mr_diffs: fetching MR diff — leaving NULL for retry",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.String("repo", pr.GetRepoFullName()),
			zap.Int32("pr_number", pr.GetPrNumber()),
			zap.Error(err),
		)
		return
	}

	if len(diffBytes) > sizeCap {
		op.markSentinel(ctx, analytics, pr.GetPullRequestId(), mrDiffSentinelTooLarge, orgID)
		return
	}

	s3Key := fmt.Sprintf("pr-diffs/%s/%s/%s/%d.%s", orgID, connectorID, pr.GetPlatformRepoId(), pr.GetPrNumber(), s3Suffix)
	if err := s3Client.Write(ctx, s3Key, diffBytes, ""); err != nil {
		logger.Error(ctx, "fetch_mr_diffs: uploading diff to S3",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.String("s3_key", s3Key),
			zap.Error(err),
		)
		return
	}

	if _, err := analytics.UpdatePRDiffStatus(ctx, &workqueuev1.UpdatePRDiffStatusRequest{
		OrganizationId: orgID,
		PullRequestId:  pr.GetPullRequestId(),
		DiffS3Key:      s3Key,
		FetchedAt:      timestamppb.Now(),
	}); err != nil {
		logger.Error(ctx, "fetch_mr_diffs: updating MR diff status",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.Error(err),
		)
	}
}

func (op *FetchMRDiffsOp) markSentinel(
	ctx context.Context,
	analytics sdk.WorkQueue,
	pullRequestID, sentinel, orgID string,
) {
	if _, err := analytics.UpdatePRDiffStatus(ctx, &workqueuev1.UpdatePRDiffStatusRequest{
		OrganizationId: orgID,
		PullRequestId:  pullRequestID,
		DiffS3Key:      sentinel,
		FetchedAt:      timestamppb.Now(),
	}); err != nil {
		logger.Error(ctx, "fetch_mr_diffs: marking sentinel",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pullRequestID),
			zap.String("sentinel", sentinel),
			zap.Error(err),
		)
	}
}

// mrS3Writer is the narrow S3 surface this op needs. Declared locally so tests
// can stub it directly.
type mrS3Writer interface {
	Write(ctx context.Context, dst string, content []byte, aclString string) error
}

// fetchGitLabMRDiff issues GET /api/v4/projects/{id}/merge_requests/{iid}/diffs.
// Returns raw JSON bytes, the underlying *http.Response, or a typed sentinel
// error.
// fetchGitLabMRDiff returns the diff bytes and the response headers (for rate-limit
// budget refresh). It closes the response body itself, so callers only see headers.
func fetchGitLabMRDiff(ctx context.Context, gw sdk.VendorGateway, orgID, projectID string, mrIID int) ([]byte, http.Header, error) {
	url := fmt.Sprintf("%s/projects/%s/merge_requests/%d/diffs", gitlabRESTBase(orgID), projectID, mrIID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("building diff request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := gw.Do(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("executing diff request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close on read-only diff fetch

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, mrDiffMaxSizeBytes+1))
		if err != nil {
			return nil, resp.Header, fmt.Errorf("reading diff body: %w", err)
		}
		return body, resp.Header, nil
	case http.StatusNotFound:
		return nil, resp.Header, errMRDiffNotAvailable
	case http.StatusRequestEntityTooLarge:
		return nil, resp.Header, errMRDiffTooLarge
	default:
		return nil, resp.Header, fmt.Errorf("unexpected status %d fetching diff", resp.StatusCode)
	}
}

// fetchGitLabMRDiffViaPages reconstructs a unified diff by paginating
// GET /projects/:id/merge_requests/:iid/diffs (the proto Execute path, so the
// gateway's rate-limit + circuit-breaker middleware still applies) and wrapping
// each entry's `diff` hunk body in the `diff --git`/`--- a`/`+++ b` envelope
// that a unified-diff parser needs to start a new file. Used as the fallback when the
// monolithic /diffs call is too large. Mirrors GitHub's fetchGitHubPRDiffViaFiles.
//
// Returns errMRDiffTooLarge when:
//   - the reconstructed bytes exceed mrDiffFallbackMaxSizeBytes;
//   - the page cap is exhausted with pages still pending; or
//   - too many modified text files come back with an empty `diff` body (GitLab
//     dropped content a diff reader needed) — see the truncated-text guard.
//
// GitLab's diff entry carries no per-file additions/deletions or `too_large`
// flag, so "content we expected but didn't get" is proxied by a MODIFIED
// (non-new, non-deleted, non-rename) file with an empty `diff`; new/binary,
// rename-only, and delete entries legitimately carry no textual diff and are
// excluded from the truncation count.
func fetchGitLabMRDiffViaPages(ctx context.Context, gw sdk.VendorGateway, projectID string, mrIID int) ([]byte, error) {
	pid, err := strconv.ParseInt(projectID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse project id %q: %w", projectID, err)
	}

	restGW := gw.ForGroup(constants.GroupGitLabREST)
	var (
		buf bytes.Buffer
		// modifiedCandidates counts only entries that could possibly have their
		// diff body dropped by GitLab (excludes new/renamed/deleted/pathless
		// entries, which never increment truncatedFiles below) — the truncation
		// ratio's denominator must match its numerator's population, or a large
		// MR with many structural (non-modified) files dilutes the ratio and
		// hides a genuinely incomplete reconstruction.
		modifiedCandidates int
		truncatedFiles     int
		completedPages     bool
	)

	for page := int32(1); page <= mrDiffFallbackMaxPages; page++ {
		protoResp, execErr := restGW.Execute(ctx, &connectorpb.GitLabListMRDiffsRequest{
			ProjectId:       pid,
			MergeRequestIid: int64(mrIID),
			Page:            page,
			PerPage:         100,
		})
		if execErr != nil {
			// The proto Execute path reports terminal vendor statuses as a
			// *sdk.SyncError, not the errMRDiffTooLarge/errMRDiffNotAvailable
			// sentinels the primary raw-HTTP fetch uses — translate them so the
			// caller can sentinel TOO_LARGE/NOT_AVAILABLE instead of retrying a
			// page that will 413/404 identically forever.
			var syncErr *sdk.SyncError
			if errors.As(execErr, &syncErr) {
				switch syncErr.HTTPStatus {
				case http.StatusRequestEntityTooLarge:
					return nil, errMRDiffTooLarge
				case http.StatusNotFound:
					return nil, errMRDiffNotAvailable
				}
			}
			return nil, fmt.Errorf("fetch MR diffs page %d: %w", page, execErr)
		}
		resp, ok := protoResp.(*connectorpb.GitLabListMRDiffsResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T fetching MR diffs page %d", protoResp, page)
		}

		for _, diffRaw := range resp.GetDiffs() {
			var entry glMRDiffEntry
			if err := json.Unmarshal(diffRaw, &entry); err != nil {
				logger.WarnWithCtx(ctx, "fetch_mr_diffs: skipping malformed diff entry in fallback",
					zap.Int("mr_iid", mrIID), zap.Error(err))
				continue
			}
			path := entry.NewPath
			if entry.DeletedFile {
				path = entry.OldPath
			}
			if path == "" {
				continue
			}
			isModifiedCandidate := !entry.DeletedFile && !entry.RenamedFile && !entry.NewFile
			if isModifiedCandidate {
				modifiedCandidates++
			}

			if entry.Diff == "" {
				if isModifiedCandidate {
					truncatedFiles++
				}
				continue
			}

			// Synthesize the unified-diff envelope so a unified-diff parser starts a
			// new file on the `+++ b/` line; GitLab's `diff` is just the hunk
			// body (starting with `@@`), no file headers. A deleted file must use
			// `+++ /dev/null`, the unified-diff marker of a deletion; without it a
			// parser would count the file as a changed file with its removed
			// lines misread as still-present content.
			newTarget := "b/" + path
			if entry.DeletedFile {
				newTarget = "/dev/null"
			}
			fmt.Fprintf(&buf, "diff --git a/%s b/%s\n--- a/%s\n+++ %s\n%s\n", path, path, path, newTarget, entry.Diff)
			if buf.Len() > mrDiffFallbackMaxSizeBytes {
				return nil, errMRDiffTooLarge
			}
		}

		if resp.GetNextPageUrl() == "" {
			completedPages = true
			break
		}
	}

	// Exhausted the page cap with more pages pending — unrecoverable via pagination.
	if !completedPages {
		return nil, errMRDiffTooLarge
	}

	// Too many modified text files missing their diff body → a reader would
	// see a materially incomplete picture; sentinel rather than persist
	// a misleading line count.
	if mrDiffsMateriallyTruncated(truncatedFiles, modifiedCandidates) {
		return nil, errMRDiffTooLarge
	}

	return buf.Bytes(), nil
}
