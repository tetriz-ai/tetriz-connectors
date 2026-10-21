// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// closeDiffRespBody closes a discarded /files response body, logging a
// non-nil close error rather than dropping it.
func closeDiffRespBody(ctx context.Context, resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if cerr := resp.Body.Close(); cerr != nil {
		logger.WarnWithCtx(ctx, "closing /files response body", zap.Error(cerr))
	}
}

// Responsibilities per tick:
//   - Ask the host for a batch of merged PRs with a NULL diff_s3_key.
//   - For each PR: check the header-aware budget; fetch the unified
//     diff via VendorGateway.Do (raw HTTP, Accept: application/vnd.github.diff);
//     refresh the budget from response headers; classify sentinels (404/422/
//     >10MB -> "NOT_AVAILABLE" / "TOO_LARGE").
//   - On HTTP 406 from the unified-diff endpoint (GitHub refusing to render
//     a too-large diff — exceeds ~3000-20000 lines or 300 files), fall back
//     to the /pulls/{n}/files paginated endpoint and reconstruct a unified
//     diff from each file's `patch` field. The reconstructed bytes go to S3
//     under the same key shape so a reader of the key gets the same bytes either way. The
//     fallback path uses maxFallbackDiffSizeBytes (see const).
//   - Upload successful diffs to S3 at pr-diffs/{org}/github/{platform_repo_id}/
//     {pr_number}.patch and UpdatePRDiffStatus with the resulting key.
//   - Per-PR 30s deadline guards against a single stuck request starving the
//     batch.
//
// Every Execute returns the current cursor and a nil error: a single PR
// failure must never fail the operation. The next tick reuses
// ListPRsPendingDiffFetch as the work queue.

const (
	perPRTimeout     = 30 * time.Second
	maxDiffSizeBytes = 10 * 1024 * 1024
	// maxFallbackDiffSizeBytes caps the reconstructed unified diff from the
	// /files paginated fallback path. Larger than maxDiffSizeBytes because
	// the fallback exists precisely to handle PRs the primary endpoint
	// refuses, and because pagination avoids the monolithic single-response
	// memory pressure that motivated the 10 MB primary cap. It also bounds
	// the memory of whatever reads the stored diffs.
	maxFallbackDiffSizeBytes = 50 * 1024 * 1024
	diffFetchBatchSize       = 50

	sentinelNotAvailable = "NOT_AVAILABLE"
	sentinelTooLarge     = "TOO_LARGE"

	opFetchPRDiffs = "fetch_pr_diffs"
)

// fetchPRDiffsRequiresHost lists the host services the operation cannot run without.
var fetchPRDiffsRequiresHost = []sdk.HostService{sdk.HostWorkQueue}

// diffRespMeta carries the response fields the caller needs (rate-limit header
// refresh, status classification) after the diff helper has already drained and
// closed the response body. Returning this instead of a live *http.Response
// keeps the body-close responsibility entirely inside the helper.
type diffRespMeta struct {
	Header     http.Header
	StatusCode int
}

func newDiffRespMeta(resp *http.Response) *diffRespMeta {
	if resp == nil {
		return nil
	}
	return &diffRespMeta{Header: resp.Header, StatusCode: resp.StatusCode}
}

var (
	// errDiffNotAvailable signals a 404 — PR deleted or otherwise unreachable.
	// Mark sentinel so the PR is never re-queued.
	errDiffNotAvailable = errors.New("diff not available")
	// errDiffTooLarge signals a 422 from the unified-diff endpoint or 406 from
	// the /files fallback (PR exceeds /files own ~3000-file cap). Mark sentinel.
	errDiffTooLarge = errors.New("diff too large")
	// errDiffNeedsPerFileFallback signals a 406 from the unified-diff endpoint —
	// GitHub refuses to compute a single-response diff because the PR exceeds
	// its server-side size or file-count limit, but the PR is still recoverable
	// via the paginated /files endpoint. Triggers fetchGitHubPRDiffViaFiles.
	errDiffNeedsPerFileFallback = errors.New("diff needs per-file fallback")
)

// FetchPRDiffsOp fetches unified diffs for merged GitHub PRs whose entity row
// has diff_s3_key = NULL, uploads each diff to S3, and notifies the host
// via the UpdatePRDiffStatus gRPC.
type FetchPRDiffsOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchPRDiffsOp) OperationID() sdk.OperationID { return opFetchPRDiffs }

// Execute fetches one batch of pending diffs. It fails, terminal, without the
// host's work queue, a blob store or a budget limiter.
func (op *FetchPRDiffsOp) Execute(parentCtx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID

	if missing := params.Host.Missing(fetchPRDiffsRequiresHost); len(missing) > 0 {
		return nil, sdk.MissingHostServicesError(connectorID, op.OperationID(), orgID, missing)
	}
	if params.BlobStore == nil || params.BudgetLimiter == nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid, Terminal: true,
			Cause: fmt.Errorf("fetch_pr_diffs needs a blob store and a budget limiter (blob store set: %t, budget limiter set: %t)",
				params.BlobStore != nil, params.BudgetLimiter != nil),
		}
	}

	resp, err := params.Host.WorkQueue.ListPRsPendingDiffFetch(parentCtx, &workqueuev1.ListPRsPendingDiffFetchRequest{
		OrganizationId: orgID,
		ConnectorId:    string(connectorID),
		Limit:          diffFetchBatchSize,
	})
	if err != nil {
		logger.Error(parentCtx, "listing PRs pending diff fetch",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.ConnectorID, string(connectorID)),
			zap.Error(err),
		)
		return nil, fmt.Errorf("listing PRs pending diff fetch: %w", err)
	}

	prs := resp.GetPrs()
	if len(prs) == 0 {
		return params.Cursor, nil
	}

	gw := params.VendorGateway.ForGroup(constants.GroupGitHubREST)

	for _, pr := range prs {
		ok, wait, err := params.BudgetLimiter.Allow(parentCtx, orgID, ratelimiter.BucketGitHubCore, ratelimiter.GitHubCoreFloor)
		if err != nil {
			logger.Error(parentCtx, "fetch_pr_diffs: budget check failed — deferring batch",
				zap.String(logger.OrgID, orgID),
				zap.Error(err),
			)
			return params.Cursor, nil
		}
		if !ok {
			logger.Info(parentCtx, "fetch_pr_diffs: budget floor hit — deferring remaining PRs to next tick",
				zap.String(logger.OrgID, orgID),
				zap.Duration("reset_in", wait),
			)
			return params.Cursor, nil
		}

		ctx, cancel := context.WithTimeout(parentCtx, perPRTimeout)
		op.processOne(ctx, gw, params.BlobStore, params.Host.WorkQueue, params.BudgetLimiter, orgID, pr)
		cancel()
	}

	return params.Cursor, nil
}

// processOne fetches a single PR diff and records the outcome. Per-PR errors
// never propagate — they are logged and swallowed so the batch can continue.
func (op *FetchPRDiffsOp) processOne(
	ctx context.Context,
	gw sdk.VendorGateway,
	s3Client s3Writer,
	analytics sdk.WorkQueue,
	budget sdk.BudgetObserver,
	orgID string,
	pr *workqueuev1.PRDiffFetchCandidate,
) {
	owner, repo, ok := splitRepoFullName(pr.GetRepoFullName())
	if !ok {
		logger.Error(ctx, "fetch_pr_diffs: invalid repo full name — skipping",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.String("repo", pr.GetRepoFullName()),
		)
		return
	}

	diffBytes, httpResp, err := fetchGitHubPRDiff(ctx, gw, orgID, owner, repo, int(pr.GetPrNumber()))
	if httpResp != nil {
		if refreshErr := budget.UpdateFromResponse(ctx, orgID, httpResp.Header); refreshErr != nil {
			logger.Error(ctx, "fetch_pr_diffs: budget refresh from response headers failed",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.PullRequestID, pr.GetPullRequestId()),
				zap.Error(refreshErr),
			)
		}
	}

	// Primary path uses maxDiffSizeBytes; /files fallback overrides to
	// maxFallbackDiffSizeBytes when it kicks in below.
	sizeCap := maxDiffSizeBytes

	switch {
	case errors.Is(err, errDiffNotAvailable):
		op.markSentinel(ctx, analytics, pr.GetPullRequestId(), sentinelNotAvailable, orgID)
		return
	case errors.Is(err, errDiffNeedsPerFileFallback):
		logger.Info(ctx, "fetch_pr_diffs: falling back to /files API for large PR",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.String("repo", pr.GetRepoFullName()),
			zap.Int32("pr_number", pr.GetPrNumber()),
		)
		sizeCap = maxFallbackDiffSizeBytes
		diffBytes, httpResp, err = fetchGitHubPRDiffViaFiles(ctx, gw, orgID, owner, repo, int(pr.GetPrNumber()))
		if httpResp != nil {
			if refreshErr := budget.UpdateFromResponse(ctx, orgID, httpResp.Header); refreshErr != nil {
				logger.Error(ctx, "fetch_pr_diffs: budget refresh from /files response failed",
					zap.String(logger.OrgID, orgID),
					zap.String(logger.PullRequestID, pr.GetPullRequestId()),
					zap.Error(refreshErr),
				)
			}
		}
		switch {
		case errors.Is(err, errDiffNotAvailable):
			op.markSentinel(ctx, analytics, pr.GetPullRequestId(), sentinelNotAvailable, orgID)
			return
		case errors.Is(err, errDiffTooLarge):
			op.markSentinel(ctx, analytics, pr.GetPullRequestId(), sentinelTooLarge, orgID)
			return
		case err != nil:
			logger.Error(ctx, "fetch_pr_diffs: /files fallback failed — leaving NULL for retry",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.PullRequestID, pr.GetPullRequestId()),
				zap.String("repo", pr.GetRepoFullName()),
				zap.Int32("pr_number", pr.GetPrNumber()),
				zap.Error(err),
			)
			return
		}
		// Fallback succeeded; diffBytes carries the reconstructed unified diff.
		// Fall through to the size check + S3 upload below.
	case errors.Is(err, errDiffTooLarge):
		op.markSentinel(ctx, analytics, pr.GetPullRequestId(), sentinelTooLarge, orgID)
		return
	case err != nil:
		if httpResp != nil {
			emitPermissionDeniedIfApplicable(string(connectorID), opFetchPRDiffs, orgID, httpResp.StatusCode)
		}
		logger.Error(ctx, "fetch_pr_diffs: fetching PR diff — leaving NULL for retry",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.String("repo", pr.GetRepoFullName()),
			zap.Int32("pr_number", pr.GetPrNumber()),
			zap.Error(err),
		)
		return
	}

	// Outer size guard. Primary path: io.LimitReader caps at sizeCap+1, so
	// reaching sizeCap+1 here signals GitHub sent more than we should store.
	// Fallback path: redundant with the in-loop check inside
	// fetchGitHubPRDiffViaFiles, kept as belt-and-suspenders.
	if len(diffBytes) > sizeCap {
		op.markSentinel(ctx, analytics, pr.GetPullRequestId(), sentinelTooLarge, orgID)
		return
	}

	s3Key := fmt.Sprintf("pr-diffs/%s/%s/%s/%d.patch", orgID, connectorID, pr.GetPlatformRepoId(), pr.GetPrNumber())
	if err := s3Client.Write(ctx, s3Key, diffBytes, ""); err != nil {
		logger.Error(ctx, "fetch_pr_diffs: uploading diff to S3",
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
		logger.Error(ctx, "fetch_pr_diffs: updating PR diff status",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.Error(err),
		)
	}
}

// markSentinel records a terminal non-recoverable outcome so the PR is
// excluded from future ListPRsPendingDiffFetch results.
func (op *FetchPRDiffsOp) markSentinel(
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
		logger.Error(ctx, "fetch_pr_diffs: marking sentinel",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pullRequestID),
			zap.String("sentinel", sentinel),
			zap.Error(err),
		)
	}
}

// s3Writer is the narrow S3 surface this op needs, matching the signature of
// blobstore.Store.Write — declared locally so tests can stub it
// without pulling in the full AWS SDK surface.
type s3Writer interface {
	Write(ctx context.Context, dst string, content []byte, aclString string) error
}

// splitRepoFullName splits "owner/repo" into its two components. Returns
// ok=false for malformed input so the caller can skip the PR safely.
func splitRepoFullName(fullName string) (owner, repo string, ok bool) {
	idx := strings.IndexByte(fullName, '/')
	if idx <= 0 || idx == len(fullName)-1 {
		return "", "", false
	}
	return fullName[:idx], fullName[idx+1:], true
}

// resolveRepoOwner derives the GraphQL owner + repo name for a tracked repo.
// A repo is normally stored as "owner/repo" (the connect picker and
// auto-discovery both persist full names), so the owner comes from the name
// itself — this is what lets a PAT individual (whose settings.organization is
// empty) and any cross-owner repo resolve correctly. A bare name with no "/"
// takes fallbackOwner (settings.organization) as its owner.
func resolveRepoOwner(repoFullName, fallbackOwner string) (owner, repo string) {
	if o, r, ok := splitRepoFullName(repoFullName); ok {
		return o, r
	}
	return fallbackOwner, repoFullName
}

// emitPermissionDeniedIfApplicable bumps the shared
// VendorPermissionDeniedTotal counter when the vendor response status
// indicates a per-org permission gap. 403 = forbidden, which means the
// installation lacks the scope and an admin must re-consent. Other 4xx
// statuses are unrelated and handled elsewhere in the switch:
// 404 → errDiffNotAvailable, 406 → errDiffNeedsPerFileFallback (size
// limit on the unified-diff endpoint, recoverable via /files), 422 →
// errDiffTooLarge.
func emitPermissionDeniedIfApplicable(connectorID, operationID, orgID string, statusCode int) {
	if statusCode != http.StatusForbidden {
		return
	}
	obs.VendorPermissionDeniedTotal.WithLabelValues(
		connectorID,
		operationID,
		strconv.Itoa(statusCode),
		tenantlabel.OrgID(orgID),
	).Inc()
}

// fetchGitHubPRDiff issues GET /repos/{owner}/{repo}/pulls/{number} with
// Accept: application/vnd.github.diff and returns the raw diff bytes, the
// response metadata (for header-based budget refresh), or a typed
// sentinel error (errDiffNotAvailable / errDiffTooLarge).
//
// NOTE: This uses VendorGateway.Do so it bypasses the proto Execute path.
// The vendor gateway's rate-limit and circuit-breaker middleware still apply.
// GitHub App installation-token auth is injected by the concrete gateway's
// HTTPDoer — the gateway tests and connector tests cover the live path.
func fetchGitHubPRDiff(ctx context.Context, gw sdk.VendorGateway, orgID, owner, repo string, prNumber int) ([]byte, *diffRespMeta, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d", githubRESTBase(orgID), owner, repo, prNumber)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("building diff request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.diff")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := gw.Do(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("executing diff request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close on read-only diff fetch
	meta := newDiffRespMeta(resp)

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxDiffSizeBytes+1))
		if err != nil {
			return nil, meta, fmt.Errorf("reading diff body: %w", err)
		}
		return body, meta, nil
	case http.StatusNotFound:
		return nil, meta, errDiffNotAvailable
	case http.StatusUnprocessableEntity:
		return nil, meta, errDiffTooLarge
	case http.StatusNotAcceptable:
		// GitHub returns 406 with a JSON `{"code":"too_large"}` body when the
		// PR's diff exceeds its server-side size or file-count cap. The PR is
		// still recoverable via the paginated /files endpoint — caller flips
		// to fetchGitHubPRDiffViaFiles.
		return nil, meta, errDiffNeedsPerFileFallback
	default:
		return nil, meta, fmt.Errorf("unexpected status %d fetching diff", resp.StatusCode)
	}
}

const (
	// perFilePerPage is the maximum GitHub allows on /pulls/{n}/files.
	perFilePerPage = 100
	// perFileMaxPages bounds the loop. /files paginates up to ~3000 files
	// (30 pages × 100/page); anything beyond exceeds the endpoint's own cap.
	perFileMaxPages = 30
	// truncatedTextRatioThreshold + truncatedTextAbsoluteFloor: if too many
	// files have empty `patch` AND non-zero additions/deletions (real per-
	// file content loss; binaries / renames / mode-only / submodule entries
	// report 0/0 and don't count), the reconstructed diff is materially
	// incomplete and we sentinel TOO_LARGE rather than persist a misleading
	// line count.
	truncatedTextRatioThreshold = 0.10
	truncatedTextAbsoluteFloor  = 10
)

// gitHubPRFile is the subset of /pulls/{n}/files we read to reconstruct a
// unified diff. `patch` is empty in several distinct cases (binary file,
// per-file >300 KB truncation, rename/mode-change/submodule update with no
// content delta). Additions+Deletions distinguish "GitHub stripped content
// we needed" from "there was no content to diff in the first place".
type gitHubPRFile struct {
	Filename  string `json:"filename"`
	Status    string `json:"status"`
	Patch     string `json:"patch"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

// fetchGitHubPRDiffViaFiles reconstructs a unified diff from
// /repos/{owner}/{repo}/pulls/{n}/files paginated. Used as a fallback when
// the primary /pulls/{n} unified-diff endpoint returns 406.
//
// Returns errDiffTooLarge when:
//   - /files itself returns 406 (PR exceeds the /files endpoint's own ~3000
//     -file cap; genuinely unrecoverable via API).
//   - Reconstructed bytes exceed maxFallbackDiffSizeBytes.
//   - More than truncatedTextAbsoluteFloor files have empty `patch` AND
//     non-zero additions/deletions, AND those files exceed
//     truncatedTextRatioThreshold of the total (a reader would see a
//     materially incomplete diff). Cases where GitHub reports
//     `additions+deletions == 0` (binary files, renames without edits,
//     mode-only changes, submodule/LFS pointer updates) don't count —
//     there was no content to lose.
func fetchGitHubPRDiffViaFiles(ctx context.Context, gw sdk.VendorGateway, orgID, owner, repo string, prNumber int) ([]byte, *diffRespMeta, error) {
	var (
		buf                bytes.Buffer
		lastMeta           *diffRespMeta
		totalFiles         int
		totalTextFiles     int
		truncatedTextFiles int
	)

	restBase := githubRESTBase(orgID)
	for page := 1; page <= perFileMaxPages; page++ {
		url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/files?per_page=%d&page=%d",
			restBase, owner, repo, prNumber, perFilePerPage, page)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, lastMeta, fmt.Errorf("building /files request page %d: %w", page, err)
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

		resp, err := gw.Do(ctx, req)
		if err != nil {
			return nil, lastMeta, fmt.Errorf("executing /files request page %d: %w", page, err)
		}
		meta := newDiffRespMeta(resp)
		lastMeta = meta

		switch resp.StatusCode {
		case http.StatusOK:
		case http.StatusNotAcceptable:
			// /files own ceiling — PR exceeds even the paginated endpoint's
			// ~3000-file cap. Genuinely unrecoverable via the API.
			closeDiffRespBody(ctx, resp)
			return nil, meta, errDiffTooLarge
		case http.StatusNotFound:
			// PR was deleted between the primary diff call and the fallback.
			closeDiffRespBody(ctx, resp)
			return nil, meta, errDiffNotAvailable
		default:
			closeDiffRespBody(ctx, resp)
			return nil, meta, fmt.Errorf("unexpected status %d fetching /files page %d", resp.StatusCode, page)
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxFallbackDiffSizeBytes+1))
		linkHeader := resp.Header.Get("Link")
		closeDiffRespBody(ctx, resp)
		if err != nil {
			return nil, meta, fmt.Errorf("reading /files response page %d: %w", page, err)
		}

		var files []gitHubPRFile
		if err := json.Unmarshal(body, &files); err != nil {
			return nil, meta, fmt.Errorf("parsing /files JSON page %d: %w", page, err)
		}

		for _, f := range files {
			totalFiles++
			totalTextFiles++
			if f.Patch == "" {
				// Empty `patch` covers four cases — only one is real content
				// loss a diff reader would have used:
				//   - per-file >300 KB content stripped by GitHub (real loss)
				//   - binary file (GitHub never computes a textual diff)
				//   - rename without edits / mode-only change
				//   - submodule or LFS pointer update
				// GitHub reports `additions: 0, deletions: 0` for the last
				// three; the first carries the real line counts. Use that
				// invariant instead of a maintained extension list.
				if f.Additions > 0 || f.Deletions > 0 {
					truncatedTextFiles++
				}
				continue
			}
			// Synthesize per-file unified-diff headers so the stored object is
			// a complete unified diff: a unified-diff parser starts a new file
			// at the `+++ b/<path>` line and counts the patch's +/- lines under
			// it. GitHub's /files `patch` field is just the hunk body (starting
			// with `@@`); without the `--- a/X` / `+++ b/X` envelope a parser
			// reads every line as header before the first `+++`.
			fmt.Fprintf(&buf, "diff --git a/%s b/%s\n--- a/%s\n+++ b/%s\n%s\n",
				f.Filename, f.Filename, f.Filename, f.Filename, f.Patch)
			if buf.Len() > maxFallbackDiffSizeBytes {
				return nil, meta, errDiffTooLarge
			}
		}

		if !hasNextPage(linkHeader) {
			break
		}
	}

	if truncatedTextFiles > 0 {
		logger.Info(ctx, "fetch_pr_diffs: /files fallback produced partial diff — some text files lacked patch content",
			zap.Int("total_files", totalFiles),
			zap.Int("text_files", totalTextFiles),
			zap.Int("truncated_text_files", truncatedTextFiles),
		)
	}

	// Threshold check: too many text-file patches missing → a reader
	// would see a materially incomplete picture; sentinel rather than
	// persist a misleading line count.
	if truncatedTextFiles >= truncatedTextAbsoluteFloor &&
		totalTextFiles > 0 &&
		float64(truncatedTextFiles)/float64(totalTextFiles) > truncatedTextRatioThreshold {
		return nil, lastMeta, errDiffTooLarge
	}

	return buf.Bytes(), lastMeta, nil
}

// hasNextPage returns true if a GitHub Link header carries a `rel="next"`
// entry. The presence check is sufficient; we generate page URLs ourselves
// with an incrementing index, so we don't need to parse the URL out.
func hasNextPage(linkHeader string) bool {
	return strings.Contains(linkHeader, `rel="next"`)
}
