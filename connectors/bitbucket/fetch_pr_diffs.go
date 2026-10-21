// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// fetch_pr_diffs for Bitbucket.
//
// Endpoint: GET /2.0/repositories/{workspace}/{repo}/pullrequests/{id}/diff
// Returns: text/plain unified diff.
// S3 key: pr-diffs/{orgID}/bitbucket/{platform_repo_id}/{pr_number}.patch
// Sentinels: 404 → NOT_AVAILABLE, 555 → TOO_LARGE, body > 10MB → TOO_LARGE.

const (
	bbPerPRTimeout     = 30 * time.Second
	bbMaxDiffSizeBytes = 10 * 1024 * 1024
	bbDiffFetchBatch   = 50
	bbSentinelNotAvail = "NOT_AVAILABLE"
	bbSentinelTooLarge = "TOO_LARGE"
	opFetchPRDiffs     = "fetch_pr_diffs"
	opFetchRepos       = "fetch_repos"
	opFetchRepoTree    = "fetch_repo_tree"
)

var (
	errBBDiffNotAvailable = errors.New("bitbucket diff not available")
	errBBDiffTooLarge     = errors.New("bitbucket diff too large")
)

// fetchPRDiffsRequiresHost lists the host services the operation cannot run without.
var fetchPRDiffsRequiresHost = []sdk.HostService{sdk.HostWorkQueue}

// bbDiffRespMeta carries the response fields the caller needs (rate-limit
// header refresh) after the diff helper has already drained and closed the
// response body. Returning this instead of a live *http.Response keeps the
// body-close responsibility entirely inside the helper.
type bbDiffRespMeta struct {
	Header http.Header
}

func newBBDiffRespMeta(resp *http.Response) *bbDiffRespMeta {
	if resp == nil {
		return nil
	}
	return &bbDiffRespMeta{Header: resp.Header}
}

// FetchPRDiffsOp fetches unified diffs for merged Bitbucket PRs and records
// the resulting S3 key (or a sentinel) via host's gRPC WorkQueue.
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
		Limit:          bbDiffFetchBatch,
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

	gw := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)

	for _, pr := range prs {
		// Bitbucket rate-limits per user token; scope key includes org+user.
		ok, wait, err := params.BudgetLimiter.Allow(parentCtx, orgID, ratelimiter.BucketDefault, ratelimiter.BitbucketCoreFloor)
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

		ctx, cancel := context.WithTimeout(parentCtx, bbPerPRTimeout)
		op.processOne(ctx, gw, params.BlobStore, params.Host.WorkQueue, params.BudgetLimiter, orgID, pr)
		cancel()
	}

	return params.Cursor, nil
}

// processOne fetches a single PR diff. Per-PR errors never propagate.
func (op *FetchPRDiffsOp) processOne(
	ctx context.Context,
	gw sdk.VendorGateway,
	s3Client bbS3Writer,
	analytics sdk.WorkQueue,
	budget sdk.BudgetObserver,
	orgID string,
	pr *workqueuev1.PRDiffFetchCandidate,
) {
	workspace, repoSlug, ok := splitRepoFullName(pr.GetRepoFullName())
	if !ok {
		logger.Error(ctx, "fetch_pr_diffs: invalid repo full name — skipping",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.String("repo", pr.GetRepoFullName()),
		)
		return
	}

	diffBytes, httpResp, err := fetchBitbucketPRDiff(ctx, gw, orgID, workspace, repoSlug, int(pr.GetPrNumber()))
	if httpResp != nil {
		if refreshErr := budget.UpdateFromResponse(ctx, orgID, httpResp.Header); refreshErr != nil {
			logger.Error(ctx, "fetch_pr_diffs: budget refresh from response headers failed",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.PullRequestID, pr.GetPullRequestId()),
				zap.Error(refreshErr),
			)
		}
	}

	switch {
	case errors.Is(err, errBBDiffNotAvailable):
		op.markSentinel(ctx, analytics, pr.GetPullRequestId(), bbSentinelNotAvail, orgID)
		return
	case errors.Is(err, errBBDiffTooLarge):
		op.markSentinel(ctx, analytics, pr.GetPullRequestId(), bbSentinelTooLarge, orgID)
		return
	case err != nil:
		logger.Error(ctx, "fetch_pr_diffs: fetching PR diff — leaving NULL for retry",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.PullRequestID, pr.GetPullRequestId()),
			zap.String("repo", pr.GetRepoFullName()),
			zap.Int32("pr_number", pr.GetPrNumber()),
			zap.Error(err),
		)
		return
	}

	if len(diffBytes) > bbMaxDiffSizeBytes {
		op.markSentinel(ctx, analytics, pr.GetPullRequestId(), bbSentinelTooLarge, orgID)
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

// bbS3Writer is the narrow S3 surface this op needs.
type bbS3Writer interface {
	Write(ctx context.Context, dst string, content []byte, aclString string) error
}

// splitRepoFullName splits "workspace/repo_slug" into its components.
func splitRepoFullName(fullName string) (workspace, repo string, ok bool) {
	idx := strings.IndexByte(fullName, '/')
	if idx <= 0 || idx == len(fullName)-1 {
		return "", "", false
	}
	return fullName[:idx], fullName[idx+1:], true
}

// fetchBitbucketPRDiff issues GET /2.0/repositories/{ws}/{repo}/pullrequests/{id}/diff.
// Returns raw diff bytes, the response metadata (for budget refresh),
// or a typed sentinel error.
func fetchBitbucketPRDiff(ctx context.Context, gw sdk.VendorGateway, orgID, workspace, repoSlug string, prID int) ([]byte, *bbDiffRespMeta, error) {
	url := fmt.Sprintf("%s/repositories/%s/%s/pullrequests/%d/diff", bitbucketRESTBase(orgID), workspace, repoSlug, prID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("building diff request: %w", err)
	}

	resp, err := gw.Do(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("executing diff request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close on read-only diff fetch
	meta := newBBDiffRespMeta(resp)

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, bbMaxDiffSizeBytes+1))
		if err != nil {
			return nil, meta, fmt.Errorf("reading diff body: %w", err)
		}
		return body, meta, nil
	case http.StatusNotFound:
		return nil, meta, errBBDiffNotAvailable
	// Bitbucket returns 555 for oversized diffs; also accept 413.
	case 555, http.StatusRequestEntityTooLarge:
		return nil, meta, errBBDiffTooLarge
	default:
		return nil, meta, fmt.Errorf("unexpected status %d fetching diff", resp.StatusCode)
	}
}
