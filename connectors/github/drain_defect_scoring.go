// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"

	"go.uber.org/zap"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk/lineage"
)

const (
	opDrainDefectScoring = "drain_defect_scoring"
	// defectScoringBatchSize caps how many candidate rows one tick claims. Larger than the blame
	// batch: scoring a candidate is at most two file fetches (usually served from the verdict cache),
	// not a multi-request blame walk.
	defectScoringBatchSize = 50
	// perFixScoringTimeout bounds scoring one fix's candidates (the file fetches + AST compares).
	// It is a worst-case cap, not the expected time: a fix's files are usually served from the
	// verdict cache, so a tick normally scores its whole batch well inside the 10m scheduler-default
	// ExecutionTimeout this op runs under. When a degraded vendor makes the sequential worst case
	// (batch * this cap) overrun 10m, the tick is cut mid-loop: the candidates it did not reach
	// stay unscored for a later tick. That is safe: SubmitStructuralVerdict is idempotent, and the
	// host offers only unscored candidates again. Keep batch * cap within a small multiple of the
	// 10m deadline so a healthy tick drains its claim; do not raise the batch without also bounding
	// the per-fix cap.
	perFixScoringTimeout = 60 * time.Second
)

// drainDefectScoringRequiresHost lists the host services the operation cannot run without.
var drainDefectScoringRequiresHost = []sdk.HostService{sdk.HostBlameSink, sdk.HostDefectScorer}

// DrainDefectScoringOp is the pull side of the structural-scoring pass: it claims unscored candidates
// from the host (grouped by fix), computes each candidate's AST proximity verdict against the fix's
// anchors, and submits it. The host combines the verdict with the other components and writes the
// score; this op owns only the provider fetch + AST compare.
type DrainDefectScoringOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *DrainDefectScoringOp) OperationID() sdk.OperationID { return opDrainDefectScoring }

// Execute claims one batch of unscored candidates and scores each fix's group. It fails,
// terminal, without the host's blame sink and defect scorer.
func (op *DrainDefectScoringOp) Execute(parentCtx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID

	if missing := params.Host.Missing(drainDefectScoringRequiresHost); len(missing) > 0 {
		return nil, sdk.MissingHostServicesError(connectorID, op.OperationID(), orgID, missing)
	}

	resp, err := params.Host.BlameSink.ListPendingStructuralScoring(parentCtx, &defectlineagev1.ListPendingStructuralScoringRequest{
		OrganizationId: orgID,
		ConnectorId:    string(connectorID),
		Limit:          defectScoringBatchSize,
	})
	if err != nil {
		logger.Error(parentCtx, "drain_defect_scoring: claiming pending scoring failed",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, string(connectorID)), zap.Error(err))
		return nil, fmt.Errorf("claiming pending structural scoring: %w", err)
	}

	pending := resp.GetPending()
	if len(pending) == 0 {
		return params.Cursor, nil
	}

	// FileAtRevision + FirstParent are GraphQL; the culprit commit-diff is a REST call, so it is metered
	// against GitHub's separate REST budget via its own gateway group rather than polluting the GraphQL
	// limiter.
	gw := params.VendorGateway.ForGroup(constants.GroupGitHubGraphQL)
	restGW := params.VendorGateway.ForGroup(constants.GroupGitHubREST)
	for _, fix := range pending {
		ctx, cancel := context.WithTimeout(parentCtx, perFixScoringTimeout)
		repoFullName := fix.GetRepoFullName()
		src := &githubBlameSource{gw: gw, repoFullName: repoFullName}
		fetch := func(fctx context.Context, revision, path string) ([]byte, string, error) {
			return FileAtRevision(fctx, gw, orgID, repoFullName, revision, path)
		}
		culpritLines := func(fctx context.Context, sha, path string) ([]lineage.AddedLine, error) {
			return fetchGitHubCulpritAddedLines(fctx, params.Host.DefectScorer, restGW, orgID, repoFullName, sha, path)
		}
		params.Host.DefectScorer.ScorePendingFix(ctx, params.Host.BlameSink, params.FetchCache, string(connectorID), orgID, fetch, src.FirstParent, culpritLines, fix)
		cancel()
	}
	return params.Cursor, nil
}

// fetchGitHubCulpritAddedLines returns the lines the commit added to path, each with its 1-based
// new-file line number, by reading the commit's own unified diff. GitHub's blame stores only the
// culprit path (its blame reports the queried-revision position, which drifts), so scoring recovers the
// exact line from this diff. A commit that did not touch path (or an unavailable diff — 404/too-large)
// returns nil so scoring degrades to same-file coverage; only a transient error propagates for retry.
func fetchGitHubCulpritAddedLines(ctx context.Context, scorer lineage.Scorer, gw sdk.VendorGateway, orgID, repoFullName, sha, path string) ([]lineage.AddedLine, error) {
	owner, repo, ok := splitRepoFullName(repoFullName)
	if !ok {
		return nil, fmt.Errorf("bad repo full name %q", repoFullName)
	}
	url := fmt.Sprintf("%s/repos/%s/%s/commits/%s", githubRESTBase(orgID), owner, repo, sha)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building commit-diff request %s: %w", sha, err)
	}
	req.Header.Set("Accept", "application/vnd.github.diff")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := gw.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("commit-diff request %s: %w", sha, err)
	}
	defer resp.Body.Close() //nolint:errcheck,gosec // best-effort close on a read-only diff fetch
	switch resp.StatusCode {
	case http.StatusOK:
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxDiffSizeBytes+1))
		if readErr != nil {
			return nil, fmt.Errorf("reading commit diff %s: %w", sha, readErr)
		}
		if len(body) > maxDiffSizeBytes {
			// Over the cap: the body is truncated, so parsing it could miss the target file's added
			// line and silently recover a wrong one. Degrade to same-file coverage instead.
			return nil, nil
		}
		return scorer.AddedLinesForPath(body, path), nil
	case http.StatusNotFound, http.StatusUnprocessableEntity, http.StatusNotAcceptable:
		// Commit gone, or a diff too large / not renderable to serve — recovery unavailable, degrade.
		return nil, nil
	default:
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("commit-diff request %s: %w", sha, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, errBody, resp.Header))
	}
}
