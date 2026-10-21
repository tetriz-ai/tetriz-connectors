// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
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
	// defectScoringBatchSize caps how many candidate rows one tick claims. Scoring a candidate is at
	// most one file fetch per anchor in the candidate's own file (usually served from the verdict
	// cache) plus one commit lookup per fix, far cheaper than a reconstruction, so it claims more
	// fixes than the lineage drain.
	defectScoringBatchSize = 50
	// perFixScoringTimeout is a worst-case cap, not the expected time (files are usually served from
	// the verdict cache), so a healthy tick scores its whole batch inside the 10m scheduler-default
	// ExecutionTimeout. If a degraded vendor makes batch*cap overrun 10m, the tick is cut and the
	// candidates it did not reach stay unscored for a later tick. That is safe: SubmitStructuralVerdict
	// is idempotent, and the host offers only unscored candidates again. Do not raise batch without
	// bounding this cap.
	perFixScoringTimeout = 60 * time.Second
	// commitBodyLimit caps the in-memory size of a commit lookup response, read only for its parents.
	commitBodyLimit = 1 << 20 // 1 MiB
)

// drainDefectScoringRequiresHost lists the host services the operation cannot run without.
var drainDefectScoringRequiresHost = []sdk.HostService{sdk.HostBlameSink, sdk.HostDefectScorer}

// DrainDefectScoringOp is the pull side of the structural-scoring pass for Bitbucket: it claims
// unscored candidates from the host (grouped by fix), computes each candidate's AST proximity
// verdict against the fix's anchors, and submits it. The host combines the verdict with the other
// components and writes the score; this op owns only the provider fetch + AST compare.
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

	gw := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)
	for _, fix := range pending {
		ctx, cancel := context.WithTimeout(parentCtx, perFixScoringTimeout)
		repoFullName := fix.GetRepoFullName()
		fetch := func(fctx context.Context, revision, path string) ([]byte, string, error) {
			return FileAtRevision(fctx, gw, orgID, repoFullName, revision, path)
		}
		firstParent := func(fctx context.Context, sha string) (string, error) {
			return bitbucketFirstParent(fctx, gw, orgID, repoFullName, sha)
		}
		// nil culprit-lines fn: Bitbucket's reconstruction already stored the exact candidate line, so
		// scoring never needs to recover it from the commit diff (unlike GitHub/GitLab blame).
		params.Host.DefectScorer.ScorePendingFix(ctx, params.Host.BlameSink, params.FetchCache, string(connectorID), orgID, fetch, firstParent, nil, fix)
		cancel()
	}
	return params.Cursor, nil
}

// bbCommitResponse models the commit lookup, read only for its parents.
type bbCommitResponse struct {
	Parents []struct {
		Hash string `json:"hash"`
	} `json:"parents"`
}

// bitbucketFirstParent returns sha's first-parent commit, or "" when sha is a root commit. Bitbucket
// has no blame source with a FirstParent method (its lineage walk reconstructs from file history), so
// scoring resolves the fix's parent revision — the revision the removed lines still exist in — with a
// direct commit lookup.
func bitbucketFirstParent(ctx context.Context, gw sdk.VendorGateway, orgID, repoFullName, sha string) (string, error) {
	workspace, repoSlug, ok := splitRepoFullName(repoFullName)
	if !ok {
		return "", fmt.Errorf("bad repo full name %q", repoFullName)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, bitbucketCommitURL(orgID, workspace, repoSlug, sha), nil)
	if err != nil {
		return "", fmt.Errorf("building commit request %s: %w", sha, err)
	}
	resp, err := gw.Do(ctx, req)
	if err != nil {
		return "", fmt.Errorf("commit lookup %s: %w", sha, err)
	}
	if resp.Body == nil {
		return "", fmt.Errorf("commit lookup %s: nil response body", sha)
	}
	// Read one past the cap so an over-limit (truncated) body is detected rather than decoded as
	// complete JSON, which would fail and, treated as transient, re-claim the fix's candidates forever.
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, commitBodyLimit+1))
	resp.Body.Close() //nolint:errcheck,gosec // best-effort close on a read-only commit lookup
	if resp.StatusCode == http.StatusNotFound {
		// The fix commit is gone (force-push / GC). Signal it distinctly so scoring degrades this fix's
		// candidates rather than erroring and re-claiming them forever.
		return "", fmt.Errorf("commit lookup %s: %w", sha, lineage.ErrFixCommitGone)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("commit lookup %s: %w", sha, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, body, resp.Header))
	}
	if readErr != nil {
		return "", fmt.Errorf("reading commit body %s: %w", sha, readErr)
	}
	if len(body) > commitBodyLimit {
		// The commit metadata is too large to read (a giant message); the parents are unrecoverable.
		// Degrade like a gone commit so the fix's candidates advance rather than re-claiming forever.
		return "", fmt.Errorf("commit lookup %s: body over %d bytes: %w", sha, commitBodyLimit, lineage.ErrFixCommitGone)
	}
	var c bbCommitResponse
	if err := json.Unmarshal(body, &c); err != nil {
		return "", fmt.Errorf("decoding commit response %s: %w", sha, err)
	}
	if len(c.Parents) == 0 {
		return "", nil // root commit
	}
	return c.Parents[0].Hash, nil
}

// bitbucketCommitURL builds the single-commit endpoint (read for its parents).
func bitbucketCommitURL(orgID, workspace, repoSlug, sha string) string {
	return fmt.Sprintf("%s/repositories/%s/%s/commit/%s", bitbucketRESTBase(orgID), workspace, repoSlug, sha)
}
