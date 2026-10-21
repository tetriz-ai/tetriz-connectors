// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	opDrainDefectScoring   = "drain_defect_scoring"
	defectScoringBatchSize = 50
	// perFixScoringTimeout is a worst-case cap, not the expected time (files are usually served from
	// the verdict cache), so a healthy tick scores its whole batch inside the 10m scheduler-default
	// ExecutionTimeout. If a degraded vendor makes batch*cap overrun 10m, the tick is cut and the
	// candidates it did not reach stay unscored for a later tick. That is safe: SubmitStructuralVerdict
	// is idempotent, and the host offers only unscored candidates again. Do not raise batch without
	// bounding this cap.
	perFixScoringTimeout = 60 * time.Second
)

// drainDefectScoringRequiresHost lists the host services the operation cannot run without.
var drainDefectScoringRequiresHost = []sdk.HostService{sdk.HostBlameSink, sdk.HostDefectScorer}

// DrainDefectScoringOp is the pull side of structural scoring for GitLab: it claims unscored
// candidates, computes each candidate's AST proximity verdict against the fix's anchors, and submits
// it. The host combines and writes the score.
type DrainDefectScoringOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *DrainDefectScoringOp) OperationID() sdk.OperationID { return opDrainDefectScoring }

// Execute claims one batch of unscored candidates and scores each fix's group.
// It fails, terminal, without the host's blame sink and defect scorer.
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

	gw := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
	for _, fix := range pending {
		ctx, cancel := context.WithTimeout(parentCtx, perFixScoringTimeout)
		// GitLab addresses files by numeric project id, which is the candidate's platform_repo_id.
		projectID := fix.GetPlatformRepoId()
		src := &gitlabBlameSource{gw: gw, orgID: orgID, projectID: projectID}
		fetch := func(fctx context.Context, revision, path string) ([]byte, string, error) {
			return FileAtRevision(fctx, gw, orgID, projectID, revision, path)
		}
		culpritLines := func(fctx context.Context, sha, path string) ([]lineage.AddedLine, error) {
			return fetchGitLabCulpritAddedLines(fctx, params.Host.DefectScorer, gw, orgID, projectID, sha, path)
		}
		params.Host.DefectScorer.ScorePendingFix(ctx, params.Host.BlameSink, params.FetchCache, string(connectorID), orgID, fetch, src.FirstParent, culpritLines, fix)
		cancel()
	}
	return params.Cursor, nil
}

// gitlabCommitDiffEntry is the subset of a commit-diff element scoring reads: the post-image path, its
// per-file hunk diff, and GitLab's size flags. GitLab omits the diff body for a large file, flagging it
// collapsed (excluded to bound the payload, still fetchable on its own) or too_large (over the hard cap,
// not served) — either way the body is empty, so exact-line recovery degrades to same-file coverage.
type gitlabCommitDiffEntry struct {
	NewPath   string `json:"new_path"`
	Diff      string `json:"diff"`
	Collapsed bool   `json:"collapsed"`
	TooLarge  bool   `json:"too_large"`
}

// fetchGitLabCulpritAddedLines returns the lines the commit added to path, each with its 1-based
// new-file line number, from the commit's own diff. GitLab's blame stores only the culprit path (its
// blame reports the queried-revision position, which drifts), so scoring recovers the exact line here.
// A commit that did not touch path (or a 404) returns nil so scoring degrades to same-file coverage;
// only a transient error propagates for retry.
func fetchGitLabCulpritAddedLines(ctx context.Context, scorer lineage.Scorer, gw sdk.VendorGateway, orgID, projectID, sha, path string) ([]lineage.AddedLine, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gitlabCommitDiffURL(orgID, projectID, sha), nil)
	if err != nil {
		return nil, fmt.Errorf("building commit-diff request %s: %w", sha, err)
	}
	resp, err := gw.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("commit-diff request %s: %w", sha, err)
	}
	// Read one past the cap so an over-limit (truncated) body is detected rather than parsed as
	// complete JSON, which would fail to decode and poison-retry the candidate.
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, blameBodyLimit+1))
	resp.Body.Close() //nolint:errcheck,gosec // best-effort close on a read-only diff fetch
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // commit gone — recovery unavailable, degrade
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("commit-diff request %s: %w", sha, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, body, resp.Header))
	}
	if readErr != nil {
		return nil, fmt.Errorf("reading commit diff %s: %w", sha, readErr)
	}
	if len(body) > blameBodyLimit {
		return nil, nil // diff too large — recovery unavailable, degrade rather than fail to decode
	}
	var entries []gitlabCommitDiffEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("decoding commit diff %s: %w", sha, err)
	}
	for _, e := range entries {
		if e.NewPath != path {
			continue
		}
		if e.Diff == "" && (e.Collapsed || e.TooLarge) {
			// GitLab omitted the diff body for a large file. Recovery is unavailable, so degrade to
			// same-file coverage rather than reading it as "added nothing". (A collapsed file's diff is
			// separately fetchable, so exact-line precision is not recovered.)
			return nil, nil
		}
		return scorer.AddedLinesFromSingleFileDiff([]byte(e.Diff)), nil
	}
	return nil, nil // commit did not touch path — degrade
}

// gitlabCommitDiffURL builds the per-commit diff endpoint (the changes the commit itself introduced),
// routed through gitlabRESTBase so a simulator org reaches the simulator.
func gitlabCommitDiffURL(orgID, projectID, sha string) string {
	return fmt.Sprintf("%s/projects/%s/repository/commits/%s/diff", gitlabRESTBase(orgID), projectID, url.PathEscape(sha))
}
