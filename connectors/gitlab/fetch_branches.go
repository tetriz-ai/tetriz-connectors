// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// fetch_branches lists every branch of each group project and emits one
// record per branch. Branch listing has no incremental filter, so each run
// fetches the full list per project — no cursor is kept.
//
// Record key = "{project_id}:{branch_name}" so branches of different projects
// never collide and re-runs overwrite the same dedup row. Payload is the vendor
// branch JSON (name, commit.id, default, protected). Metadata carries the
// computed head sha, protected flag, and is_default so a consumer has an
// explicit, vendor-agnostic contract.

// bytesLimitPerBranchPage caps a single branch-listing page body read. GitLab
// returns at most per_page=100 branch objects; 8 MiB is far above that.
const bytesLimitPerBranchPage = 8 << 20

// maxBranchPages bounds the branch-listing pagination loop. 100 pages ×
// per_page=100 = 10 000 branches, well above any real project; hitting the cap
// signals a malformed pagination header rather than legitimate data.
const maxBranchPages = 100

// FetchBranchesOp lists all branches for every project and emits one record per branch.
type FetchBranchesOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchBranchesOp) OperationID() sdk.OperationID { return opFetchBranches }

// glBranchListEntry is the slim branch object returned by
// GET /projects/{id}/repository/branches. Raw bytes are kept for the payload;
// only the fields needed for the record key + metadata are parsed.
type glBranchListEntry struct {
	Name   string `json:"name"`
	Commit struct {
		ID string `json:"id"` // full commit SHA
		// CommittedDate is the head commit's committed time — a branch-activity
		// recency prior for merge-flow detection. Zero when the payload omits it.
		CommittedDate time.Time `json:"committed_date"`
	} `json:"commit"`
	Default   bool `json:"default"`
	Protected bool `json:"protected"`
	// Merged is GitLab's flag for a branch whose head is already merged into the
	// default branch — a terminal-branch prior (a merged branch is not a parallel lane).
	Merged bool `json:"merged"`
}

// Execute iterates all projects, lists their branches (paginated via the
// X-Next-Page header), and emits one record per branch. A per-project
// listing failure is logged and skipped; a sink failure is fatal.
func (op *FetchBranchesOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    op.OperationID(),
			OrganizationID: params.Config.OrganizationID,
			Kind:           sdk.ErrKindConfigInvalid,
			Cause:          err,
		}
	}

	projects, err := fetchAllProjects(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_branches: list projects: %w", err)
	}

	gw := params.VendorGateway
	orgID := params.Config.OrganizationID
	fetchedAt := time.Now().UTC()

	var tally repotally.Tally
	for _, proj := range projects {
		projectIDStr := strconv.Itoa(proj.ID)

		tally.Attempt()
		branches, listErr := listGitLabBranches(ctx, gw, orgID, proj.ID)
		if listErr != nil {
			logger.Error(ctx, "fetch_branches: list branches failed — skipping project",
				zap.String(logger.OrgID, orgID),
				zap.String("project", proj.PathWithNamespace),
				zap.Error(listErr),
			)
			tally.Fail(proj.PathWithNamespace, listErr)
			continue
		}

		for _, br := range branches {
			var parsed glBranchListEntry
			if err := json.Unmarshal(br, &parsed); err != nil || parsed.Name == "" {
				logger.WarnWithCtx(ctx, "fetch_branches: skipping unparseable branch entry",
					zap.String("project", proj.PathWithNamespace), zap.Error(err))
				continue
			}

			md := map[string]string{
				"repo":             proj.PathWithNamespace,
				"platform_repo_id": projectIDStr,
				"branch_name":      parsed.Name,
				"head_sha":         parsed.Commit.ID,
				"protected":        boolStr(parsed.Protected),
				"is_default":       boolStr(parsed.Default),
				// The project's full branch count this fetch, so a consumer
				// can tell a complete listing from a torn one before reconciling.
				"branch_total": strconv.Itoa(len(branches)),
				// Detection priors: the
				// merged flag is always known; the head-commit date is stamped only
				// when present so an absent date reads as unknown, not epoch.
				"merged": boolStr(parsed.Merged),
			}
			if !parsed.Commit.CommittedDate.IsZero() {
				md["vendor_updated_at"] = parsed.Commit.CommittedDate.UTC().Format(time.RFC3339)
			}
			if emitErr := params.Sink.Emit(ctx, &sdk.RawRecord{
				ConnectorID:    string(connectorID),
				OperationID:    string(op.OperationID()),
				OrganizationID: orgID,
				ExternalID:     projectIDStr + ":" + parsed.Name,
				Payload:        br,
				FetchedAt:      fetchedAt,
				Metadata:       md,
			}); emitErr != nil {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: op.OperationID(),
					Kind:        sdk.ErrKindSinkFailure,
					Cause:       emitErr,
				}
			}
		}
	}

	if err := tally.Err(len(projects)); err != nil {
		return nil, fmt.Errorf("fetch_branches: %w", err)
	}
	logger.Info(ctx, "fetch_branches complete",
		zap.String(logger.OrgID, orgID), zap.Int("projects", len(projects)))
	// Full-list per project each run; no incremental cursor to persist.
	return params.Cursor, nil
}

// listGitLabBranches returns the raw JSON of every branch of the project,
// paginating via the X-Next-Page response header. It uses gw.Do (not the proto
// Execute path) so it can read that header while still going through the
// gateway middleware (retries, rate limiting, auth).
func listGitLabBranches(ctx context.Context, gw sdk.VendorGateway, orgID string, projectID int) ([]json.RawMessage, error) {
	var all []json.RawMessage
	for page := 1; page <= maxBranchPages; page++ {
		apiURL := fmt.Sprintf(
			"%s/projects/%d/repository/branches?per_page=100&page=%d",
			gitlabRESTBase(orgID), projectID, page,
		)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			return nil, fmt.Errorf("building branches request page %d: %w", page, err)
		}

		resp, err := gw.Do(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("listing branches page %d: %w", page, err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, bytesLimitPerBranchPage))
		nextPage := strings.TrimSpace(resp.Header.Get("X-Next-Page"))
		resp.Body.Close() //nolint:errcheck,gosec // best-effort close on read-only branch listing
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("listing branches page %d: %w", page, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, body, resp.Header))
		}
		if readErr != nil {
			return nil, fmt.Errorf("reading branches body page %d: %w", page, readErr)
		}

		var pageBranches []json.RawMessage
		if err := json.Unmarshal(body, &pageBranches); err != nil {
			return nil, fmt.Errorf("parsing branches JSON page %d: %w", page, err)
		}
		all = append(all, pageBranches...)

		// GitLab leaves X-Next-Page empty on the last page.
		if nextPage == "" {
			return all, nil
		}
	}
	return nil, fmt.Errorf("branch listing for project %d exceeded %d pages, possibly malformed pagination header", projectID, maxBranchPages)
}

// boolStr renders a bool as the "true"/"false" metadata strings consumers
// consume (matching the reviewer_is_bot convention used elsewhere).
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
