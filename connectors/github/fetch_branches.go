// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// fetch_branches lists every branch of each accessible repo and emits one
// record per branch. Branch listing has no incremental `since` filter, so
// each run fetches the full list per repo — no cursor is kept.
//
// Record key = "{platform_repo_id}:{branch_name}" so branches of different
// repos never collide in the downstream record store and re-runs overwrite the same dedup row.
// Payload is the vendor branch JSON (name, commit.sha, protected). Metadata
// carries the computed head sha, protected flag, and is_default (a branch is
// default when its name equals the repo's default_branch) so a consumer has
// an explicit, vendor-agnostic contract without re-deriving it.

// bytesLimitPerBranchPage caps a single branch-listing page body read. GitHub
// returns at most per_page=100 slim branch objects; 8 MiB is far above that and
// only guards against a pathological response.
const bytesLimitPerBranchPage = 8 << 20

// FetchBranchesOp lists all branches for every repo and emits one record per branch.
type FetchBranchesOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchBranchesOp) OperationID() sdk.OperationID { return opFetchBranches }

// ghBranchListEntry is the slim branch object returned by
// GET /repos/{owner}/{repo}/branches. We keep the raw bytes for the payload and
// only parse the fields needed to build the record key + metadata.
type ghBranchListEntry struct {
	Name   string `json:"name"`
	Commit struct {
		SHA string `json:"sha"`
	} `json:"commit"`
	Protected bool `json:"protected"`
}

// Execute iterates all repos, lists their branches (paginated via the Link
// header), and emits one record per branch. A per-repo listing failure is
// logged and skipped; a sink failure is fatal so asynq retries the run.
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

	repos, err := fetchAllRepos(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_branches: list repos: %w", err)
	}

	gw := params.VendorGateway
	orgID := params.Config.OrganizationID
	restBase := githubRESTBase(orgID)
	fetchedAt := time.Now().UTC()

	var tally repotally.Tally
	for _, ri := range repos {
		owner, repo, ok := splitRepoFullName(ri.FullName)
		if !ok {
			logger.WarnWithCtx(ctx, "fetch_branches: skipping malformed full_name",
				zap.String("full_name", ri.FullName))
			continue
		}

		platformRepoID := fmt.Sprintf("%d", ri.DatabaseID)
		if ri.DatabaseID == 0 {
			platformRepoID = ri.FullName
		}

		tally.Attempt()
		branches, listErr := listGitHubBranches(ctx, gw, restBase, owner, repo)
		if listErr != nil {
			logger.Error(ctx, "fetch_branches: list branches failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", ri.FullName),
				zap.Error(listErr),
			)
			tally.Fail(ri.FullName, listErr)
			continue
		}

		for _, br := range branches {
			var parsed ghBranchListEntry
			if err := json.Unmarshal(br, &parsed); err != nil || parsed.Name == "" {
				logger.WarnWithCtx(ctx, "fetch_branches: skipping unparseable branch entry",
					zap.String("repo", ri.FullName), zap.Error(err))
				continue
			}

			isDefault := ri.DefaultBranch != "" && parsed.Name == ri.DefaultBranch
			if emitErr := params.Sink.Emit(ctx, &sdk.RawRecord{
				ConnectorID:    string(connectorID),
				OperationID:    string(op.OperationID()),
				OrganizationID: orgID,
				ExternalID:     platformRepoID + ":" + parsed.Name,
				Payload:        br,
				FetchedAt:      fetchedAt,
				// No DataDate: a branch is current state (sdk.RawRecord.DataDate).
				Metadata: map[string]string{
					"repo_full_name":   ri.FullName,
					"platform_repo_id": platformRepoID,
					"branch_name":      parsed.Name,
					"head_sha":         parsed.Commit.SHA,
					"protected":        boolStr(parsed.Protected),
					"is_default":       boolStr(isDefault),
					// The repo's full branch count this fetch, so a consumer can
					// tell a complete listing from a torn one before reconciling.
					"branch_total": fmt.Sprintf("%d", len(branches)),
				},
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

	if err := tally.Err(len(repos)); err != nil {
		return nil, fmt.Errorf("fetch_branches: %w", err)
	}
	logger.Info(ctx, "fetch_branches complete",
		zap.String(logger.OrgID, orgID), zap.Int("repos", len(repos)))
	// Full-list per repo each run; no incremental cursor to persist.
	return params.Cursor, nil
}

// maxBranchPages bounds the branch-listing pagination loop. 100 pages ×
// per_page=100 = 10 000 branches, well above any real repo, so hitting the cap
// signals a malformed or adversarial Link header rather than legitimate data.
const maxBranchPages = 100

// listGitHubBranches returns the raw JSON of every branch of owner/repo,
// paginating via the Link rel="next" header. It uses gw.Do (not the proto
// Execute path) so it can read the Link response header while still going
// through the gateway middleware (retries, rate limiting, auth).
func listGitHubBranches(ctx context.Context, gw sdk.VendorGateway, restBase, owner, repo string) ([]json.RawMessage, error) {
	var all []json.RawMessage
	for page := 1; page <= maxBranchPages; page++ {
		apiURL := fmt.Sprintf("%s/repos/%s/%s/branches?per_page=100&page=%d", restBase, owner, repo, page)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			return nil, fmt.Errorf("building branches request page %d: %w", page, err)
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

		resp, err := gw.Do(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("listing branches page %d: %w", page, err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, bytesLimitPerBranchPage))
		linkHeader := resp.Header.Get("Link")
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

		if !hasNextPage(linkHeader) {
			return all, nil
		}
	}
	return nil, fmt.Errorf("branch listing for %s/%s exceeded %d pages, possibly malformed Link header", owner, repo, maxBranchPages)
}

// boolStr renders a bool as the "true"/"false" metadata strings consumers
// consume (matching the reviewer_is_bot convention used elsewhere).
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
