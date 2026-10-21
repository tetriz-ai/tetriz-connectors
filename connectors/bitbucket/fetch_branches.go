// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// opFetchBranches lists every branch of each workspace repo. Full-list per repo
// each run; no cursor.
const opFetchBranches = "fetch_branches"

// fetch_branches lists every branch of each workspace repo and emits one
// record per branch. Bitbucket has no per-branch default flag — the default
// branch lives on the repo (mainbranch) — so is_default is computed by
// comparing the branch name to the repo's resolved default branch.
//
// Record key = "{platform_repo_id}:{branch_name}" so branches of different
// repos never collide and re-runs overwrite the same dedup row. Payload is the
// vendor branch JSON (name, target.hash). Metadata carries the computed head
// sha and is_default so a consumer has an explicit, vendor-agnostic contract.
//
// Pagination follows the `next` URL in the response body (the Bitbucket
// paged-collection shape), validated against the canonical API host to avoid
// SSRF from a tampered next link — the same guard walkBitbucketTree applies.

// maxBranchPages bounds the branch-listing pagination loop. 100 pages ×
// pagelen=100 = 10 000 branches, well above any real repo; hitting the cap
// signals a malformed `next` chain rather than legitimate data.
const maxBranchPages = 100

// The Bitbucket branching-model prior: the
// declared development/production model per repo (GET .../branching-model) feeds
// merge-flow detection as a declared-flow prior. It has no column of its own, so
// fetch_branches emits it as a per-repo companion record under
// vendorids.OpFetchBranchingModel (so a vendor-agnostic consumer, which
// lists only fetch_branches, never sees it) keyed by vendorids.BranchingModelRecordSuffix.
// The op-id and suffix live in internal/vendorids so this emit and the downstream
// reader stay one source of truth. A reversible default (blocker 1).

// FetchBranchesOp lists all branches for every repo and emits one record per branch.
type FetchBranchesOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchBranchesOp) OperationID() sdk.OperationID { return opFetchBranches }

// bbBranchListEntry is the slim branch object in a Bitbucket refs/branches
// listing. Raw bytes are kept for the payload; only the fields needed for the
// record key + metadata are parsed.
type bbBranchListEntry struct {
	Name   string `json:"name"`
	Target struct {
		Hash string `json:"hash"`
		// Date is the head commit's date — a branch-activity recency prior for
		// merge-flow detection. Zero when the payload omits it.
		Date time.Time `json:"date"`
	} `json:"target"`
}

// bbBranchingModelResponse is the slim shape of GET .../branching-model. Only the
// declared development/production branch names are read; branch_types prefixes are
// not consumed today. Either arm may be absent (the field stays "").
type bbBranchingModelResponse struct {
	Development struct {
		Name string `json:"name"`
	} `json:"development"`
	Production struct {
		Name string `json:"name"`
	} `json:"production"`
}

// bbBranchListResponse is the paginated response from the Bitbucket
// refs/branches API. `values` holds this page; `next` is the absolute URL of
// the following page (empty on the last page).
type bbBranchListResponse struct {
	Values []json.RawMessage `json:"values"`
	Next   string            `json:"next"`
}

// Execute iterates all repos, lists their branches (paginated via the `next`
// URL), and emits one record per branch. A per-repo listing failure is
// logged and skipped; a sink failure is fatal.
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

	repos, err := fetchAllReposInfo(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_branches: list repos: %w", err)
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)
	orgID := params.Config.OrganizationID
	fetchedAt := time.Now().UTC()

	var tally repotally.Tally
	for _, repo := range repos {
		workspace, repoSlug, splitErr := splitFullName(repo.FullName)
		if splitErr != nil {
			logger.WarnWithCtx(ctx, "fetch_branches: skipping malformed full_name",
				zap.String("full_name", repo.FullName), zap.Error(splitErr))
			continue
		}

		platformRepoID := repo.UUID
		if platformRepoID == "" {
			platformRepoID = repo.FullName
		}
		// Default branch: repo.Mainbranch when the listing/GetRepo resolved it,
		// else resolveCommitBranch's "main" fallback. A branch is default when
		// its name equals this resolved value.
		defaultBranch := settings.resolveCommitBranch(repo.Mainbranch)

		tally.Attempt()
		branches, listErr := listBitbucketBranches(ctx, restGW, orgID, workspace, repoSlug)
		if listErr != nil {
			logger.Error(ctx, "fetch_branches: list branches failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", repo.FullName),
				zap.Error(listErr),
			)
			tally.Fail(repo.FullName, listErr)
			continue
		}

		for _, br := range branches {
			var parsed bbBranchListEntry
			if err := json.Unmarshal(br, &parsed); err != nil || parsed.Name == "" {
				logger.WarnWithCtx(ctx, "fetch_branches: skipping unparseable branch entry",
					zap.String("repo", repo.FullName), zap.Error(err))
				continue
			}

			isDefault := defaultBranch != "" && parsed.Name == defaultBranch
			md := map[string]string{
				"repo_full_name":   repo.FullName,
				"platform_repo_id": platformRepoID,
				"branch_name":      parsed.Name,
				"head_sha":         parsed.Target.Hash,
				"is_default":       boolStr(isDefault),
				// The repo's full branch count this fetch, so a consumer can
				// tell a complete listing from a torn one before reconciling.
				"branch_total": fmt.Sprintf("%d", len(branches)),
			}
			// Detection prior: head-commit recency, stamped only when present so an
			// absent date reads as unknown, not epoch. Bitbucket branch listings carry
			// no merged flag, so no merged prior is emitted here.
			if !parsed.Target.Date.IsZero() {
				md["vendor_updated_at"] = parsed.Target.Date.UTC().Format(time.RFC3339)
			}
			if emitErr := params.Sink.Emit(ctx, &sdk.RawRecord{
				ConnectorID:    string(connectorID),
				OperationID:    string(op.OperationID()),
				OrganizationID: orgID,
				ExternalID:     platformRepoID + ":" + parsed.Name,
				Payload:        br,
				FetchedAt:      fetchedAt,
				// A branch is current state: no DataDate (sdk.RawRecord.DataDate).
				Metadata: md,
			}); emitErr != nil {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: op.OperationID(),
					Kind:        sdk.ErrKindSinkFailure,
					Cause:       emitErr,
				}
			}
		}

		// Declared branching-model prior, one call per repo. Fail-open: a 404 (no
		// model configured) or any fetch error yields no companion record, never an
		// error — the connector degrade-to-empty rule.
		if emitErr := emitBranchingModel(ctx, restGW, params.Sink, orgID, workspace, repoSlug, platformRepoID, fetchedAt); emitErr != nil {
			return nil, fmt.Errorf("emitting branching model: %w", emitErr)
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

// listBitbucketBranches returns the raw JSON of every branch of workspace/repoSlug,
// paginating via the `next` URL in the response body. The next host is validated
// against the org's configured REST host before following it (SSRF guard, matching
// walkBitbucketTree) — the real host for a normal org, the simulator's for a
// simulator org.
func listBitbucketBranches(ctx context.Context, gw sdk.VendorGateway, orgID, workspace, repoSlug string) ([]json.RawMessage, error) {
	var all []json.RawMessage
	// orgID fixes the base and the expected next-page host for the whole call,
	// so resolve both once rather than per page.
	wantHost := bitbucketRESTHost(orgID)
	nextURL := fmt.Sprintf(
		"%s/repositories/%s/%s/refs/branches?pagelen=100",
		bitbucketRESTBase(orgID), workspace, repoSlug,
	)

	for page := 0; nextURL != ""; page++ {
		if page >= maxBranchPages {
			return nil, fmt.Errorf("branch listing for %s/%s exceeded %d pages, possibly malformed next chain", workspace, repoSlug, maxBranchPages)
		}

		var resp bbBranchListResponse
		if err := gw.DoJSON(ctx, "GET", nextURL, nil, &resp); err != nil {
			return nil, fmt.Errorf("refs/branches %s/%s: %w", workspace, repoSlug, err)
		}
		all = append(all, resp.Values...)

		if resp.Next == "" {
			break
		}
		parsed, parseErr := url.Parse(resp.Next)
		if parseErr != nil {
			return nil, fmt.Errorf("refs/branches %s/%s: parse next URL: %w", workspace, repoSlug, parseErr)
		}
		if parsed.Host != wantHost {
			return nil, fmt.Errorf("refs/branches %s/%s: unexpected next URL host %q (want %q)",
				workspace, repoSlug, parsed.Host, wantHost)
		}
		nextURL = resp.Next
	}
	return all, nil
}

// emitBranchingModel fetches a repo's declared branching model (one call) and, on
// success, emits the per-repo companion record the merge-flow composer reads as
// a declared-flow prior. It fails open: a 404 (no model configured), any fetch
// error, or an unparseable body yields no companion record and a nil error — the
// prior is simply absent for that repo. Only a sink failure is fatal (returned so
// the run retries), matching the branch-emit path.
func emitBranchingModel(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, orgID, workspace, repoSlug, platformRepoID string, fetchedAt time.Time) error {
	apiURL := fmt.Sprintf("%s/repositories/%s/%s/branching-model", bitbucketRESTBase(orgID), workspace, repoSlug)
	var raw json.RawMessage
	if err := gw.DoJSON(ctx, "GET", apiURL, nil, &raw); err != nil {
		logger.WarnWithCtx(ctx, "fetch_branches: branching-model fetch failed — no declared-flow prior for repo",
			zap.String(logger.OrgID, orgID), zap.String("repo", workspace+"/"+repoSlug), zap.Error(err))
		return nil
	}
	var model bbBranchingModelResponse
	if err := json.Unmarshal(raw, &model); err != nil {
		logger.WarnWithCtx(ctx, "fetch_branches: unparseable branching-model response — skipping declared-flow prior",
			zap.String(logger.OrgID, orgID), zap.String("repo", workspace+"/"+repoSlug), zap.Error(err))
		return nil
	}
	if emitErr := sink.Emit(ctx, &sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    vendorids.OpFetchBranchingModel,
		OrganizationID: orgID,
		ExternalID:     platformRepoID + vendorids.BranchingModelRecordSuffix,
		Payload:        raw,
		FetchedAt:      fetchedAt,
		// The branching model is current state: no DataDate.
		Metadata: map[string]string{
			"platform_repo_id": platformRepoID,
			"development":      model.Development.Name,
			"production":       model.Production.Name,
		},
	}); emitErr != nil {
		return &sdk.SyncError{
			ConnectorID: connectorID,
			OperationID: vendorids.OpFetchBranchingModel,
			Kind:        sdk.ErrKindSinkFailure,
			Cause:       emitErr,
		}
	}
	return nil
}

// boolStr renders a bool as the "true"/"false" metadata strings consumers
// consume (matching the reviewer_is_bot convention used elsewhere).
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
