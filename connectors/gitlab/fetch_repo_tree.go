// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	connectorv1 "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/internal/vcs/treewalker"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorName is the label used in treewalker metrics for this connector.
const connectorName = "gitlab"

// fetch_repo_tree resolves the HEAD SHA of each project's tracked branch,
// skips projects whose tree SHA matches the last-known value, walks the full
// tree via the shared treewalker, uploads the JSON blob to S3, and emits one
// RepoTreeRecord record per project.
//
// Per-project last-known tree SHA is stored in a composite cursor keyed by the
// numeric project ID (as a string) for stable identity across renames.
//
// S3 key layout:
//
//	repo-trees/{org_id}/{connector_id}/{platform_repo_id}/{tree_sha}.json
//
// GitLab paginates tree responses instead of truncating them, so the treewalker
// fast-path always returns truncated=false — the BFS fallback is never triggered.

const treeWalkerParallelism = 8

// FetchRepoTreeOp fetches and stores the git tree for every org project whose
// HEAD SHA has changed since the previous run.
type FetchRepoTreeOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchRepoTreeOp) OperationID() sdk.OperationID { return opFetchRepoTree }

// Execute iterates all group projects, resolves the HEAD SHA for the tracked
// branch, short-circuits on SHA match, and walks + uploads the tree otherwise.
func (op *FetchRepoTreeOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	if params.BlobStore == nil {
		return nil, fmt.Errorf("fetch_repo_tree: BlobStore is nil — wire misconfiguration")
	}

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

	// Load per-project SHA cursor (keyed by numeric project ID string).
	// A missing/malformed cursor is expected on first sync — recover by
	// treating it as empty state and doing a full fetch this run.
	shaCursor, err := sdk.CompositeFromCursor(params.Cursor)
	if err != nil {
		logger.Info(ctx, "no usable repo-tree cursor; doing full fetch",
			zap.String(logger.OrgID, params.Config.OrganizationID), zap.Error(err))
		shaCursor = map[string]json.RawMessage{}
	}

	projects, err := fetchAllProjects(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_repo_tree: list projects: %w", err)
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
	orgID := params.Config.OrganizationID
	fetchedAt := time.Now().UTC()

	var tally repotally.Tally
	var ctxErr error
	for n, proj := range projects {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		// Trees already walked are saved, so a run that ends early does not walk
		// them again.
		if n > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, sdk.NewCompositeCursor(shaCursor)); err != nil {
				logger.Error(ctx, "fetch_repo_tree: checkpoint failed (continuing)", zap.Error(err))
			}
		}
		// Use the numeric project ID as the stable cursor key (survives renames).
		projectIDStr := fmt.Sprintf("%d", proj.ID)

		// The repo tree always follows the default branch, independent of the
		// tracked-branch / all-branches setting (which governs only the PR
		// read-filter).
		branch := settings.resolveCommitBranch(proj.DefaultBranch)
		if branch == "" {
			branch = "main"
		}

		tally.Attempt()
		// Resolve the HEAD commit SHA for the branch.
		headSHA, err := getGitLabBranchSHA(ctx, restGW, orgID, proj.ID, branch)
		if err != nil {
			treewalker.RecordTreeFetchNetworkError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: get branch SHA failed — skipping project",
				zap.String(logger.OrgID, orgID),
				zap.String("project", proj.PathWithNamespace),
				zap.String("branch", branch),
				zap.Error(err),
			)
			tally.Fail(proj.PathWithNamespace, err)
			continue
		}

		// Check the per-project cursor for the last-known SHA.
		lastSHARaw := shaCursor[projectIDStr]
		var lastSHA string
		// Malformed cursor entry is treated as "no prior state" — we'll do
		// a full fetch this run and overwrite the entry below. An absent entry
		// (nil bytes) is the expected first-sync-per-project case, not a fault,
		// so only surface a populated-but-unparseable entry.
		if err := json.Unmarshal(lastSHARaw, &lastSHA); err != nil {
			if len(lastSHARaw) > 0 {
				logger.WarnWithCtx(ctx, "malformed repo-tree cursor entry; doing full fetch for project",
					zap.String(logger.OrgID, orgID),
					zap.String("project", proj.PathWithNamespace),
					zap.Error(err))
			}
			lastSHA = ""
		}

		if headSHA == lastSHA {
			// SHA unchanged — emit a no_change record so a consumer can
			// record that we checked without updating tree fields. The record
			// key includes the SHA so re-runs on the same day for the same project
			// produce distinct dedup keys and do not overwrite a prior
			// changed-tree record that a consumer hasn't applied yet.
			if emitErr := emitGitLabTreeRecord(ctx, params.Sink, params.Config, projectIDStr, fetchedAt, projectIDStr+":"+headSHA, &connectorv1.RepoTreeRecord{
				PlatformRepoId: projectIDStr,
				TreeSha:        headSHA,
				TreeBranch:     branch,
				TreeS3Key:      gitlabTreeS3Key(orgID, projectIDStr, headSHA),
				NoChange:       true,
				FetchedAt:      timestamppb.New(fetchedAt),
			}); emitErr != nil {
				return nil, fmt.Errorf("emitting no-change tree record: %w", emitErr)
			}
			continue
		}

		// Walk the tree.
		api := &gitlabTreeAPI{gw: restGW, orgID: orgID, projectID: proj.ID}
		entries, truncated, walkErr := treewalker.Walk(ctx, api, headSHA, treeWalkerParallelism)
		if walkErr != nil {
			treewalker.RecordTreeFetchNetworkError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: tree walk failed — skipping project",
				zap.String(logger.OrgID, orgID),
				zap.String("project", proj.PathWithNamespace),
				zap.String("head_sha", headSHA),
				zap.Error(walkErr),
			)
			tally.Fail(proj.PathWithNamespace, walkErr)
			continue
		}

		// Marshal the tree blob.
		blob := gitlabRepoTreeBlob{
			TreeSHA:   headSHA,
			Branch:    branch,
			FetchedAt: fetchedAt,
			Truncated: truncated,
			Entries:   entries,
		}
		body, marshalErr := json.Marshal(blob)
		if marshalErr != nil {
			treewalker.RecordTreeFetchInternalError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: marshal tree blob failed — skipping project",
				zap.String(logger.OrgID, orgID),
				zap.String("project", proj.PathWithNamespace),
				zap.Error(marshalErr),
			)
			tally.Fail(proj.PathWithNamespace, marshalErr)
			continue
		}

		s3Key := gitlabTreeS3Key(orgID, projectIDStr, headSHA)

		if uploadErr := params.BlobStore.Write(ctx, s3Key, body, ""); uploadErr != nil {
			treewalker.RecordTreeFetchS3UploadError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: S3 upload failed — skipping project",
				zap.String(logger.OrgID, orgID),
				zap.String("project", proj.PathWithNamespace),
				zap.String("s3_key", s3Key),
				zap.Error(uploadErr),
			)
			tally.Fail(proj.PathWithNamespace, uploadErr)
			continue
		}

		treewalker.RecordTreeFetched(connectorName, truncated)

		// Emit the tree record. Record key includes the SHA so each distinct
		// tree version gets its own dedup row in the downstream record store.
		if emitErr := emitGitLabTreeRecord(ctx, params.Sink, params.Config, projectIDStr, fetchedAt, projectIDStr+":"+headSHA, &connectorv1.RepoTreeRecord{
			PlatformRepoId: projectIDStr,
			TreeSha:        headSHA,
			TreeBranch:     branch,
			TreeS3Key:      s3Key,
			TreeTruncated:  truncated,
			FetchedAt:      timestamppb.New(fetchedAt),
		}); emitErr != nil {
			return nil, fmt.Errorf("emitting tree record: %w", emitErr)
		}

		// Advance the per-project cursor only for a COMPLETE tree. GitLab paginates
		// rather than truncates (truncated is always false here), so this guard is a
		// no-op today — kept identical to the other connectors so a truncated partial
		// tree could never advance the cursor past the last fully-processed record.
		if !truncated {
			shaBytes, err := json.Marshal(headSHA)
			if err != nil {
				return nil, fmt.Errorf("marshal head sha for cursor: %w", err)
			}
			shaCursor[projectIDStr] = shaBytes
		}
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), sdk.NewCompositeCursor(shaCursor))
		}
		return sdk.NewCompositeCursor(shaCursor), fmt.Errorf("fetch_repo_tree: %w", errors.Join(ctxErr, cpErr))
	}
	if err := tally.Err(len(projects)); err != nil {
		return nil, fmt.Errorf("fetch_repo_tree: %w", err)
	}
	return sdk.NewCompositeCursor(shaCursor), nil
}

// gitlabRepoTreeBlob is the JSON structure written to S3.
type gitlabRepoTreeBlob struct {
	TreeSHA   string                 `json:"tree_sha"`
	Branch    string                 `json:"branch"`
	FetchedAt time.Time              `json:"fetched_at"`
	Truncated bool                   `json:"truncated"`
	Entries   []treewalker.TreeEntry `json:"entries"`
}

// emitGitLabTreeRecord marshals rec and emits a fetch_repo_tree raw record.
// recordKey is "{platformRepoID}:{treeSHA}" — including the SHA ensures that
// two runs on the same data_date for the same project produce distinct dedup rows
// in the downstream record store rather than the second run silently overwriting the first.
func emitGitLabTreeRecord(ctx context.Context, sink sdk.DataSink, cfg *sdk.ConnectorConfig, platformRepoID string, fetchedAt time.Time, recordKey string, rec *connectorv1.RepoTreeRecord) error {
	payload, err := protojson.Marshal(rec)
	if err != nil {
		return fmt.Errorf("fetch_repo_tree: marshal RepoTreeRecord: %w", err)
	}
	if err := sink.Emit(ctx, &sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    string(opFetchRepoTree),
		OrganizationID: cfg.OrganizationID,
		ExternalID:     recordKey,
		Payload:        payload,
		FetchedAt:      fetchedAt,
		Metadata: map[string]string{
			"platform_repo_id": platformRepoID,
		},
	}); err != nil {
		return &sdk.SyncError{
			ConnectorID: connectorID,
			OperationID: opFetchRepoTree,
			Kind:        sdk.ErrKindSinkFailure,
			Cause:       err,
		}
	}
	return nil
}

// gitlabTreeAPI adapts the GitLab repository/tree REST endpoint to
// treewalker.VendorTreeAPI. It uses gw.DoJSON so it goes through the
// gateway middleware (retries, rate limiting, circuit breaker, auth)
// without requiring a new gateway proto request type.
//
// GitLab paginates tree results rather than truncating them, so GetTree loops
// through all pages and always returns truncated=false. The treewalker
// fast-path therefore never falls back to BFS for GitLab.
type gitlabTreeAPI struct {
	gw        sdk.VendorGateway
	orgID     string
	projectID int
}

// glTreeEntry is a single element returned by the GitLab repository/tree API.
type glTreeEntry struct {
	ID   string `json:"id"` // SHA of this entry
	Name string `json:"name"`
	Type string `json:"type"` // "blob" or "tree"
	Path string `json:"path"`
	Mode string `json:"mode"`
}

// maxTreePages is the upper bound on pagination pages fetched per tree request.
// 500 pages × 100 entries/page = 50 000 entries maximum. This matches GitHub's
// recursive git/trees truncation ceiling and accommodates large monorepos
// without looping indefinitely on a malformed or adversarial API response.
const maxTreePages = 500

// GetTree fetches the tree at sha. When recursive is true, it fetches all
// pages of the recursive tree listing. GitLab never truncates — it paginates —
// so truncated is always false.
func (g *gitlabTreeAPI) GetTree(ctx context.Context, sha string, recursive bool) ([]treewalker.TreeEntry, bool, error) {
	const perPage = 100
	var all []treewalker.TreeEntry

	for page := 1; ; page++ {
		// Safety guard: abort if pagination exceeds the expected ceiling.
		// This prevents an infinite loop on malformed responses that always
		// return a full page without ever terminating.
		if page > maxTreePages {
			return nil, false, fmt.Errorf("repository/tree %s: fetch exceeded %d pages, possibly malformed response", sha, maxTreePages)
		}

		apiURL := fmt.Sprintf(
			"%s/projects/%d/repository/tree?ref=%s&per_page=%d&page=%d",
			gitlabRESTBase(g.orgID), g.projectID, url.QueryEscape(sha), perPage, page,
		)
		if recursive {
			apiURL += "&recursive=true"
		} else {
			// Non-recursive call is a BFS fallback subtree fetch — record it.
			treewalker.RecordSubtreeFetch(connectorName)
		}

		var entries []glTreeEntry
		if err := g.gw.DoJSON(ctx, "GET", apiURL, nil, &entries); err != nil {
			return nil, false, fmt.Errorf("repository/tree %s page %d: %w", sha, page, err)
		}

		for _, e := range entries {
			all = append(all, treewalker.TreeEntry{
				Path: e.Path,
				Type: e.Type,
				Mode: e.Mode,
				SHA:  e.ID,
			})
		}

		// GitLab returns an empty slice on the last page (or a page shorter
		// than per_page). Stop when we receive fewer entries than requested.
		if len(entries) < perPage {
			break
		}
	}

	// GitLab paginates, never truncates — always return false.
	return all, false, nil
}

// glBranchResponse mirrors the GitLab GET /projects/:id/repository/branches/:branch response.
type glBranchResponse struct {
	Name   string `json:"name"`
	Commit struct {
		ID string `json:"id"` // full commit SHA
	} `json:"commit"`
}

// getGitLabBranchSHA resolves the HEAD commit SHA for the given branch.
func getGitLabBranchSHA(ctx context.Context, gw sdk.VendorGateway, orgID string, projectID int, branch string) (string, error) {
	apiURL := fmt.Sprintf(
		"%s/projects/%d/repository/branches/%s",
		gitlabRESTBase(orgID), projectID, url.PathEscape(branch),
	)

	var resp glBranchResponse
	if err := gw.DoJSON(ctx, "GET", apiURL, nil, &resp); err != nil {
		return "", fmt.Errorf("get branch %s: %w", branch, err)
	}
	if resp.Commit.ID == "" {
		return "", fmt.Errorf("empty commit SHA for branch %q of project %d", branch, projectID)
	}
	return resp.Commit.ID, nil
}

// gitlabTreeS3Key is where the tree for sha is stored. The no-change record names it
// too: it shares the ExternalID of the record that uploaded the tree, so a sink
// that overwrites on ExternalID keeps the key.
func gitlabTreeS3Key(orgID, platformRepoID, sha string) string {
	return fmt.Sprintf("repo-trees/%s/%s/%s/%s.json",
		orgID, string(connectorID), url.PathEscape(platformRepoID), sha)
}
