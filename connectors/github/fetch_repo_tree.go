// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

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
const connectorName = "github"

// fetch_repo_tree resolves the HEAD SHA of each repo's tracked branch, skips
// repos whose tree SHA matches the last-known value, walks the full tree via
// the shared treewalker (with truncation fallback), uploads the JSON blob to S3,
// and emits one RepoTreeRecord record per repo.
//
// Per-repo last-known tree SHA is stored in a composite cursor keyed by
// "owner/repo" so a scheduler-driven FetchRepoTreeJob can also pass it via
// FetchRepoTreeJob.LastKnownTreeSha.
//
// S3 key layout: repo-trees/{org_id}/{connector_id}/{platform_repo_id}/{tree_sha}.json

const treeWalkerParallelism = 8

// FetchRepoTreeOp fetches and stores the git tree for every org repository whose
// HEAD SHA has changed since the previous run.
type FetchRepoTreeOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchRepoTreeOp) OperationID() sdk.OperationID { return opFetchRepoTree }

// Execute iterates all org repositories, resolves the HEAD SHA for the tracked
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

	// Load per-repo SHA cursor (keyed by "owner/repo"). A corrupt cursor is
	// recoverable: reset to empty so this run does a full fetch and overwrites it.
	shaCursor, err := sdk.CompositeFromCursor(params.Cursor)
	if err != nil {
		logger.WarnWithCtx(ctx, "fetch_repo_tree: composite cursor parse failed; resetting to empty",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.Error(err))
		shaCursor = map[string]json.RawMessage{}
	}

	repos, err := fetchAllRepos(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_repo_tree: list repos: %w", err)
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupGitHubREST)
	orgID := params.Config.OrganizationID
	restBase := githubRESTBase(orgID)
	fetchedAt := time.Now().UTC()

	var tally repotally.Tally
	var ctxErr error
	for n, ri := range repos {
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
		owner, repo, ok := splitRepoFullName(ri.FullName)
		if !ok {
			logger.WarnWithCtx(ctx, "fetch_repo_tree: skipping malformed full_name",
				zap.String("full_name", ri.FullName))
			continue
		}
		tally.Attempt()

		// The repo tree always follows the default branch, independent of the
		// tracked-branch / all-branches setting (which governs only the PR
		// read-filter).
		branch := settings.resolveCommitBranch(ri.DefaultBranch)
		if branch == "" {
			branch = "main"
		}

		// Resolve the HEAD commit SHA for the branch.
		headSHA, err := getGitHubBranchSHA(ctx, restGW, restBase, owner, repo, branch)
		if err != nil {
			treewalker.RecordTreeFetchNetworkError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: get branch SHA failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", ri.FullName),
				zap.String("branch", branch),
				zap.Error(err),
			)
			tally.Fail(ri.FullName, err)
			continue
		}

		// Compute platform_repo_id: numeric ID when available (auto-discovered
		// repos), fallback to full_name for explicit repo lists where DatabaseID
		// is not populated. In the fallback case UpdateTree will get
		// ErrRecordNotFound (no-match), which is logged and skipped safely.
		platformRepoID := fmt.Sprintf("%d", ri.DatabaseID)
		if ri.DatabaseID == 0 {
			platformRepoID = ri.FullName
		}

		// Check the per-repo cursor for the last-known SHA.
		// Keyed by platformRepoID (numeric DatabaseID string when available, FullName
		// otherwise) so the cursor survives repo renames on the auto-discovery path.
		lastSHARaw := shaCursor[platformRepoID]
		var lastSHA string
		// Malformed cursor entry is treated as "no prior state" — we'll do
		// a full fetch this run and overwrite the entry below.
		if err := json.Unmarshal(lastSHARaw, &lastSHA); err != nil {
			logger.WarnWithCtx(ctx, "fetch_repo_tree: malformed per-repo cursor entry; treating as no prior state",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", ri.FullName),
				zap.Error(err))
			lastSHA = ""
		}

		if headSHA == lastSHA {
			// SHA unchanged — emit a no_change record so a consumer can
			// record that we checked without updating tree fields. The record
			// key includes the SHA so re-runs on the same day for the same repo
			// produce distinct dedup keys and do not overwrite a prior
			// changed-tree record that a consumer hasn't applied yet.
			if emitErr := emitTreeRecord(ctx, params.Sink, params.Config, ri.FullName, fetchedAt, platformRepoID+":"+headSHA, &connectorv1.RepoTreeRecord{
				PlatformRepoId: platformRepoID,
				TreeSha:        headSHA,
				TreeBranch:     branch,
				TreeS3Key:      githubTreeS3Key(orgID, platformRepoID, headSHA),
				NoChange:       true,
				FetchedAt:      timestamppb.New(fetchedAt),
			}); emitErr != nil {
				return nil, fmt.Errorf("emitting no-change tree record: %w", emitErr)
			}
			continue
		}

		// Walk the tree.
		api := &githubTreeAPI{gw: restGW, restBaseURL: restBase, owner: owner, repo: repo}
		entries, truncated, walkErr := treewalker.Walk(ctx, api, headSHA, treeWalkerParallelism)
		if walkErr != nil {
			treewalker.RecordTreeFetchNetworkError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: tree walk failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", ri.FullName),
				zap.String("head_sha", headSHA),
				zap.Error(walkErr),
			)
			tally.Fail(ri.FullName, walkErr)
			continue
		}

		// Marshal the tree blob.
		blob := repoTreeBlob{
			TreeSHA:   headSHA,
			Branch:    branch,
			FetchedAt: fetchedAt,
			Truncated: truncated,
			Entries:   entries,
		}
		body, marshalErr := json.Marshal(blob)
		if marshalErr != nil {
			treewalker.RecordTreeFetchInternalError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: marshal tree blob failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", ri.FullName),
				zap.Error(marshalErr),
			)
			tally.Fail(ri.FullName, marshalErr)
			continue
		}

		s3Key := githubTreeS3Key(orgID, platformRepoID, headSHA)

		if uploadErr := params.BlobStore.Write(ctx, s3Key, body, ""); uploadErr != nil {
			treewalker.RecordTreeFetchS3UploadError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: S3 upload failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", ri.FullName),
				zap.String("s3_key", s3Key),
				zap.Error(uploadErr),
			)
			tally.Fail(ri.FullName, uploadErr)
			continue
		}

		treewalker.RecordTreeFetched(connectorName, truncated)

		// Emit the tree record. Record key includes the SHA so each distinct
		// tree version gets its own dedup row in the downstream record store.
		if emitErr := emitTreeRecord(ctx, params.Sink, params.Config, ri.FullName, fetchedAt, platformRepoID+":"+headSHA, &connectorv1.RepoTreeRecord{
			PlatformRepoId: platformRepoID,
			TreeSha:        headSHA,
			TreeBranch:     branch,
			TreeS3Key:      s3Key,
			TreeTruncated:  truncated,
			FetchedAt:      timestamppb.New(fetchedAt),
		}); emitErr != nil {
			return nil, fmt.Errorf("emitting tree record: %w", emitErr)
		}

		// Advance the per-repo cursor only for a COMPLETE tree (same key as the read
		// above). A truncated walk emitted a partial tree; leaving the cursor at the
		// last fully-processed SHA re-walks this repo next run so it self-heals once the
		// tree fits. The cursor sits at the last fully-processed record.
		if !truncated {
			shaBytes, err := json.Marshal(headSHA)
			if err != nil {
				return nil, fmt.Errorf("marshal head sha for cursor: %w", err)
			}
			shaCursor[platformRepoID] = shaBytes
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
	if err := tally.Err(len(repos)); err != nil {
		return nil, fmt.Errorf("fetch_repo_tree: %w", err)
	}
	return sdk.NewCompositeCursor(shaCursor), nil
}

// repoTreeBlob is the JSON structure written to S3.
type repoTreeBlob struct {
	TreeSHA   string                 `json:"tree_sha"`
	Branch    string                 `json:"branch"`
	FetchedAt time.Time              `json:"fetched_at"`
	Truncated bool                   `json:"truncated"`
	Entries   []treewalker.TreeEntry `json:"entries"`
}

// emitTreeRecord marshals rec and emits a fetch_repo_tree raw record.
// recordKey is "{platformRepoID}:{treeSHA}" — including the SHA ensures that
// two runs on the same data_date for the same repo produce distinct dedup rows
// in the downstream record store rather than the second run silently overwriting the first.
func emitTreeRecord(ctx context.Context, sink sdk.DataSink, cfg *sdk.ConnectorConfig, repoFullName string, fetchedAt time.Time, recordKey string, rec *connectorv1.RepoTreeRecord) error {
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
			"repo_full_name": repoFullName,
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

// githubTreeAPI adapts the GitHub git/trees REST endpoint to treewalker.VendorTreeAPI.
// It uses gw.DoJSON so it goes through the gateway middleware (retries,
// rate limiting, circuit breaker, auth) without requiring a new gateway
// proto request type.
type githubTreeAPI struct {
	gw          sdk.VendorGateway
	restBaseURL string
	owner, repo string
}

// ghTreeResponse mirrors the GitHub GET /repos/{owner}/{repo}/git/trees/{sha} response.
type ghTreeResponse struct {
	SHA       string        `json:"sha"`
	Truncated bool          `json:"truncated"`
	Tree      []ghTreeEntry `json:"tree"`
}

// ghTreeEntry is a single element returned by the GitHub trees API.
type ghTreeEntry struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
	Type string `json:"type"` // "blob" or "tree"
	SHA  string `json:"sha"`
	Size int64  `json:"size,omitempty"`
}

// GetTree fetches the tree at sha. When recursive is true it appends
// ?recursive=1 to ask GitHub for the full tree in one call.
func (g *githubTreeAPI) GetTree(ctx context.Context, sha string, recursive bool) ([]treewalker.TreeEntry, bool, error) {
	apiURL := fmt.Sprintf("%s/repos/%s/%s/git/trees/%s",
		g.restBaseURL, g.owner, g.repo, sha)
	if recursive {
		apiURL += "?recursive=1"
	} else {
		// Non-recursive call is a BFS fallback subtree fetch — record it.
		treewalker.RecordSubtreeFetch(connectorName)
	}

	var resp ghTreeResponse
	if err := g.gw.DoJSON(ctx, "GET", apiURL, nil, &resp); err != nil {
		return nil, false, fmt.Errorf("git/trees %s: %w", sha, err)
	}

	out := make([]treewalker.TreeEntry, 0, len(resp.Tree))
	for _, e := range resp.Tree {
		te := treewalker.TreeEntry{
			Path: e.Path,
			Type: e.Type,
			Mode: e.Mode,
			SHA:  e.SHA,
		}
		if e.Size > 0 {
			s := e.Size
			te.Size = &s
		}
		out = append(out, te)
	}
	return out, resp.Truncated, nil
}

// ghBranchResponse mirrors the GitHub GET /repos/{owner}/{repo}/branches/{branch} response.
type ghBranchResponse struct {
	Name   string `json:"name"`
	Commit struct {
		SHA string `json:"sha"`
	} `json:"commit"`
}

// getGitHubBranchSHA resolves the HEAD commit SHA for the given branch.
// baseURL must be the REST API root (e.g. "https://api.github.com" or the
// simulator base) so the URL is portable across environments.
func getGitHubBranchSHA(ctx context.Context, gw sdk.VendorGateway, baseURL, owner, repo, branch string) (string, error) {
	apiURL := fmt.Sprintf("%s/repos/%s/%s/branches/%s",
		baseURL, owner, repo, url.PathEscape(branch))

	var resp ghBranchResponse
	if err := gw.DoJSON(ctx, "GET", apiURL, nil, &resp); err != nil {
		return "", fmt.Errorf("get branch %s: %w", branch, err)
	}
	if resp.Commit.SHA == "" {
		return "", fmt.Errorf("empty commit SHA for branch %q of %s/%s", branch, owner, repo)
	}
	return resp.Commit.SHA, nil
}

// githubTreeS3Key is where the tree for sha is stored. The no-change record names it
// too: it shares the ExternalID of the record that uploaded the tree, so a sink
// that overwrites on ExternalID keeps the key.
func githubTreeS3Key(orgID, platformRepoID, sha string) string {
	return fmt.Sprintf("repo-trees/%s/%s/%s/%s.json",
		orgID, string(connectorID), url.PathEscape(platformRepoID), sha)
}
