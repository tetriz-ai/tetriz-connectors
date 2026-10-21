// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	connectorv1 "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	errgrp "github.com/tetriz-ai/tetriz-connectors/internal/errgroup"
	"github.com/tetriz-ai/tetriz-connectors/internal/httputil"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/internal/vcs/treewalker"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorName is the label used in treewalker metrics for this connector.
const connectorName = "bitbucket"

// fetch_repo_tree for Bitbucket Cloud.
//
// Tree listings are fetched via the Bitbucket src/ API:
//
//	GET /2.0/repositories/{workspace}/{repo_slug}/src/{commit}/{path}/?pagelen=100&max_depth=N
//
// max_depth makes the vendor walk the subtree itself, breadth-first, and return
// every entry down to N nested directories in one paginated sequence. Omitting it
// means max_depth=1 — the direct contents only — which is why a per-directory
// walk costs one call per directory and, far worse, fans out into as many
// concurrent waiters as the tree is wide. Asking the vendor to recurse turns the
// walk into a single pagination chain instead.
//
// The directory listing is the default response — no `format` param; see
// buildSrcURL for the format and trailing-slash invariants.
//
// Branch HEAD commit SHA is resolved via:
//
//	GET /2.0/repositories/{workspace}/{repo_slug}/refs/branches/{name}
//
// Because the Bitbucket src/ API addresses entries by (commit_hash, path) rather
// than by tree SHA, we do not use the shared treewalker.Walk adapter. Instead,
// walkBitbucketTree is a Bitbucket-specific BFS helper that recursively fetches
// each directory level using the commit hash + accumulated path prefix.
//
// S3 key layout: repo-trees/{org_id}/{connector_id}/{platform_repo_id}/{tree_sha}.json
//
// Per-repo last-known commit SHA is stored in a composite cursor keyed by
// "workspace/repo_slug" (stable across renames for a fixed workspace).

const bbTreeWalkerParallelism = 8

// FetchRepoTreeOp fetches and stores the git tree for every org repository whose
// HEAD SHA has changed since the previous run.
type FetchRepoTreeOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchRepoTreeOp) OperationID() sdk.OperationID { return opFetchRepoTree }

// Execute iterates all org repositories, resolves the HEAD commit SHA for the
// tracked branch, short-circuits on SHA match, and walks + uploads the tree
// otherwise. Any walk error is logged and the repo is skipped — a sink error
// is fatal and terminates the operation.
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

	// Load per-repo SHA cursor (keyed by "workspace/repo_slug").
	shaCursor, err := sdk.CompositeFromCursor(params.Cursor)
	if err != nil {
		logger.WarnWithCtx(ctx, "fetch_repo_tree: SHA cursor unparseable, treating as empty (all repos re-walk)",
			zap.String(logger.OrgID, params.Config.OrganizationID), zap.Error(err))
		shaCursor = map[string]json.RawMessage{}
	}

	repos, err := fetchAllReposInfo(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_repo_tree: list repos: %w", err)
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)
	orgID := params.Config.OrganizationID
	fetchedAt := time.Now().UTC()

	// One semaphore for the whole run: branch-SHA lookups and every repo's tree
	// walk contend for the same per-org vendor budget, so the bound has to span
	// them rather than resetting per repo.
	runSem := make(chan struct{}, bbTreeWalkerParallelism)

	var tally repotally.Tally
	var ctxErr error
	for i, repo := range repos {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		// Trees already walked are saved, so a run that ends early does not walk
		// them again.
		if i > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, sdk.NewCompositeCursor(shaCursor)); err != nil {
				logger.Error(ctx, "fetch_repo_tree: checkpoint failed (continuing)", zap.Error(err))
			}
		}
		repoFullName := repo.FullName
		workspace, repoSlug, err := splitFullName(repoFullName)
		if err != nil {
			logger.WarnWithCtx(ctx, "fetch_repo_tree: skipping malformed full_name",
				zap.String("full_name", repoFullName), zap.Error(err))
			continue
		}
		tally.Attempt()

		// This repo's slice of the run budget, recomputed each iteration so the
		// time repos ahead of it did not spend rolls forward rather than expiring.
		// Both the branch-SHA lookup and the walk draw on the one instant.
		waitUntil := repoWalkDeadline(ctx, len(repos)-i)

		// The repo tree always follows the repo's default branch — repo.Mainbranch
		// is the platform default, sourced from the workspace listing (auto-
		// discovery) or a per-repo BitbucketGetRepo call (explicit repo list) —
		// independent of the tracked-branch / all-branches setting (which governs
		// only the PR read-filter). resolveCommitBranch substitutes "main" only
		// when Mainbranch is still unknown after that resolution (GetRepo failed).
		branch := settings.resolveCommitBranch(repo.Mainbranch)

		// Resolve the HEAD commit SHA for the branch.
		headSHA, err := getBitbucketBranchSHA(ctx, restGW, orgID, workspace, repoSlug, branch, runSem, waitUntil)
		if err != nil {
			treewalker.RecordTreeFetchNetworkError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: get branch SHA failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", repoFullName),
				zap.String("branch", branch),
				zap.Error(err),
			)
			tally.Fail(repoFullName, err)
			continue
		}

		// platform_repo_id is the Bitbucket UUID — from the workspace listing
		// (auto-discovery) or the per-repo GetRepo call (explicit repo list).
		// Falls back to full_name only when GetRepo failed to resolve a UUID.
		platformRepoID := repo.UUID
		if platformRepoID == "" {
			platformRepoID = repoFullName
		}

		// Check the per-repo cursor for the last-known SHA.
		lastSHARaw := shaCursor[repoFullName]
		var lastSHA string
		if len(lastSHARaw) > 0 {
			if err := json.Unmarshal(lastSHARaw, &lastSHA); err != nil {
				logger.WarnWithCtx(ctx, "fetch_repo_tree: failed to decode cursor SHA — treating as new repo",
					zap.String(logger.OrgID, orgID),
					zap.String("repo", repoFullName),
					zap.Error(err),
				)
			}
		}

		if headSHA == lastSHA {
			// SHA unchanged — emit a no_change record so a consumer can
			// record that we checked. It shares the ExternalID of the record that
			// uploaded this SHA's tree, so it carries the same treeS3Key: a sink
			// that overwrites on ExternalID keeps the key. The key is a function
			// of the SHA, and the cursor holds a SHA only after its tree was
			// uploaded and emitted.
			if emitErr := emitBitbucketTreeRecord(ctx, params.Sink, params.Config, repoFullName, fetchedAt, platformRepoID+":"+headSHA, &connectorv1.RepoTreeRecord{
				PlatformRepoId: platformRepoID,
				TreeSha:        headSHA,
				TreeBranch:     branch,
				TreeS3Key:      bitbucketTreeS3Key(orgID, platformRepoID, headSHA),
				NoChange:       true,
				FetchedAt:      timestamppb.New(fetchedAt),
			}); emitErr != nil {
				return nil, fmt.Errorf("emit no_change record for %s: %w", repoFullName, emitErr)
			}
			continue
		}

		// Walk the tree using Bitbucket-specific BFS recursion, sharing the
		// run-wide semaphore so nesting depth cannot multiply in-flight calls.
		entries, walkErr := walkBitbucketTree(ctx, restGW, orgID, workspace, repoSlug, headSHA, "", bbTreeWalkerParallelism, runSem, waitUntil, bbTreeMaxDepth)
		if walkErr != nil {
			treewalker.RecordTreeFetchNetworkError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: tree walk failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", repoFullName),
				zap.String("head_sha", headSHA),
				zap.Error(walkErr),
			)
			tally.Fail(repoFullName, walkErr)
			continue
		}

		// Bitbucket paginates rather than truncates, so a walked tree is always
		// complete. The named value keeps the cursor-advance guard below identical to
		// the other connectors — a functional no-op here.
		truncated := false

		// Every entry above carries headSHA as its SHA, because the src/ listing
		// stamps entries with the commit that was browsed rather than the one that
		// last touched the file. Without the changed set below, a consumer keyed on
		// that SHA re-reads every file in the repo on any commit to it.
		changedPaths, changedKnown := fetchChangedPaths(ctx, restGW, orgID, workspace, repoSlug, headSHA, lastSHA, runSem, waitUntil)
		changedList := make([]string, 0, len(changedPaths))
		for p := range changedPaths {
			changedList = append(changedList, p)
		}
		sort.Strings(changedList) // stable blob bytes for identical input

		// Marshal the tree blob.
		blob := bbRepoTreeBlob{
			TreeSHA:           headSHA,
			Branch:            branch,
			FetchedAt:         fetchedAt,
			Truncated:         truncated,
			Entries:           entries,
			ChangedPathsKnown: changedKnown,
			ChangedPathsBase:  lastSHA,
			ChangedPaths:      changedList,
		}
		body, marshalErr := json.Marshal(blob)
		if marshalErr != nil {
			treewalker.RecordTreeFetchInternalError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: marshal tree blob failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", repoFullName),
				zap.Error(marshalErr),
			)
			tally.Fail(repoFullName, marshalErr)
			continue
		}

		s3Key := bitbucketTreeS3Key(orgID, platformRepoID, headSHA)

		if uploadErr := params.BlobStore.Write(ctx, s3Key, body, ""); uploadErr != nil {
			treewalker.RecordTreeFetchS3UploadError(connectorName)
			logger.Error(ctx, "fetch_repo_tree: S3 upload failed — skipping repo",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", repoFullName),
				zap.String("s3_key", s3Key),
				zap.Error(uploadErr),
			)
			tally.Fail(repoFullName, uploadErr)
			continue
		}

		treewalker.RecordTreeFetched(connectorName, truncated)

		// Emit the tree record. Record key includes the SHA so each tree
		// version has its own external ID.
		if emitErr := emitBitbucketTreeRecord(ctx, params.Sink, params.Config, repoFullName, fetchedAt, platformRepoID+":"+headSHA, &connectorv1.RepoTreeRecord{
			PlatformRepoId: platformRepoID,
			TreeSha:        headSHA,
			TreeBranch:     branch,
			TreeS3Key:      s3Key,
			TreeTruncated:  truncated,
			FetchedAt:      timestamppb.New(fetchedAt),
		}); emitErr != nil {
			return nil, fmt.Errorf("emitting bitbucket tree record: %w", emitErr)
		}

		// Advance the per-repo cursor only for a COMPLETE tree (a no-op guard on
		// Bitbucket, which never truncates), kept identical to the other connectors so
		// a truncated partial tree could never advance the cursor.
		if !truncated {
			shaBytes, err := json.Marshal(headSHA)
			if err != nil {
				return nil, fmt.Errorf("marshal head sha for cursor: %w", err)
			}
			shaCursor[repoFullName] = shaBytes
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

// bbRepoTreeBlob is the JSON structure written to S3.
type bbRepoTreeBlob struct {
	TreeSHA   string                 `json:"tree_sha"`
	Branch    string                 `json:"branch"`
	FetchedAt time.Time              `json:"fetched_at"`
	Truncated bool                   `json:"truncated"`
	Entries   []treewalker.TreeEntry `json:"entries"`

	// ChangedPathsKnown says whether ChangedPaths is authoritative. An empty
	// ChangedPaths is ambiguous alone — "nothing changed" or "we could not tell"
	// — and reading the second as the first would let a consumer skip a file it
	// never stored. False is the safe reading, and is what every blob written
	// before these fields existed decodes to.
	ChangedPathsKnown bool `json:"changed_paths_known,omitempty"`

	// ChangedPathsBase is the commit the change set is measured FROM. It is
	// load-bearing, not informational: the set describes Base..TreeSHA only, so a
	// consumer whose stored copy of a file predates Base has a gap this set does
	// not cover, and must re-read that file even though it is absent here. A tree
	// cursor advances on a successful WALK, while a file's content fetch can be
	// deferred or fail afterwards, so the two routinely sit at different commits.
	ChangedPathsBase string `json:"changed_paths_base,omitempty"`

	// ChangedPaths lists repo-absolute paths whose content differs from the
	// previous capture; a path absent from it is byte-identical.
	//
	// It exists because Bitbucket stamps every src/ entry with the commit that
	// was BROWSED rather than the one that last touched the file, so every
	// Entries[].SHA equals TreeSHA and every file looks changed on any commit to
	// the repo. This set is what lets a consumer keep the per-file fingerprint
	// it already holds. Vendors returning a real blob SHA need neither field.
	ChangedPaths []string `json:"changed_paths,omitempty"`
}

// bbDiffstatPage is the paginated diffstat range response. The element type
// is the shared bbDiffstatEntry (api_types.go), whose old/new file pointers give
// both sides of a rename.
type bbDiffstatPage struct {
	Values []bbDiffstatEntry `json:"values"`
	Next   string            `json:"next"`
}

// fetchChangedPaths asks the vendor which paths differ between two commits, so
// unchanged files can keep the fingerprint a consumer already holds rather than
// being re-read wholesale on every commit to the repo.
//
// The second return says whether the answer is trustworthy. Every failure path
// returns false rather than an error: change detection is an optimization, and
// degrading to "assume everything changed" costs extra reads, whereas failing
// the walk would cost the whole repo's tree. The asymmetry is deliberate — the
// only unacceptable outcome is under-reporting a change.
func fetchChangedPaths(ctx context.Context, gw sdk.VendorGateway, orgID, workspace, repoSlug, newSHA, oldSHA string, sem chan struct{}, waitUntil time.Time) (map[string]struct{}, bool) {
	if oldSHA == "" || newSHA == "" || oldSHA == newSHA {
		return nil, false
	}
	changed := map[string]struct{}{}
	nextURL := fmt.Sprintf("%s/repositories/%s/%s/diffstat/%s..%s?pagelen=100",
		bitbucketRESTBase(orgID), workspace, repoSlug, url.PathEscape(newSHA), url.PathEscape(oldSHA))
	wantHost := bitbucketRESTHost(orgID)

	for nextURL != "" {
		var resp bbDiffstatPage
		if err := getJSONWaitingForCapacity(ctx, gw, nextURL, &resp, sem, waitUntil); err != nil {
			logger.WarnWithCtx(ctx, "fetch_repo_tree: diffstat failed — treating every file as changed",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", workspace+"/"+repoSlug),
				zap.Error(err),
			)
			return nil, false
		}
		for _, v := range resp.Values {
			// Both sides are recorded: a rename changes content addressing for the
			// new path and removes the old, and a consumer keyed on either must
			// see it move.
			if v.Old != nil && v.Old.Path != "" {
				changed[v.Old.Path] = struct{}{}
			}
			if v.New != nil && v.New.Path != "" {
				changed[v.New.Path] = struct{}{}
			}
		}
		if resp.Next == "" {
			break
		}
		parsed, parseErr := url.Parse(resp.Next)
		if parseErr != nil {
			logger.WarnWithCtx(ctx, "fetch_repo_tree: diffstat next URL unparseable — treating every file as changed",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", workspace+"/"+repoSlug),
				zap.Error(parseErr),
			)
			return nil, false
		}
		if parsed.Host != wantHost {
			logger.WarnWithCtx(ctx, "fetch_repo_tree: diffstat next URL host rejected — treating every file as changed",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", workspace+"/"+repoSlug),
				zap.String("got_host", parsed.Host),
				zap.String("want_host", wantHost),
			)
			return nil, false
		}
		nextURL = resp.Next
	}
	return changed, true
}

// bitbucketTreeS3Key is the blob key of the tree captured at sha.
func bitbucketTreeS3Key(orgID, platformRepoID, sha string) string {
	return fmt.Sprintf("repo-trees/%s/%s/%s/%s.json",
		orgID, string(connectorID), url.PathEscape(platformRepoID), sha)
}

// emitBitbucketTreeRecord marshals rec and emits a fetch_repo_tree raw record.
// recordKey is "{platformRepoID}:{treeSHA}" — including the SHA ensures that
// two runs on the same data_date for the same repo produce distinct dedup rows
// in the downstream record store rather than the second run silently overwriting the first.
func emitBitbucketTreeRecord(ctx context.Context, sink sdk.DataSink, cfg *sdk.ConnectorConfig, repoFullName string, fetchedAt time.Time, recordKey string, rec *connectorv1.RepoTreeRecord) error {
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

// bbBranchSHAResponse mirrors the Bitbucket GET
// /2.0/repositories/{workspace}/{repo_slug}/refs/branches/{name} response.
type bbBranchSHAResponse struct {
	Target struct {
		Hash string `json:"hash"`
	} `json:"target"`
}

// bbSrcDepthTimeoutStatus is Bitbucket's documented response when a src/ listing
// with max_depth takes too long to walk. It is not a server fault and not
// retryable as-is — the same call at a shallower depth is the remedy.
const bbSrcDepthTimeoutStatus = 555

// isSrcDepthTimeout reports whether err is the vendor telling us the requested
// recursion depth was too large.
func isSrcDepthTimeout(err error) bool {
	var syncErr *sdk.SyncError
	return errors.As(err, &syncErr) && syncErr.HTTPStatus == bbSrcDepthTimeoutStatus
}

// pathDepth counts the directory levels in a repo-absolute src/ path. The root
// is depth 0, "a" is 1, "a/b" is 2.
func pathDepth(p string) int {
	if p == "" {
		return 0
	}
	return strings.Count(p, "/") + 1
}

// getBitbucketBranchSHA resolves the HEAD commit SHA for the given branch.
// waitUntil bounds how long this lookup may wait on local capacity — it shares
// the repo's budget with the walk that follows it.
func getBitbucketBranchSHA(ctx context.Context, gw sdk.VendorGateway, orgID, workspace, repoSlug, branch string, sem chan struct{}, waitUntil time.Time) (string, error) {
	apiURL := fmt.Sprintf(
		"%s/repositories/%s/%s/refs/branches/%s",
		bitbucketRESTBase(orgID), workspace, repoSlug, url.PathEscape(branch),
	)

	var resp bbBranchSHAResponse
	if err := getJSONWaitingForCapacity(ctx, gw, apiURL, &resp, sem, waitUntil); err != nil {
		return "", fmt.Errorf("get branch %s: %w", branch, err)
	}
	if resp.Target.Hash == "" {
		return "", fmt.Errorf("empty commit SHA for branch %q of %s/%s", branch, workspace, repoSlug)
	}
	return resp.Target.Hash, nil
}

// bbSrcEntry is a single element in a Bitbucket src/ directory listing.
// type is "commit_file" for blobs and "commit_directory" for trees.
type bbSrcEntry struct {
	Type   string `json:"type"` // "commit_file" or "commit_directory"
	Path   string `json:"path"`
	Size   int64  `json:"size,omitempty"`
	Commit *struct {
		Hash string `json:"hash"`
	} `json:"commit,omitempty"`
}

// bbSrcResponse is the paginated response from the Bitbucket src/ API.
type bbSrcResponse struct {
	Values []bbSrcEntry `json:"values"`
	Next   string       `json:"next"`
}

// Capacity-wait tuning. Vars rather than consts so tests can shrink the sleeps;
// production never reassigns them.
var (
	// treeWalkCapacityWait is how long a directory listing waits before
	// re-asking the local limiter for capacity. Sized above the ~2s a token
	// takes to free at the configured per-minute rate, so a wait usually
	// yields on the first retry rather than spinning.
	treeWalkCapacityWait = 4 * time.Second

	// treeWalkCapacityWaitJitter decorrelates the walk's concurrent listings,
	// which would otherwise all wake together and re-exhaust the window in the
	// same instant — the same herd jitteredRetryAfterSeconds guards against.
	treeWalkCapacityWaitJitter = 2 * time.Second

	// bbTreeMaxDepth is how many nested directory levels each src/ listing asks
	// the vendor to expand. Depth is traded against the vendor's own patience:
	// too large and the call returns 555 (a documented "recursion depth too
	// large" timeout), too small and the walk pays a round trip per level. A
	// subtree that does time out is retried shallower rather than failed, so
	// this is a starting point, not a ceiling.
	bbTreeMaxDepth = 10

	// treeWalkFallbackRunBudget bounds a walk whose caller supplied no deadline
	// (local runs, tests). Scheduled runs always inherit the operation's asynq
	// deadline; this only stops an un-deadlined context from waiting forever.
	treeWalkFallbackRunBudget = 30 * time.Minute
)

// repoWalkDeadline returns the instant by which the repo currently being walked
// must give up, given how many repos (itself included) are still unprocessed.
//
// Each repo may spend an even share of the run's REMAINING time, so a repo that
// short-circuits on an unchanged SHA hands its unspent share to the repos behind
// it, while no single repo can consume the whole run.
//
// The bound must be a deadline rather than a count of rejections. Capacity frees
// in bursts — the limiter is a sliding-window log, so tokens return in the shape
// they were spent — and acquisition is an unsynchronized race with no queue. A
// waiter can therefore lose every race for minutes while the window is in fact
// granting. Counting one waiter's losses measures contention, not budget, and
// abandoned a repo with most of the run's allowance still unspent.
func repoWalkDeadline(ctx context.Context, reposRemaining int) time.Time {
	runEnd, ok := ctx.Deadline()
	if !ok {
		runEnd = time.Now().Add(treeWalkFallbackRunBudget)
	}
	if reposRemaining < 1 {
		reposRemaining = 1
	}
	remaining := time.Until(runEnd)
	if remaining <= 0 {
		return runEnd
	}
	return time.Now().Add(remaining / time.Duration(reposRemaining))
}

// getJSONWaitingForCapacity performs one GET, waiting out rejections from our
// OWN rate limiter rather than failing on them.
//
// The same wait-out-local-limit-vs-abort policy also lives in
// pacing.ExecuteWaitingForLocalCapacity and cursor/operations.go
// fetchPagePaced; the three are deliberately not merged and must stay in sync.
//
// This operation is a bulk reader — one call per directory, plus one per repo
// to resolve its branch head — so it routinely exhausts a per-minute window the
// fail-fast ops never approach. Aborting on that strands work permanently:
// walkBitbucketTree must not write a partial tree, and Execute skips a failed
// repo WITHOUT advancing its cursor, so the same call would fail identically
// every cycle. Worse, because repos are iterated in a stable order, a rejection
// partway through starves every later repo on every subsequent run. Local
// capacity is a queue, not a failure, so it is waited out.
//
// Every other error still aborts — a vendor 429 and an open circuit mean the
// vendor wants us to stop, which is the opposite of slowing down.
//
// waitUntil is this repo's slice of the run budget (see repoWalkDeadline);
// capacity is waited out until that instant and no longer. A zero value means
// "wait as long as ctx allows".
//
// out is written only on success; DoJSON leaves it untouched on every path
// that can return a retryable local-limiter rejection.
func getJSONWaitingForCapacity(ctx context.Context, gw sdk.VendorGateway, url string, out any, sem chan struct{}, waitUntil time.Time) error {
	for waits := 0; ; waits++ {
		// The slot covers the vendor call only, never the wait below: a
		// goroutine that is merely sleeping must not occupy the run-wide
		// concurrency that other repos and subdirectories still need.
		err := func() error {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return fmt.Errorf("awaiting request slot: %w", ctx.Err())
			}
			defer func() { <-sem }()
			return gw.DoJSON(ctx, "GET", url, nil, out)
		}()
		if err == nil {
			return nil
		}
		if !errors.Is(err, sdk.ErrLocalRateLimit) {
			return fmt.Errorf("get %s: %w", httputil.RedactURL(url), err)
		}
		//nolint:gosec // jitter is a herd spreader, not a security primitive
		delay := treeWalkCapacityWait + time.Duration(rand.Int63n(int64(treeWalkCapacityWaitJitter)))
		// Give up only once this repo's share of the run budget is spent —
		// checked against the instant the wait would END, so the walk never
		// sleeps past a deadline it already knows it cannot beat.
		if !waitUntil.IsZero() && time.Now().Add(delay).After(waitUntil) {
			return fmt.Errorf("no local rate-limit capacity within this repo's share of the run budget (after %d waits): %w", waits, err)
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return fmt.Errorf("waiting for rate-limit capacity: %w", ctx.Err())
		}
	}
}

// walkBitbucketTree recursively fetches all entries under dirPath for the given
// commitHash using the Bitbucket src/ API. The returned slice contains entries
// for all blobs and trees in the subtree rooted at dirPath.
//
// sem bounds concurrent vendor calls across the WHOLE walk. It has to be
// walk-wide rather than per level: each level fans out through its own
// errgroup, so a per-level limit of N multiplies to N^depth in-flight requests
// down a deep tree — which is how one repo's tree fetch alone could spend an
// org's entire Bitbucket budget in seconds. A slot is held only for the
// duration of one vendor call — never across a capacity wait and never across
// the recursion — so neither a sleeping goroutine nor a parent awaiting
// children occupies concurrency the rest of the tree needs.
//
// waitUntil bounds how long any listing in this walk may wait on local
// capacity; every level and every page shares the one repo-wide instant, so a
// deep subtree cannot extend the repo's budget by restarting the clock.
//
// Any API error propagates immediately — callers MUST NOT write a partial tree.
func walkBitbucketTree(ctx context.Context, gw sdk.VendorGateway, orgID, workspace, repoSlug, commitHash, dirPath string, maxParallel int, sem chan struct{}, waitUntil time.Time, maxDepth int) ([]treewalker.TreeEntry, error) {
	// orgID fixes the expected next-page host for the whole walk; resolve once.
	wantHost := bitbucketRESTHost(orgID)

	// Fetch every entry the vendor will expand under dirPath at depth d,
	// paginating as needed. Each page takes a semaphore slot for its own call;
	// nothing holds one across the recursion below.
	listAtDepth := func(d int) ([]bbSrcEntry, error) {
		var levelEntries []bbSrcEntry
		nextURL := buildSrcURL(orgID, workspace, repoSlug, commitHash, dirPath, "", d)
		for nextURL != "" {
			var resp bbSrcResponse
			if err := getJSONWaitingForCapacity(ctx, gw, nextURL, &resp, sem, waitUntil); err != nil {
				return nil, fmt.Errorf("src/ listing %s/%s @ %s path=%q: %w",
					workspace, repoSlug, commitHash, dirPath, err)
			}
			levelEntries = append(levelEntries, resp.Values...)

			// Validate the next-page URL host before following it. The response
			// should only ever return URLs on the org's configured REST host (the
			// real api.bitbucket.org, or the simulator's host for a simulator org);
			// an unexpected host could indicate response tampering or an SSRF vector.
			if resp.Next == "" {
				nextURL = ""
			} else {
				parsed, parseErr := url.Parse(resp.Next)
				if parseErr != nil {
					return nil, fmt.Errorf("src/ listing %s/%s: parse next URL: %w", workspace, repoSlug, parseErr)
				}
				if parsed.Host != wantHost {
					return nil, fmt.Errorf("src/ listing %s/%s: unexpected next URL host %q (want %q)",
						workspace, repoSlug, parsed.Host, wantHost)
				}
				nextURL = resp.Next
			}
		}
		return levelEntries, nil
	}

	// Ask for the deepest expansion first and step down only when the vendor
	// says it could not finish. Halving rather than decrementing bounds the
	// retries at log2(maxDepth); depth 1 is the floor, where a 555 is a real
	// failure rather than an over-ambitious request.
	depth := maxDepth
	var (
		levelEntries []bbSrcEntry
		listErr      error
	)
	for {
		levelEntries, listErr = listAtDepth(depth)
		if listErr == nil {
			break
		}
		if depth <= 1 || !isSrcDepthTimeout(listErr) {
			return nil, fmt.Errorf("listing directory level: %w", listErr)
		}
		logger.WarnWithCtx(ctx, "fetch_repo_tree: src/ listing timed out, retrying shallower",
			zap.String(logger.OrgID, orgID),
			zap.String("repo", workspace+"/"+repoSlug),
			zap.String("path", dirPath),
			zap.Int("depth", depth),
		)
		depth /= 2
	}

	// Collect this level's entries, converting to TreeEntry.
	out := make([]treewalker.TreeEntry, 0, len(levelEntries))
	for _, e := range levelEntries {
		te := treewalker.TreeEntry{
			Path: e.Path,
			Type: bbSrcTypeToTreeType(e.Type),
		}
		// Populate the per-entry SHA from the src listing's commit.hash. This is the
		// change-detection baseline the consumer keys on — without
		// it the SHA is empty and the artifact is dropped by the defensive
		// blob_sha != "" skip, so no VCS content ever flows for Bitbucket. If this
		// hash proves to be the browsed-ref commit rather than the per-file
		// last-touched commit, Bitbucket over-fetches every artifact on any repo
		// commit (bounded and correctness-preserving), pending a possible per-file
		// format=meta upgrade. Not verified against the live API; a test pins the assumed shape.
		if e.Commit != nil {
			te.SHA = e.Commit.Hash
		}
		if e.Type == "commit_file" && e.Size > 0 {
			s := e.Size
			te.Size = &s
		}
		out = append(out, te)
	}

	// The vendor already expanded everything above the depth cap, so the only
	// directories still owing a call are the ones sitting exactly at it —
	// listed as entries, but with their contents withheld. Anything shallower
	// has already been returned in full, and re-walking it would duplicate
	// entries. `depth` (not maxDepth) is the cap that actually applied, which
	// differs whenever the retry above stepped down.
	var subdirs []bbSrcEntry
	base := pathDepth(dirPath)
	for _, e := range levelEntries {
		if e.Type == "commit_directory" && pathDepth(e.Path)-base == depth {
			subdirs = append(subdirs, e)
		}
	}

	if len(subdirs) == 0 {
		return out, nil
	}

	// Fan out to subdirectories concurrently. This bounds goroutine breadth at
	// this level; the actual vendor-call concurrency is bounded walk-wide by
	// sem, which is what the rate limit cares about.
	g, gCtx := errgrp.WithContext(ctx)
	g.SetLimit(maxParallel)

	var (
		mu      sync.Mutex
		subOuts [][]treewalker.TreeEntry
	)

	for _, sub := range subdirs {
		subEntry := sub
		treewalker.RecordSubtreeFetch(connectorName)
		g.Go(func() error {
			children, err := walkBitbucketTree(gCtx, gw, orgID, workspace, repoSlug, commitHash, subEntry.Path, maxParallel, sem, waitUntil, maxDepth)
			if err != nil {
				return fmt.Errorf("walk bitbucket tree: %w", err)
			}
			mu.Lock()
			subOuts = append(subOuts, children)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("waiting: %w", err)
	}

	for _, children := range subOuts {
		out = append(out, children...)
	}
	return out, nil
}

// buildSrcURL constructs the Bitbucket src/ API URL for a directory listing.
// When afterPath is non-empty it is appended as the `after` pagination cursor.
//
// The Bitbucket src/ API path format is:
//
//	/2.0/repositories/{workspace}/{repo_slug}/src/{commit}/{dir_path}/
//
// Each path segment is encoded individually so that the "/" separators between
// the commit hash and the directory components are preserved as literal slashes
// in the URL path (url.PathEscape would encode "/" as "%2F" which is invalid
// for Bitbucket's routing). maxDepth > 1 adds max_depth so the vendor expands
// the subtree itself; 1 is the API default and is left off the query string.
// No `format` query param is sent: the paginated
// directory listing is the default response, and `format=meta` is a file-only
// selector that 400s on a directory/root path. The trailing slash keeps the
// bare-commit root on the listing response instead of a 302 redirect.
func buildSrcURL(orgID, workspace, repoSlug, commitHash, dirPath, afterPath string, maxDepth int) string {
	base := fmt.Sprintf(
		"%s/repositories/%s/%s/src/%s",
		bitbucketRESTBase(orgID), workspace, repoSlug, url.PathEscape(commitHash),
	)
	// Append directory path segments individually so "/" delimiters are preserved.
	if dirPath != "" {
		for _, segment := range strings.Split(dirPath, "/") {
			if segment != "" {
				base += "/" + url.PathEscape(segment)
			}
		}
	}
	apiURL := base + "/?pagelen=100"
	if maxDepth > 1 {
		apiURL += "&max_depth=" + strconv.Itoa(maxDepth)
	}
	if afterPath != "" {
		apiURL += "&after=" + url.QueryEscape(afterPath)
	}
	return apiURL
}

// bbSrcTypeToTreeType converts a Bitbucket src/ API entry type string to the
// canonical "blob"/"tree" type used by treewalker.TreeEntry.
func bbSrcTypeToTreeType(bbType string) string {
	switch bbType {
	case "commit_directory":
		return "tree"
	default:
		return "blob"
	}
}
