// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/vcs"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/internal/settingskey"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// errNoMRCommit signals that an MR has no datable commit, so first_commit_at is
// left unset. It is not a fetch failure — callers handle it quietly.
var errNoMRCommit = errors.New("no datable commit found for MR")

// defaultLookback is the maximum history window for first-sync operations.
// When no cursor exists, operations fetch data from this far back instead of
// fetching the entire history.
const defaultLookback = 12 * 30 * 24 * time.Hour // ~12 months

// metaKeyPRNumber is the metadata key carrying the MR IID. Stamped on
// fetch_reviews note and approval emissions plus the matching webhook hook
// so a consumer's pendingReviewers map can key by (platform_repo_id,
// pr_number) without parsing the record key.
const metaKeyPRNumber = "pr_number"

// stateClosed is the GitLab MR/issue state value (and matching event_type) for
// items closed without merge.
const stateClosed = "closed"

// firstSyncSince returns a floor timestamp for first-sync runs.
// If since is zero (no prior cursor), it returns now minus defaultLookback.
// Otherwise it returns since unchanged.
func firstSyncSince(since time.Time) time.Time {
	if since.IsZero() {
		return time.Now().UTC().Add(-defaultLookback)
	}
	return since
}

// --- Common helpers ---

// fetchAllProjects fetches all projects in the configured GitLab group via REST API.
func fetchAllProjects(ctx context.Context, gw sdk.VendorGateway, settings GitLabSettings) ([]glProject, error) {
	restGW := gw.ForGroup(constants.GroupGitLabREST)
	var projects []glProject
	page := int32(1)

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListProjectsRequest{
			GroupId: settings.GroupID,
			Page:    page,
			PerPage: 100,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch group projects: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.GitLabListProjectsResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *GitLabListProjectsResponse", protoResp)
		}

		for _, projRaw := range resp.GetProjects() {
			var p glProject
			if err := json.Unmarshal(projRaw, &p); err != nil {
				logger.WarnWithCtx(ctx, "skipping malformed project entry", zap.Error(err))
				continue
			}
			projects = append(projects, p)
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
		if err := checkPageCap("group project listing", page); err != nil {
			return nil, err
		}
	}

	return projects, nil
}

// withEventType returns a copy of meta with "event_type" set to eventType.
func withEventType(meta map[string]string, eventType string) map[string]string {
	out := make(map[string]string, len(meta)+1)
	for k, v := range meta {
		out[k] = v
	}
	out["event_type"] = eventType
	return out
}

// enrichMRResult holds per-file metadata derived from the MR diffs endpoint.
type enrichMRResult struct {
	FilesStr     string // JSON-encoded []string of file paths for files_changed metadata
	FilesStatStr string // JSON-encoded []glMRFileLineStat for files_line_stats metadata
	// Degraded reports that the file list is INCOMPLETE even though the fetch
	// returned no error: a skipped unparseable entry, an unexpected response type,
	// or the page cap reached with more pages available. The caller must not persist
	// a degraded list as an authoritative files_line_stats (it would overwrite a
	// complete stored value), the same contract as Bitbucket's enrichPRResult.Degraded.
	Degraded bool
}

// enrichMR fetches the diffs for a merge request and returns per-file path list
// and per-file line stats for metadata. On a fetch error the empty JSON arrays are
// returned with the error; on a partial read (see Degraded) the arrays hold what was
// parsed and Degraded is set so the caller can proceed without persisting a partial
// answer as authoritative.
func enrichMR(ctx context.Context, gw sdk.VendorGateway, projectID int, mrIID int) (enrichMRResult, error) {
	restGW := gw.ForGroup(constants.GroupGitLabREST)

	var filePaths []string
	var fileStats []glMRFileLineStat
	degraded := false
	// GitLab's /diffs entry carries no per-file line counts or too_large flag, so a
	// dropped diff body (collapsed / too_large) is proxied by a MODIFIED file (non-new,
	// non-deleted, non-rename) with an empty diff — the same proxy fetch_mr_diffs.go
	// uses for its line counts. Count them to flag a materially incomplete file list.
	modifiedCandidates := 0
	truncatedFiles := 0

	const maxDiffPages = 50
	for page := int32(1); page <= maxDiffPages; page++ {
		protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListMRDiffsRequest{
			ProjectId:       int64(projectID),
			MergeRequestIid: int64(mrIID),
			Page:            page,
			PerPage:         100,
		})
		if err != nil {
			return enrichMRResult{FilesStr: "[]", FilesStatStr: "[]"},
				fmt.Errorf("fetching MR diffs page %d: %w", page, err)
		}

		resp, ok := protoResp.(*connectorpb.GitLabListMRDiffsResponse)
		if !ok {
			// An unexpected response type truncates the file list; mark degraded so a
			// partial answer is not persisted as authoritative.
			degraded = true
			break
		}

		for _, diffRaw := range resp.GetDiffs() {
			var entry glMRDiffEntry
			if err := json.Unmarshal(diffRaw, &entry); err != nil {
				logger.WarnWithCtx(ctx, "skipping malformed MR diff entry", zap.Int("mr_iid", mrIID), zap.Error(err))
				degraded = true
				continue
			}

			additions, deletions := countDiffLines(entry.Diff)

			// Use new_path for added/modified files; old_path for deletions.
			path := entry.NewPath
			if entry.DeletedFile {
				path = entry.OldPath
			}
			if path == "" {
				continue
			}

			// A modified text file that came back with an empty diff body is content
			// GitLab dropped; the path is still captured (path coverage needs it) but it
			// counts toward the truncation ratio below. new/rename/delete entries
			// legitimately carry no textual diff and are excluded.
			if !entry.DeletedFile && !entry.RenamedFile && !entry.NewFile {
				modifiedCandidates++
				if entry.Diff == "" {
					truncatedFiles++
				}
			}

			filePaths = append(filePaths, path)
			fileStats = append(fileStats, glMRFileLineStat{
				Path:      path,
				Additions: additions,
				Deletions: deletions,
			})
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		if page == maxDiffPages {
			// The cap is reached with another page still available: the file list is
			// truncated, so the aggregate is incomplete.
			logger.WarnWithCtx(ctx, "MR diffs exceeded max pages; file list truncated",
				zap.Int("mr_iid", mrIID), zap.Int32("pages_walked", page))
			degraded = true
		}
	}

	// Too many modified files missing their diff body → the file list is materially
	// incomplete even though every page fetched cleanly. Shares the truncation test
	// with fetch_mr_diffs.go so both diff paths draw the same line.
	if mrDiffsMateriallyTruncated(truncatedFiles, modifiedCandidates) {
		logger.WarnWithCtx(ctx, "MR diffs materially truncated; file list incomplete",
			zap.Int("mr_iid", mrIID), zap.Int("truncated_files", truncatedFiles), zap.Int("modified_candidates", modifiedCandidates))
		degraded = true
	}

	if len(filePaths) == 0 {
		return enrichMRResult{FilesStr: "[]", FilesStatStr: "[]", Degraded: degraded}, nil
	}

	filesJSON, err := json.Marshal(filePaths)
	if err != nil {
		return enrichMRResult{FilesStr: "[]", FilesStatStr: "[]"},
			fmt.Errorf("marshaling file paths: %w", err)
	}
	fileStatsJSON, err := json.Marshal(fileStats)
	if err != nil {
		return enrichMRResult{FilesStr: "[]", FilesStatStr: "[]"},
			fmt.Errorf("marshaling file line stats: %w", err)
	}
	return enrichMRResult{
		FilesStr:     string(filesJSON),
		FilesStatStr: string(fileStatsJSON),
		Degraded:     degraded,
	}, nil
}

// formatAuthorID returns the author ID as a string, or empty string if zero (ghost/bot user).
func formatAuthorID(id int64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatInt(id, 10)
}

// detectIsBot maps a GitLab user `bot` flag to the canonical "true"/"false"
// metadata string consumed by a consumer.
func detectIsBot(bot bool) string {
	if bot {
		return "true"
	}
	return "false"
}

// countDiffLines counts additions and deletions from a unified diff string.
// Lines starting with "+" (excluding the "+++" header) are additions;
// lines starting with "-" (excluding the "---" header) are deletions.
//
// Known limitations: binary diffs return 0/0 (correct — no text lines to count).
// "No newline at end of file" markers (lines starting with "\") are ignored (correct).
// GitLab's /diffs endpoint does not provide per-file stats directly, so
// parsing the diff string is the only option for per-file granularity.
func countDiffLines(diff string) (additions, deletions int) {
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++") {
			additions++
		} else if strings.HasPrefix(line, "-") && !strings.HasPrefix(line, "---") {
			deletions++
		}
	}
	return
}

// formatPRCreatedAt renders a parent MR/PR creation time for the pr_created_at
// metadata key: RFC3339/UTC when set, empty string when zero so a consumer's
// date gate stays fail-open.
func formatPRCreatedAt(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// emitRecord emits a single raw JSON record to the sink.
// dataDate must always be a meaningful event timestamp (e.g., created_at, updated_at, committed_date).
func emitRecord(ctx context.Context, sink sdk.DataSink, opID sdk.OperationID, orgID string, externalID string, payload json.RawMessage, metadata map[string]string, dataDate time.Time) error {
	if err := sink.Emit(ctx, &sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    string(opID),
		OrganizationID: orgID,
		ExternalID:     externalID,
		Payload:        payload,
		FetchedAt:      time.Now().UTC(),
		DataDate:       dataDate,
		Metadata:       metadata,
	}); err != nil {
		return fmt.Errorf("emitting %s record: %w", opID, err)
	}
	return nil
}

// getCompositeCursorTime extracts a per-project timestamp from a composite cursor.
// Returns zero time if not found (first sync).
func getCompositeCursorTime(ctx context.Context, cursor *sdk.Cursor, projectKey string) time.Time {
	if cursor == nil {
		return time.Time{}
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.WarnWithCtx(ctx, "malformed composite cursor; treating as first sync", zap.Error(err))
		return time.Time{}
	}
	raw, ok := parts[projectKey]
	if !ok {
		return time.Time{}
	}
	var ts time.Time
	if err := json.Unmarshal(raw, &ts); err != nil {
		logger.WarnWithCtx(ctx, "malformed cursor entry for project; treating as first sync",
			zap.String("project_key", projectKey), zap.Error(err))
		return time.Time{}
	}
	return ts
}

// buildCompositeCursor creates a composite cursor from per-project timestamps.
func buildCompositeCursor(timestamps map[string]time.Time) *sdk.Cursor {
	parts := make(map[string]json.RawMessage, len(timestamps))
	for k, v := range timestamps {
		raw, err := json.Marshal(v.UTC())
		if err != nil {
			logger.DebugNoCtx("gitlab: failed to marshal timestamp part; skipping", zap.String("key", k), zap.Error(err))
			continue
		}
		parts[k] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// saveDiscoveredRepos persists the list of discovered projects into connector settings.
func saveDiscoveredRepos(ctx context.Context, params *sdk.ExecutionParams, settings GitLabSettings, projects []glProject) {
	if params.ConfigUpdater == nil {
		return
	}
	discovered := make([]GitLabDiscoveredRepo, 0, len(projects))
	for _, p := range projects {
		discovered = append(discovered, GitLabDiscoveredRepo{
			FullName:      p.PathWithNamespace,
			DefaultBranch: p.DefaultBranch,
			Private:       p.Visibility == "private",
			ProjectID:     int64(p.ID),
		})
	}
	// Patch only discovered_repos, so a settings change made while this run
	// was in flight is kept.
	err := settingskey.Patch(ctx, params.ConfigUpdater, params.Config.OrganizationID, params.Config.ConnectorID, "discovered_repos",
		func(json.RawMessage) (any, bool, error) { return discovered, true, nil })
	if err != nil {
		logger.WarnWithCtx(ctx, "failed to save discovered repos", zap.Error(err))
	}
}

// --- FetchProjectsOp ---

type FetchProjectsOp struct{}

func (op *FetchProjectsOp) OperationID() sdk.OperationID { return "fetch_projects" }

func (op *FetchProjectsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
	page := int32(1)
	count := 0
	var allProjects []glProject

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListProjectsRequest{
			GroupId: settings.GroupID,
			Page:    page,
			PerPage: 100,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch projects: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.GitLabListProjectsResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T", protoResp)
		}

		for _, projRaw := range resp.GetProjects() {
			var p glProject
			if err := json.Unmarshal(projRaw, &p); err != nil {
				logger.WarnWithCtx(ctx, "skipping malformed project entry", zap.Error(err))
				continue
			}

			allProjects = append(allProjects, p)
			externalID := p.ExternalID()
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, projRaw, map[string]string{"group": settings.GroupName}, time.Time{}); emitErr != nil {
				return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}
			count++
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
		if err := checkPageCap("fetch projects", page); err != nil {
			return nil, err
		}
	}

	saveDiscoveredRepos(ctx, params, settings, allProjects)
	logger.Info(ctx, "fetched projects", zap.Int64("group_id", settings.GroupID), zap.Int("count", count))
	return sdk.NewTimestampCursor(time.Now().UTC()), nil
}

// --- fetch_prs per-page checkpoint (same scheme as the GitHub connector) ---

// repoCursorState tracks per-project fetch_prs sync progress: the high-water
// mark (max updated_at seen across processed pages). Every resume — whether
// clean or after a mid-pagination kill — restarts at page 1 with
// updated_after = firstSyncSince(Timestamp). Resume is always by value
// (updated_after), never by a saved page NUMBER: GitLab's merge_requests list
// has no keyset-pagination alternative (confirmed against GitLab's REST API
// pagination docs — only Groups/Projects/Users/Issues/Audit Events support
// keyset), so pagination is offset-based over order_by=updated_at&sort=asc,
// and an offset page number is not stable across a resume — an already-
// fetched MR's updated_at changing in the gap re-sorts the list and can shift
// a not-yet-fetched MR onto an earlier page than the one a saved-page resume
// would ask for, silently and permanently skipping it (its updated_at can
// never regress below the already-advanced high-water mark). Resuming by
// value instead of by page number never skips one across a resume boundary
// (idempotent re-emit if it re-walks anything); in practice the re-walk cost
// is small — since ascending order means a resume's updated_after already
// lands at (or via the endpoint's confirmed-inclusive semantics, at-or-past)
// the exact page the checkpoint last advanced through — except for the
// deliberate `stuckAt` freeze in fetch_prs_data.go, which re-walks back to a
// still-unresolved malformed entry on purpose.
type repoCursorState struct {
	Timestamp time.Time `json:"ts"`
}

// decodeCursorStateEntry parses one composite-cursor entry, a repoCursorState
// object. ok is false when the entry does not parse or carries no timestamp, in
// which case the caller treats the project as having no prior state.
func decodeCursorStateEntry(ctx context.Context, projectKey string, raw json.RawMessage) (state repoCursorState, ok bool) {
	err := json.Unmarshal(raw, &state)
	if err == nil && !state.Timestamp.IsZero() {
		return state, true
	}
	logger.WarnWithCtx(ctx, "decodeCursorStateEntry: cursor entry unparseable",
		zap.String("project_key", projectKey), zap.Error(err))
	return repoCursorState{}, false
}

// getRepoCursorState extracts one project's cursor state from a composite cursor.
func getRepoCursorState(ctx context.Context, cursor *sdk.Cursor, projectKey string) repoCursorState {
	if cursor == nil {
		return repoCursorState{}
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.WarnWithCtx(ctx, "getRepoCursorState: composite cursor parse failed; treating as no prior state",
			zap.String("project_key", projectKey), zap.Error(err))
		return repoCursorState{}
	}
	raw, ok := parts[projectKey]
	if !ok {
		return repoCursorState{}
	}
	state, _ := decodeCursorStateEntry(ctx, projectKey, raw)
	return state
}

// existingCursorStates decodes every per-project cursor state from a composite
// cursor, pre-populating the working map so per-page checkpoints preserve the
// progress of projects not yet re-synced in the current run. Same parse as
// getRepoCursorState.
func existingCursorStates(ctx context.Context, cursor *sdk.Cursor) map[string]repoCursorState {
	states := make(map[string]repoCursorState)
	if cursor == nil {
		return states
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.WarnWithCtx(ctx, "existingCursorStates: composite cursor parse failed; returning no states", zap.Error(err))
		return states
	}
	for projectKey, raw := range parts {
		if state, ok := decodeCursorStateEntry(ctx, projectKey, raw); ok {
			states[projectKey] = state
		}
	}
	return states
}

// buildStateCursor serializes per-project cursor states into a composite cursor.
func buildStateCursor(ctx context.Context, states map[string]repoCursorState) *sdk.Cursor {
	parts := make(map[string]json.RawMessage, len(states))
	for projectKey, state := range states {
		state.Timestamp = state.Timestamp.UTC()
		raw, err := json.Marshal(state)
		if err != nil {
			logger.WarnWithCtx(ctx, "buildStateCursor: marshaling project cursor state failed; skipping project",
				zap.String("project_key", projectKey), zap.Error(err))
			continue
		}
		parts[projectKey] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// pageCheckpointFn is invoked after each fully-processed MR page with the
// current high-water mark, so a mid-run kill loses at most the pages since the
// last checkpoint rather than the whole run's progress. The next resume always
// restarts at page 1 (see repoCursorState) — this checkpoint advances how far
// back that resume's updated_after filter needs to reach, not which page it
// starts from.
type pageCheckpointFn func(ctx context.Context, highWater time.Time) error

// makePageCheckpoint wires a page-level checkpoint into the framework's
// run-level Checkpoint. Returns nil when the run has no Checkpoint (e.g. in
// tests), making onPage a no-op.
func makePageCheckpoint(checkpoint sdk.CheckpointFunc, cursorStates map[string]repoCursorState, projectKey string) pageCheckpointFn {
	if checkpoint == nil {
		return nil
	}
	return func(ctx context.Context, highWater time.Time) error {
		cursorStates[projectKey] = repoCursorState{Timestamp: highWater}
		return checkpoint(ctx, buildStateCursor(ctx, cursorStates))
	}
}

// --- FetchPRsOp ---

type FetchPRsOp struct{}

func (op *FetchPRsOp) OperationID() sdk.OperationID { return "fetch_prs" }

func (op *FetchPRsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	projects, err := fetchAllProjects(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching projects: %w", err)
	}

	// Pre-populate every project's state so a per-page checkpoint preserves the
	// progress of projects not touched in this run.
	cursorStates := existingCursorStates(ctx, params.Cursor)

	var skipped repotally.Skipper
	var ctxErr error
repos:
	for _, project := range projects {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		projectKey := strconv.Itoa(project.ID)
		skipped.Attempt()

		// Every run — clean or resuming after a mid-pagination kill — starts at
		// page 1 with updated_after = the last checkpointed high-water mark. See
		// repoCursorState's doc comment for why resuming by a saved page NUMBER
		// is unsafe on this endpoint.
		state := getRepoCursorState(ctx, params.Cursor, projectKey)
		since := firstSyncSince(state.Timestamp)
		page := int32(1)

		// since == state.Timestamp today (firstSyncSince is the identity for a
		// non-zero input), but this guard keeps that monotonic even if
		// firstSyncSince ever grows a clock-skew overlap buffer (e.g.
		// since.Add(-1*time.Hour)) — without it, a tick with no new MRs would
		// persist a high-water mark BELOW the prior one, regressing the cursor.
		latestDate := since
		if state.Timestamp.After(latestDate) {
			latestDate = state.Timestamp
		}
		updatedAfter := since.UTC().Format(time.RFC3339)
		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, projectKey)

		restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
		count := 0

		for {
			protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListProjectMergeRequestsRequest{
				ProjectId:    int64(project.ID),
				UpdatedAfter: updatedAfter,
				Page:         page,
				PerPage:      100,
				WithStats:    true,
			})
			if err != nil {
				if skipped.Skip(project.PathWithNamespace, err) {
					logger.WarnWithCtx(ctx, "skipping a project that failed", zap.String("project", project.PathWithNamespace), zap.Error(err))
					continue repos
				}
				return nil, fmt.Errorf("fetch merge requests for project %s: %w", project.PathWithNamespace, err)
			}

			resp, ok := protoResp.(*connectorpb.GitLabListProjectMergeRequestsResponse)
			if !ok {
				return nil, fmt.Errorf("unexpected response type %T", protoResp)
			}

			for _, mrRaw := range resp.GetMergeRequests() {
				var mr glMergeRequest
				if err := json.Unmarshal(mrRaw, &mr); err != nil {
					logger.WarnWithCtx(ctx, "skipping malformed merge request entry",
						zap.String("project", project.PathWithNamespace), zap.Error(err))
					continue
				}

				externalID := mr.ExternalID()

				// Fetch per-file diffs to extract file paths and line stats.
				enriched, enrichErr := enrichMR(ctx, params.VendorGateway, project.ID, mr.IID)
				if enrichErr != nil {
					logger.WarnWithCtx(ctx, "failed to enrich MR file metadata, using empty defaults",
						zap.String("repo", project.PathWithNamespace), zap.Int("mr_iid", mr.IID), zap.Error(enrichErr))
				}

				// first_commit_at enrichment — chained call per MR.
				// Rate-limit budget: ~14,400 calls/day per token (tightest of the three VCS,
				//). Large-band tenants land at ~70% of headroom. Promotion
				// threshold: rate-limit hits on fetch_prs exceed 1% of runs in
				// 24h window sustained over 3 days → promote to chained-secondary asynq op.
				firstCommitAt, mrCommitSHAs, fcErr := fetchEarliestMRCommit(ctx, restGW, project.ID, mr.IID)
				if fcErr != nil {
					if !errors.Is(fcErr, errNoMRCommit) {
						logger.WarnWithCtx(ctx, "fetch earliest MR commit failed; first_commit_at will be unset",
							zap.String("repo", project.PathWithNamespace),
							zap.Int("mr_iid", mr.IID),
							zap.Error(fcErr))
					}
					firstCommitAt = nil
				}

				// Native PR→issue linkage. closes_issues lives on its own
				// GitLab endpoint, so this is a distinct THIRD chained per-MR REST
				// call on top of the two-call baseline (enrichMR diffs +
				// fetchEarliestMRCommit), raising per-MR consumption on GitLab's
				// tightest-of-three VCS budget by ~50%. It is therefore gated on
				// the org's FetchNativeIssueLinks opt-in; when unset the org keeps
				// the two-call budget and native linkage stays unset. An org
				// already near the budget ceiling on the two-call baseline cannot
				// absorb the third call — its path is the chained-secondary asynq
				// promotion, not this flag. On failure (or when gated
				// off) we omit the metadata key so a consumer preserves prior
				// linkage rather than clobbering it.
				var closesIssuesStr string
				if settings.FetchNativeIssueLinks {
					var ciErr error
					closesIssuesStr, ciErr = fetchMRClosesIssues(ctx, restGW, project.ID, mr.IID)
					if ciErr != nil {
						logger.WarnWithCtx(ctx, "fetch MR closes_issues failed; native linkage will be unset",
							zap.String("repo", project.PathWithNamespace),
							zap.Int("mr_iid", mr.IID),
							zap.Error(ciErr))
						closesIssuesStr = ""
					}
				}

				meta := map[string]string{
					"group":               settings.GroupName,
					"repo":                project.PathWithNamespace,
					"pr_author_id":        formatAuthorID(mr.Author.ID),
					"files_changed":       enriched.FilesStr,
					"reviews_requested":   strconv.Itoa(len(mr.Reviewers)),
					"head_branch":         mr.SourceBranch,
					"target_branch":       mr.TargetBranch,
					"is_cross_repository": strconv.FormatBool(mr.SourceProjectID != mr.TargetProjectID && mr.SourceProjectID != 0),
				}
				// files_line_stats is authoritative only on a trustworthy enrichment. A failed
				// or degraded (truncated) enrichMR returns "[]" or a partial list a consumer
				// cannot tell from a genuinely empty diff, and the DAO COALESCEs a non-null
				// value over the stored stats — so stamping it would erase real path coverage.
				// Omit the key otherwise, mirroring closes_issues below and Bitbucket's guard;
				// a clean nil-error "[]" is a real empty diff and is stamped.
				if enrichErr == nil && !enriched.Degraded {
					meta["files_line_stats"] = enriched.FilesStatStr
				}
				if firstCommitAt != nil {
					meta["first_commit_at"] = firstCommitAt.UTC().Format(time.RFC3339)
				}
				// The commit-to-merge-request link, read back off the page fetched above.
				// Omitted rather than emptied when the fetch failed: the record upsert
				// assigns metadata wholesale, so writing an empty value would erase a list
				// an earlier run recorded, turning a transient API failure into lost links.
				if refs := vcs.JoinCommitSHAs(mrCommitSHAs); refs != "" {
					meta[vcs.MetaKeyPRCommitSHAs] = refs
				}
				if closesIssuesStr != "" {
					meta["closes_issues"] = closesIssuesStr
				}

				// "opened" — every MR was opened; DataDate is createdAt so the count
				// always lands on the creation date regardless of subsequent updates.
				if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID+"#opened", mrRaw, withEventType(meta, "opened"), mr.CreatedAt); emitErr != nil {
					return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}

				// "snapshot" — mutable counters (comments, reviewers, diff stats, etc.). A
				// current-state record, so no DataDate: repeated fetches overwrite one row.
				if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID+"#snapshot", mrRaw, withEventType(meta, "snapshot"), time.Time{}); emitErr != nil {
					return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}

				// "merged" — only when mergedAt is set; DataDate is mergedAt so the
				// merge count lands on the actual merge date.
				if mr.MergedAt != nil {
					if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID+"#merged", mrRaw, withEventType(meta, "merged"), *mr.MergedAt); emitErr != nil {
						return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
					}
				}

				// "closed" — only for MRs closed without merge; DataDate is closedAt.
				if mr.State == "closed" && mr.MergedAt == nil {
					closedAt := mr.CreatedAt // fallback if closedAt somehow nil
					if mr.ClosedAt != nil {
						closedAt = *mr.ClosedAt
					}
					if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID+"#closed", mrRaw, withEventType(meta, "closed"), closedAt); emitErr != nil {
						return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
					}
				}

				if mr.UpdatedAt.After(latestDate) {
					latestDate = mr.UpdatedAt
				}
				count++
			}

			finished := resp.GetNextPageUrl() == ""
			if onPage != nil {
				if cpErr := onPage(ctx, latestDate); cpErr != nil {
					// Best-effort progress, not the success contract — log and keep
					// going so a checkpoint blip doesn't abort an otherwise-good run.
					logger.Error(ctx, "fetch_prs: per-page checkpoint failed (continuing)",
						zap.String("project", project.PathWithNamespace),
						zap.Int32("page", page),
						zap.Error(cpErr))
				}
			}

			if finished {
				break
			}
			page++
			if err := checkPageCap("merge request listing for project "+project.PathWithNamespace, page); err != nil {
				return nil, err
			}
		}

		cursorStates[projectKey] = repoCursorState{Timestamp: latestDate}
		logger.Info(ctx, "fetched merge requests", zap.String("project", project.PathWithNamespace), zap.Int("count", count))
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), buildStateCursor(ctx, cursorStates))
		}
		return buildStateCursor(ctx, cursorStates), fmt.Errorf("fetch_prs: %w", errors.Join(ctxErr, cpErr))
	}
	if err := skipped.Err(len(projects)); err != nil {
		return nil, err
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	return buildStateCursor(ctx, cursorStates), nil
}

// fetchEarliestMRCommit fetches an MR's commits via the chained
// GET /api/v4/projects/{id}/merge_requests/{iid}/commits endpoint
// and returns both the earliest authored date and the page's commit
// SHAs. Returns (nil, nil, nil) when the MR has no commits. Returns (nil, nil, err)
// on API failure — the caller emits the MR without first_commit_at rather than
// dropping the record.
//
// The SHAs are the commit-to-merge-request link, and they are free: this page is
// already fetched for the date, and GitLab's commit payload names no merge request,
// so reading the link back off this list is the only channel that does not cost a
// request per commit.
//
// Timestamp field: authored_date is used rather than created_at because it reflects
// "when the work began" — i.e. the author's local commit time. created_at is the API
// record creation timestamp which may differ in edge cases (e.g. force-pushed history,
// import from another system).
//
// Ordering: the GitLab REST endpoint does NOT document `order_by` or `sort` for
// /merge_requests/:iid/commits (only the path params are supported per
// https://docs.gitlab.com/api/merge_requests/#get-single-merge-request-commits).
// We fetch a full page (PerPage=100) and take MIN(authored_date) client-side rather
// than relying on undocumented server ordering. MRs with >100 commits are rare; if a
// production signal shows the truncation is meaningful, follow up with full
// pagination.
func fetchEarliestMRCommit(ctx context.Context, restGW sdk.VendorGateway, projectID, mrIID int) (*time.Time, []string, error) {
	protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListMRCommitsRequest{
		ProjectId: strconv.Itoa(projectID),
		//nolint:gosec // MR IIDs are GitLab-internal and well below int32 range
		MrIid:   int32(mrIID),
		PerPage: 100,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list MR commits: %w", err)
	}
	resp, ok := protoResp.(*connectorpb.GitLabListMRCommitsResponse)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected response type %T", protoResp)
	}
	commits := resp.GetCommits()
	if len(commits) == 0 {
		return nil, nil, errNoMRCommit
	}
	// Each element is a raw JSON commit object from the gateway. Take MIN
	// authored_date across the page — server ordering is undocumented for this
	// endpoint, so we cannot trust commits[0] to be the earliest.
	var earliest time.Time
	shas := make([]string, 0, len(commits))
	for _, raw := range commits {
		var c struct {
			ID           string    `json:"id"`
			AuthoredDate time.Time `json:"authored_date"`
		}
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, nil, fmt.Errorf("unmarshal MR commit: %w", err)
		}
		if c.ID != "" {
			shas = append(shas, c.ID)
		}
		if c.AuthoredDate.IsZero() {
			continue
		}
		if earliest.IsZero() || c.AuthoredDate.Before(earliest) {
			earliest = c.AuthoredDate
		}
	}
	// A commit with no usable date still carries a usable link, so the SHAs are
	// returned even when the date lookup finds nothing to report.
	if earliest.IsZero() {
		return nil, shas, errNoMRCommit
	}
	t := earliest.UTC()
	return &t, shas, nil
}

// fetchMRClosesIssues fetches the issues an MR closes via keyword refs and
// returns the raw JSON array verbatim, for a consumer's native PR→issue
// linkage extractor. A successful zero-result fetch returns "[]" (a non-empty
// string the caller stamps, recording "checked, none"); an empty string means
// the fetch failed or the endpoint returned an empty body, and the caller then
// omits the metadata key so a failed fetch leaves prior linkage untouched
// rather than clobbering it with an empty set. Chained per-MR call — see the
// GitLab rate-budget note at the FetchPRsOp per-MR loop.
func fetchMRClosesIssues(ctx context.Context, restGW sdk.VendorGateway, projectID, mrIID int) (string, error) {
	protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListMRClosesIssuesRequest{
		ProjectId:       int64(projectID),
		MergeRequestIid: int64(mrIID),
	})
	if err != nil {
		return "", fmt.Errorf("list MR closes_issues: %w", err)
	}
	resp, ok := protoResp.(*connectorpb.GitLabListMRClosesIssuesResponse)
	if !ok {
		return "", fmt.Errorf("unexpected response type %T", protoResp)
	}
	return string(resp.GetIssues()), nil
}

// --- FetchCommitsOp ---

type FetchCommitsOp struct{}

func (op *FetchCommitsOp) OperationID() sdk.OperationID { return "fetch_commits" }

// metaKeyRefs is the record-metadata key naming the refs a commit was reached from,
// comma-separated. A consumer side declares the same literal
// (the consumer's refs metadata key) rather than sharing a constant: a connector must not
// import its consumer, and platform_repo_id crosses the same boundary the same way. The two
// must be changed together.
const metaKeyRefs = "refs"

// maxRefsPerRepo bounds how many tracked refs one project's commit walk visits. No provider
// call covers several refs *and* attributes each commit to the ref it came from — GitLab
// takes one ref_name — so cost is strictly refs × pages per project. Same posture as the
// other pagination bounds: a pathological input must not run the sync indefinitely.

// refsBySHA tracks which refs a commit has been reached from so far in this repository's
// walk. Only the ref names are held here, and the walk keeps streaming each page to the
// sink as it did before multi-ref. When more than one ref is walked, the commit loop also
// keeps each commit's enriched payload for the project, so a re-emission from a later ref
// carries the line stats rather than the list payload; that memory grows with the
// project's commits in the window.
//
// The union survives without buffering because refs only ever accumulate and the record is
// re-emitted on every sighting: the dedup key does not vary by ref and the upsert assigns
// metadata wholesale, so the LAST write wins and the last write carries the largest ref set.
type refsBySHA map[string]map[string]struct{}

// add records a sighting and returns the full ref set for the record's metadata.
func (r refsBySHA) add(sha, ref string) string {
	set, ok := r[sha]
	if !ok {
		set = make(map[string]struct{}, 1)
		r[sha] = set
	}
	set[ref] = struct{}{}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// commitDetailStops reports whether a failed commit-detail fetch must stop the
// walk at its commit instead of emitting the list payload, which carries no line
// stats. Anything a later attempt can fix stops it — a rate limit, a 5xx or
// network failure, an auth failure, the run's context ending — because the
// cursor would otherwise move past the commit for good. A failure no retry can
// fix (a 404 for one commit) keeps the best-effort emit.
func commitDetailStops(err error) bool {
	if repotally.StopsTheRun(err) {
		return true
	}
	var syncErr *sdk.SyncError
	return errors.As(err, &syncErr) && syncErr.Retryable
}

func (op *FetchCommitsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	projects, err := fetchAllProjects(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching projects: %w", err)
	}

	cursorTimestamps := make(map[string]time.Time)

	// The administrator's tracked-branch set, globs already expanded server-side. Any failure
	// returns (nil, false) and every project degrades to its default branch — never to all,
	// which would turn a settings outage into a provider-budget incident.
	trackedRefs, refsResolved := vcs.ResolveTrackedRefs(ctx, params.Host.TrackedRefs, params.Config.OrganizationID, string(connectorID))
	refsByRepo := vcs.IndexTrackedRefs(trackedRefs)

	// Every listed project starts from its stored watermark, so a project the run
	// does not reach (its deadline) keeps it in the cursor.
	for _, project := range projects {
		if prior := getCompositeCursorTime(ctx, params.Cursor, strconv.Itoa(project.ID)); !prior.IsZero() {
			cursorTimestamps[strconv.Itoa(project.ID)] = prior
		}
	}
	var skipped repotally.Skipper
	var ctxErr error
repos:
	for n, project := range projects {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		if n > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, buildCompositeCursor(cursorTimestamps)); err != nil {
				logger.Error(ctx, "fetch_commits: checkpoint failed (continuing)", zap.String("project", project.PathWithNamespace), zap.Error(err))
			}
		}
		projectKey := strconv.Itoa(project.ID)
		skipped.Attempt()
		refs, skipProject := vcs.CommitRefsForRepo(ctx, refsByRepo, refsResolved, project.PathWithNamespace, project.DefaultBranch)
		if skipProject {
			// MatchNone: the project resolved to no countable refs, so fetching nothing is
			// the resolved answer rather than a failure. The watermark is carried forward
			// explicitly — cursorTimestamps is rebuilt each run, so returning here without
			// it would drop the entry and re-walk the whole lookback when the project is
			// tracked again.
			if prior := getCompositeCursorTime(ctx, params.Cursor, projectKey); !prior.IsZero() {
				cursorTimestamps[projectKey] = prior
			}
			continue
		}

		since := firstSyncSince(getCompositeCursorTime(ctx, params.Cursor, projectKey))
		latestDate := since
		sinceStr := since.UTC().Format(time.RFC3339)

		restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)

		// Ref sets only — the payloads stream straight through, as before multi-ref.
		seenRefs := make(refsBySHA)
		// enrichedBySHA keeps the detail payload of a commit already emitted, so a later
		// sighting from another ref re-emits it rather than the stats-less list payload:
		// the upsert keeps the last write. Only needed when more than one ref is walked.
		var enrichedBySHA map[string]json.RawMessage
		if len(refs) > 1 {
			enrichedBySHA = make(map[string]json.RawMessage)
		}

		for _, ref := range refs {
			page := int32(1)

			for {
				protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListCommitsRequest{
					ProjectId:   int64(project.ID),
					Since:       sinceStr,
					Page:        page,
					PerPage:     100,
					RefName:     ref,
					FirstParent: true, // restrict to trunk history; excludes merged-branch commits
				})
				if err != nil {
					if skipped.Skip(project.PathWithNamespace, err) {
						logger.WarnWithCtx(ctx, "skipping a project that failed", zap.String("project", project.PathWithNamespace), zap.Error(err))
						if prior := getCompositeCursorTime(ctx, params.Cursor, projectKey); !prior.IsZero() {
							cursorTimestamps[projectKey] = prior
						}
						continue repos
					}
					return nil, fmt.Errorf("fetch commits for project %s: %w", project.PathWithNamespace, err)
				}

				resp, ok := protoResp.(*connectorpb.GitLabListCommitsResponse)
				if !ok {
					return nil, fmt.Errorf("unexpected response type %T", protoResp)
				}

				done := false
				for _, commitRaw := range resp.GetCommits() {
					var commit glCommit
					if err := json.Unmarshal(commitRaw, &commit); err != nil {
						logger.WarnWithCtx(ctx, "skipping malformed commit entry",
							zap.String("project", project.PathWithNamespace), zap.Error(err))
						continue
					}

					// Stop if we've reached commits older than our cursor.
					if !since.IsZero() && commit.CommittedDate.Before(since) {
						done = true
						break
					}

					// A commit already reached from another ref is re-emitted with the
					// widened ref set, but must NOT pay a second detail call — that is a
					// per-commit request which would otherwise multiply by ref count.
					_, alreadySeen := seenRefs[commit.ID]

					// Enrich commit with line stats from the detail endpoint.
					// Non-fatal: if the detail call fails, fall back to the list payload.
					enrichedRaw := commitRaw
					if alreadySeen {
						if prior, ok := enrichedBySHA[commit.ID]; ok {
							enrichedRaw = prior
						}
					} else {
						detailResp, detailErr := restGW.Execute(ctx, &connectorpb.GitLabGetCommitDetailRequest{
							ProjectId: int64(project.ID),
							Sha:       commit.ID,
						})
						if detailErr != nil && commitDetailStops(detailErr) {
							// Stop at this commit rather than emit the stats-less list
							// payload: this project's watermark is not advanced, so the
							// next run re-walks from here and fetches the stats.
							if err := ctx.Err(); err != nil {
								ctxErr = err
								break repos
							}
							if skipped.Skip(project.PathWithNamespace, detailErr) {
								logger.WarnWithCtx(ctx, "skipping a project whose commit detail failed", zap.String("project", project.PathWithNamespace), zap.String("sha", commit.ID), zap.Error(detailErr))
								if prior := getCompositeCursorTime(ctx, params.Cursor, projectKey); !prior.IsZero() {
									cursorTimestamps[projectKey] = prior
								}
								continue repos
							}
							return nil, fmt.Errorf("fetch commit detail for project %s@%s: %w", project.PathWithNamespace, commit.ID, detailErr)
						}
						if detailErr == nil {
							if detail, ok := detailResp.(*connectorpb.GitLabGetCommitDetailResponse); ok && len(detail.GetCommit()) > 0 {
								enrichedRaw = detail.GetCommit()
							}
						} else {
							logger.WarnWithCtx(ctx, "failed to fetch commit detail, using list payload", zap.String("project", project.PathWithNamespace), zap.String("sha", commit.ID), zap.Error(detailErr))
						}
					}

					if enrichedBySHA != nil {
						enrichedBySHA[commit.ID] = enrichedRaw
					}

					meta := map[string]string{
						"repo":      project.PathWithNamespace,
						metaKeyRefs: seenRefs.add(commit.ID, ref),
					}
					// Emitted here rather than after the ref loop: the walk checkpoints as
					// it pages, and a cursor saved past commits that never reached the sink
					// would skip them permanently.
					externalID := commit.ExternalID(project.PathWithNamespace)
					if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, enrichedRaw, meta, commit.CommittedDate); emitErr != nil {
						return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
					}

					if commit.CommittedDate.After(latestDate) {
						latestDate = commit.CommittedDate
					}
				}

				if done || resp.GetNextPageUrl() == "" {
					break
				}
				page++
				if err := checkPageCap("commit listing for project "+project.PathWithNamespace+" ref "+ref, page); err != nil {
					return nil, err
				}
			}
		}

		cursorTimestamps[projectKey] = latestDate
		logger.Info(ctx, "fetched commits", zap.String("project", project.PathWithNamespace), zap.Time("cursor", latestDate))
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), buildCompositeCursor(cursorTimestamps))
		}
		return buildCompositeCursor(cursorTimestamps), fmt.Errorf("fetch_commits: %w", errors.Join(ctxErr, cpErr))
	}
	if err := skipped.Err(len(projects)); err != nil {
		return nil, err
	}
	if len(cursorTimestamps) == 0 {
		return params.Cursor, nil
	}
	return buildCompositeCursor(cursorTimestamps), nil
}

// --- FetchIssuesOp ---

type FetchIssuesOp struct{}

func (op *FetchIssuesOp) OperationID() sdk.OperationID { return "fetch_issues" }

func (op *FetchIssuesOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	var sinceIssues time.Time
	if s, cursorErr := sdk.TimestampFromCursor(params.Cursor); cursorErr == nil {
		sinceIssues = s
	} else {
		logger.Info(ctx, "no usable issues cursor; using default lookback", zap.Error(cursorErr))
	}
	sinceIssues = firstSyncSince(sinceIssues)
	updatedAfterIssues := sinceIssues.UTC().Format(time.RFC3339)

	restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
	latestUpdated := time.Time{}
	page := int32(1)
	count := 0

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListIssuesRequest{
			GroupId:      settings.GroupID,
			UpdatedAfter: updatedAfterIssues,
			Page:         page,
			PerPage:      100,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch issues: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.GitLabListIssuesResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T", protoResp)
		}

		for _, issueRaw := range resp.GetIssues() {
			var issue glIssue
			if err := json.Unmarshal(issueRaw, &issue); err != nil {
				logger.WarnWithCtx(ctx, "skipping malformed issue entry", zap.Error(err))
				continue
			}

			externalID := issue.ExternalID()
			meta := map[string]string{"group": settings.GroupName}

			// "opened" — every issue was opened; DataDate is createdAt.
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID+"#opened", issueRaw, withEventType(meta, "opened"), issue.CreatedAt); emitErr != nil {
				return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// "snapshot" — mutable counters. A current-state record, so no DataDate:
			// repeated fetches overwrite one row.
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID+"#snapshot", issueRaw, withEventType(meta, "snapshot"), time.Time{}); emitErr != nil {
				return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// "closed" — only when issue is closed; DataDate is closedAt.
			if issue.State == "closed" && issue.ClosedAt != nil {
				if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID+"#closed", issueRaw, withEventType(meta, "closed"), *issue.ClosedAt); emitErr != nil {
					return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}
			}

			if issue.UpdatedAt.After(latestUpdated) {
				latestUpdated = issue.UpdatedAt
			}
			count++
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
		if err := checkPageCap("issue listing", page); err != nil {
			return nil, err
		}
	}

	logger.Info(ctx, "fetched issues", zap.Int64("group_id", settings.GroupID), zap.Int("count", count))

	if latestUpdated.IsZero() {
		return params.Cursor, nil
	}
	return sdk.NewTimestampCursor(latestUpdated), nil
}

// --- FetchReviewsOp ---

type FetchReviewsOp struct{}

func (op *FetchReviewsOp) OperationID() sdk.OperationID { return "fetch_reviews" }

func (op *FetchReviewsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	projects, err := fetchAllProjects(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching projects: %w", err)
	}

	cursorTimestamps := make(map[string]time.Time)

	// A project whose merge requests, notes or approvals cannot be read is that
	// project's failure: it keeps its cursor, so nothing it holds is skipped
	// for good, and the other projects still sync.
	var skipped repotally.Skipper
	var ctxErr error
	// Every listed project starts from its stored watermark, so a project the run
	// does not reach (its deadline) keeps it in the cursor.
	for _, project := range projects {
		if prior := getCompositeCursorTime(ctx, params.Cursor, strconv.Itoa(project.ID)); !prior.IsZero() {
			cursorTimestamps[strconv.Itoa(project.ID)] = prior
		}
	}
	for n, project := range projects {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		if n > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, buildCompositeCursor(cursorTimestamps)); err != nil {
				logger.Error(ctx, "fetch_reviews: checkpoint failed (continuing)", zap.String("project", project.PathWithNamespace), zap.Error(err))
			}
		}
		skipped.Attempt()
		projectKey := strconv.Itoa(project.ID)
		since := firstSyncSince(getCompositeCursorTime(ctx, params.Cursor, projectKey))
		latestDate := since
		updatedAfter := since.UTC().Format(time.RFC3339)

		restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
		noteCount := 0
		approvalCount := 0

		// Iterate MRs for this project, then fetch notes + approvals per MR.
		// GitLab Notes API is per-resource: /projects/{id}/merge_requests/{iid}/notes
		var projErr error
		mrPage := int32(1)
	mrs:
		for {
			mrResp, mrErr := restGW.Execute(ctx, &connectorpb.GitLabListProjectMergeRequestsRequest{
				ProjectId:    int64(project.ID),
				UpdatedAfter: updatedAfter,
				Page:         mrPage,
				PerPage:      100,
			})
			if mrErr != nil {
				projErr = fmt.Errorf("list merge requests: %w", mrErr)
				break
			}
			mrListResp, ok := mrResp.(*connectorpb.GitLabListProjectMergeRequestsResponse)
			if !ok {
				break
			}

			for _, mrRaw := range mrListResp.GetMergeRequests() {
				var mr glMergeRequest
				if err := json.Unmarshal(mrRaw, &mr); err != nil {
					logger.WarnWithCtx(ctx, "skipping malformed merge request entry for reviews",
						zap.String("project", project.PathWithNamespace), zap.Error(err))
					continue
				}

				// Fetch notes (comments) for this MR.
				notePage := int32(1)
				for {
					protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListNotesRequest{
						ProjectId:       int64(project.ID),
						MergeRequestIid: int64(mr.IID),
						Page:            notePage,
						PerPage:         100,
					})
					if err != nil {
						projErr = fmt.Errorf("list notes for MR %d: %w", mr.IID, err)
						break mrs
					}

					resp, ok := protoResp.(*connectorpb.GitLabListNotesResponse)
					if !ok {
						break
					}

					for _, noteRaw := range resp.GetNotes() {
						var note glNote
						if err := json.Unmarshal(noteRaw, &note); err != nil {
							logger.WarnWithCtx(ctx, "skipping malformed note entry",
								zap.String("project", project.PathWithNamespace), zap.Int("mr_iid", mr.IID), zap.Error(err))
							continue
						}

						// Skip system-generated notes — assignments, label changes, "merged", etc.
						// They would otherwise be counted as reviewer activity by a consumer.
						if note.System {
							continue
						}

						externalID := note.ExternalID()
						noteMeta := map[string]string{
							"repo":             project.PathWithNamespace,
							"pr_key":           fmt.Sprintf("mr-%d!%d", project.ID, mr.IID),
							metaKeyPRNumber:    strconv.Itoa(mr.IID),
							"state":            "COMMENTED",
							"pr_author_login":  mr.Author.Username,
							"pr_author_id":     formatAuthorID(mr.Author.ID),
							"platform_repo_id": strconv.Itoa(project.ID),
							"pr_created_at":    formatPRCreatedAt(mr.CreatedAt),
							"reviewer_is_bot":  detectIsBot(note.Author.Bot),
						}
						if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, noteRaw, noteMeta, note.CreatedAt); emitErr != nil {
							return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
						}

						if note.UpdatedAt.After(latestDate) {
							latestDate = note.UpdatedAt
						}
						noteCount++
					}

					if resp.GetNextPageUrl() == "" {
						break
					}
					notePage++
					if err := checkPageCap(fmt.Sprintf("note listing for MR %d in %s", mr.IID, project.PathWithNamespace), notePage); err != nil {
						return nil, err
					}
				}

				// Fetch approvals for this MR.
				approvalsResp, appErr := restGW.Execute(ctx, &connectorpb.GitLabListMRApprovalsRequest{
					ProjectId: int64(project.ID),
					Iid:       int64(mr.IID),
				})
				if appErr != nil {
					projErr = fmt.Errorf("list approvals for MR %d: %w", mr.IID, appErr)
					break mrs
				}
				approvalsData, ok := approvalsResp.(*connectorpb.GitLabListMRApprovalsResponse)
				if !ok || len(approvalsData.GetApprovals()) == 0 {
					continue
				}
				var approvalsBody glApprovalsResponse
				if err := json.Unmarshal(approvalsData.GetApprovals(), &approvalsBody); err != nil {
					logger.WarnWithCtx(ctx, "skipping malformed MR approvals body",
						zap.String("project", project.PathWithNamespace), zap.Int("mr_iid", mr.IID), zap.Error(err))
					continue
				}
				for _, approval := range approvalsBody.ApprovedBy {
					approvalPayload, marshalErr := json.Marshal(map[string]interface{}{
						"user":       map[string]interface{}{"id": approval.User.ID, "username": approval.User.Username, "bot": approval.User.Bot},
						"state":      "APPROVED",
						"created_at": approval.CreatedAt.UTC().Format(time.RFC3339),
					})
					if marshalErr != nil {
						logger.WarnWithCtx(ctx, "skipping approval with unmarshalable payload",
							zap.String("project", project.PathWithNamespace), zap.Int("mr_iid", mr.IID), zap.Error(marshalErr))
						continue
					}
					externalID := fmt.Sprintf("approval-%d!%d-%d", project.ID, mr.IID, approval.User.ID)
					approvalMeta := map[string]string{
						"repo":             project.PathWithNamespace,
						"review_type":      "approval",
						"pr_key":           fmt.Sprintf("mr-%d!%d", project.ID, mr.IID),
						metaKeyPRNumber:    strconv.Itoa(mr.IID),
						"pr_author_login":  mr.Author.Username,
						"pr_author_id":     formatAuthorID(mr.Author.ID),
						"platform_repo_id": strconv.Itoa(project.ID),
						"pr_created_at":    formatPRCreatedAt(mr.CreatedAt),
						"reviewer_is_bot":  detectIsBot(approval.User.Bot),
					}
					if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, approvalPayload, approvalMeta, approval.CreatedAt); emitErr != nil {
						return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
					}
					approvalCount++
				}
			}

			if mrListResp.GetNextPageUrl() == "" {
				break
			}
			mrPage++
			if err := checkPageCap("merge request listing for project "+project.PathWithNamespace, mrPage); err != nil {
				return nil, err
			}
		}

		if projErr != nil {
			if !skipped.Skip(project.PathWithNamespace, projErr) {
				return nil, fmt.Errorf("fetch reviews for project %s: %w", project.PathWithNamespace, projErr)
			}
			logger.WarnWithCtx(ctx, "skipping a project that failed", zap.String("project", project.PathWithNamespace), zap.Error(projErr))
			if prior := getCompositeCursorTime(ctx, params.Cursor, projectKey); !prior.IsZero() {
				cursorTimestamps[projectKey] = prior
			}
			continue
		}
		cursorTimestamps[projectKey] = latestDate
		logger.Info(ctx, "fetched MR notes", zap.String("project", project.PathWithNamespace), zap.Int("count", noteCount))
		logger.Info(ctx, "fetched MR approvals", zap.String("project", project.PathWithNamespace), zap.Int("count", approvalCount))
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), buildCompositeCursor(cursorTimestamps))
		}
		return buildCompositeCursor(cursorTimestamps), fmt.Errorf("fetch_reviews: %w", errors.Join(ctxErr, cpErr))
	}
	if err := skipped.Err(len(projects)); err != nil {
		return nil, err
	}
	if len(cursorTimestamps) == 0 {
		return params.Cursor, nil
	}
	return buildCompositeCursor(cursorTimestamps), nil
}

// --- FetchPRCommentsOp ---

const (
	opFetchPRComments = "fetch_pr_comments"
	opFetchRepos      = "fetch_repos"
	opFetchRepoTree   = "fetch_repo_tree"
	// opFetchBranches lists every branch of each group project. Full-list per
	// project each run; no cursor.
	opFetchBranches = "fetch_branches"
	// opFetchConfig captures per-project governance config (merge_method,
	// squash_option, default_branch) + per-group defaults.
	opFetchConfig = "fetch_config"
)

type FetchPRCommentsOp struct{}

func (op *FetchPRCommentsOp) OperationID() sdk.OperationID { return opFetchPRComments }

func (op *FetchPRCommentsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	projects, err := fetchAllProjects(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching projects: %w", err)
	}

	cursorTimestamps := make(map[string]time.Time)

	// Every listed project starts from its stored watermark, so a project the run
	// does not reach (its deadline) keeps it in the cursor.
	for _, project := range projects {
		if prior := getCompositeCursorTime(ctx, params.Cursor, strconv.Itoa(project.ID)); !prior.IsZero() {
			cursorTimestamps[strconv.Itoa(project.ID)] = prior
		}
	}
	var skipped repotally.Skipper
	var ctxErr error
repos:
	for n, project := range projects {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		if n > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, buildCompositeCursor(cursorTimestamps)); err != nil {
				logger.Error(ctx, "fetch_pr_comments: checkpoint failed (continuing)", zap.String("project", project.PathWithNamespace), zap.Error(err))
			}
		}
		projectKey := strconv.Itoa(project.ID)
		skipped.Attempt()
		since := firstSyncSince(getCompositeCursorTime(ctx, params.Cursor, projectKey))
		latestDate := since
		updatedAfter := since.UTC().Format(time.RFC3339)

		restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
		commentCount := 0

		// Iterate MRs for this project, then fetch notes per MR.
		mrPage := int32(1)
		for {
			mrResp, mrErr := restGW.Execute(ctx, &connectorpb.GitLabListProjectMergeRequestsRequest{
				ProjectId:    int64(project.ID),
				UpdatedAfter: updatedAfter,
				Page:         mrPage,
				PerPage:      100,
			})
			if mrErr != nil {
				if skipped.Skip(project.PathWithNamespace, mrErr) {
					logger.WarnWithCtx(ctx, "skipping a project that failed", zap.String("project", project.PathWithNamespace), zap.Error(mrErr))
					if prior := getCompositeCursorTime(ctx, params.Cursor, projectKey); !prior.IsZero() {
						cursorTimestamps[projectKey] = prior
					}
					continue repos
				}
				return nil, fmt.Errorf("fetch MRs for pr comments %s: %w", project.PathWithNamespace, mrErr)
			}
			mrListResp, ok := mrResp.(*connectorpb.GitLabListProjectMergeRequestsResponse)
			if !ok {
				break
			}

			for _, mrRaw := range mrListResp.GetMergeRequests() {
				var mr glMergeRequest
				if err := json.Unmarshal(mrRaw, &mr); err != nil {
					logger.WarnWithCtx(ctx, "skipping malformed merge request entry for pr comments",
						zap.String("project", project.PathWithNamespace), zap.Error(err))
					continue
				}

				projectPath := project.PathWithNamespace
				mrIID := mr.IID
				mrAuthorUsername := mr.Author.Username
				mrAuthorID := mr.Author.ID

				// Fetch notes for this MR, filtering to user-generated comments only.
				notePage := int32(1)
				for {
					protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListNotesRequest{
						ProjectId:       int64(project.ID),
						MergeRequestIid: int64(mrIID),
						Page:            notePage,
						PerPage:         100,
					})
					if err != nil {
						if skipped.Skip(project.PathWithNamespace, err) {
							logger.WarnWithCtx(ctx, "skipping a project that failed", zap.String("project", project.PathWithNamespace), zap.Error(err))
							if prior := getCompositeCursorTime(ctx, params.Cursor, projectKey); !prior.IsZero() {
								cursorTimestamps[projectKey] = prior
							}
							continue repos
						}
						return nil, fmt.Errorf("fetch notes for MR comments %s#MR-%d: %w", projectPath, mrIID, err)
					}

					resp, ok := protoResp.(*connectorpb.GitLabListNotesResponse)
					if !ok {
						break
					}

					for _, noteRaw := range resp.GetNotes() {
						var note glNote
						if err := json.Unmarshal(noteRaw, &note); err != nil {
							logger.WarnWithCtx(ctx, "skipping malformed note entry for pr comments",
								zap.String("project", projectPath), zap.Int("mr_iid", mrIID), zap.Error(err))
							continue
						}

						// Skip notes already processed in a previous sync.
						if !since.IsZero() && note.CreatedAt.Before(since) {
							continue
						}

						// Skip system-generated notes (e.g., "merged", "approved", label changes).
						if note.System {
							continue
						}

						externalID := fmt.Sprintf("%s#MR-%d/COMMENT-%d", projectPath, mrIID, note.ID)
						meta := map[string]string{
							"repo":               projectPath,
							"mr_iid":             strconv.Itoa(mrIID),
							"mr_author_username": mrAuthorUsername,
							"mr_author_id":       strconv.FormatInt(mrAuthorID, 10),
							"pr_author_login":    mrAuthorUsername,
							"pr_author_id":       strconv.FormatInt(mrAuthorID, 10),
							"platform_repo_id":   strconv.Itoa(project.ID),
							"pr_created_at":      formatPRCreatedAt(mr.CreatedAt),
							"reviewer_is_bot":    detectIsBot(note.Author.Bot),
						}

						if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, noteRaw, meta, note.CreatedAt); emitErr != nil {
							return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
						}

						if note.CreatedAt.After(latestDate) {
							latestDate = note.CreatedAt
						}
						commentCount++
					}

					if resp.GetNextPageUrl() == "" {
						break
					}
					notePage++
					if err := checkPageCap(fmt.Sprintf("note listing for MR %d in %s", mr.IID, project.PathWithNamespace), notePage); err != nil {
						return nil, err
					}
				}
			}

			if mrListResp.GetNextPageUrl() == "" {
				break
			}
			mrPage++
			if err := checkPageCap("merge request listing for project "+project.PathWithNamespace, mrPage); err != nil {
				return nil, err
			}
		}

		cursorTimestamps[projectKey] = latestDate
		logger.Info(ctx, "fetched MR comments", zap.String("project", project.PathWithNamespace), zap.Int("count", commentCount))
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), buildCompositeCursor(cursorTimestamps))
		}
		return buildCompositeCursor(cursorTimestamps), fmt.Errorf("fetch_pr_comments: %w", errors.Join(ctxErr, cpErr))
	}
	if err := skipped.Err(len(projects)); err != nil {
		return nil, err
	}
	if len(cursorTimestamps) == 0 {
		return params.Cursor, nil
	}
	return buildCompositeCursor(cursorTimestamps), nil
}
