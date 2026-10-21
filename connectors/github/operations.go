// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/vcs"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/internal/settingskey"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Operation ID constants for GitHub connector operations.
const (
	opFetchPRs        = "fetch_prs"
	opFetchCommits    = "fetch_commits"
	opFetchIssues     = "fetch_issues"
	opFetchReviews    = "fetch_reviews"
	opFetchPRComments = "fetch_pr_comments"
	opFetchRepos      = "fetch_repos"
	opFetchRepoTree   = "fetch_repo_tree"
	// opFetchBranches lists every branch of each accessible repo. Full-list per
	// repo each run; no cursor.
	opFetchBranches = "fetch_branches"
	// opFetchConfig captures per-repo governance config (default branch +
	// effective branch rulesets: required reviews, allowed merge methods).
	opFetchConfig = "fetch_config"
	// opFetchPRsData ships PR descriptions to S3.
	// Sibling of opFetchPRs.
	opFetchPRsData = "fetch_prs_data"
	// opFetchIssuesData ships issue descriptions to S3. Sibling of
	// opFetchIssues; same design record.
	opFetchIssuesData = "fetch_issues_data"
)

// hasTimeRemaining returns false if the context deadline is within
// the checkpoint reserve window, signaling that the operation should
// stop and save progress.
func hasTimeRemaining(ctx context.Context, reserve time.Duration) bool {
	if reserve == 0 {
		return true
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(deadline) > reserve
}

// checkpointWriteTimeout caps a single Checkpoint() call. Checkpoint writes
// are sub-second Redis ops, so 10s is generous headroom — much longer than
// the parent ctx's remaining budget could be when the surrounding fetch ran
// up against the asynq deadline.
const checkpointWriteTimeout = 10 * time.Second

// checkpointContext returns a context detached from the parent's deadline
// but still propagating cancellation values (logger, trace_id). Used so a
// Checkpoint() call right at the edge of the asynq task deadline does not
// inherit an already-expired ctx — that was the root cause of the
// "checkpoint for {repo}: context deadline exceeded" errors in fetch_reviews
// (a large repository ran past its deadline mid-loop).
func checkpointContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), checkpointWriteTimeout) //nolint:gosec // G118 false positive: CancelFunc is returned to the caller
}

// safePRNumber clamps a PR number to the int32 range used by the REST request
// protos. GitHub PR numbers are small positive integers, so the clamp only
// guards against a pathological value rather than real data.
func safePRNumber(n int) int32 {
	if n >= 0 && n <= math.MaxInt32 {
		return int32(n)
	}
	return math.MaxInt32
}

// prAlreadyProcessed checks if a PR number is in the processed list.
func prAlreadyProcessed(processedPRs []int, prNum int) bool {
	for _, n := range processedPRs {
		if n == prNum {
			return true
		}
	}
	return false
}

// Default values for fetch_issues inline/tail comment fetching, used when
// the Operations entry omits these fields or sets them to zero.
const (
	defaultIssueCommentsInlinePerIssue = 25
	defaultIssueCommentsMaxTailPages   = 10
)

// issueCommentsTailConfig returns inline-per-issue and max-tail-pages for
// fetch_issues, sourced from the same Operations.fetch_issues entry that
// carries PageSize/MinPageSize/BackoffDivisor (same shape as every other
// GitHub op). Falls back to package defaults when the entry is missing or
// the fields are zero.
func issueCommentsTailConfig(paginationCfg *config.GitHubPaginationConfig) (inlinePerIssue, maxTailPages int) {
	inlinePerIssue = defaultIssueCommentsInlinePerIssue
	maxTailPages = defaultIssueCommentsMaxTailPages
	if paginationCfg == nil || paginationCfg.Operations == nil {
		return
	}
	cfg, ok := paginationCfg.Operations[opFetchIssues]
	if !ok || cfg == nil {
		return
	}
	if v := cfg.InlinePerIssue; v > 0 {
		inlinePerIssue = v
	}
	if v := cfg.MaxTailPages; v > 0 {
		maxTailPages = v
	}
	return
}

// opPageConfig returns the pagination config for the given operation,
// falling back to sensible defaults if the config is nil or missing.
//
// It returns a copy: the connector shares one config across every operation instance,
// and concurrent runs defaulting the shared entry in place would race.
func opPageConfig(paginationCfg *config.GitHubPaginationConfig, opID string) *config.OperationPaginationConfig {
	if paginationCfg != nil && paginationCfg.Operations != nil {
		if shared, ok := paginationCfg.Operations[opID]; ok && shared != nil {
			cfg := *shared
			// A divisor below 2 never shrinks the page, so a persistent 5xx would
			// retry the same page size forever.
			if cfg.BackoffDivisor < 2 {
				cfg.BackoffDivisor = 2
			}
			if cfg.MinPageSize <= 0 {
				cfg.MinPageSize = 1
			}
			if cfg.PageSize <= 0 {
				cfg.PageSize = 25
			}
			return &cfg
		}
	}
	return &config.OperationPaginationConfig{
		PageSize:       25,
		MinPageSize:    1,
		BackoffDivisor: 2,
	}
}

// firstSyncSince returns a floor timestamp for first-sync runs.
// If since is zero (no prior cursor), it returns now minus the given
// lookback window in days. Otherwise it returns since unchanged.
// lookbackDays should come from GitHubSettings.GetLookbackDays() which
// walks per-org setting -> YAML default -> hardcoded 365.
func firstSyncSince(since time.Time, lookbackDays int) time.Time {
	if since.IsZero() {
		if lookbackDays <= 0 {
			lookbackDays = DefaultLookbackDays
		}
		return time.Now().UTC().Add(-time.Duration(lookbackDays) * 24 * time.Hour)
	}
	return since
}

// --- Common helpers ---

// repoInfo holds a repo's full name, its default branch, and its stable
// numeric database ID from GitHub. DatabaseID matches the platform_repo_id
// emitted by fetch_repos (fmt.Sprintf("%d", gh.ID)) and must be used as
// PlatformRepoId in fetch_repo_tree records so UpdateTree can locate the row.
type repoInfo struct {
	FullName      string
	DefaultBranch string
	DatabaseID    int64 // GitHub numeric repo ID; 0 when list comes from settings.Repositories
}

// fetchAllRepos returns the list of repos to sync.
// If settings.Repositories is set, those are used; otherwise all org repos are fetched via REST.
// Archived repos are skipped here: PR/commit fetchers have no interest in dead repos and skipping
// them conserves GitHub API rate-limit budget. Repo entity discovery (FetchReposOp) emits archived
// repos separately so a consumer's repository table stays up to date.
func fetchAllRepos(ctx context.Context, gw sdk.VendorGateway, settings GitHubSettings) ([]repoInfo, error) {
	if len(settings.Repositories) > 0 {
		// Enrich each selected full name with the numeric repo ID + default branch
		// from the discovered_repos cache (warmed by the picker's fresh listing /
		// auto-discovery). Without the ID, fetch_repo_tree would key tree records by
		// full name while a consumer's repository records are keyed by numeric ID, so UpdateTree
		// would miss the row; without the default branch it would fall back to
		// "main" and 404 on repos whose default is something else. A repo absent
		// from the cache degrades to the prior name-only behaviour.
		byName := make(map[string]GitHubDiscoveredRepo, len(settings.DiscoveredRepos))
		for _, d := range settings.DiscoveredRepos {
			byName[d.FullName] = d
		}
		result := make([]repoInfo, len(settings.Repositories))
		for i, r := range settings.Repositories {
			ri := repoInfo{FullName: r}
			if d, ok := byName[r]; ok {
				ri.DatabaseID = d.DatabaseID
				ri.DefaultBranch = d.DefaultBranch
			}
			result[i] = ri
		}
		return result, nil
	}

	restGW := gw.ForGroup(constants.GroupGitHubREST)
	var repos []repoInfo
	page := int32(1)

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.ListOrgReposRequest{
			Organization: settings.Organization,
			Page:         page,
			PerPage:      100,
			Type:         "all",
		})
		if err != nil {
			return nil, fmt.Errorf("fetch org repos: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.ListOrgReposResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *ListOrgReposResponse", protoResp)
		}

		for _, repoRaw := range resp.GetRepos() {
			var r ghRepository
			if err := json.Unmarshal(repoRaw, &r); err != nil {
				logger.WarnWithCtx(ctx, "fetchAllRepos: skipping unparseable repo node", zap.Error(err))
				continue
			}
			if r.Archived {
				continue
			}
			if !settings.IncludePrivate && r.Private {
				continue
			}
			repos = append(repos, repoInfo{FullName: r.FullName, DefaultBranch: r.DefaultBranch, DatabaseID: r.ID})
		}

		if !resp.GetHasNextPage() {
			break
		}
		page++
	}

	return repos, nil
}

// saveDiscoveredRepos persists the list of discovered repos into connector settings.
// Only runs in auto-discovery mode (no explicit repo list) and when a ConfigUpdater is available.
func saveDiscoveredRepos(ctx context.Context, params *sdk.ExecutionParams, settings GitHubSettings, repos []repoInfo) {
	if params.ConfigUpdater == nil || len(settings.Repositories) > 0 {
		return // explicit repo list — don't overwrite
	}
	discovered := make([]GitHubDiscoveredRepo, 0, len(repos))
	for _, r := range repos {
		discovered = append(discovered, GitHubDiscoveredRepo{
			FullName:      r.FullName,
			DefaultBranch: r.DefaultBranch,
			DatabaseID:    r.DatabaseID,
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

// githubBucketToGroup maps BudgetLimiter bucket identifiers to the
// gateway/defaults.yaml group name whose cap they represent. GitHub
// Cloud splits caps per resource (core / graphql / search) and each
// observed value flows into Settings.VendorRateLimits[group] keyed by
// the matching yml group, so the gateway factory can scale only that
// group on the next run.
var githubBucketToGroup = map[ratelimiter.Bucket]string{
	ratelimiter.BucketGitHubCore:    constants.GroupGitHubREST,
	ratelimiter.BucketGitHubGraphQL: constants.GroupGitHubGraphQL,
	ratelimiter.BucketGitHubSearch:  constants.GroupGitHubRESTSearch,
}

// persistObservedRateLimitCap walks the supplied bucket list and lifts each
// non-empty BudgetLimiter snapshot into Settings.VendorRateLimits, keyed by
// the corresponding gateway group. The gateway factory reads
// the map and scales only those groups on the next invocation, leaving
// other yml groups at their existing floor, so one group's cap never leaks
// into another (e.g. the REST core cap shrinking GraphQL throughput).
//
// No-op when the snapshot is missing for every requested bucket, or when
// ConfigUpdater isn't available. It patches only vendor_rate_limits, merged
// against the value stored at write time, so every other settings key
// (an operator's change made during the run included) is left as stored.
func persistObservedRateLimitCap(ctx context.Context, params *sdk.ExecutionParams, buckets ...ratelimiter.Bucket) {
	if params.ConfigUpdater == nil || params.BudgetLimiter == nil {
		return
	}

	// Build a bucket→group subset filtered by the operation's requested
	// buckets. GitHub publishes per-resource buckets via X-RateLimit-Resource;
	// each requested bucket maps to one yml group via githubBucketToGroup,
	// and the shared merger writes one entry per fresh observation.
	subset := make(map[ratelimiter.Bucket]string, len(buckets))
	for _, b := range buckets {
		if group, ok := githubBucketToGroup[b]; ok {
			subset[b] = group
		}
	}

	err := settingskey.Patch(ctx, params.ConfigUpdater, params.Config.OrganizationID, params.Config.ConnectorID, "vendor_rate_limits",
		func(current json.RawMessage) (any, bool, error) {
			var stored map[string]pacing.VendorRateLimitObservation
			if len(current) > 0 {
				if err := json.Unmarshal(current, &stored); err != nil {
					return nil, false, fmt.Errorf("parse stored vendor_rate_limits: %w", err)
				}
			}
			merged, changed := pacing.MergeVendorRateLimits(ctx, params.BudgetLimiter, params.Config.OrganizationID, subset, stored)
			return merged, changed, nil
		})
	if err != nil {
		logger.WarnWithCtx(ctx, "persist observed rate-limit cap", zap.Error(err))
	}
}

// doGraphQL executes a GraphQL query and, when tolerateForbiddenPartial
// is set, honors GitHub's partial-success contract: a response carrying real data
// alongside ONLY permanent permission errors (FORBIDDEN / INSUFFICIENT_SCOPES) is
// ingested with an advisory, because a retry with the same credential never
// recovers a permission gap. Any other error class (RATE_LIMITED,
// SERVICE_UNAVAILABLE, resource-limit) is retry-recoverable, so it fails loud and
// the caller leaves its cursor unadvanced. Only fetch_prs and fetch_issues opt in
// — they request the org-scoped Team / projectItems fields a repo-scoped
// credential is forbidden from; every other caller goes through the strict
// doGraphQL so a null connection is never silently swallowed.
func doGraphQL(ctx context.Context, gw sdk.VendorGateway, query string, tolerateForbiddenPartial bool) (json.RawMessage, error) {
	graphQLGW := gw.ForGroup(constants.GroupGitHubGraphQL)

	protoResp, err := graphQLGW.Execute(ctx, &connectorpb.GitHubGraphQLRequest{Query: query})
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}

	resp, ok := protoResp.(*connectorpb.GitHubGraphQLResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T, expected *GitHubGraphQLResponse", protoResp)
	}

	errs := resp.GetErrors()
	data := resp.GetData()
	if len(errs) > 0 {
		if tolerateForbiddenPartial && hasGraphQLData(ctx, data) && allPermissionErrors(errs) {
			logger.WarnWithCtx(ctx, "graphql: ingesting partial data; credential lacks permission for some fields — grant the missing GitHub permission to backfill",
				zap.Strings(logger.ErrorType, distinctErrorTypes(errs)),
				zap.String(logger.ErrorMessage, errs[0].GetMessage()))
			return data, nil
		}
		for _, e := range errs {
			if e.GetType() == "RATE_LIMITED" {
				// GitHub answers a rate-limited GraphQL query with 200 and an error of
				// type RATE_LIMITED; the gateway does not pass the response headers on,
				// so there is no Retry-After to keep.
				return nil, &sdk.SyncError{ConnectorID: connectorID, Kind: sdk.ErrKindRateLimit, Retryable: true,
					Cause: fmt.Errorf("graphql error: %s", e.GetMessage())}
			}
		}
		if allPermissionErrors(errs) && hasErrorType(errs, "INSUFFICIENT_SCOPES") {
			// The token lacks a scope the query needs (such as read:project for
			// the issue query's project fields). No retry and no other
			// repository fixes it, and the token itself still works, so it is a
			// terminal config_invalid, which does not pause the connector.
			return nil, &sdk.SyncError{ConnectorID: connectorID, Kind: sdk.ErrKindConfigInvalid, HTTPStatus: http.StatusForbidden, Terminal: true,
				Cause: fmt.Errorf("the token lacks a scope this query needs%s: %s", missingScopeHint(errs), errs[0].GetMessage())}
		}
		if allPermissionErrors(errs) {
			// GitHub answers a query the token may not run with 200 and errors of
			// type FORBIDDEN or INSUFFICIENT_SCOPES: the GraphQL form of a REST 403.
			// It is classified as one, so an organization-scoped operation maps it
			// to config_invalid (orgScopeForbidden) and a repository loop treats it
			// as a forbidden repository. Not terminal here: the caller decides.
			return nil, &sdk.SyncError{ConnectorID: connectorID, Kind: sdk.ErrKindAuth, HTTPStatus: http.StatusForbidden,
				Cause: fmt.Errorf("graphql error (%s): %s", strings.Join(distinctErrorTypes(errs), ", "), errs[0].GetMessage())}
		}
		return nil, fmt.Errorf("graphql error: %s", errs[0].GetMessage())
	}

	return data, nil
}

// nextGraphQLCursor returns the cursor for the page after the one queried with prev. A
// page that claims hasNextPage but returns an empty or unchanged endCursor would make the
// next query re-read the same page (or the first page) forever, re-emitting its records
// until the run's deadline, so it is an error rather than a page to follow.
func nextGraphQLCursor(prev, end string) (string, error) {
	if end == "" || end == prev {
		return "", fmt.Errorf("pagination cursor did not advance (%q)", end)
	}
	return end, nil
}

// distinctErrorTypes returns the unique GraphQL error `type` values, preserving
// first-seen order, so the partial-data advisory names every permission class the
// credential is missing rather than only the first.
func distinctErrorTypes(errs []*connectorpb.GitHubGraphQLError) []string {
	seen := make(map[string]struct{}, len(errs))
	out := make([]string, 0, len(errs))
	for _, e := range errs {
		t := e.GetType()
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// hasGraphQLData reports whether a GraphQL `data` payload carries real content:
// at least one top-level field that is non-null and not an empty container.
// A wholesale failure returns `null`, `{"repository": null}`, or a truncated
// `{"repository": {}}`; a field-level partial returns `{"repository": {...}}`.
// Only the last is safe to ingest alongside permission errors. (A populated
// top-level object whose own critical sub-field is null — e.g.
// `{"repository": {"pullRequests": null}}` — still passes here; the per-op
// caller catches that nil connection and fails loud, since this generic helper
// cannot know which sub-field an operation treats as critical.)
func hasGraphQLData(ctx context.Context, data json.RawMessage) bool {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &top); err != nil {
		// A data payload that won't parse as a JSON object isn't usable — treat as
		// no-data, but log it: GitHub always returns an object or null here, so a
		// parse failure is an unexpected shape worth surfacing, not a silent drop.
		logger.WarnWithCtx(ctx, "hasGraphQLData: GraphQL data payload did not parse as an object", zap.Error(err))
		return false
	}
	for _, v := range top {
		// Compact first so a whitespace-padded `{ }` or `[ ]` reads as the empty
		// container it is.
		var compact bytes.Buffer
		if err := json.Compact(&compact, v); err != nil {
			continue
		}
		vt := compact.Bytes()
		if len(vt) == 0 || bytes.Equal(vt, []byte("null")) ||
			bytes.Equal(vt, []byte("{}")) || bytes.Equal(vt, []byte("[]")) {
			continue
		}
		return true
	}
	return false
}

// allPermissionErrors reports whether every GraphQL error is a permanent
// permission error (FORBIDDEN / INSUFFICIENT_SCOPES). Any other type — or an
// empty/unknown type — is treated as non-permission so the caller fails and
// retries rather than silently accepting retry-recoverable partial data.
func allPermissionErrors(errs []*connectorpb.GitHubGraphQLError) bool {
	if len(errs) == 0 {
		return false
	}
	for _, e := range errs {
		switch e.GetType() {
		case "FORBIDDEN", "INSUFFICIENT_SCOPES":
		default:
			return false
		}
	}
	return true
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

// prFileLineStat is the per-file change record stored in the files_line_stats metadata field.
type prFileLineStat struct {
	Path      string `json:"path"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

// collectPRFileData returns per-file paths and line stats for a PR.
// If the GraphQL response was truncated (>100 files), it fetches the complete list
// via the REST API. Returns (filesJSON, filesLineStatsJSON) as strings.
func collectPRFileData(ctx context.Context, gw sdk.VendorGateway, owner, repo string, pr ghGQLPRNode) (string, string, error) {
	var stats []prFileLineStat

	if pr.Files.PageInfo.HasNextPage {
		// GraphQL truncated at 100 files — fetch complete list via REST.
		restFiles := fetchAllPRFiles(ctx, gw, owner, repo, pr.Number)
		if restFiles != nil {
			stats = make([]prFileLineStat, 0, len(restFiles))
			for _, f := range restFiles {
				if f.Filename != "" {
					stats = append(stats, prFileLineStat{Path: f.Filename, Additions: f.Additions, Deletions: f.Deletions})
				}
			}
		}
	}

	// Use GraphQL data if REST was not needed or failed.
	if stats == nil {
		stats = make([]prFileLineStat, 0, len(pr.Files.Nodes))
		for _, f := range pr.Files.Nodes {
			if f.Path != "" {
				stats = append(stats, prFileLineStat{Path: f.Path, Additions: f.Additions, Deletions: f.Deletions})
			}
		}
	}

	paths := make([]string, 0, len(stats))
	for _, s := range stats {
		paths = append(paths, s.Path)
	}

	filesJSON, err := json.Marshal(paths)
	if err != nil {
		return "[]", "[]", fmt.Errorf("marshaling file paths: %w", err)
	}
	statsJSON, err := json.Marshal(stats)
	if err != nil {
		return "[]", "[]", fmt.Errorf("marshaling file line stats: %w", err)
	}
	return string(filesJSON), string(statsJSON), nil
}

// ghRESTFile represents a single file entry from the GitHub REST API
// GET /repos/{owner}/{repo}/pulls/{number}/files endpoint.
type ghRESTFile struct {
	Filename  string `json:"filename"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

// fetchAllPRFiles fetches the complete file list for a PR via the REST API.
// Used as a fallback when the GraphQL response indicates truncation (>100 files).
// Returns nil on any error so the caller can fall back to the partial GraphQL data.
func fetchAllPRFiles(ctx context.Context, gw sdk.VendorGateway, owner, repo string, prNumber int) []ghRESTFile {
	restGW := gw.ForGroup(constants.GroupGitHubREST)
	var allFiles []ghRESTFile

	const maxPages = 30 // GitHub REST caps at 3000 files (100 per page × 30)
	for page := int32(1); page <= maxPages; page++ {
		protoResp, err := restGW.Execute(ctx, &connectorpb.ListPRFilesRequest{
			Owner:    owner,
			Repo:     repo,
			PrNumber: safePRNumber(prNumber),
			Page:     page,
			PerPage:  100,
		})
		if err != nil {
			logger.WarnWithCtx(ctx, "REST fallback for PR files failed, using partial GraphQL data",
				zap.String("repo", owner+"/"+repo), zap.Int("pr", prNumber), zap.Error(err))
			return nil
		}
		resp, ok := protoResp.(*connectorpb.ListPRFilesResponse)
		if !ok {
			return nil
		}
		for _, raw := range resp.GetFiles() {
			var f ghRESTFile
			if err := json.Unmarshal(raw, &f); err != nil {
				logger.WarnWithCtx(ctx, "fetchAllPRFiles: skipping unparseable file node",
					zap.String("repo", owner+"/"+repo), zap.Int("pr", prNumber), zap.Error(err))
				continue
			}
			allFiles = append(allFiles, f)
		}
		if !resp.GetHasNextPage() {
			break
		}
	}
	return allFiles
}

// databaseIDString renders a GitHub database id for metadata: decimal, or
// empty when the id is unknown (0), as pr_author_id is on every operation.
func databaseIDString(id int64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatInt(id, 10)
}

// formatPRCreatedAt renders a parent PR creation time for the pr_created_at
// metadata key: RFC3339/UTC when set, empty string when zero so a consumer's
// date gate stays fail-open.
func formatPRCreatedAt(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// emitRecord emits a single raw JSON record to the sink.
// dataDate is the actual event timestamp (e.g., PR updatedAt, commit committedDate).
// Pass zero time for operations without a meaningful event date.
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
		return fmt.Errorf("emitting record: %w", err)
	}
	return nil
}

// repoCursorState tracks per-repo sync progress including mid-pagination state.
// When PageCursor is non-empty, it indicates an in-progress pagination that was
// checkpointed and can be resumed.
type repoCursorState struct {
	// Timestamp is the high-water mark (max updatedAt seen across all processed pages).
	Timestamp time.Time `json:"ts"`
	// PageCursor is the GraphQL endCursor for the next page. Non-empty means
	// pagination was interrupted and can be resumed from this point.
	PageCursor string `json:"pc,omitempty"`
	// Since is the original 'since' filter used for the current pagination run.
	// Stored so that resumed pagination uses the same early-exit boundary.
	Since time.Time `json:"sf,omitempty"`
	// ProcessedPRs tracks PR numbers whose reviews have been fully fetched
	// during an in-progress fetch_reviews run. Cleared once the repo completes.
	ProcessedPRs []int `json:"prs,omitempty"`
	// ProcessedCommentPRs tracks PR numbers whose comments have been fully
	// fetched during an in-progress fetch_pr_comments run.
	ProcessedCommentPRs []int `json:"cprs,omitempty"`
}

// maxLoggedCursorBytes bounds how much of an unparseable cursor entry is echoed
// into logs, so a corrupt or oversized stored blob cannot be dumped wholesale.
const maxLoggedCursorBytes = 256

// decodeCursorStateEntry parses one composite-cursor entry, which is a JSON
// object. ok is false for any other entry.
//
// A struct entry whose Timestamp is the zero value — a repo checkpointed mid-run
// before its watermark advanced, e.g. fetch_reviews persisting ProcessedPRs on
// a first sync — decodes with its ProcessedPRs intact.
//
// A non-nil err accompanies a false ok when the entry is an object whose decode
// failed (a corrupt entry the caller logs); ok is false with a nil err for an
// entry that is not an object.
func decodeCursorStateEntry(raw json.RawMessage) (state repoCursorState, ok bool, err error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return repoCursorState{}, false, nil
	}
	if trimmed[0] != '{' {
		return repoCursorState{}, false, nil
	}
	if decErr := json.Unmarshal(trimmed, &state); decErr != nil {
		return repoCursorState{}, false, fmt.Errorf("decode struct cursor entry: %w", decErr)
	}
	return state, true, nil
}

// getRepoCursorState extracts per-repo cursor state ({"ts": ..., "pc": ..., "sf": ...})
// from a composite cursor via decodeCursorStateEntry.
func getRepoCursorState(cursor *sdk.Cursor, repoKey string) repoCursorState {
	if cursor == nil {
		return repoCursorState{}
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.Warn("getRepoCursorState: composite cursor parse failed; treating as no prior state",
			zap.String("repo_key", repoKey), zap.Error(err))
		return repoCursorState{}
	}
	raw, ok := parts[repoKey]
	if !ok {
		return repoCursorState{}
	}
	state, decoded, decErr := decodeCursorStateEntry(raw)
	if !decoded {
		// A genuinely-corrupt entry — the warning is real here.
		logger.Warn("getRepoCursorState: cursor entry unparseable; treating as no prior state",
			zap.String("repo_key", repoKey),
			zap.ByteString("raw", raw[:min(len(raw), maxLoggedCursorBytes)]),
			zap.Error(decErr))
	}
	return state
}

// existingCursorStates extracts all per-repo cursor states from a composite cursor.
// It pre-populates the state map so that checkpoints preserve progress for
// repos not yet re-synced in the current run.
func existingCursorStates(cursor *sdk.Cursor) map[string]repoCursorState {
	states := make(map[string]repoCursorState)
	if cursor == nil {
		return states
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.Warn("existingCursorStates: composite cursor parse failed; returning no states", zap.Error(err))
		return states
	}
	for k, raw := range parts {
		state, ok, decErr := decodeCursorStateEntry(raw)
		if !ok {
			// Skip a genuinely-corrupt entry rather than carry a zero state.
			logger.Warn("existingCursorStates: cursor entry unparseable; skipping",
				zap.String("repo_key", k),
				zap.ByteString("raw", raw[:min(len(raw), maxLoggedCursorBytes)]),
				zap.Error(decErr))
			continue
		}
		states[k] = state
	}
	return states
}

// buildStateCursor creates a composite cursor from per-repo cursor states.
func buildStateCursor(states map[string]repoCursorState) *sdk.Cursor {
	parts := make(map[string]json.RawMessage, len(states))
	for k, v := range states {
		raw, err := json.Marshal(v)
		if err != nil {
			logger.Warn("buildStateCursor: marshaling repo cursor state failed; skipping repo",
				zap.String("repo_key", k), zap.Error(err))
			continue
		}
		parts[k] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// pageCheckpointFn is called after each fully-processed page with the current
// high-water mark and the GraphQL cursor for the next page.
type pageCheckpointFn func(ctx context.Context, highWater time.Time, nextPageCursor string) error

// makePageCheckpoint builds a pageCheckpointFn that updates the per-repo cursor
// state and delegates to the framework checkpoint.
func makePageCheckpoint(
	checkpoint sdk.CheckpointFunc,
	cursorStates map[string]repoCursorState,
	repoFullName string,
	since time.Time,
) pageCheckpointFn {
	if checkpoint == nil {
		return nil
	}
	return func(ctx context.Context, highWater time.Time, nextPageCursor string) error {
		cursorStates[repoFullName] = repoCursorState{
			Timestamp:  highWater,
			PageCursor: nextPageCursor,
			Since:      since,
		}
		return checkpoint(ctx, buildStateCursor(cursorStates))
	}
}

// --- FetchPRsOp ---

type FetchPRsOp struct {
	paginationCfg     *config.GitHubPaginationConfig
	checkpointReserve time.Duration
}

func (op *FetchPRsOp) OperationID() sdk.OperationID { return opFetchPRs }

func (op *FetchPRsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	// Persist the observed vendor rate-limit cap so the gateway factory can
	// scale WithRateLimits per-installation on the next run. End-of-
	// run is the right scope: fetch_prs is the hottest GitHub operation, so
	// the BudgetLimiter snapshot is fresh by the time the defer fires.
	//
	// fetch_prs exercises both GraphQL (PR pages) and REST (repo discovery
	// in the no-explicit-repo path), so we pass both buckets. The helper
	// walks every bucket and writes one Settings.VendorRateLimits entry per
	// non-empty snapshot (graphql -> github-graphql, core -> github-rest),
	// because GitHub Cloud splits caps per-resource and Premium/enterprise
	// tiers can bump each independently — the gateway factory then scales
	// the two groups independently, which per-group persistence allows.
	defer persistObservedRateLimitCap(ctx, params, ratelimiter.BucketGitHubGraphQL, ratelimiter.BucketGitHubCore)

	repoInfos, err := fetchAllRepos(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching repos: %w", err)
	}
	repos := make([]string, len(repoInfos))
	for i, ri := range repoInfos {
		repos[i] = ri.FullName
	}

	cursorStates := existingCursorStates(params.Cursor)

	var repoErrors []error
	var tally repotally.Tally
	for _, repoFullName := range repos {
		if !hasTimeRemaining(ctx, op.checkpointReserve) {
			logger.WarnWithCtx(ctx, "deadline approaching, returning partial cursor",
				zap.String("operation", string(op.OperationID())))
			break
		}

		owner, repo := resolveRepoOwner(repoFullName, settings.Organization)

		state := getRepoCursorState(params.Cursor, repoFullName)
		tally.Attempt()

		// If PageCursor is set, we're resuming an interrupted pagination.
		// Use the stored Since for early-exit and PageCursor as the starting position.
		since := firstSyncSince(state.Timestamp, settings.GetLookbackDays())
		resumePageCursor := ""
		if state.PageCursor != "" {
			since = state.Since
			resumePageCursor = state.PageCursor
		}

		pageCfg := opPageConfig(op.paginationCfg, opFetchPRs)
		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, repoFullName, since)
		individualScope := params.TenantType == sdk.TenantKindIndividual
		latestUpdated, err := fetchPRsForRepo(ctx, params.VendorGateway, params.Sink, op.OperationID(), params.Config.OrganizationID, owner, repo, repoFullName, since, resumePageCursor, state.Timestamp, onPage, pageCfg, individualScope)
		if err != nil {
			// Log and continue — a single failing repo must not block snapshot
			// updates for all other repos. The failing repo retains its previous
			// cursor and will be retried on the next sync run.
			logger.Error(ctx, "skipping repo after fetch failure",
				zap.String("repo", repoFullName), zap.Error(err))
			repoErrors = append(repoErrors, fmt.Errorf("%s: %w", repoFullName, err))
			tally.Fail(repoFullName, err)
			continue
		}

		cursorStates[repoFullName] = repoCursorState{Timestamp: latestUpdated}
		logger.Info(ctx, "fetched PRs", zap.String("repo", repoFullName), zap.Time("cursor", latestUpdated))
	}

	if err := tally.Err(len(repos)); err != nil {
		return nil, fmt.Errorf("fetch_prs: %w", err)
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	// Return the updated cursor even when some repos failed. Successful repos
	// get their cursors advanced; failed repos retain their previous state and
	// will be retried on the next run.
	if len(repoErrors) > 0 {
		logger.WarnWithCtx(ctx, "fetch_prs completed with repo-level failures",
			zap.Int("failed_repos", len(repoErrors)),
			zap.Int("total_repos", len(repos)))
	}
	return buildStateCursor(cursorStates), nil
}

// isVendorServerError checks whether err wraps a SyncError with a 5xx HTTP status,
// indicating a vendor-side failure that may succeed with a smaller page size.
func isVendorServerError(err error) bool {
	var syncErr *sdk.SyncError
	if errors.As(err, &syncErr) {
		return syncErr.HTTPStatus >= 500
	}
	return false
}

// doGraphQLAdaptive executes a GraphQL query with adaptive page size reduction
// on 5xx vendor errors. buildQuery receives the current page size and returns
// the query string. Config is loaded from connector YAML per operation.
func doGraphQLAdaptive(ctx context.Context, gw sdk.VendorGateway, pageCfg *config.OperationPaginationConfig, buildQuery func(pageSize int) string, repoFullName string, tolerateForbiddenPartial bool) (json.RawMessage, error) {
	if pageCfg.PageSize < pageCfg.MinPageSize {
		return nil, fmt.Errorf("invalid pagination config for %s: PageSize (%d) < MinPageSize (%d)",
			repoFullName, pageCfg.PageSize, pageCfg.MinPageSize)
	}
	pageSize := pageCfg.PageSize
	var lastErr error
	for pageSize >= pageCfg.MinPageSize {
		query := buildQuery(pageSize)
		body, err := doGraphQL(ctx, gw, query, tolerateForbiddenPartial)
		if err == nil {
			return body, nil
		}
		if !isVendorServerError(err) {
			return nil, fmt.Errorf("executing graphql query: %w", err)
		}
		lastErr = err
		nextPageSize := pageSize / pageCfg.BackoffDivisor
		if nextPageSize < pageCfg.MinPageSize {
			logger.WarnWithCtx(ctx, "GraphQL page size exhausted, giving up",
				zap.String("repo", repoFullName),
				zap.Int("last_page_size", pageSize),
				zap.Error(err))
			break
		}
		logger.WarnWithCtx(ctx, "reducing GraphQL page size after vendor error",
			zap.String("repo", repoFullName),
			zap.Int("failed_page_size", pageSize),
			zap.Int("retry_page_size", nextPageSize),
			zap.Error(err))
		pageSize = nextPageSize
	}
	return nil, lastErr
}

// fetchPRsForRepo fetches all PRs for a single repo since the given timestamp.
// resumePageCursor allows resuming from a checkpointed GraphQL cursor.
// highWater is the recorded high-water mark (may differ from since when resuming).
func fetchPRsForRepo(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, owner, repo, repoFullName string, since time.Time, resumePageCursor string, highWater time.Time, onPage pageCheckpointFn, pageCfg *config.OperationPaginationConfig, individualScope bool) (time.Time, error) {
	latestUpdated := highWater
	graphQLCursor := resumePageCursor

	for {
		// Adaptive page size: start at configured page size, reduce on 5xx vendor errors.
		// Resets for each new page — the problematic item may only appear on one page.
		body, err := doGraphQLAdaptive(ctx, gw, pageCfg, func(ps int) string {
			return buildPRsQuery(owner, repo, since, graphQLCursor, ps, nil, individualScope)
		}, repoFullName, true)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch PRs for %s: %w", repoFullName, err)
		}

		var data ghGraphQLData
		if err := json.Unmarshal(body, &data); err != nil {
			return time.Time{}, fmt.Errorf("parse PRs response for %s: %w", repoFullName, err)
		}

		prs := data.Repository.PullRequests
		repoDatabaseID := data.Repository.DatabaseID
		if prs == nil {
			// A null pullRequests connection is never normal EOF — an empty repo
			// returns a non-null connection with nodes:[]. nil means the connection
			// itself was forbidden or the response was truncated (the partial-success
			// path returns data whose critical field is null). Fail loud so the repo
			// is skipped and retried with its cursor unadvanced, never silently
			// recorded as a clean empty page.
			return time.Time{}, fmt.Errorf("fetch PRs for %s: null pullRequests connection (forbidden or truncated response)", repoFullName)
		}

		done := false
		for i, pr := range prs.Nodes {
			if !since.IsZero() && pr.UpdatedAt.Before(since) {
				done = true
				break
			}

			nodeJSON, err := json.Marshal(prs.Nodes[i])
			if err != nil {
				logger.WarnWithCtx(ctx, "fetch_prs: skipping PR node that failed to marshal",
					zap.String("repo", repoFullName), zap.Int("pr", pr.Number), zap.Error(err))
				continue
			}

			externalID := fmt.Sprintf("%s#PR-%d", repoFullName, pr.Number)
			authorID := databaseIDString(pr.Author.DatabaseID)
			filesStr, filesLineStatsStr, err := collectPRFileData(ctx, gw, owner, repo, pr)
			if err != nil {
				logger.WarnWithCtx(ctx, "failed to collect PR file data, using empty defaults",
					zap.String("repo", repoFullName), zap.Int("pr", pr.Number), zap.Error(err))
			}

			// Emit lifecycle event records (ADR-0003).
			//
			// PRs are stateful entities that transition through lifecycle states
			// (opened → merged/closed) over days or weeks. Instead of one mutable
			// record per PR (which would misattribute merge/close activity to the
			// creation date), we emit separate records for each lifecycle transition.
			//
			// Each record gets a suffixed record key (e.g. "#opened", "#merged") and
			// its own DataDate matching when that transition actually happened. The
			// upsert uniqueness constraint (org, connector, operation, record_key,
			// data_date) ensures these land as distinct rows in the downstream record store.
			// The #snapshot is current state and carries no DataDate.
			//
			// A consumer dispatches on the "event_type" metadata field to count
			// the right metric on the right date.

			// "opened" — emitted on every fetch because every PR was opened.
			// Uses createdAt as DataDate so the "opened" count lands on the actual
			// creation date. Idempotent: repeated upserts with the same key + date
			// overwrite the same row without double-counting.
			prMeta := map[string]string{
				"repo":             repoFullName,
				"owner":            owner,
				"pr_author_id":     authorID,
				"files_changed":    filesStr,
				"files_line_stats": filesLineStatsStr,
				"head_branch":      pr.HeadRefName,
				"base_ref_name":    pr.BaseRefName,
				"is_draft":         strconv.FormatBool(pr.IsDraft),
			}
			if repoDatabaseID != 0 {
				prMeta["platform_repo_id"] = strconv.FormatInt(repoDatabaseID, 10)
			}

			if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#opened", nodeJSON,
				withEventType(prMeta, "opened"), pr.CreatedAt); emitErr != nil {
				return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// "snapshot" — captures mutable aggregate counters (comments, review
			// requests, etc.) that accumulate over the PR's lifetime. These don't
			// belong to any single lifecycle event, so we store them separately.
			//
			// DataDate is empty: the snapshot is current state (ADR-0003
			// amendment), so it always upserts the same row and the latest
			// counter values overwrite prior values. This is intentional — we
			// always want the most recent counters, not a historical series.
			if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#snapshot", nodeJSON,
				withEventType(prMeta, "snapshot"), time.Time{}); emitErr != nil {
				return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// "merged" — emitted only when mergedAt is set. Once GitHub sets
			// mergedAt, it never changes, so this record is effectively immutable
			// after first emission. DataDate is mergedAt so the merge count lands
			// on the actual merge date, not the creation date.
			if pr.MergedAt != nil {
				if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#merged", nodeJSON,
					withEventType(prMeta, "merged"), *pr.MergedAt); emitErr != nil {
					return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}
			}

			// "closed" — emitted only for PRs that were closed without being merged
			// (i.e. abandoned/rejected). Merged PRs have state=MERGED, not CLOSED,
			// so this branch is mutually exclusive with the merged emission above.
			// DataDate is closedAt so the close count lands on the actual close date.
			if pr.State == "CLOSED" && pr.MergedAt == nil {
				closedAt := pr.CreatedAt // fallback if closedAt somehow nil
				if pr.ClosedAt != nil {
					closedAt = *pr.ClosedAt
				}
				if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#closed", nodeJSON,
					withEventType(prMeta, "closed"), closedAt); emitErr != nil {
					return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}
			}

			if pr.UpdatedAt.After(latestUpdated) {
				latestUpdated = pr.UpdatedAt
			}
		}

		if done || !prs.PageInfo.HasNextPage {
			break
		}
		if graphQLCursor, err = nextGraphQLCursor(graphQLCursor, prs.PageInfo.EndCursor); err != nil {
			return time.Time{}, fmt.Errorf("fetch PRs for %s: %w", repoFullName, err)
		}

		// Checkpoint after each page so progress is preserved on failure.
		if onPage != nil {
			if err := onPage(ctx, latestUpdated, graphQLCursor); err != nil {
				return time.Time{}, fmt.Errorf("checkpoint for %s: %w", repoFullName, err)
			}
		}
	}

	return latestUpdated, nil
}

// --- FetchCommitsOp ---

type FetchCommitsOp struct {
	paginationCfg     *config.GitHubPaginationConfig
	checkpointReserve time.Duration
}

func (op *FetchCommitsOp) OperationID() sdk.OperationID { return opFetchCommits }

// metaKeyRefs is the record-metadata key naming the refs a commit was reached from,
// comma-separated. A consumer side declares the same literal
// (the consumer's refs metadata key) rather than sharing a constant: a connector must not
// import its consumer, and platform_repo_id is carried across the same boundary the same way.
// The two must be changed together.
const metaKeyRefs = "refs"

// refsBySHA tracks which refs a commit has been reached from so far in this repository's
// walk. Only the ref names are held — never the payloads — so the walk keeps streaming each
// page to the sink as it did before multi-ref, and memory does not grow with commit count.
//
// The union survives without buffering because refs only ever accumulate and the record is
// re-emitted on every sighting: the dedup key is (organization_id, connector_id,
// operation_id, record_key, data_date), record_key is "<repo>#<sha>" and data_date is the
// commit date — neither varies by ref — and the upsert assigns metadata wholesale, so the
// LAST write wins and the last write carries the largest ref set.
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

func (op *FetchCommitsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	repoInfos, err := fetchAllRepos(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching repos: %w", err)
	}

	saveDiscoveredRepos(ctx, params, settings, repoInfos)

	cursorStates := existingCursorStates(params.Cursor)

	// The administrator's tracked-branch set, globs already expanded server-side. A failure,
	// a timeout or an unwired client returns (nil, false) and every repo degrades to its
	// default branch — never to all, which would turn a settings outage into a provider-budget
	// incident.
	trackedRefs, refsResolved := vcs.ResolveTrackedRefs(ctx, params.Host.TrackedRefs, params.Config.OrganizationID, string(connectorID))
	refsByRepo := vcs.IndexTrackedRefs(trackedRefs)

	var skipped repotally.Skipper
repos:
	for _, ri := range repoInfos {
		if !hasTimeRemaining(ctx, op.checkpointReserve) {
			logger.WarnWithCtx(ctx, "deadline approaching, returning partial cursor",
				zap.String("operation", string(op.OperationID())))
			break
		}
		skipped.Attempt()

		repoFullName := ri.FullName
		owner, repo := resolveRepoOwner(repoFullName, settings.Organization)

		refs, skipRepo := vcs.CommitRefsForRepo(ctx, refsByRepo, refsResolved, repoFullName, ri.DefaultBranch)
		if skipRepo {
			// MatchNone: the repo resolved to no countable refs. Fetching nothing is the
			// resolved answer, not a failure.
			continue
		}

		state := getRepoCursorState(params.Cursor, repoFullName)
		since := firstSyncSince(state.Timestamp, settings.GetLookbackDays())
		resumePageCursor := ""
		// A GraphQL endCursor is scoped to the ref it was issued for, so resuming a second
		// ref from the first ref's saved cursor would page through the wrong history. The
		// cursor is keyed by repo only, so mid-page resume is available exactly when this
		// repo walks a single ref; a multi-ref walk restarts every ref from the floor the
		// interrupted run started at (state.Since), and the downstream dedup key absorbs the
		// re-emitted head pages. Not from state.Timestamp: a checkpoint stores the walk's
		// in-progress high-water there, so restarting from it would skip the interrupted
		// ref's unread pages and every later ref's commits older than it.
		if state.PageCursor != "" {
			since = state.Since
			if len(refs) == 1 {
				resumePageCursor = state.PageCursor
			}
		}

		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, repoFullName, since)
		latestDate := state.Timestamp

		// Ref sets only — the payloads stream straight through, as before multi-ref.
		seenRefs := make(refsBySHA)

		for _, ref := range refs {
			graphQLCursor := resumePageCursor
			for {
				pageCfg := opPageConfig(op.paginationCfg, opFetchCommits)
				body, err := doGraphQLAdaptive(ctx, params.VendorGateway, pageCfg, func(ps int) string {
					if ref != "" {
						return buildCommitsQueryForBranch(owner, repo, ref, since, graphQLCursor, ps)
					}
					return buildCommitsQuery(owner, repo, since, graphQLCursor, ps)
				}, repoFullName, false)
				if err != nil {
					if skipped.Skip(repoFullName, err) {
						logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
						continue repos
					}
					return nil, fmt.Errorf("fetch commits for %s ref %s: %w", repoFullName, ref, err)
				}

				var data ghGraphQLData
				if err := json.Unmarshal(body, &data); err != nil {
					return nil, fmt.Errorf("parse commits response for %s ref %s: %w", repoFullName, ref, err)
				}

				historyRef := data.Repository.Ref
				if ref != "" {
					historyRef = data.Repository.CustomRef
				}
				if historyRef == nil || historyRef.Target.History == nil {
					break
				}
				history := historyRef.Target.History
				repoDatabaseID := data.Repository.DatabaseID

				for i, commit := range history.Nodes {
					nodeJSON, err := json.Marshal(history.Nodes[i])
					if err != nil {
						logger.WarnWithCtx(ctx, "fetch_commits: skipping commit node that failed to marshal",
							zap.String("repo", repoFullName), zap.String("oid", commit.OID), zap.Error(err))
						continue
					}

					// platform_repo_id mirrors fetch_prs so a consumer can build
					// commit→PR replay keys with the same `<databaseId>#N` shape that
					// the PR collector uses. Without this, the
					// associatedPullRequests first_commit_at enrichment is a no-op
					// because pendingCommitLinks buffers under repoFullName while the
					// PR collector keys under databaseId — keys never match.
					commitMeta := map[string]string{"repo": repoFullName}
					if repoDatabaseID != 0 {
						commitMeta["platform_repo_id"] = strconv.FormatInt(repoDatabaseID, 10)
					}
					commitMeta[metaKeyRefs] = seenRefs.add(commit.OID, ref)

					// Emitted here, before the checkpoint below, because the checkpoint
					// durably advances the cursor: saving a page whose commits had not
					// reached the sink would skip them for good on the next run.
					externalID := fmt.Sprintf("%s#%s", repoFullName, commit.OID)
					if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, nodeJSON, commitMeta, commit.CommittedDate); emitErr != nil {
						return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
					}

					if commit.CommittedDate.After(latestDate) {
						latestDate = commit.CommittedDate
					}
				}

				if !history.PageInfo.HasNextPage {
					break
				}
				next, cursorErr := nextGraphQLCursor(graphQLCursor, history.PageInfo.EndCursor)
				if cursorErr != nil {
					cursorErr = fmt.Errorf("fetch commits for %s ref %s: %w", repoFullName, ref, cursorErr)
					if skipped.Skip(repoFullName, cursorErr) {
						logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(cursorErr))
						continue repos
					}
					return nil, cursorErr
				}
				graphQLCursor = next

				if onPage != nil {
					if err := onPage(ctx, latestDate, graphQLCursor); err != nil {
						return nil, fmt.Errorf("checkpoint for %s: %w", repoFullName, err)
					}
				}
			}
			// Only the first ref may resume mid-page; the saved cursor belongs to it.
			resumePageCursor = ""
		}

		cursorStates[repoFullName] = repoCursorState{Timestamp: latestDate}
		logger.Info(ctx, "fetched commits", zap.String("repo", repoFullName), zap.Time("cursor", latestDate))
	}

	if err := skipped.Err(len(repoInfos)); err != nil {
		return nil, err
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	return buildStateCursor(cursorStates), nil
}

// --- FetchIssuesOp ---

type FetchIssuesOp struct {
	paginationCfg     *config.GitHubPaginationConfig
	checkpointReserve time.Duration
}

func (op *FetchIssuesOp) OperationID() sdk.OperationID { return opFetchIssues }

func (op *FetchIssuesOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	repoInfos, err := fetchAllRepos(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching repos: %w", err)
	}
	repos := make([]string, len(repoInfos))
	for i, ri := range repoInfos {
		repos[i] = ri.FullName
	}

	cursorStates := existingCursorStates(params.Cursor)

	var skipped repotally.Skipper
repos:
	for _, repoFullName := range repos {
		if !hasTimeRemaining(ctx, op.checkpointReserve) {
			logger.WarnWithCtx(ctx, "deadline approaching, returning partial cursor",
				zap.String("operation", string(op.OperationID())))
			break
		}
		skipped.Attempt()

		owner, repo := resolveRepoOwner(repoFullName, settings.Organization)

		state := getRepoCursorState(params.Cursor, repoFullName)
		since := firstSyncSince(state.Timestamp, settings.GetLookbackDays())
		resumePageCursor := ""
		if state.PageCursor != "" {
			since = state.Since
			resumePageCursor = state.PageCursor
		}

		pageCfg := opPageConfig(op.paginationCfg, opFetchIssues)
		inlinePerIssue, maxTailPages := issueCommentsTailConfig(op.paginationCfg)
		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, repoFullName, since)
		latestUpdated, err := fetchIssuesForRepo(ctx, params.VendorGateway, params.Sink, op.OperationID(), params.Config.OrganizationID, owner, repo, repoFullName, since, resumePageCursor, state.Timestamp, onPage, pageCfg, inlinePerIssue, maxTailPages)
		if err != nil {
			if skipped.Skip(repoFullName, err) {
				logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
				continue repos
			}
			return nil, fmt.Errorf("fetching issues for repo %s: %w", repoFullName, err)
		}

		cursorStates[repoFullName] = repoCursorState{Timestamp: latestUpdated}
		logger.Info(ctx, "fetched issues", zap.String("repo", repoFullName), zap.Time("cursor", latestUpdated))
	}

	if err := skipped.Err(len(repos)); err != nil {
		return nil, err
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	return buildStateCursor(cursorStates), nil
}

// fetchRemainingComments paginates through any additional comment pages for an
// issue when the initial fetch_issues response indicated
// comments.pageInfo.hasNextPage. Newly fetched nodes are appended to
// issue.Comments.Nodes in place so the marshaled the record store payload carries
// the full thread. maxTailPages caps the loop (defends against a stuck
// endCursor and bounds per-issue cost). Best-effort: errors are returned so
// the caller can log and emit with partial data rather than abort the run.
func fetchRemainingComments(ctx context.Context, gw sdk.VendorGateway, pageCfg *config.OperationPaginationConfig, maxTailPages int, owner, repo string, issue *ghGQLIssueNode) error {
	if !issue.Comments.PageInfo.HasNextPage {
		return nil
	}
	cursor := issue.Comments.PageInfo.EndCursor
	tag := fmt.Sprintf("%s/%s#issue-%d-comments-tail", owner, repo, issue.Number)
	for i := 0; i < maxTailPages; i++ {
		body, err := doGraphQLAdaptive(ctx, gw, pageCfg, func(ps int) string {
			return buildIssueCommentsTailQuery(owner, repo, issue.Number, cursor, ps)
		}, tag, false)
		if err != nil {
			return fmt.Errorf("tail-fetch comments for %s: %w", tag, err)
		}

		var data struct {
			Repository struct {
				Issue struct {
					Comments struct {
						Nodes    []ghGQLIssueComment `json:"nodes"`
						PageInfo ghPageInfo          `json:"pageInfo"`
					} `json:"comments"`
				} `json:"issue"`
			} `json:"repository"`
		}
		if err := json.Unmarshal(body, &data); err != nil {
			return fmt.Errorf("parse comments-tail response for %s: %w", tag, err)
		}

		page := data.Repository.Issue.Comments
		issue.Comments.Nodes = append(issue.Comments.Nodes, page.Nodes...)
		// Refresh the issue's PageInfo so re-emit reflects the final pagination
		// state rather than the stale "more pages exist" signal.
		issue.Comments.PageInfo = page.PageInfo

		if !page.PageInfo.HasNextPage {
			return nil
		}
		if cursor, err = nextGraphQLCursor(cursor, page.PageInfo.EndCursor); err != nil {
			return fmt.Errorf("tail-fetch comments for %s: %w", tag, err)
		}
	}
	return fmt.Errorf("tail-fetch comments for %s exceeded max page cap of %d (fetched %d comments before bailing)", tag, maxTailPages, len(issue.Comments.Nodes))
}

// fetchIssuesForRepo fetches all issues for a single repo since the given timestamp.
func fetchIssuesForRepo(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, owner, repo, repoFullName string, since time.Time, resumePageCursor string, highWater time.Time, onPage pageCheckpointFn, pageCfg *config.OperationPaginationConfig, inlineCommentsPerIssue, maxCommentTailPages int) (time.Time, error) {
	latestUpdated := highWater
	graphQLCursor := resumePageCursor

	for {
		body, err := doGraphQLAdaptive(ctx, gw, pageCfg, func(ps int) string {
			return buildIssuesQuery(owner, repo, since, graphQLCursor, ps, inlineCommentsPerIssue)
		}, repoFullName, true)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch issues for %s: %w", repoFullName, err)
		}

		var data ghGraphQLData
		if err := json.Unmarshal(body, &data); err != nil {
			return time.Time{}, fmt.Errorf("parse issues response for %s: %w", repoFullName, err)
		}

		issues := data.Repository.Issues
		if issues == nil {
			// A null issues connection is never normal EOF (an empty repo returns a
			// non-null connection with nodes:[]); nil means it was forbidden or the
			// response truncated. Fail loud — skip + retry, cursor unadvanced —
			// rather than silently record a clean empty page. (The partial-success
			// path legitimately nulls only ancillary fields like projectItems, which
			// leaves this connection non-null.)
			return time.Time{}, fmt.Errorf("fetch issues for %s: null issues connection (forbidden or truncated response)", repoFullName)
		}

		done := false
		for i, issue := range issues.Nodes {
			if !since.IsZero() && issue.UpdatedAt.Before(since) {
				done = true
				break
			}

			// Fetch any additional comment pages so the emitted payload carries
			// the full comment thread, not just the first page returned inline by
			// fetch_issues. Best-effort: tail-fetch failures don't abort the run —
			// we log and emit with whatever comments came through.
			if tailErr := fetchRemainingComments(ctx, gw, pageCfg, maxCommentTailPages, owner, repo, &issues.Nodes[i]); tailErr != nil {
				logger.WarnWithCtx(ctx, "tail-fetch comments failed; emitting issue with partial comments",
					zap.String(logger.OrgID, orgID),
					zap.String("repo", repoFullName),
					zap.Int("issue", issue.Number),
					zap.Error(tailErr))
			}

			nodeJSON, err := json.Marshal(issues.Nodes[i])
			if err != nil {
				logger.WarnWithCtx(ctx, "fetch_issues: skipping issue node that failed to marshal",
					zap.String("repo", repoFullName), zap.Int("issue", issue.Number), zap.Error(err))
				continue
			}

			externalID := fmt.Sprintf("%s#ISSUE-%d", repoFullName, issue.Number)

			// Emit lifecycle event records (ADR-0003).
			// Same pattern as PRs — separate records per lifecycle transition so
			// that close activity is attributed to the close date, not creation date.
			// Issues have 3 lifecycle records (no "merged" — issues can't be merged).

			// "opened" — emitted on every fetch. DataDate is createdAt so the
			// opened count is permanently preserved on the creation date, even
			// after the issue is later closed.
			if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#opened", nodeJSON,
				map[string]string{"repo": repoFullName, "event_type": "opened"}, issue.CreatedAt); emitErr != nil {
				return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// "snapshot" — mutable aggregate counters (e.g. comment count).
			// Same rationale as the PR snapshot: always overwrites the same row
			// (no DataDate: it is current state) so a consumer sees the latest totals.
			if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#snapshot", nodeJSON,
				map[string]string{"repo": repoFullName, "event_type": "snapshot"}, time.Time{}); emitErr != nil {
				return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// "closed" — emitted only when the issue is closed. DataDate is
			// closedAt so the close count lands on the actual close date.
			if issue.State == "CLOSED" && issue.ClosedAt != nil {
				if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#closed", nodeJSON,
					map[string]string{"repo": repoFullName, "event_type": "closed"}, *issue.ClosedAt); emitErr != nil {
					return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}
			}

			if issue.UpdatedAt.After(latestUpdated) {
				latestUpdated = issue.UpdatedAt
			}
		}

		if done || !issues.PageInfo.HasNextPage {
			break
		}
		if graphQLCursor, err = nextGraphQLCursor(graphQLCursor, issues.PageInfo.EndCursor); err != nil {
			return time.Time{}, fmt.Errorf("fetch issues for %s: %w", repoFullName, err)
		}

		if onPage != nil {
			if err := onPage(ctx, latestUpdated, graphQLCursor); err != nil {
				return time.Time{}, fmt.Errorf("checkpoint for %s: %w", repoFullName, err)
			}
		}
	}

	return latestUpdated, nil
}

// --- FetchReviewsOp ---

type FetchReviewsOp struct {
	paginationCfg     *config.GitHubPaginationConfig
	checkpointReserve time.Duration
}

func (op *FetchReviewsOp) OperationID() sdk.OperationID { return opFetchReviews }

func (op *FetchReviewsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	repoInfos, err := fetchAllRepos(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching repos: %w", err)
	}
	repos := make([]string, len(repoInfos))
	for i, ri := range repoInfos {
		repos[i] = ri.FullName
	}

	cursorStates := existingCursorStates(params.Cursor)

	var skipped repotally.Skipper
repos:
	for _, repoFullName := range repos {
		if !hasTimeRemaining(ctx, op.checkpointReserve) {
			logger.WarnWithCtx(ctx, "deadline approaching, returning partial cursor",
				zap.String("operation", string(op.OperationID())))
			break
		}
		skipped.Attempt()

		owner, repo := resolveRepoOwner(repoFullName, settings.Organization)

		state := getRepoCursorState(params.Cursor, repoFullName)
		since := firstSyncSince(state.Timestamp, settings.GetLookbackDays())

		// Step 1: Collect recently-updated PR numbers + author info via GraphQL.
		// Author/repo IDs come along so the per-review records carry enough
		// metadata for self-PR + bot filtering downstream.
		prInfos, platformRepoID, latestUpdated, err := collectRecentPRNumbers(ctx, params.VendorGateway, owner, repo, repoFullName, since, state.Timestamp, op.paginationCfg)
		if err != nil {
			if skipped.Skip(repoFullName, err) {
				logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
				continue repos
			}
			return nil, fmt.Errorf("collecting recent pr numbers for repo %s: %w", repoFullName, err)
		}

		// Build set of already-processed PRs from a previous interrupted run.
		processedSet := make(map[int]struct{}, len(state.ProcessedPRs))
		for _, n := range state.ProcessedPRs {
			processedSet[n] = struct{}{}
		}
		processedPRs := append([]int(nil), state.ProcessedPRs...)

		// Step 2: For each PR, fetch reviews via REST.
		// Checkpoint after each PR so progress is preserved on timeout.
		restGW := params.VendorGateway.ForGroup(constants.GroupGitHubREST)
		deadlineReached := false
		for _, pr := range prInfos {
			if _, done := processedSet[pr.Number]; done {
				continue
			}

			// Per-PR deadline check (was: only checked between repos). Repos
			// with thousands of PRs blew past the asynq
			// task deadline mid-loop, and the surrounding Checkpoint call
			// inherited the expired context — surfacing as a fatal
			// ctx_exhausted instead of a clean partial-progress save.
			if !hasTimeRemaining(ctx, op.checkpointReserve) {
				logger.WarnWithCtx(ctx, "deadline approaching mid-repo, persisting partial progress and exiting",
					zap.String("operation", string(op.OperationID())),
					zap.String("repo", repoFullName),
					zap.Int("processed_prs", len(processedPRs)))
				deadlineReached = true
				break
			}

			if err := fetchReviewsForPR(ctx, restGW, params.Sink, op.OperationID(), params.Config.OrganizationID, owner, repo, repoFullName, pr.Number, pr.AuthorLogin, pr.AuthorDatabaseID, platformRepoID, pr.CreatedAt); err != nil {
				if skipped.Skip(repoFullName, err) {
					logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
					continue repos
				}
				return nil, fmt.Errorf("fetching reviews for pr %d: %w", pr.Number, err)
			}

			processedPRs = append(processedPRs, pr.Number)

			// Checkpoint with in-progress state so the next run can skip processed PRs.
			// Use a detached context (NotCancel) so a deadline that fires
			// between fetchReviewsForPR and Checkpoint does not lose progress
			// — Checkpoint writes are sub-second Redis ops and the parent
			// ctx already gates the surrounding work.
			if params.Checkpoint != nil {
				cursorStates[repoFullName] = repoCursorState{
					Timestamp:    state.Timestamp,
					Since:        since,
					ProcessedPRs: processedPRs,
				}
				ckCtx, ckCancel := checkpointContext(ctx)
				err := params.Checkpoint(ckCtx, buildStateCursor(cursorStates))
				ckCancel()
				if err != nil {
					return nil, fmt.Errorf("checkpoint for %s: %w", repoFullName, err)
				}
			}
		}

		if deadlineReached {
			// Return the in-progress cursor cleanly so asynq retries the task
			// and resumes where we left off; do not advance the timestamp.
			return buildStateCursor(cursorStates), nil
		}

		// Repo complete — clear ProcessedPRs and advance the timestamp.
		cursorStates[repoFullName] = repoCursorState{Timestamp: latestUpdated}
		if params.Checkpoint != nil {
			ckCtx, ckCancel := checkpointContext(ctx)
			err := params.Checkpoint(ckCtx, buildStateCursor(cursorStates))
			ckCancel()
			if err != nil {
				return nil, fmt.Errorf("checkpoint for %s: %w", repoFullName, err)
			}
		}
		logger.Info(ctx, "fetched reviews", zap.String("repo", repoFullName), zap.Int("pr_count", len(prInfos)), zap.Time("cursor", latestUpdated))
	}

	if err := skipped.Err(len(repos)); err != nil {
		return nil, err
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	return buildStateCursor(cursorStates), nil
}

// fetchReviewsForPR fetches all review pages for a single PR via REST.
// PR author + platform repo ID are threaded through so the emitted records
// carry the metadata a consumer needs for self-PR detection and stable
// repo lookups (no separate DB call per review).
func fetchReviewsForPR(ctx context.Context, restGW sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, owner, repo, repoFullName string, prNum int, prAuthorLogin string, prAuthorDatabaseID int64, platformRepoID int64, prCreatedAt time.Time) error {
	page := int32(1)
	prAuthorIDStr := databaseIDString(prAuthorDatabaseID)
	platformRepoIDStr := ""
	if platformRepoID != 0 {
		platformRepoIDStr = strconv.FormatInt(platformRepoID, 10)
	}
	prCreatedAtStr := formatPRCreatedAt(prCreatedAt)
	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.ListPRReviewsRequest{
			Owner:    owner,
			Repo:     repo,
			PrNumber: safePRNumber(prNum),
			Page:     page,
			PerPage:  100,
		})
		if err != nil {
			logger.WarnWithCtx(ctx, "failed to fetch reviews for PR", zap.Int("pr_number", prNum), zap.Error(err))
			return nil
		}

		resp, ok := protoResp.(*connectorpb.ListPRReviewsResponse)
		if !ok {
			return nil
		}

		for _, reviewJSON := range resp.GetReviews() {
			var rev ghReview
			if err := json.Unmarshal(reviewJSON, &rev); err != nil {
				logger.WarnWithCtx(ctx, "fetch_reviews: skipping review that failed to unmarshal",
					zap.String("repo", repoFullName), zap.Int("pr_number", prNum), zap.Error(err))
				continue
			}
			externalID := fmt.Sprintf("%s#PR-%d#REVIEW-%d", repoFullName, prNum, rev.ID)
			metadata := map[string]string{
				"repo":             repoFullName,
				"pr_number":        strconv.Itoa(prNum),
				"pr_key":           fmt.Sprintf("%s#PR-%d", repoFullName, prNum),
				"pr_author_login":  prAuthorLogin,
				"pr_author_id":     prAuthorIDStr,
				"platform_repo_id": platformRepoIDStr,
				"pr_created_at":    prCreatedAtStr,
				"reviewer_is_bot":  strconv.FormatBool(detectIsBot(rev.User.Login, rev.User.Type)),
			}
			if emitErr := emitRecord(ctx, sink, opID, orgID, externalID, reviewJSON, metadata, rev.SubmittedAt); emitErr != nil {
				return &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}
		}

		if !resp.GetHasNextPage() {
			return nil
		}
		page++
	}
}

// detectIsBot reports whether a GitHub account looks like an automated
// reviewer/commenter. The REST review and PR comment payloads carry user.type
// = "Bot" for GitHub Apps, which is the primary signal. Some accounts do not
// expose this (older payloads, unusual Apps) so we also fall back to the
// "[bot]" login suffix used by Dependabot, Renovate, CodeRabbit, and
// github-actions. Returns the boolean only — callers stringify via
// `strconv.FormatBool` to keep the existing record metadata format.
func detectIsBot(login, userType string) bool {
	if userType == "Bot" {
		return true
	}
	if strings.HasSuffix(login, "[bot]") {
		return true
	}
	return false
}

// collectRecentPRNumbers fetches recently-updated PRs for a repo via GraphQL.
// Returns ghPRInfo entries (number, author login/ID) plus the platform repo
// database ID and the latest seen updatedAt. The author and repo IDs are
// threaded into fetch_reviews record metadata so a consumer can resolve
// self-PR detection and platform_repo_id without extra DB lookups.
//
// highWater seeds the returned watermark for the same reason it does in
// collectRecentPRsWithAuthor — see that function for why `since` must never be
// substituted here.
func collectRecentPRNumbers(ctx context.Context, gw sdk.VendorGateway, owner, repo, repoFullName string, since, highWater time.Time, paginationCfg *config.GitHubPaginationConfig) ([]ghPRInfo, int64, time.Time, error) {
	var prInfos []ghPRInfo
	var platformRepoID int64
	latestUpdated := highWater
	graphQLCursor := ""

	for {
		pageCfg := opPageConfig(paginationCfg, opFetchReviews)
		query := buildPRNumbersQuery(owner, repo, since, graphQLCursor, pageCfg.PageSize)
		body, err := doGraphQL(ctx, gw, query, false)
		if err != nil {
			return nil, 0, time.Time{}, fmt.Errorf("fetch PRs for reviews in %s: %w", repoFullName, err)
		}

		var data ghGraphQLData
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, 0, time.Time{}, fmt.Errorf("parse PRs for reviews in %s: %w", repoFullName, err)
		}

		if data.Repository.DatabaseID != 0 {
			platformRepoID = data.Repository.DatabaseID
		}

		prs := data.Repository.PullRequests
		if prs == nil {
			break
		}

		done := false
		for _, pr := range prs.Nodes {
			if !since.IsZero() && pr.UpdatedAt.Before(since) {
				done = true
				break
			}
			prInfos = append(prInfos, ghPRInfo{
				Number:           pr.Number,
				AuthorLogin:      pr.Author.Login,
				AuthorDatabaseID: pr.Author.DatabaseID,
				CreatedAt:        pr.CreatedAt,
			})
			if pr.UpdatedAt.After(latestUpdated) {
				latestUpdated = pr.UpdatedAt
			}
		}

		if done || !prs.PageInfo.HasNextPage {
			break
		}
		if graphQLCursor, err = nextGraphQLCursor(graphQLCursor, prs.PageInfo.EndCursor); err != nil {
			return nil, 0, time.Time{}, fmt.Errorf("collect PR numbers for %s: %w", repoFullName, err)
		}
	}

	return prInfos, platformRepoID, latestUpdated, nil
}

// --- FetchPRCommentsOp ---

// ghPRInfo holds lightweight PR metadata for the 2-phase review/comment fetch.
type ghPRInfo struct {
	Number           int
	AuthorLogin      string
	AuthorDatabaseID int64
	// CreatedAt is the PR's creation time, emitted as the pr_created_at metadata key.
	CreatedAt time.Time
}

// collectRecentPRsWithAuthor fetches recently-updated PR numbers plus author info via GraphQL.
// Also returns the repository's platform database ID so callers can stamp it on
// emitted records (used by a consumer for stable repo lookups).
//
// highWater is the caller's stored watermark and seeds the returned one, so a repo
// that yields no PRs returns it unchanged — leaving a never-advanced repo at zero
// rather than at `since`. Seeding from `since` instead would persist a query bound
// as though data had carried it, and on a first sync that bound is one lookback old:
// the next run reads it back as its own floor, finds nothing again, and rewrites it,
// pinning the entry permanently. Callers must not substitute `since`.
func collectRecentPRsWithAuthor(ctx context.Context, gw sdk.VendorGateway, owner, repo, repoFullName string, since, highWater time.Time, paginationCfg *config.GitHubPaginationConfig) ([]ghPRInfo, int64, time.Time, error) {
	var prs []ghPRInfo
	var platformRepoID int64
	latestUpdated := highWater
	graphQLCursor := ""

	for {
		pageCfg := opPageConfig(paginationCfg, opFetchPRComments)
		query := buildPRNumbersWithAuthorQuery(owner, repo, graphQLCursor, pageCfg.PageSize)
		body, err := doGraphQL(ctx, gw, query, false)
		if err != nil {
			return nil, 0, time.Time{}, fmt.Errorf("fetch PRs for comments in %s: %w", repoFullName, err)
		}

		var data ghGraphQLData
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, 0, time.Time{}, fmt.Errorf("parse PRs for comments in %s: %w", repoFullName, err)
		}

		if data.Repository.DatabaseID != 0 {
			platformRepoID = data.Repository.DatabaseID
		}

		prConn := data.Repository.PullRequests
		if prConn == nil {
			break
		}

		done := false
		for _, pr := range prConn.Nodes {
			if !since.IsZero() && pr.UpdatedAt.Before(since) {
				done = true
				break
			}
			prs = append(prs, ghPRInfo{
				Number:           pr.Number,
				AuthorLogin:      pr.Author.Login,
				AuthorDatabaseID: pr.Author.DatabaseID,
				CreatedAt:        pr.CreatedAt,
			})
			if pr.UpdatedAt.After(latestUpdated) {
				latestUpdated = pr.UpdatedAt
			}
		}

		if done || !prConn.PageInfo.HasNextPage {
			break
		}
		if graphQLCursor, err = nextGraphQLCursor(graphQLCursor, prConn.PageInfo.EndCursor); err != nil {
			return nil, 0, time.Time{}, fmt.Errorf("collect PRs with author for %s: %w", repoFullName, err)
		}
	}

	return prs, platformRepoID, latestUpdated, nil
}

type FetchPRCommentsOp struct {
	paginationCfg     *config.GitHubPaginationConfig
	checkpointReserve time.Duration
}

func (op *FetchPRCommentsOp) OperationID() sdk.OperationID { return opFetchPRComments }

func (op *FetchPRCommentsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	repoInfos, err := fetchAllRepos(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching repos: %w", err)
	}
	repos := make([]string, len(repoInfos))
	for i, ri := range repoInfos {
		repos[i] = ri.FullName
	}

	cursorStates := existingCursorStates(params.Cursor)

	var skipped repotally.Skipper
repos:
	for _, repoFullName := range repos {
		if !hasTimeRemaining(ctx, op.checkpointReserve) {
			logger.WarnWithCtx(ctx, "deadline approaching, returning partial cursor",
				zap.String("operation", string(op.OperationID())))
			break
		}
		skipped.Attempt()

		owner, repo := resolveRepoOwner(repoFullName, settings.Organization)

		state := getRepoCursorState(params.Cursor, repoFullName)
		since := firstSyncSince(state.Timestamp, settings.GetLookbackDays())
		// Resuming an interrupted run reuses the exact since floor from the first
		// pass. A first sync leaves Timestamp at zero, so recomputing firstSyncSince
		// each cycle would slide the floor forward with wall-clock time between
		// scheduled runs and drop PRs near the lookback boundary. Since is written
		// only by the in-progress checkpoints below and cleared on repo completion.
		if !state.Since.IsZero() {
			since = state.Since
		}

		// Step 1: Collect recently-updated PR numbers + author info via GraphQL.
		// platformRepoID is stamped on every emitted comment record so the
		// consumer can resolve the PR via stable repo ID, not repo full name.
		prInfos, platformRepoID, latestUpdated, err := collectRecentPRsWithAuthor(ctx, params.VendorGateway, owner, repo, repoFullName, since, state.Timestamp, op.paginationCfg)
		if err != nil {
			if skipped.Skip(repoFullName, err) {
				logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
				continue repos
			}
			return nil, fmt.Errorf("collecting recent prs with author for repo %s: %w", repoFullName, err)
		}
		platformRepoIDStr := ""
		if platformRepoID != 0 {
			platformRepoIDStr = strconv.FormatInt(platformRepoID, 10)
		}

		// Step 2: For each PR, fetch comments via REST.
		// Skip PRs already processed in a previous partial run.
		processedPRs := getRepoCursorState(params.Cursor, repoFullName).ProcessedCommentPRs
		restGW := params.VendorGateway.ForGroup(constants.GroupGitHubREST)
		for _, pr := range prInfos {
			if prAlreadyProcessed(processedPRs, pr.Number) {
				continue
			}

			if !hasTimeRemaining(ctx, op.checkpointReserve) {
				logger.WarnWithCtx(ctx, "deadline approaching, checkpointing early",
					zap.String("repo", repoFullName),
					zap.Int("processed_prs", len(processedPRs)),
					zap.Int("total_prs", len(prInfos)))
				// While in-progress, persist the OLD high-water and the frozen since
				// floor, not latestUpdated: unprocessed PRs below latestUpdated must
				// remain in-window on resume so their comments are re-collected. The
				// watermark advances only at repo completion below.
				cursorStates[repoFullName] = repoCursorState{
					Timestamp:           state.Timestamp,
					Since:               since,
					ProcessedCommentPRs: processedPRs,
				}
				// Best-effort: emergency checkpoint saves partial progress.
				// If it fails, the next run re-processes these PRs via idempotent cursor state.
				if params.Checkpoint != nil {
					cpCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
					if cpErr := params.Checkpoint(cpCtx, buildStateCursor(cursorStates)); cpErr != nil {
						logger.Error(ctx, "emergency checkpoint failed",
							zap.String("repo", repoFullName),
							zap.Int("processed_prs", len(processedPRs)),
							zap.Error(cpErr))
					}
					cancel()
				}
				return buildStateCursor(cursorStates), nil
			}

			page := int32(1)
			for {
				protoResp, err := restGW.Execute(ctx, &connectorpb.ListPRCommentsRequest{
					Owner:    owner,
					Repo:     repo,
					PrNumber: safePRNumber(pr.Number),
					Page:     page,
					PerPage:  100,
				})
				if err != nil {
					if skipped.Skip(repoFullName, err) {
						logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
						continue repos
					}
					return nil, fmt.Errorf("fetch comments for %s#PR-%d: %w", repoFullName, pr.Number, err)
				}

				resp, ok := protoResp.(*connectorpb.ListPRCommentsResponse)
				if !ok {
					break
				}

				for _, commentJSON := range resp.GetComments() {
					var comment ghPRComment
					if err := json.Unmarshal(commentJSON, &comment); err != nil {
						logger.WarnWithCtx(ctx, "fetch_pr_comments: skipping comment that failed to unmarshal",
							zap.String("repo", repoFullName), zap.Int("pr_number", pr.Number), zap.Error(err))
						continue
					}
					if !since.IsZero() && comment.CreatedAt.Before(since) {
						continue
					}
					externalID := fmt.Sprintf("%s#PR-%d/COMMENT-%d", repoFullName, pr.Number, comment.ID)
					prCreatedAtStr := formatPRCreatedAt(pr.CreatedAt)
					metadata := map[string]string{
						"repo":             repoFullName,
						"owner":            owner,
						"pr_number":        strconv.Itoa(pr.Number),
						"pr_key":           fmt.Sprintf("%s#PR-%d", repoFullName, pr.Number),
						"pr_author_login":  pr.AuthorLogin,
						"pr_author_id":     databaseIDString(pr.AuthorDatabaseID),
						"platform_repo_id": platformRepoIDStr,
						"pr_created_at":    prCreatedAtStr,
						"reviewer_is_bot":  strconv.FormatBool(detectIsBot(comment.User.Login, comment.User.Type)),
					}
					if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, commentJSON, metadata, comment.CreatedAt); emitErr != nil {
						return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
					}
				}

				if !resp.GetHasNextPage() {
					break
				}
				page++
			}

			// Track this PR as processed and checkpoint. Same in-progress invariant
			// as the deadline checkpoint above: hold the old high-water and frozen
			// floor so a crash here cannot skip PRs not yet processed.
			processedPRs = append(processedPRs, pr.Number)
			cursorStates[repoFullName] = repoCursorState{
				Timestamp:           state.Timestamp,
				Since:               since,
				ProcessedCommentPRs: processedPRs,
			}
			if params.Checkpoint != nil {
				if err := params.Checkpoint(ctx, buildStateCursor(cursorStates)); err != nil {
					return nil, fmt.Errorf("checkpoint for %s: %w", repoFullName, err)
				}
			}
		}

		// Repo fully completed — clear ProcessedCommentPRs for next run.
		cursorStates[repoFullName] = repoCursorState{Timestamp: latestUpdated}
		logger.Info(ctx, "fetched pr comments", zap.String("repo", repoFullName), zap.Int("pr_count", len(prInfos)), zap.Time("cursor", latestUpdated))
	}

	if err := skipped.Err(len(repos)); err != nil {
		return nil, err
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	return buildStateCursor(cursorStates), nil
}

// --- FetchCopilotUsageOp ---

type FetchCopilotUsageOp struct{}

func (op *FetchCopilotUsageOp) OperationID() sdk.OperationID { return "fetch_copilot_usage" }

func (op *FetchCopilotUsageOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	org := settings.Organization
	restGW := params.VendorGateway.ForGroup(constants.GroupGitHubREST)

	// Determine the start date from cursor.
	var lastDate string
	if params.Cursor != nil {
		var cursorData struct {
			LastDate string `json:"last_date"`
		}
		if err := json.Unmarshal(params.Cursor.Data, &cursorData); err != nil {
			logger.WarnWithCtx(ctx, "fetch_copilot_metrics: malformed cursor; starting from no prior date",
				zap.String(logger.OrgID, params.Config.OrganizationID), zap.Error(err))
		} else if cursorData.LastDate != "" {
			lastDate = cursorData.LastDate
		}
	}

	// Fetch org-level Copilot metrics.
	protoResp, err := restGW.Execute(ctx, &connectorpb.CopilotMetricsRequest{Organization: org})
	if err != nil {
		// Copilot API may not be available for all orgs; treat a 404, and only a
		// 404, as no data. Any other failure fails the run and keeps the cursor.
		syncErr := &sdk.SyncError{}
		if errors.As(err, &syncErr) && syncErr.Kind == sdk.ErrKindNotFound {
			logger.Info(ctx, "copilot metrics not available for org")
			return params.Cursor, nil
		}
		return nil, fmt.Errorf("fetch copilot metrics: %w", orgScopeForbidden(err, op.OperationID(), params.Config.OrganizationID, "the organization's Copilot metrics"))
	}

	metricsResp, ok := protoResp.(*connectorpb.CopilotMetricsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", protoResp)
	}

	// skipFloor trails lastDate by copilotFinalizationLagDays so finalized counts
	// re-emit; latestDate still advances to the true max. See copilotFinalizationLagDays.
	skipFloor := lastDate
	if lastDate != "" {
		if parsed, parseErr := time.Parse("2006-01-02", lastDate); parseErr == nil {
			skipFloor = parsed.AddDate(0, 0, -copilotFinalizationLagDays).Format("2006-01-02")
		} else {
			// lastDate is always written by this op in 2006-01-02 format; an unparseable
			// value means a corrupted cursor. Fall back to no re-pull window (skipFloor ==
			// lastDate) but surface it so the corruption is visible.
			logger.WarnWithCtx(ctx, "fetch_copilot_metrics: unparseable cursor last_date; skipping trailing re-pull window",
				zap.String("org", org), zap.String("last_date", lastDate), zap.Error(parseErr))
		}
	}

	latestDate := lastDate
	for _, metricJSON := range metricsResp.GetMetrics() {
		var m ghCopilotMetrics
		if err := json.Unmarshal(metricJSON, &m); err != nil {
			logger.WarnWithCtx(ctx, "fetch_copilot_metrics: skipping org metric that failed to unmarshal",
				zap.String("org", org), zap.Error(err))
			continue
		}
		if lastDate != "" && m.Date <= skipFloor {
			continue
		}

		copilotDate, parseErr := time.Parse("2006-01-02", m.Date)
		if parseErr != nil {
			logger.WarnWithCtx(ctx, "fetch_copilot_metrics: unparseable org metric date; emitting with zero record time",
				zap.String("org", org), zap.String("date", m.Date), zap.Error(parseErr))
		}
		externalID := fmt.Sprintf("copilot-%s-%s", org, m.Date)
		if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, metricJSON, map[string]string{"org": org, "type": "org_metrics"}, copilotDate); emitErr != nil {
			return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
		}

		if m.Date > latestDate {
			latestDate = m.Date
		}
	}

	// Fetch per-user Copilot usage for recent days.
	userProtoResp, err := restGW.Execute(ctx, &connectorpb.CopilotUserUsageRequest{Organization: org})
	if err != nil {
		syncErr := &sdk.SyncError{}
		if !errors.As(err, &syncErr) || syncErr.Kind != sdk.ErrKindNotFound {
			return nil, fmt.Errorf("fetch copilot user metrics: %w", orgScopeForbidden(err, op.OperationID(), params.Config.OrganizationID, "the organization's Copilot user metrics"))
		}
		logger.Info(ctx, "copilot user metrics not available")
	} else {
		userResp, ok := userProtoResp.(*connectorpb.CopilotUserUsageResponse)
		if ok {
			for _, umJSON := range userResp.GetUserMetrics() {
				var um ghCopilotUsageResponse
				if err := json.Unmarshal(umJSON, &um); err != nil {
					logger.WarnWithCtx(ctx, "fetch_copilot_metrics: skipping user metric that failed to unmarshal",
						zap.String("org", org), zap.Error(err))
					continue
				}
				if lastDate != "" && um.Day <= skipFloor {
					continue
				}

				copilotUserDate, parseErr := time.Parse("2006-01-02", um.Day)
				if parseErr != nil {
					logger.WarnWithCtx(ctx, "fetch_copilot_metrics: unparseable user metric date; emitting with zero record time",
						zap.String("org", org), zap.String("date", um.Day), zap.Error(parseErr))
				}
				// The user id is half the row's identity: without it every such row of a
				// day would share one ExternalID and overwrite each other. Refuse it.
				if um.GitHubComUserID == "" {
					logger.WarnWithCtx(ctx, "fetch_copilot_metrics: skipping user metric with no github_com_user_id; it has no identity to store under",
						zap.String("org", org), zap.String("date", um.Day))
					continue
				}
				externalID := fmt.Sprintf("copilot-user-%s-%s-%s", org, um.Day, um.GitHubComUserID)
				if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, umJSON, map[string]string{"org": org, "type": "user_metrics"}, copilotUserDate); emitErr != nil {
					return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}

				if um.Day > latestDate {
					latestDate = um.Day
				}
			}
		}
	}

	if latestDate == "" {
		return params.Cursor, nil
	}

	cursorData, err := json.Marshal(map[string]string{"last_date": latestDate})
	if err != nil {
		return nil, fmt.Errorf("marshaling copilot cursor: %w", err)
	}
	logger.Info(ctx, "fetched copilot usage", zap.String("latest_date", latestDate))
	return &sdk.Cursor{Data: cursorData}, nil
}

// hasErrorType reports whether any GraphQL error has the given type.
func hasErrorType(errs []*connectorpb.GitHubGraphQLError, typ string) bool {
	for _, e := range errs {
		if e.GetType() == typ {
			return true
		}
	}
	return false
}

// scopeListRE matches the scope list GitHub's INSUFFICIENT_SCOPES message
// names: "requires one of the following scopes: ['read:project']".
var scopeListRE = regexp.MustCompile(`requires one of the following scopes: \[([^\]]*)\]`)

// missingScopeHint names the scopes the first INSUFFICIENT_SCOPES error asks
// for, as " (needs read:project)", or "" when the message names none.
func missingScopeHint(errs []*connectorpb.GitHubGraphQLError) string {
	for _, e := range errs {
		if e.GetType() != "INSUFFICIENT_SCOPES" {
			continue
		}
		if m := scopeListRE.FindStringSubmatch(e.GetMessage()); m != nil {
			scopes := strings.ReplaceAll(strings.ReplaceAll(m[1], "'", ""), " ", "")
			return " (needs " + strings.ReplaceAll(scopes, ",", " or ") + ")"
		}
	}
	return ""
}
