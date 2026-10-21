// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/vcs"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	bitbucketpb "github.com/tetriz-ai/tetriz-connectors/gen/bitbucket/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// defaultLookback is the maximum history window for first-sync operations.
// When no cursor exists, operations fetch data from this far back instead of
// fetching the entire history.
const defaultLookback = 12 * 30 * 24 * time.Hour // ~12 months

// maxConsecutiveCursorHolds bounds how many runs in a row may hold a repo's
// watermark back for a transient enrichment failure. fetch_prs ticks every 15
// minutes, so 8 is ~2 hours — long enough to ride out a genuine rate-limit window,
// short enough that a repo always regains forward progress. On giving up the cursor
// advances and the degraded PRs are repairable by a cursor rewind: a direct
// store write of a cursor from before the gap.
const maxConsecutiveCursorHolds = 8

// Metadata keys stamped on emitted records and consumed by the Bitbucket
// consumer. Bitbucket PRs are keyed by numeric pr_id (workspace-scoped),
// not the human-facing pr_number — keeping this as a single constant prevents
// drift between fetch_prs / fetch_reviews / fetch_pr_comments emissions.
const metaKeyPRID = "pr_id"

// Fetch-cache cacheType namespaces in connector_fetch_cache:
// three immutable-value caches and two `_last_fetched` timestamp caches.
// Exported so a host's eviction job scans the exact same namespaces — this package
// is the single source of truth for the cacheType strings.
const (
	CacheTypePRFirstCommit  = "pr_first_commit"
	CacheTypePRCommitSHAs   = "pr_commit_shas"
	CacheTypePRDiffstat     = "pr_diffstat"
	CacheTypeCommitDiffstat = "commit_diffstat"
	CacheTypePRActivity     = "pr_activity_last_fetched"
	CacheTypePRComments     = "pr_comments_last_fetched"
	// CacheTypeFileHistory / CacheTypeCommitDiff cache the defect-lineage reconstruction's inputs.
	// Both are immutable at a fixed commit, so they are cached indefinitely and re-drives are free.
	CacheTypeFileHistory = "file_history"
	CacheTypeCommitDiff  = "commit_diff"
)

// primeFetchCache batches one GetMany for a page's keys and merges the hits into the
// operation's in-memory read map, so the existing hit/miss lookups serve from the
// connector_fetch_cache table transparently. No-op when the capability is absent.
// Batched per page because the streaming list+enrich loops discover keys mid-pagination
// (a GetMany per page rather than one at loop top). The capability
// already degrades a GetMany error to an empty result; on the off chance a non-degrading
// implementation returns one, log it and leave the map un-primed (the entries fall through
// to the existing per-key miss + vendor re-fetch) rather than fail the op.
func primeFetchCache[V any](ctx context.Context, fc sdk.FetchCache, cacheType string, keys []string, dst map[string]V) {
	if fc == nil || len(keys) == 0 {
		return
	}
	raw, err := fc.GetMany(ctx, cacheType, keys)
	if err != nil {
		logger.WarnWithCtx(ctx, "fetch cache prime failed, degrading to per-key miss",
			zap.String("cache_type", cacheType), zap.Int("keys", len(keys)), zap.Error(err))
		return
	}
	for k, r := range raw {
		var v V
		if err := json.Unmarshal(r, &v); err != nil {
			logger.WarnWithCtx(ctx, "fetch cache: skipping unparseable cached entry; treating as miss",
				zap.String("cache_type", cacheType), zap.String("entry_key", k), zap.Error(err))
			continue
		}
		dst[k] = v
	}
}

// marshalFetchCacheEntries serializes a dirty map's values into the raw-JSON shape
// PutMany expects. A value that fails to marshal is dropped with a warning rather than
// failing the run (the cache is best-effort). The JSONB column compares by normalized
// value, so the timestamp caches marshal to a quoted JSON scalar and the diffstat caches
// to a JSON object — both round-trip byte-identical on read. Returns nil for
// an empty map so callers skip the PutMany entirely.
func marshalFetchCacheEntries[V any](ctx context.Context, cacheType string, m map[string]V) map[string]json.RawMessage {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(m))
	for k, v := range m {
		raw, err := json.Marshal(v)
		if err != nil {
			logger.WarnWithCtx(ctx, "fetch cache: dropping dirty entry that failed to marshal",
				zap.String("cache_type", cacheType), zap.String("entry_key", k), zap.Error(err))
			continue
		}
		out[k] = raw
	}
	return out
}

// putFetchCacheDirty flushes one cache_type's dirty set to the new table via the
// capability's conditional upsert and, on success, CLEARS the set. Callers flush
// per page so a hard kill re-fetches at most one page, not the whole run.
// The dirty map is shared across a
// repo's pages and across repos, so the clear-on-success is what stops a later
// flush (or, for fetch_prs/fetch_commits, the end-of-run tail defer) from
// re-sending entries already written; entries a failed flush retained instead roll
// forward to the next flush.
//
// Best-effort throughout: a nil capability or empty set is a no-op; a store error
// is logged and the entries are RETAINED (not cleared) so the next flush retries
// them — never a hard failure of the sync, the cached values are re-derivable. NB
// the production adapter degrades a store error to a drop (returns nil), so in real use
// the retain path is effectively a no-op and a failed write just re-fetches next
// cycle; retain matters only for a hypothetical non-degrading FetchCache.
func putFetchCacheDirty[V any](ctx context.Context, fc sdk.FetchCache, cacheType string, dirty map[string]V) {
	if fc == nil || len(dirty) == 0 {
		return
	}
	raw := marshalFetchCacheEntries(ctx, cacheType, dirty)
	if len(raw) == 0 {
		return
	}
	if err := fc.PutMany(ctx, cacheType, raw); err != nil {
		logger.WarnWithCtx(ctx, "fetch cache dirty flush failed (best-effort; entries retained for next flush)",
			zap.String("cache_type", cacheType), zap.Int("entries", len(raw)), zap.Error(err))
		return
	}
	clear(dirty)
}

// firstSyncSince returns a floor timestamp for first-sync runs.
// If since is zero (no prior cursor), it returns now minus defaultLookback.
// Otherwise it returns since unchanged.
func firstSyncSince(since time.Time) time.Time {
	if since.IsZero() {
		return time.Now().UTC().Add(-defaultLookback)
	}
	return since
}

// watermarkIsPinnedBeyondFloor reports whether a stored per-repo watermark is a
// synthetic first-sync floor that has aged past the widest window this connector ever
// scans (defaultLookback). Cursor loaders treat such an entry as absent and drop it
// from the write-back: it asserts progress no vendor data ever carried, ages a day per
// day (holding the oldest-entry age gauge open), and forces a scan window WIDER than a
// first sync on every tick. This is the self-heal for the residue of floors minted
// before the pin fixes — the repos they name have no activity to observe, so
// nothing else can ever move those entries.
//
// Both conditions are load-bearing. Age alone cannot separate a pin from a REAL
// watermark on a repo whose sync was dark long enough to age past the floor (a dead
// credential, a DELETED repo awaiting restore) — dropping a real watermark would move
// the next scan's lower bound FORWARD and silently skip the unconsumed window, so a
// real watermark is never dropped here no matter its age; it is allowed to age, and
// an operator resets the cursor. The discriminator
// is precision: a minted floor comes from time.Now() and carries nanosecond digits,
// while every vendor watermark parses from Bitbucket's microsecond-or-coarser
// timestamps, so a non-zero sub-microsecond component is only reachable by a mint.
func watermarkIsPinnedBeyondFloor(ts time.Time) bool {
	return !ts.IsZero() &&
		ts.Before(time.Now().UTC().Add(-defaultLookback)) &&
		ts.Nanosecond()%int(time.Microsecond) != 0
}

// --- Common helpers ---

// bbRepoInfo holds a Bitbucket repo's full name, UUID, and mainbranch name.
// UUID matches the platform_repo_id emitted by fetch_repos (bb.UUID) and must
// be used as PlatformRepoId in fetch_repo_tree records so UpdateTree can locate
// the repository row by the same stable key.
// Mainbranch is the API-reported default branch (bb.Mainbranch.Name); used by
// fetch_repo_tree as the fallback when no TrackedBranch or RepoBranchOverrides apply.
type bbRepoInfo struct {
	FullName   string
	UUID       string // Bitbucket UUID, e.g. "{abc-def-...}"
	Mainbranch string // API-reported default branch name; resolved via GetRepo for explicit repo lists, empty only if that fetch failed
}

// fetchAllRepos returns the list of repos to sync.
//
// Resolution order:
//  1. settings.Repositories (manual override) — used as-is.
//  2. settings.DiscoveredRepos (auto-discovery cache populated by
//     FetchReposOp every 6h) — used as-is to avoid burning the
//     0.5556 RPS bitbucket-rest budget on every */15 sync tick.
//     With three pull ops sharing the same per-org bucket, re-paginating
//     the repo list on each op call deterministically saturates the
//     limiter for any workspace with >1 repo.
//  3. Live REST pagination as a last resort (first sync before
//     FetchReposOp has run, or when DiscoveredRepos was wiped).
//
// The last-resort listing loop is paced through pacing.ExecuteWaitingForLocalCapacity
// (threaded budget/opID/orgID): on a cold org this discovery is the FIRST bitbucket-rest
// call of the run, so a local-limit rejection here would otherwise fail the whole run
// before a single repo is looked at.
func fetchAllRepos(ctx context.Context, gw sdk.VendorGateway, settings BitbucketSettings, opID sdk.OperationID, orgID string, budget *pacing.PacingBudget) ([]string, error) {
	if len(settings.Repositories) > 0 {
		return settings.Repositories, nil
	}
	if len(settings.DiscoveredRepos) > 0 {
		cached := make([]string, 0, len(settings.DiscoveredRepos))
		for _, r := range settings.DiscoveredRepos {
			// Skip repos marked deleted. They stay in
			// DiscoveredRepos, so a later fetch_repos reconciliation promotes
			// them back to active when the vendor restores access.
			if !r.IsActive() {
				continue
			}
			cached = append(cached, r.FullName)
		}
		return cached, nil
	}

	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	var repos []string
	page := int32(1)

	for {
		if page > maxListPages {
			return nil, errListPageCap("fetch workspace repos")
		}
		protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketListReposRequest{
			Workspace: settings.Workspace,
			Page:      page,
			Pagelen:   100,
		}, budget, connectorID, opID, orgID)
		if err != nil {
			return nil, fmt.Errorf("fetch workspace repos: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListReposResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *BitbucketListReposResponse", protoResp)
		}

		for _, repoRaw := range resp.GetRepos() {
			var r bbRepository
			if err := json.Unmarshal(repoRaw, &r); err != nil {
				logger.WarnWithCtx(ctx, "fetch repos: skipping repo entry that failed to unmarshal",
					zap.Error(err))
				continue
			}
			repos = append(repos, r.FullName)
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
	}

	return repos, nil
}

// getRepoInfo fetches a single repository via BitbucketGetRepo and returns its
// API-reported default branch (mainbranch.name) and UUID, which fill the gap
// on the explicit settings.Repositories path, where the workspace listing —
// which auto-discovery uses to learn these — is never paged. Returns the zero
// values (and a logged warning) on fetch / unmarshal failure so the caller can
// fall back to its own last-resort default rather than fail the whole sync.
//
// gw must already be group-scoped (ForGroup(GroupBitbucketREST)) so the call
// is metered by the existing bitbucket-rest budget.
// A non-nil budget paces the GetRepo call against our own limiter (waits out an
// ErrLocalRateLimit, then retries) instead of failing to resolve; callers that
// use the resolved branch to advance a cursor pass one, because guessing "main"
// under a transient local-limit rejection can walk the wrong branch and skip
// real commits on the true default. A nil budget keeps the plain fail-fast fetch.
//
// The returned error lets a cursor-advancing caller tell a rate-limit give-up
// (errors.Is ErrLocalRateLimit — branch genuinely unknown, do not guess) apart
// from a resolved-but-branchless repo (empty branch, nil error). A branchless
// repo and a genuine 4xx/parse failure still leave the branch empty for the
// caller's "main" last resort.
func getRepoInfo(ctx context.Context, gw sdk.VendorGateway, workspace, repoSlug string, budget *pacing.PacingBudget, opID sdk.OperationID, orgID string) (defaultBranch, uuid string, err error) {
	req := &connectorpb.BitbucketGetRepoRequest{
		Workspace: workspace,
		RepoSlug:  repoSlug,
	}
	var protoResp proto.Message
	if budget != nil {
		protoResp, err = pacing.ExecuteWaitingForLocalCapacity(ctx, gw, req, budget, connectorID, opID, orgID)
	} else {
		protoResp, err = gw.Execute(ctx, req)
	}
	// Pure helper: return the failure reason (wrapped) and let the caller log it
	// with its own context — the cursor-critical fetch_commits path and the
	// best-effort fetch_repo_tree/fetch_branches path log differently.
	if err != nil {
		return "", "", fmt.Errorf("bitbucket get_repo: %w", err)
	}
	resp, ok := protoResp.(*connectorpb.BitbucketGetRepoResponse)
	if !ok {
		return "", "", fmt.Errorf("get_repo unexpected response type %T", protoResp)
	}
	var r bbRepository
	if err := json.Unmarshal(resp.GetRepo(), &r); err != nil {
		return "", "", fmt.Errorf("get_repo payload unparseable: %w", err)
	}
	if r.Mainbranch != nil {
		defaultBranch = r.Mainbranch.Name
	}
	return defaultBranch, r.UUID, nil
}

// resolveDefaultBranch returns the repo's real default branch. It short-circuits
// to cached when non-empty (the workspace-listing value for auto-discovered
// repos), otherwise issues one BitbucketGetRepo call. Returns "" only when even
// that fetch fails — callers turn "" into their own concrete last-resort branch
// (resolveCommitBranch's "main"). gw must be group-scoped to GroupBitbucketREST.
//
// Cost: on the auto-discovery path `cached` is always populated, so this makes
// ZERO extra calls. The GetRepo fetch fires only on the explicit
// settings.Repositories path, where DiscoveredRepos is empty — once per repo
// per run, on both fetch_commits and fetch_repo_tree (the default branch isn't
// persisted there, deliberately: write-back would grow the connector_configs
// settings blob). Bounded + acceptable because explicit repo lists are
// rare and small; large repo sets use auto-discovery.
func resolveDefaultBranch(ctx context.Context, gw sdk.VendorGateway, workspace, repoSlug, cached string, budget *pacing.PacingBudget, opID sdk.OperationID, orgID string) (string, error) {
	if cached != "" {
		return cached, nil
	}
	branch, _, err := getRepoInfo(ctx, gw, workspace, repoSlug, budget, opID, orgID)
	if err != nil {
		return branch, fmt.Errorf("resolving default branch for %s/%s: %w", workspace, repoSlug, err)
	}
	return branch, nil
}

// fetchAllReposInfo returns bbRepoInfo (full name + UUID + mainbranch) for every
// repo to sync. Used by fetch_repo_tree which needs the UUID as platform_repo_id
// and the default branch for the tree walk. On the explicit settings.Repositories
// path the workspace listing is never paged, so each entry's UUID + Mainbranch
// are filled via a per-repo BitbucketGetRepo call (metered by bitbucket-rest).
// A failed GetRepo leaves both empty: fetch_repo_tree then falls back to the
// full_name cursor key (empty platform_repo_id → consumer no-op) and
// resolveCommitBranch's "main".
func fetchAllReposInfo(ctx context.Context, gw sdk.VendorGateway, settings BitbucketSettings) ([]bbRepoInfo, error) {
	if len(settings.Repositories) > 0 {
		restGW := gw.ForGroup(constants.GroupBitbucketREST)
		out := make([]bbRepoInfo, len(settings.Repositories))
		for i, fn := range settings.Repositories {
			ri := bbRepoInfo{FullName: fn}
			if workspace, repoSlug, err := splitFullName(fn); err == nil {
				mainbranch, uuid, repoErr := getRepoInfo(ctx, restGW, workspace, repoSlug, nil, "", "")
				if repoErr != nil {
					// Best-effort: fetch_repo_tree/fetch_branches fall back to the
					// full_name cursor key + resolveCommitBranch's "main" when the
					// branch is unresolved, so ri keeps its zero-value branch/uuid.
					logger.WarnWithCtx(ctx, "fetch repos info: default branch unresolved; using fallback",
						zap.String("full_name", fn), zap.Error(repoErr))
				} else {
					ri.Mainbranch, ri.UUID = mainbranch, uuid
				}
			} else {
				logger.WarnWithCtx(ctx, "fetch repos info: skipping default-branch resolution for malformed full_name",
					zap.String("full_name", fn), zap.Error(err))
			}
			out[i] = ri
		}
		return out, nil
	}

	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	var repos []bbRepoInfo
	page := int32(1)

	for {
		if page > maxListPages {
			return nil, errListPageCap("fetch workspace repos")
		}
		protoResp, err := restGW.Execute(ctx, &connectorpb.BitbucketListReposRequest{
			Workspace: settings.Workspace,
			Page:      page,
			Pagelen:   100,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch workspace repos: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListReposResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *BitbucketListReposResponse", protoResp)
		}

		for _, repoRaw := range resp.GetRepos() {
			var r bbRepository
			if err := json.Unmarshal(repoRaw, &r); err != nil {
				logger.WarnWithCtx(ctx, "fetch repos info: skipping repo entry that failed to unmarshal",
					zap.Error(err))
				continue
			}
			ri := bbRepoInfo{FullName: r.FullName, UUID: r.UUID}
			if r.Mainbranch != nil {
				ri.Mainbranch = r.Mainbranch.Name
			}
			repos = append(repos, ri)
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
	}

	return repos, nil
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

// withEventType returns a copy of meta with the "event_type" key set.
func withEventType(meta map[string]string, eventType string) map[string]string {
	out := make(map[string]string, len(meta)+1)
	for k, v := range meta {
		out[k] = v
	}
	out["event_type"] = eventType
	return out
}

// getCompositeCursorTime extracts a per-repo timestamp from a composite cursor.
// Returns zero time if not found (first sync).
func getCompositeCursorTime(ctx context.Context, cursor *sdk.Cursor, repoKey string) time.Time {
	if cursor == nil {
		return time.Time{}
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.WarnWithCtx(ctx, "fetch: composite cursor unparseable, treating repo as first sync",
			zap.String("repo", repoKey), zap.Error(err))
		return time.Time{}
	}
	raw, ok := parts[repoKey]
	if !ok {
		return time.Time{}
	}
	ts := decodeCursorTime(ctx, repoKey, raw)
	if dropPinnedWatermark(ctx, repoKey, ts) {
		return time.Time{}
	}
	return ts
}

// decodeCursorTime decodes one bare-timestamp composite entry, returning zero for
// anything it cannot read — which every caller treats as "first sync for this repo".
func decodeCursorTime(ctx context.Context, repoKey string, raw json.RawMessage) time.Time {
	var ts time.Time
	if err := json.Unmarshal(raw, &ts); err != nil {
		logger.WarnWithCtx(ctx, "fetch: cursor timestamp unparseable, treating repo as first sync",
			zap.String("repo", repoKey), zap.Error(err))
		return time.Time{}
	}
	return ts
}

// dropPinnedWatermark reports whether a decoded watermark is a synthetic pin aged
// beyond the first-sync floor (see [watermarkIsPinnedBeyondFloor]), logging the drop.
// Callers treat a dropped watermark exactly like an absent one: the repo fetches from
// the ordinary first-sync window and the entry is not written back.
func dropPinnedWatermark(ctx context.Context, repoKey string, ts time.Time) bool {
	if !watermarkIsPinnedBeyondFloor(ts) {
		return false
	}
	logger.WarnWithCtx(ctx, "fetch: cursor watermark is a synthetic floor aged past the first-sync lookback, dropping entry and treating repo as first sync",
		zap.String("repo", repoKey), zap.Time("watermark", ts))
	return true
}

// carryForwardCursorTimes seeds a run's per-repo watermark map with the watermarks the
// incoming cursor holds for repos the config still knows about, so a repo the run does
// not visit keeps its progress.
//
// [buildCompositeCursor] REPLACES the whole cursor blob, so a repo omitted from the map
// is an ERASED repo — and an erased watermark is not a neutral loss. The next run reads
// that repo as a first sync, [firstSyncSince] substitutes a synthetic
// now-defaultLookback floor, and a warm per-PR fetch cache then skips every PR at the
// freshness gate, so latestDate never rises above that floor and the floor is written
// back as if it were observed progress. Every later run reproduces it from itself, and
// the repo re-paginates a year of PR list on each tick. A repo leaves a run for ordinary
// reasons — an unparseable full name, or an absence from the workspace listing marking it
// DELETED while it stays in DiscoveredRepos awaiting reconciliation — so its watermark
// must survive the run that skipped it.
//
// known bounds what may be carried: a repo the config no longer lists at all (renamed,
// or dropped from an explicit Repositories list) is gone for good, and carrying its
// watermark unconditionally would grow cursor_data without any path that ever prunes it.
// An empty known means the caller could not enumerate a durable repo set (workspace-
// listing mode keeps no DiscoveredRepos bookkeeping), and everything is carried — an
// unbounded blob is the lesser fault against re-walking a year of history.
//
// Entries are decoded with the same reader the run's own `since` uses, so a value this
// cannot read is one the op would have treated as a first sync anyway: it is left out
// rather than re-emitted in a shape the op cannot consume.
//
// A pinned watermark aged beyond the first-sync floor is not carried either (see
// [watermarkIsPinnedBeyondFloor]); droppedAged reports that at least one entry was
// discarded for that reason, so a caller whose watermark map ends up empty can tell a
// deliberate erasure (persist the now-empty cursor) from having nothing to say
// (return the incoming cursor unchanged — the shield for an unparseable blob).
func carryForwardCursorTimes(ctx context.Context, cursor *sdk.Cursor, known []string) (carried map[string]time.Time, droppedAged bool) {
	carried = make(map[string]time.Time)
	if cursor == nil {
		return carried, false
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.WarnWithCtx(ctx, "fetch: composite cursor unparseable, carrying no prior watermarks forward", zap.Error(err))
		return carried, false
	}
	knownSet := make(map[string]struct{}, len(known))
	for _, repoKey := range known {
		knownSet[repoKey] = struct{}{}
	}
	for repoKey, raw := range parts {
		if len(knownSet) > 0 {
			if _, ok := knownSet[repoKey]; !ok {
				continue
			}
		}
		ts := decodeCursorTime(ctx, repoKey, raw)
		if ts.IsZero() {
			continue
		}
		if dropPinnedWatermark(ctx, repoKey, ts) {
			droppedAged = true
			continue
		}
		carried[repoKey] = ts
	}
	return carried, droppedAged
}

// carryableRepoKeys is the repo set a cursor may retain a watermark for: everything the
// config still knows about, plus the repos this run resolved. Empty when the config
// enumerates no durable set — see carryForwardCursorTimes on what that means.
//
// It mirrors [fetchAllRepos]'s precedence exactly, and must keep doing so. An explicit
// Repositories allowlist SHORT-CIRCUITS there — DiscoveredRepos is not consulted at all —
// so an org that moved from auto-discovery to a narrower manual list can still carry a
// stale discovery cache in its settings. Treating that cache as carryable would retain
// watermarks for repos the operator deliberately stopped syncing: no run ever visits them
// to advance or drop the entry, so they accrue in cursor_data and can hold the
// oldest-entry age gauge open forever.
//
// In discovery mode a DELETED entry IS carryable, deliberately: it awaits a fetch_repos
// reconciliation that can restore it, so discarding its progress would cost a full
// re-walk on restore.
func carryableRepoKeys(settings BitbucketSettings, runRepos []string) []string {
	if len(settings.Repositories) > 0 {
		return append(append(make([]string, 0, len(settings.Repositories)+len(runRepos)),
			settings.Repositories...), runRepos...)
	}
	keys := make([]string, 0, len(settings.DiscoveredRepos)+len(runRepos))
	for _, r := range settings.DiscoveredRepos {
		keys = append(keys, r.FullName)
	}
	return append(keys, runRepos...)
}

// buildCompositeCursor creates a composite cursor from per-repo timestamps.
//
// FetchCommitsOp, FetchReviewsOp and FetchPRCommentsOp use this plain-timestamp
// cursor format. FetchPRsOp uses the typed repoCursorState (see
// [buildStateCursor]) for mid-pagination resume support.
func buildCompositeCursor(timestamps map[string]time.Time) *sdk.Cursor {
	parts := make(map[string]json.RawMessage, len(timestamps))
	for k, v := range timestamps {
		raw, err := json.Marshal(v.UTC())
		if err != nil {
			logger.DebugNoCtx("bitbucket: failed to marshal timestamp part; skipping", zap.String("key", k), zap.Error(err))
			continue
		}
		parts[k] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// repoCursorState tracks per-repo sync progress for fetch_prs, including
// mid-pagination resume state. The semantic gotcha solved here: Bitbucket's
// `?sort=-updated_on` pagination descends newest→oldest, so the page-1 max
// updated_on (latestUpdated) is the WATERMARK FOR THE WHOLE ITERATION — not
// a per-page boundary. Naively persisting it mid-iteration as a per-page
// cursor would make the next run's `since` filter skip every older PR the
// interrupted iteration was still working through.
//
// PageCursor + Since together solve resume safely:
//   - PageCursor (non-empty) means "an iteration was interrupted mid-flight;
//     resume from this page". For Bitbucket this is the stringified next-page
//     number (e.g. "3"); GitHub uses an opaque GraphQL endCursor — both are
//     opaque to the SDK.
//   - Since is the `since` filter that was active when the iteration started.
//     The resume MUST use this exact value so the older pages still satisfy
//     `pr.UpdatedOn >= since` and aren't early-exited.
//
// After a repo finishes fully (no more pages, or early-exit on cursor),
// PageCursor + Since are cleared and Timestamp is set to the iteration's
// final latestUpdated. That becomes the high-water mark for the next run's
// fresh iteration starting at page 1.
type repoCursorState struct {
	// Timestamp is the high-water mark (max updated_on across pages
	// processed so far in this iteration, or the final value after a clean
	// finish). On a resume run with PageCursor set, the FRESH `since` filter
	// is NOT derived from Timestamp; it comes from Since.
	Timestamp time.Time `json:"timestamp"`
	// HoldFloor, when non-zero, is a REPAIR FLOOR: the oldest PR whose enrichment
	// failed transiently, which the next run must re-walk from. It is deliberately a
	// separate field from Timestamp rather than a clamp applied to it. Timestamp is
	// the true high-water mark and the resume path uses it as priorWatermark, so
	// clamping it made the interrupted run's real progress unrecoverable (a resumed
	// run walks only OLDER pages, so the clamp became a permanent ceiling). Keeping
	// the two apart lets the hold be durable AND the watermark monotonic.
	//
	// `omitzero`, not `omitempty`: encoding/json never treats a struct as empty, so
	// `omitempty` on a time.Time is a silent no-op (see Since below) and would stamp
	// "0001-01-01T00:00:00Z" into every repo entry. omitzero consults IsZero, so a
	// repo with no hold contributes no bytes to the cursor blob.
	HoldFloor time.Time `json:"hf,omitzero"`
	// HoldCount is how many CONSECUTIVE runs have held this repo back. It bounds the
	// hold: ErrKindRateLimit is raised by our own local token bucket as well as by a
	// vendor 429, and a locally-throttled org never self-clears, so an unbounded hold
	// would pin the repo while the re-walked window grows every tick — needing more
	// budget and making the throttle worse.
	HoldCount int `json:"hc,omitempty"`
	// HoldReleasedAt records the point the bound gave up on, so a later run does not
	// immediately restart a streak on the same PR and cycle 0 -> limit -> 0 forever.
	// `omitzero` for the same reason as HoldFloor.
	HoldReleasedAt time.Time `json:"hr,omitzero"`
	// PageCursor is the next page identifier to fetch when resuming an
	// interrupted iteration. Empty means "no in-flight pagination — start
	// at page 1 with `since = firstSyncSince(Timestamp)`".
	PageCursor string `json:"page_cursor,omitempty"`
	// Since is the `since` filter resolved at the start of the in-flight
	// iteration. Stored so a resume can use the same early-exit boundary
	// and not skip older pages. The JSON tag intentionally OMITS `,omitempty`
	// — encoding/json doesn't honour omitempty for time.Time (it has no
	// IsZero introspection path), so the tag would be a misleading no-op.
	// Always serializing the field is the honest behaviour; on a clean
	// finish Execute writes a state with Since=zero and the reader doesn't
	// consult Since unless PageCursor is also non-empty.
	Since time.Time `json:"since"`
}

// loadCursorStates decodes a composite cursor into per-repo cursor states and
// reports whether any pinned entry was dropped — the Execute paths whose
// nothing-to-write fallback returns the incoming cursor when the state map is empty
// need that report, or a cursor whose ONLY entries were dropped as pinned would be
// resurrected verbatim by that fallback.
//
// An entry that does not decode as a repoCursorState object is dropped with a
// logger.Warn so an operator has a grep target — a silently-dropped entry would
// cause that repo's next run to start from `firstSyncSince(zero)` (12-month
// lookback). The outer cursor parse failure is similarly logged.
//
// A PLAIN entry whose watermark is a synthetic pin aged beyond the first-sync floor is
// dropped too. Only a plain entry qualifies: resume state (PageCursor) or a repair
// floor (HoldFloor) marks an iteration whose bookkeeping is live, and the hold/resume
// machinery still needs it.
func loadCursorStates(ctx context.Context, cursor *sdk.Cursor) (map[string]repoCursorState, bool) {
	states := make(map[string]repoCursorState)
	droppedPinned := false
	if cursor == nil {
		return states, false
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.Error(ctx, "fetch_prs: input cursor unparseable, treating as empty",
			zap.Error(err))
		return states, false
	}
	dropIfPinned := func(repoKey string, state repoCursorState) bool {
		if state.PageCursor != "" || !state.HoldFloor.IsZero() {
			return false
		}
		if !dropPinnedWatermark(ctx, repoKey, state.Timestamp) {
			return false
		}
		droppedPinned = true
		return true
	}
	for repoKey, raw := range parts {
		var state repoCursorState
		structErr := json.Unmarshal(raw, &state)
		if structErr == nil {
			if !state.Timestamp.IsZero() {
				if dropIfPinned(repoKey, state) {
					continue
				}
				states[repoKey] = state
				continue
			}
			// Struct parsed cleanly but Timestamp is the zero value — no
			// useful watermark to drive the next iteration. Log so an
			// operator can tell this apart from a corrupt-blob drop and
			// hunt down the writer that produced a zero-Timestamp state.
			logger.WarnWithCtx(ctx, "fetch_prs: dropping cursor entry with zero Timestamp; repo will re-fetch from lookback floor",
				zap.String("repo", repoKey),
				zap.String("page_cursor", state.PageCursor),
				zap.Time("since", state.Since))
			continue
		}
		// A corrupt or schema-drifted entry. Surface the decode cause so the
		// resulting re-fetch from the lookback floor is debuggable.
		logger.WarnWithCtx(ctx, "fetch_prs: dropping unparseable cursor entry; repo will re-fetch from lookback floor",
			zap.String("repo", repoKey),
			zap.ByteString("raw", raw),
			zap.NamedError("struct_decode_err", structErr))
	}
	return states, droppedPinned
}

// buildStateCursor serializes per-repo cursor states into a composite cursor.
// Marshal errors are logged and the offending repo's entry is dropped — the
// state struct holds only time.Time + string fields so a marshal failure is
// unreachable in practice, but swallowing the error silently would let a
// future field addition mask data loss without a single log line to grep on.
func buildStateCursor(ctx context.Context, states map[string]repoCursorState) *sdk.Cursor {
	parts := make(map[string]json.RawMessage, len(states))
	for repoKey, state := range states {
		state.Timestamp = state.Timestamp.UTC()
		state.Since = state.Since.UTC()
		raw, err := json.Marshal(state)
		if err != nil {
			logger.Error(ctx, "fetch_prs: failed to marshal cursor state, dropping repo entry",
				zap.String("repo", repoKey), zap.Error(err))
			continue
		}
		parts[repoKey] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// pageCheckpointFn is invoked after each fully-processed page of a single
// repo's pagination. It receives the current high-water mark and the next
// page identifier (empty if no further pages); implementations persist a
// repoCursorState with PageCursor set so an interrupted run can resume.
type pageCheckpointFn func(ctx context.Context, highWater time.Time, nextPageCursor string) error

// makePageCheckpoint wires a page-level checkpoint into the framework's
// run-level Checkpoint. `since` is the snapshot of the iteration's `since`
// filter at the start of THIS repo's pagination; it is stored on every
// per-page write so a future resume reuses the same lower bound. Returns
// nil when the run has no Checkpoint (e.g. in tests), making the per-page
// call a no-op.
//
// Scale note: each per-page invocation serializes the FULL composite cursor
// (every repo's state, not just the one whose page just finished). A 1000-
// repo workspace at 60 pages/repo would write ~9MB over a single sync via
// the ISyncStateDAO UPSERT path. Today's workspaces are under 10 repos so
// this is comfortably bounded. If a much larger workspace onboards, the
// next step is either a per-K-page throttle here or per-repo cursor rows
// in the sync_states table — both keep the resumability guarantee.
func makePageCheckpoint(checkpoint sdk.CheckpointFunc, cursorStates map[string]repoCursorState, repoFullName string, since time.Time) pageCheckpointFn {
	if checkpoint == nil {
		return nil
	}
	return func(ctx context.Context, highWater time.Time, nextPageCursor string) error {
		// Carry the hold fields forward. Rebuilding the state without them let every
		// per-page write erase the streak, so the bound could never count past 1 on
		// the path that actually persists during a throttle.
		prev := cursorStates[repoFullName]
		cursorStates[repoFullName] = repoCursorState{
			Timestamp:      highWater,
			HoldFloor:      prev.HoldFloor,
			HoldCount:      prev.HoldCount,
			HoldReleasedAt: prev.HoldReleasedAt,
			PageCursor:     nextPageCursor,
			Since:          since,
		}
		return checkpoint(ctx, buildStateCursor(ctx, cursorStates))
	}
}

// splitFullName splits "workspace/repo" into workspace and repo slug.
func splitFullName(fullName string) (workspace, repoSlug string, err error) {
	for i := range fullName {
		if fullName[i] == '/' {
			if i == 0 || i == len(fullName)-1 {
				break
			}
			return fullName[:i], fullName[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("invalid repo format %q, expected workspace/repo", fullName)
}

// saveDiscoveredRepos reconciles the live repo list returned by Bitbucket
// with the cached DiscoveredRepos in settings. Reconciliation semantics:
//   - Repo present in the live list → Status promoted back to active,
//     LastSeenAt refreshed to now. Any previous Status=deleted is cleared
//     because the vendor obviously restored access.
//   - Repo absent from the live list but present in the cache → Status set
//     to "deleted" (skipped if already deleted so the original LastErrorAt
//     stamp is preserved across reconciliation passes).
//   - Brand-new repo (no cache entry) → appended with Status=active.
//
// PatchSettings (read-modify-write), not a whole-blob overwrite, keeps
// fetch_prs' concurrent cache writes intact.
func saveDiscoveredRepos(ctx context.Context, params *sdk.ExecutionParams, settings BitbucketSettings, repos []bbRepository) {
	if params.ConfigUpdater == nil || len(settings.Repositories) > 0 {
		return
	}
	liveByName := make(map[string]bbRepository, len(repos))
	for _, r := range repos {
		liveByName[r.FullName] = r
	}
	now := time.Now().UTC().Format(time.RFC3339)

	err := params.ConfigUpdater.PatchSettings(ctx, params.Config.OrganizationID, params.Config.ConnectorID, func(current json.RawMessage) (json.RawMessage, error) {
		var live BitbucketSettings
		if len(current) > 0 {
			if uerr := json.Unmarshal(current, &live); uerr != nil {
				return nil, fmt.Errorf("parse live settings: %w", uerr)
			}
		}

		// Index existing cache for in-place merge.
		index := make(map[string]int, len(live.DiscoveredRepos))
		for i, dr := range live.DiscoveredRepos {
			index[dr.FullName] = i
		}

		// Promote live repos to active, refresh metadata.
		for fullName, bb := range liveByName {
			defaultBranch := ""
			if bb.Mainbranch != nil {
				defaultBranch = bb.Mainbranch.Name
			}
			if idx, ok := index[fullName]; ok {
				live.DiscoveredRepos[idx].Status = bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE
				live.DiscoveredRepos[idx].LastSeenAt = now
				// Clear the failure stamp on restore so monitoring does not
				// surface stale "last 4xx" data.
				live.DiscoveredRepos[idx].LastErrorAt = ""
				live.DiscoveredRepos[idx].LastHTTPStatus = 0
				live.DiscoveredRepos[idx].DefaultBranch = defaultBranch
				live.DiscoveredRepos[idx].Private = bb.IsPrivate
				continue
			}
			live.DiscoveredRepos = append(live.DiscoveredRepos, BitbucketDiscoveredRepo{
				FullName:      fullName,
				DefaultBranch: defaultBranch,
				Private:       bb.IsPrivate,
				Status:        bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE,
				LastSeenAt:    now,
			})
		}

		// Mark cache-only repos as deleted. LastHTTPStatus stays 0 because
		// no per-repo HTTP request was made here — the workspace listing
		// just didn't return this repo. Compare with fetch_prs, which
		// records the actual HTTP status (404 / 401 / 503 / ...) for any
		// per-repo failure it observes.
		for i := range live.DiscoveredRepos {
			fullName := live.DiscoveredRepos[i].FullName
			if _, alive := liveByName[fullName]; alive {
				continue
			}
			if live.DiscoveredRepos[i].Status != bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED {
				live.DiscoveredRepos[i].Status = bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED
				live.DiscoveredRepos[i].LastErrorAt = now
				live.DiscoveredRepos[i].LastHTTPStatus = 0
			}
		}

		return json.Marshal(live)
	})
	if err != nil {
		logger.WarnWithCtx(ctx, "failed to reconcile discovered repos", zap.Error(err))
	}
}

// --- FetchPRsOp ---

const opFetchPRsData = "fetch_prs_data"

type FetchPRsOp struct {
	paginationCfg *config.BitbucketPaginationConfig
}

func (op *FetchPRsOp) OperationID() sdk.OperationID { return "fetch_prs" }

func (op *FetchPRsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	// One pacing budget for the whole run — shared across repo discovery, every
	// repo, and every enrichment call — so a local-limit rejection is waited out
	// (bounded by the run deadline and the per-run cap) instead of failing the run.
	// It must be a single budget for the run, not per-repo or per-call, to bound
	// the run's total wait, and it is created BEFORE repo discovery so the
	// discovery listing is paced too (a cold org's first call).
	budget := &pacing.PacingBudget{}

	repos, err := fetchAllRepos(ctx, params.VendorGateway, settings, op.OperationID(), params.Config.OrganizationID, budget)
	if err != nil {
		return nil, fmt.Errorf("fetching all repos: %w", err)
	}

	// The PR caches (first-commit + diffstat) are served from and written to
	// connector_fetch_cache. The in-memory maps start empty and are primed per
	// page from GetMany (see fetchPRsForRepo), so a stale settings blob is never
	// consulted; freshly-resolved entries accumulate in the dirty sets and flush
	// to the table (ADR-0004 §7 + §8).
	firstCommitCache := make(map[string]string, 64)
	prCommitSHACache := make(map[string]string, 64)
	diffstatCache := make(map[string]PRDiffstatCacheEntry, 64)

	// dirty sets accumulate only entries fetched/changed this run (the
	// eviction-safety invariant) — the exact cache misses fetchPRsForRepo just
	// resolved. Flushed to the table with one conditional upsert per cache_type.
	dirtyFirstCommit := make(map[string]string)
	dirtyPRCommitSHAs := make(map[string]string)
	dirtyDiffstat := make(map[string]PRDiffstatCacheEntry)
	defer putFetchCacheDirty(ctx, params.FetchCache, CacheTypePRFirstCommit, dirtyFirstCommit)
	defer putFetchCacheDirty(ctx, params.FetchCache, CacheTypePRCommitSHAs, dirtyPRCommitSHAs)
	defer putFetchCacheDirty(ctx, params.FetchCache, CacheTypePRDiffstat, dirtyDiffstat)

	// cursorStates is mutated from two sites: (a) the per-page checkpoint
	// closure created by makePageCheckpoint during fetchPRsForRepo, and
	// (b) the per-repo overwrite below after fetchPRsForRepo returns. Both
	// sites run on the same goroutine because the repo loop is sequential —
	// a future change that goroutine-izes the per-repo iteration MUST add a
	// mutex (or per-repo state copy) to keep the map race-free.
	cursorStates, droppedPinned := loadCursorStates(ctx, params.Cursor)

	// repoStateUpdates collects per-repo lifecycle transitions during the loop
	// (404 → deleted; auth/transient kinds fail the run instead). Persisted in a
	// single end-of-run settings write alongside the observed vendor rate-limit
	// cap so a deleted repo is durably skipped on the next run. Two
	// separate UpdateSettings calls cannot be used: both helpers read-modify-write
	// params.Config.Settings (the start-of-run snapshot), so whichever fires last
	// in LIFO defer order silently overwrites the other's fields. Merging into
	// one read-modify-write keeps both updates atomic against the snapshot. The PR
	// caches themselves live in connector_fetch_cache, not settings.
	repoStateUpdates := make(map[string]BitbucketDiscoveredRepo)
	defer persistFetchPRsSettings(ctx, params, bitbucketBucketToGroup, repoStateUpdates)

	// repoFailures preserves repo-encounter order so the aggregator's
	// `firstTypedSyncError` returns a deterministic wrapped cause across
	// identical re-runs (a map would surface a different error_kind /
	// RetryAfterSeconds each time when ≥2 repos fail with different kinds).
	repoFailures := make([]repoFailure, 0, len(repos))
	var ctxBreakErr error
	successfulRepos := 0

	for _, repoFullName := range repos {
		if err := ctx.Err(); err != nil {
			ctxBreakErr = err
			logger.Error(ctx, "fetch_prs: context done, breaking out of repo loop",
				zap.String("next_repo", repoFullName), zap.Error(ctxBreakErr))
			break
		}

		workspace, repoSlug, splitErr := splitFullName(repoFullName)
		if splitErr != nil {
			logger.Error(ctx, "skipping invalid repo", zap.String("repo", repoFullName), zap.Error(splitErr))
			continue
		}

		// Resume-aware setup: if the prior run was interrupted mid-pagination
		// for this repo, state.PageCursor is non-empty and state.Since is the
		// ORIGINAL `since` filter used at that iteration's start. Reuse both
		// so the newer Timestamp (which is just the page-1 max of the
		// interrupted run, NOT a safe lower bound) does not silently skip
		// older pages.
		//
		// A state with PageCursor set but Since==zero is malformed — no
		// writer in this package produces that, but a hand-crafted or
		// migrated cursor could. Using zero as the lower bound would disable
		// the loop's `pr.UpdatedOn.Before(since)` early-exit and walk the
		// repo's entire pagination history. Defensive fallback: treat such a
		// state as a fresh iteration starting at page 1 with the standard
		// since-from-Timestamp filter, dropping the (untrustworthy)
		// PageCursor on the floor.
		state := cursorStates[repoFullName]
		since := firstSyncSince(state.Timestamp)
		since = applyRepairFloor(since, state.HoldFloor)
		resumePage := ""
		if state.PageCursor != "" {
			switch {
			case state.Since.IsZero():
				// Malformed mid-resume state: see comment above. Log so the
				// fallback to fresh-iteration is observable in real use.
				logger.WarnWithCtx(ctx, "fetch_prs: dropping mid-pagination PageCursor with zero Since; treating as fresh iteration",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String("repo", repoFullName),
					zap.String("page_cursor", state.PageCursor))
			case !state.HoldFloor.IsZero():
				// A repair floor OUTRANKS the resume. The degraded PR that set the
				// floor can sit on a page the resume would skip — the failure path
				// persists the floor and the page checkpoint together, so a run that
				// degrades on page 1 and then fails on page 2 resumes AT page 2 and
				// never re-reads page 1. That run then finishes clean, reports no
				// degradation, and applyCursorHold clears the floor as if the PR had
				// been repaired: the silent false-clear this whole mechanism exists to
				// prevent.
				//
				// So restart at page 1. The lower bound stays the MINIMUM of the two —
				// dropping to the floor alone would strand the older pages
				// [state.Since, HoldFloor) that the interrupted run had not reached yet,
				// and a clean finish would then advance Timestamp past them for good.
				// holdWatermark is "keep the older of the two, ignoring zero" — exactly
				// the min needed here, and both values are non-zero in this branch.
				since = holdWatermark(state.Since, state.HoldFloor)
				logger.WarnWithCtx(ctx, "fetch_prs: repair floor set; restarting at page 1 instead of resuming mid-pagination",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String("repo", repoFullName),
					zap.String("dropped_page_cursor", state.PageCursor),
					zap.Time("repair_floor", state.HoldFloor),
					zap.Time("since", since))
			default:
				since = state.Since
				resumePage = state.PageCursor
			}
		}

		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, repoFullName, since)
		// priorWatermark guards against the resumed iteration regressing the
		// cursor below the interrupted run's page-1 max. Pass state.Timestamp
		// unconditionally — on a fresh run it's zero so the max-init in
		// fetchPRsForRepo collapses to `since`.
		latestUpdated, earliestDegraded, fetchErr := fetchPRsForRepo(ctx, params.VendorGateway, op.paginationCfg, params.Sink, op.OperationID(), params.Config.OrganizationID, workspace, repoSlug, repoFullName, since, state.Timestamp, resumePage, firstCommitCache, prCommitSHACache, diffstatCache, params.FetchCache, dirtyFirstCommit, dirtyPRCommitSHAs, dirtyDiffstat, onPage, budget)
		if fetchErr != nil {
			kind, httpStatus := classifyRepoError(fetchErr)
			logger.Error(ctx, "fetch_prs: skipping repo after fetch failure",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.String("repo", repoFullName),
				zap.Stringer("repo_err_kind", kind),
				zap.Int("http_status", httpStatus),
				zap.Error(fetchErr))
			repoFailures = append(repoFailures, repoFailure{name: repoFullName, err: fetchErr, kind: kind})
			// Stage diagnostics on EVERY per-repo failure — LastErrorAt /
			// LastHTTPStatus are operator-facing fields, not just
			// lifecycle transitions. Status is only set when the kind
			// warrants a transition (404 → DELETED today); other kinds
			// leave Status as UNSPECIFIED, and mergeRepoStateUpdates
			// preserves the existing Status in that case. The defer above
			// persists this even when the run fails so a "last 5xx was at
			// X" stamp survives across failed retries.
			newStatus, _ := repoStatusForKind(kind)
			repoStateUpdates[repoFullName] = BitbucketDiscoveredRepo{
				FullName:       repoFullName,
				Status:         newStatus,
				LastErrorAt:    time.Now().UTC().Format(time.RFC3339),
				LastHTTPStatus: httpStatus,
			}
			// Persist the hold even though the repo failed. Under a real throttle the
			// repo-level LIST call is itself rate-limited, so this is the COMMON path
			// — returning here without recording the hold would discard it in exactly
			// the regime it exists for, leaving the degraded PRs behind a cursor that
			// a later clean run advances past. Only the watermark is held back; the
			// mid-pagination PageCursor/Since the per-page checkpoint wrote stay as-is
			// so the interrupted iteration can still resume.
			// Seed from the RUN-START snapshot, not from whatever a per-page write
			// left, so the streak is counted once per run.
			held := cursorStates[repoFullName]
			held.HoldFloor, held.HoldCount, held.HoldReleasedAt = state.HoldFloor, state.HoldCount, state.HoldReleasedAt
			// A FAILED run carries no evidence that degradation cleared — it may have
			// died before looking at a single PR (a resumed run whose first LIST is
			// rate-limited). So never clear the floor here. If a floor is already set
			// and this run made no progress, re-assert it and count the attempt, so the
			// bound stays reachable instead of freezing the streak at 1 forever.
			candidate := earliestDegraded
			if candidate.IsZero() {
				candidate = held.HoldFloor
			}
			if !candidate.IsZero() && applyCursorHold(ctx, repoFullName, &held, candidate) {
				cursorStates[repoFullName] = held
				// Persist explicitly: the orchestrator sends a failed run to failRun
				// and never saves Execute's returned cursor, so without this write the
				// hold is lost in exactly the throttle regime it exists for — and a
				// run that failed before completing any page wrote nothing at all.
				if params.Checkpoint != nil {
					if cpErr := params.Checkpoint(ctx, buildStateCursor(ctx, cursorStates)); cpErr != nil {
						logger.Error(ctx, "fetch_prs: failed to persist cursor hold after repo failure",
							zap.String("repo", repoFullName), zap.Error(cpErr))
					}
				}
			}
			continue
		}

		successfulRepos++
		// Iteration finished cleanly — overwrite any mid-pagination
		// PageCursor/Since the per-page checkpoint may have written so the
		// next run starts fresh at page 1 with `since = latestUpdated`.
		// HoldCount is carried over from the prior state so a hold can be bounded
		// across runs; applyCursorHold resets it when nothing is degraded.
		next := repoCursorState{
			Timestamp:      latestUpdated,
			HoldFloor:      state.HoldFloor,
			HoldCount:      state.HoldCount,
			HoldReleasedAt: state.HoldReleasedAt,
		}
		applyCursorHold(ctx, repoFullName, &next, earliestDegraded)
		// A first sync that observed nothing records nothing (see
		// watermarkIsPinnedBeyondFloor): latestUpdated still equals the synthetic `since` floor, and persisting it would publish a
		// query bound as observed progress — the exact pin the cursor loaders drop,
		// re-minted. Delete rather than merely skip, so a mid-pagination entry a
		// per-page checkpoint wrote for this cleanly-finished iteration cannot linger.
		if state.Timestamp.IsZero() && latestUpdated.Equal(since) &&
			next.HoldFloor.IsZero() && next.HoldCount == 0 && next.HoldReleasedAt.IsZero() {
			delete(cursorStates, repoFullName)
			logger.Info(ctx, "fetched PRs; nothing observed, watermark left unset", zap.String("repo", repoFullName))
		} else {
			cursorStates[repoFullName] = next
			logger.Info(ctx, "fetched PRs", zap.String("repo", repoFullName), zap.Time("cursor", next.Timestamp))
		}

		if params.Checkpoint != nil {
			if cpErr := params.Checkpoint(ctx, buildStateCursor(ctx, cursorStates)); cpErr != nil {
				logger.Error(ctx, "fetch_prs: per-repo checkpoint failed (continuing)",
					zap.String("repo", repoFullName), zap.Error(cpErr))
			}
		}
	}

	// Any repo-level fetch failure or mid-iteration ctx exhaustion makes the
	// run a failure even if some repos succeeded — without this, a sync that
	// processed 2 of 7 repos would be indistinguishable from a clean full
	// completion in the connector_sync_runs table, and monitoring would miss
	// silent degradation. Per-repo Checkpoint calls during the loop already
	// persisted partial cursors to connector_sync_states, so returning an
	// error here does not lose progress — the next run still picks up from
	// the cursor for repos that completed.
	// droppedPinned defeats the nothing-to-write fallback: a cursor whose only
	// entries were dropped as pinned must persist as empty, not be resurrected.
	cursorOut := params.Cursor
	if len(cursorStates) > 0 || droppedPinned {
		cursorOut = buildStateCursor(ctx, cursorStates)
	}

	if len(repoFailures) > 0 {
		// Aggregate per-repo failures. NotFound is repo-level: state
		// transition is recorded in repoStateUpdates and the next sync
		// skips the repo, but the run as a whole succeeds. Auth /
		// Transient / Internal fail the run so asynq retries with the
		// appropriate backoff.
		//
		// failWorthyErrs is a separate slice scoped to ONLY the kinds that
		// fail the run. When firstTypedSyncError walks it, the SyncError
		// that propagates to the orchestrator carries the correct
		// error_kind + RetryAfterSeconds — without the filter, a
		// rate-limit storm on one repo plus a 404 on another could
		// surface the 404 as the wrapped cause, misclassifying the run as
		// not_found and dropping the rate-limit RetryAfter hint.
		//
		// Iterating repoFailures (slice) instead of repoErrors (map)
		// preserves repo-encounter order so the wrapped cause is
		// deterministic across identical re-runs.
		failWorthyErrs := make([]error, 0, len(repoFailures))
		var deletedRepos []string
		var forbiddenErrs []error
		for _, f := range repoFailures {
			switch f.kind {
			case repoErrNotFound:
				deletedRepos = append(deletedRepos, f.name)
			case repoErrForbidden:
				// One repository's 403 is that repository's failure: it is skipped
				// and keeps its cursor state. Only an all-403 run is the credential's.
				forbiddenErrs = append(forbiddenErrs, fmt.Errorf("%s: %w", f.name, f.err))
			case repoErrAuth, repoErrTransient, repoErrInternal:
				failWorthyErrs = append(failWorthyErrs, f.err)
			}
		}

		if shouldFailRun(repoFailures) {
			// firstTypedSyncError wraps a typed *sdk.SyncError when one is
			// available so the orchestrator's errors.As classification recovers
			// the correct error_kind (rate_limit / sink_failure / auth / ...).
			// Walking only failWorthyErrs guarantees the wrapped cause is the
			// one driving the run failure, not an unrelated 404.
			if successfulRepos == 0 {
				logger.Error(ctx, "fetch_prs: all attempted repos failed",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.Int("failed_repos", len(repoFailures)),
					zap.Int("total_repos", len(repos)))
				return cursorOut, fmt.Errorf("fetch_prs: all %d attempted repos failed: %w", len(repoFailures), firstTypedSyncError(failWorthyErrs))
			}
			logger.Error(ctx, "fetch_prs: partial repo-level failures (org-level error present)",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.Int("failed_repos", len(repoFailures)),
				zap.Int("successful_repos", successfulRepos),
				zap.Int("total_repos", len(repos)))
			return cursorOut, fmt.Errorf("fetch_prs: %d of %d repos failed (succeeded=%d): %w", len(repoFailures), len(repos), successfulRepos, firstTypedSyncError(failWorthyErrs))
		}

		// A loop cut short by its context has not reached every repository, so
		// an all-403 result so far does not show a dead credential.
		if successfulRepos == 0 && len(forbiddenErrs) == len(repoFailures) && ctxBreakErr == nil {
			return cursorOut, fmt.Errorf("fetch_prs: every repository answered 403 (%d): %w", len(forbiddenErrs), forbiddenErrs[0])
		}

		// Only repo-level failures (404, 403). Treat the run as successful for the
		// surviving repos; the failed ones are marked in repoStateUpdates and
		// the next FetchReposOp reconciliation pass will re-promote them if
		// they come back online.
		logger.WarnWithCtx(ctx, "fetch_prs: completed with repo-level failures (no run-level error)",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.Int("successful_repos", successfulRepos),
			zap.Int("total_repos", len(repos)),
			zap.Strings("deleted_repos", deletedRepos))
	}

	if ctxBreakErr != nil {
		// Loop bailed early on ctx exhaustion with zero repo errors — some
		// repos were never attempted, so we cannot claim a successful run.
		// Wrap ctxBreakErr (typically context.DeadlineExceeded) so the
		// orchestrator can classify this as ctx_exhausted in error_kind.
		processed := successfulRepos
		logger.Error(ctx, "fetch_prs: ctx exhausted mid-iteration; some repos not attempted",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.Int("repos_attempted", processed),
			zap.Int("total_repos", len(repos)))
		return cursorOut, fmt.Errorf("fetch_prs: ctx exhausted after %d/%d repos: %w", processed, len(repos), ctxBreakErr)
	}

	return cursorOut, nil
}

// repoErrorKind classifies a per-repo failure so the aggregator at run-level
// can decide whether to fail the whole sync or carry on. 404 and 403 are
// isolated to the repo, so one deleted or forbidden repo does not fail the
// rest of the workspace. Auth (401), transient, and internal errors are
// org-level and must surface as run failures so asynq retries with the
// appropriate backoff.
//
// `transient` is reserved for genuinely retry-worthy vendor-side failures
// (5xx, 429, timeout). Non-vendor-derived errors (config bugs, internal
// bugs, sink failures) get the `internal` label so an operator triaging
// `repo_err_kind=transient` in monitoring isn't misled into "wait it
// out" when the actual cause is a permanent code path failure.
type repoErrorKind int

const (
	repoErrTransient repoErrorKind = iota // 5xx, 429, transport, ctx-deadline — retry-worthy on the vendor side
	repoErrNotFound                       // 404
	repoErrAuth                           // 401: the credential itself
	repoErrForbidden                      // 403 on one repository: that repository's failure, not the credential's
	repoErrInternal                       // ConfigInvalid / Internal / InvalidInput / SinkFailure — not a vendor signal, won't fix itself by retrying
)

// repoFailure captures one per-repo failure observed during the fetch_prs
// loop. Recording these in a slice (rather than a map keyed by name)
// preserves repo-encounter order, so the aggregator's
// `firstTypedSyncError` returns a deterministic wrapped cause across
// identical re-runs — important because the orchestrator reads
// `error_kind` / `RetryAfterSeconds` off that wrapped cause to drive
// asynq's backoff.
type repoFailure struct {
	name string
	err  error
	kind repoErrorKind
}

// String returns a stable label for log fields so operators don't have to
// memorize the iota ordering. Logged via zap.Stringer at the per-repo
// failure site.
func (k repoErrorKind) String() string {
	switch k {
	case repoErrTransient:
		return "transient"
	case repoErrNotFound:
		return "not_found"
	case repoErrAuth:
		return "auth"
	case repoErrForbidden:
		return "forbidden"
	case repoErrInternal:
		return "internal"
	}
	return "unknown"
}

// classifyRepoError maps a per-repo fetch error to a repoErrorKind and the
// HTTP status (if any). Defaults to transient when the error doesn't carry a
// typed SyncError (conservative — we want the run to fail rather than
// silently mark a healthy repo deleted).
//
// vendorapi.ClassifyHTTPErrorWithHeaders has already mapped vendor status
// codes into typed Kinds (401/403 → ErrKindAuth, 429 → ErrKindRateLimit,
// 404 → ErrKindNotFound, 5xx → ErrKindVendorError), so this function works
// off Kind. A 401 is the credential's: the run fails with the Terminal auth
// error and the connector pauses. A 403 on one repository is that
// repository's failure (repoErrForbidden): the repository is skipped and keeps
// its cursor state, and the run fails as auth only when every attempted
// repository answered 403, since a token-wide 403 (a revoked app, 2FA or IP
// enforcement) answers 403 everywhere.
//
// 404 carries a similar ambiguity: Bitbucket returns 404 (not 403) for
// repos the token cannot access, as an anti-enumeration measure. A
// missing OAuth scope (e.g. `pullrequest:read` revoked while
// `repository:read` is intact) surfaces as a 404 here for every repo in
// the workspace — flipping them all to DELETED, only for fetch_repos to
// re-promote them six hours later off the workspace listing. We accept
// the cycle today because (a) it's recoverable once the operator fixes
// the consent, and (b) no workspace-aware aggregator compares per-repo
// failure patterns to the workspace-wide pattern, so an operator reads
// last_http_status=404 across all repos as the signal.
//
// Vendor-side transient (5xx / 429 / timeout / ctx-exhausted) and
// non-vendor internal (ConfigInvalid / Internal / InvalidInput /
// SinkFailure) both fail the run but carry distinct labels so operators
// can triage: `transient` waits out, `internal` needs a code fix.
func classifyRepoError(err error) (repoErrorKind, int) {
	var syncErr *sdk.SyncError
	if errors.As(err, &syncErr) {
		switch syncErr.Kind {
		case sdk.ErrKindAuth:
			status := syncErr.HTTPStatus
			if status == http.StatusForbidden {
				return repoErrForbidden, status
			}
			if status == 0 {
				status = http.StatusUnauthorized
			}
			return repoErrAuth, status
		case sdk.ErrKindRateLimit:
			status := syncErr.HTTPStatus
			if status == 0 {
				status = http.StatusTooManyRequests
			}
			return repoErrTransient, status
		case sdk.ErrKindNotFound:
			status := syncErr.HTTPStatus
			if status == 0 {
				status = http.StatusNotFound
			}
			return repoErrNotFound, status
		case sdk.ErrKindVendorError, sdk.ErrKindTimeout, sdk.ErrKindCtxExhausted:
			// Vendor-side or context-driven — retryable in principle.
			return repoErrTransient, syncErr.HTTPStatus
		case sdk.ErrKindConfigInvalid, sdk.ErrKindInternal, sdk.ErrKindInvalidInput, sdk.ErrKindSinkFailure:
			// Code / config / infra bugs — retry will keep failing until
			// a human fixes it. Surfacing as `internal` in the log field
			// lets an operator tell a transient failure from one that
			// needs a fix.
			return repoErrInternal, syncErr.HTTPStatus
		default:
			// New SyncErrorKind landed without this switch being updated —
			// treat conservatively as transient so the run fails and the
			// gap shows up in the next log review.
			return repoErrTransient, syncErr.HTTPStatus
		}
	}
	return repoErrTransient, 0
}

// shouldFailRun reports whether the aggregated per-repo failures warrant
// failing the whole sync run. 404 (deleted) alone doesn't fail the run —
// it's recorded against the repo and the run reports success for the other
// repos. Auth, transient, and internal failures DO fail the run so asynq
// retries with the appropriate backoff (rate-limit RetryAfter, exponential
// for 5xx; internal will keep failing until a human fixes the code/config).
func shouldFailRun(failures []repoFailure) bool {
	for _, f := range failures {
		if f.kind != repoErrNotFound && f.kind != repoErrForbidden {
			return true
		}
	}
	return false
}

// repoStatusForKind maps the classification to the Status value to record
// on BitbucketDiscoveredRepo. Auth + transient kinds don't transition the
// repo state — they fail the whole run instead.
func repoStatusForKind(k repoErrorKind) (bitbucketpb.BitbucketRepoStatus, bool) {
	if k == repoErrNotFound {
		return bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED, true
	}
	return bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_UNSPECIFIED, false
}

func firstTypedSyncError(errs []error) error {
	for _, candidate := range errs {
		var syncErr *sdk.SyncError
		if errors.As(candidate, &syncErr) {
			return candidate
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// seedCursorTimestamps decodes a composite cursor into a per-repo timestamp
// map. Repos that are not iterated in the current run keep their prior entry
// when the result is written back as the new cursor. A corrupt cursor blob is
// logged and treated as empty — the next run rebuilds from scratch.
//
//nolint:unused // used by fetch_prs_cursor_test.go
func seedCursorTimestamps(ctx context.Context, cursor *sdk.Cursor) map[string]time.Time {
	seeded := make(map[string]time.Time)
	if cursor == nil {
		return seeded
	}
	repoParts, parseErr := sdk.CompositeFromCursor(cursor)
	if parseErr != nil {
		logger.Error(ctx, "fetch_prs: input cursor unparseable, treating as empty", zap.Error(parseErr))
		return seeded
	}
	for repoFullName, rawTimestamp := range repoParts {
		var lastUpdated time.Time
		if unmarshalErr := json.Unmarshal(rawTimestamp, &lastUpdated); unmarshalErr != nil {
			logger.WarnWithCtx(ctx, "fetch_prs: dropping unparseable cursor timestamp; repo will re-fetch from lookback floor",
				zap.String("repo", repoFullName), zap.Error(unmarshalErr))
			continue
		}
		seeded[repoFullName] = lastUpdated
	}
	return seeded
}

// bitbucketBucketToGroup maps BudgetLimiter bucket identifiers to the
// gateway/defaults.yaml group name whose cap they represent. The
// persist helper walks this map; the gateway factory consumes the same
// keys when merging yml defaults with per-group observations.
var bitbucketBucketToGroup = map[ratelimiter.Bucket]string{
	ratelimiter.BucketDefault: "bitbucket-rest",
}

// persistFetchPRsSettings flushes the observed vendor rate-limit cap and
// the per-repo lifecycle updates into the connector_config Settings JSON in
// a single read-modify-write. A single write is required: two separate defers
// against the same start-of-run snapshot would clobber each other in LIFO
// order. Failure is non-fatal — a missed write just means the next run falls back to
// the yml rate floor and re-stamps repo lifecycle. The PR caches (first-commit +
// diffstat) live in connector_fetch_cache, not settings.
//
// IMPORTANT: re-reads params.Config.Settings rather than trusting the
// start-of-run snapshot. fetch_prs runs for many minutes; while it runs,
// fetch_repos / fetch_workspace_members can call UpdateSettings on the same
// row (DiscoveredRepos refresh, group enrichment, etc.). Writing a marshalled
// start-of-run snapshot back would clobber those concurrent updates. Mutating
// only the cache and cap fields keeps everything else intact — the cheapest
// serialisation point without introducing row-level locks or a dedicated
// UpdatePRCaches RPC.
func persistFetchPRsSettings(
	ctx context.Context,
	params *sdk.ExecutionParams,
	bucketToGroup map[ratelimiter.Bucket]string,
	repoStateUpdates map[string]BitbucketDiscoveredRepo,
) {
	if params.ConfigUpdater == nil {
		return
	}

	err := params.ConfigUpdater.PatchSettings(ctx, params.Config.OrganizationID, params.Config.ConnectorID, func(current json.RawMessage) (json.RawMessage, error) {
		var live BitbucketSettings
		if len(current) > 0 {
			if err := json.Unmarshal(current, &live); err != nil {
				return nil, fmt.Errorf("parse live settings: %w", err)
			}
		}

		mergedLimits, capWritten := pacing.MergeVendorRateLimits(
			ctx, params.BudgetLimiter, params.Config.OrganizationID, bucketToGroup, live.VendorRateLimits,
		)
		needStateWrite := len(repoStateUpdates) > 0
		if !capWritten && !needStateWrite {
			// Returning the live bytes unchanged is a cheap no-op for the DAO
			// to detect (Updates with the same value is still cheaper than a
			// follow-up UPDATE on the row).
			return current, nil
		}

		if capWritten {
			live.VendorRateLimits = mergedLimits
		}
		if needStateWrite {
			live.DiscoveredRepos = mergeRepoStateUpdates(live.DiscoveredRepos, repoStateUpdates)
		}
		return json.Marshal(live)
	})
	if err != nil {
		logger.WarnWithCtx(ctx, "persist fetch_prs settings", zap.Error(err))
	}
}

// mergeRepoStateUpdates applies per-repo lifecycle transitions and
// diagnostic stamps to the live DiscoveredRepos slice from the start-of-run
// snapshot. Entries that exist in `live` are mutated in place; other fields
// (DefaultBranch, Private, LastSeenAt) are preserved.
//
// Status semantics: an update with Status=UNSPECIFIED is a diagnostic-only
// update (e.g. auth or transient failure — we want LastErrorAt /
// LastHTTPStatus stamped so operators can see "what last broke this repo"
// but we do NOT transition Status because the run will fail and asynq
// will retry). Only updates that explicitly carry a non-UNSPECIFIED
// Status overwrite the lifecycle state. Today that's exclusively DELETED
// (from a 404), but the same shape extends to any future lifecycle value.
//
// Updates for repos that do NOT yet appear in `live` are intentionally
// dropped. Appending them would create a cache that contains only failed
// repos, which would then cause `fetchAllRepos` to return an empty list on
// the next run (because its non-active filter excludes deleted entries) —
// blocking discovery until the next `fetch_repos` tick. The
// canonical population of DiscoveredRepos is `fetch_repos`'s
// reconciliation pass; this helper only updates already-known entries.
func mergeRepoStateUpdates(live []BitbucketDiscoveredRepo, updates map[string]BitbucketDiscoveredRepo) []BitbucketDiscoveredRepo {
	if len(updates) == 0 {
		return live
	}
	index := make(map[string]int, len(live))
	for i, r := range live {
		index[r.FullName] = i
	}
	for fullName, update := range updates {
		idx, ok := index[fullName]
		if !ok {
			// Skip: see comment above. fetch_repos owns the canonical
			// list — repos not yet in DiscoveredRepos haven't been
			// discovered. Persisting a failure-only entry here would
			// block future discovery via fetchAllRepos' non-active
			// filter.
			continue
		}
		// Diagnostic stamps are always overwritten — they represent the
		// most-recent observation for this repo.
		live[idx].LastErrorAt = update.LastErrorAt
		live[idx].LastHTTPStatus = update.LastHTTPStatus
		// Status transitions only when the update explicitly carries a
		// new lifecycle value. UNSPECIFIED on the update means
		// "diagnostic-only, preserve existing Status".
		if update.Status != bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_UNSPECIFIED {
			live[idx].Status = update.Status
		}
	}
	return live
}

// Defaults for diffstat pagination on both `/pullrequests/{id}/diffstat`
// and `/repositories/.../diffstat/{sha}`. Both endpoints share the same
// page shape so a single pagelen + max-page cap suffices. Service-level
// overrides come from `config.BitbucketPaginationConfig`; operator can dial each
// op independently by setting `Operations["fetch_pr_diffstat"].PageSize`
// or `Operations["fetch_commit_diffstat"].PageSize`.
const (
	diffstatPagelen  = 500
	maxDiffstatPages = 200
)

// maxListPages bounds the page-number list walks (workspace repos and members,
// PR activity and comments). 1000 pages is far beyond any real collection at
// these pagelens; reaching it signals a malformed `next` chain, so the walk
// fails rather than paging until the run's deadline — the same guard
// maxBranchPages and maxDiffstatPages apply.
const maxListPages = 1000

// errListPageCap is the error a page-number list walk returns at maxListPages.
func errListPageCap(what string) error {
	return fmt.Errorf("%s exceeded %d pages, possibly malformed next chain", what, maxListPages)
}

// Op IDs used as map keys in `BitbucketPaginationConfig.Operations`.
const (
	opPaginationPRDiffstat     = "fetch_pr_diffstat"
	opPaginationCommitDiffstat = "fetch_commit_diffstat"
)

// resolveDiffstatPagination returns the effective pagelen + max-pages for a
// diffstat enrichment loop, layering per-op YAML overrides on top of the
// package defaults. Nil-safe: returns defaults for every nil pointer level
// (cfg, cfg.Operations, op entry, individual fields).
func resolveDiffstatPagination(cfg *config.BitbucketPaginationConfig, opKey string) (pagelen, maxPages int32) {
	pagelen = diffstatPagelen
	maxPages = maxDiffstatPages
	if cfg == nil {
		return
	}
	if v := cfg.MaxDiffstatPages; v > 0 && v <= math.MaxInt32 {
		maxPages = int32(v)
	}
	if cfg.Operations == nil {
		return
	}
	opCfg, ok := cfg.Operations[opKey]
	if !ok || opCfg == nil {
		return
	}
	if v := opCfg.PageSize; v > 0 && v <= math.MaxInt32 {
		pagelen = int32(v)
	}
	return
}

// injectPRCounts adds `files_changed` and `reviews_requested` top-level
// integer fields onto the enriched PR payload so the downstream parser can
// read them out of the same shape as `additions` / `deletions`. Falls back
// to the input payload on unmarshal failure (preserves at-least-once emit).
func injectPRCounts(prRaw json.RawMessage, filesChanged, reviewsRequested int) json.RawMessage {
	var payloadMap map[string]json.RawMessage
	if err := json.Unmarshal(prRaw, &payloadMap); err != nil {
		logger.Warn("fetch_prs: PR payload unparseable for count injection; emitting un-enriched payload",
			zap.Error(err))
		return prRaw
	}
	// `json.Marshal` on a Go `int` cannot fail (encoding/json:intEncoder).
	// Use strconv.AppendInt to skip the marshal round-trip + lint dance.
	filesBytes := strconv.AppendInt(nil, int64(filesChanged), 10)
	reviewersBytes := strconv.AppendInt(nil, int64(reviewsRequested), 10)
	payloadMap["files_changed"] = filesBytes
	payloadMap["reviews_requested"] = reviewersBytes
	enriched, err := json.Marshal(payloadMap)
	if err != nil {
		logger.Warn("fetch_prs: failed to re-marshal PR payload after count injection; emitting un-enriched payload",
			zap.Error(err))
		return prRaw
	}
	return enriched
}

// countReviewersRequested derives the number of explicitly-requested
// reviewers from a PR list entry. Prefers the `reviewers[]` array; falls
// back to `participants[]` filtered to role="REVIEWER" when the explicit
// list is absent (some Bitbucket orgs only populate participants).
func countReviewersRequested(pr bbPullRequest) int {
	if n := len(pr.Reviewers); n > 0 {
		return n
	}
	count := 0
	for _, p := range pr.Participants {
		if p.Role == "REVIEWER" {
			count++
		}
	}
	return count
}

// diffstatEntryPath is the path a diffstat entry counts under: the new path,
// or the old path for a deletion. A rename counts once, under its new path.
func diffstatEntryPath(entry bbDiffstatEntry) string {
	if entry.New != nil && entry.New.Path != "" {
		return entry.New.Path
	}
	if entry.Old != nil && entry.Old.Path != "" {
		return entry.Old.Path
	}
	return ""
}

// countFilePathsInJSON returns the number of distinct paths in a JSON-encoded
// []string of file paths, from which files_changed is derived without
// re-fetching the diffstat. Returns 0 for empty / malformed inputs — never errors.
func countFilePathsInJSON(s string) int {
	if s == "" {
		return 0
	}
	var paths []string
	if err := json.Unmarshal([]byte(s), &paths); err != nil {
		logger.Warn("fetch_prs: file-paths JSON unparseable; counting as zero files_changed",
			zap.Error(err))
		return 0
	}
	distinct := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		distinct[p] = struct{}{}
	}
	return len(distinct)
}

// enrichCommitResult is the aggregate diffstat for a single commit. Unlike
// enrichPRResult, we do not retain per-file breakdowns because the commit
// metric surface is aggregate-only (lines_added / lines_deleted /
// files_changed).
type enrichCommitResult struct {
	Payload      json.RawMessage
	Additions    int
	Deletions    int
	FilesChanged int
}

// injectCommitLineCounts injects aggregate additions/deletions/files_changed
// onto the raw commit payload so a downstream consumer reads them from
// the same shape as a cache-hit emission.
func injectCommitLineCounts(commitRaw json.RawMessage, additions, deletions, filesChanged int) json.RawMessage {
	var payloadMap map[string]json.RawMessage
	if err := json.Unmarshal(commitRaw, &payloadMap); err != nil {
		logger.Warn("fetch_commits: commit payload unparseable for line-count injection; emitting un-enriched payload",
			zap.Error(err))
		return commitRaw
	}
	// `json.Marshal` on a Go `int` cannot fail (encoding/json:intEncoder).
	// strconv.AppendInt skips the marshal round-trip + errcheck noise.
	addedBytes := strconv.AppendInt(nil, int64(additions), 10)
	deletedBytes := strconv.AppendInt(nil, int64(deletions), 10)
	filesBytes := strconv.AppendInt(nil, int64(filesChanged), 10)
	payloadMap["additions"] = addedBytes
	payloadMap["deletions"] = deletedBytes
	payloadMap["files_changed"] = filesBytes
	enriched, err := json.Marshal(payloadMap)
	if err != nil {
		logger.Warn("fetch_commits: failed to re-marshal commit payload after line-count injection; emitting un-enriched payload",
			zap.Error(err))
		return commitRaw
	}
	return enriched
}

// enrichCommit calls `/diffstat/{sha}` and returns aggregate adds/dels/files.
// On error it returns the original payload + zero aggregates and the error;
// the caller decides (commitEnrichmentStops) whether the commit may still be
// emitted without stats. Bounded by maxDiffstatPages to keep a pathological
// `next` URL from running the sync indefinitely.
//
// Paced against the run budget like the commit-list walk: a commit emitted
// without stats is never repaired, because the cursor moves past it, so our own
// limiter's rejection is waited out rather than degraded to zero line counts.
func enrichCommit(ctx context.Context, gw sdk.VendorGateway, paginationCfg *config.BitbucketPaginationConfig, workspace, repoSlug, sha string, commitRaw json.RawMessage, budget *pacing.PacingBudget, opID sdk.OperationID, orgID string) (enrichCommitResult, error) {
	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	pagelen, maxPages := resolveDiffstatPagination(paginationCfg, opPaginationCommitDiffstat)
	if budget == nil {
		budget = &pacing.PacingBudget{}
	}

	var totalAdded, totalRemoved int
	// files_changed counts distinct paths, as fetch_prs does.
	paths := map[string]struct{}{}
	for page := int32(1); page <= maxPages; page++ {
		protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketGetCommitDiffstatRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			CommitSha: sha,
			Page:      page,
			Pagelen:   pagelen,
		}, budget, connectorID, opID, orgID)
		if err != nil {
			return enrichCommitResult{Payload: commitRaw}, fmt.Errorf("fetching commit diffstat (page %d): %w", page, err)
		}
		resp, ok := protoResp.(*connectorpb.BitbucketGetCommitDiffstatResponse)
		if !ok {
			return enrichCommitResult{Payload: commitRaw}, fmt.Errorf("unexpected response type %T", protoResp)
		}
		for _, entryRaw := range resp.GetDiffstatEntries() {
			var entry bbDiffstatEntry
			if err := json.Unmarshal(entryRaw, &entry); err != nil {
				logger.WarnWithCtx(ctx, "fetch_commits: skipping unparseable commit diffstat entry",
					zap.String("repo", workspace+"/"+repoSlug), zap.String("sha", sha), zap.Error(err))
				continue
			}
			totalAdded += entry.LinesAdded
			totalRemoved += entry.LinesRemoved
			if path := diffstatEntryPath(entry); path != "" {
				paths[path] = struct{}{}
			}
		}
		if resp.GetNextPageUrl() == "" {
			break
		}
		if page == maxPages {
			logger.WarnWithCtx(ctx, "commit diffstat exceeded max pages; aggregates truncated",
				zap.String("repo", workspace+"/"+repoSlug), zap.String("sha", sha), zap.Int32("pages_walked", page))
		}
	}

	totalFiles := len(paths)
	enriched := injectCommitLineCounts(commitRaw, totalAdded, totalRemoved, totalFiles)
	return enrichCommitResult{
		Payload:      enriched,
		Additions:    totalAdded,
		Deletions:    totalRemoved,
		FilesChanged: totalFiles,
	}, nil
}

// commitEnrichmentStops reports whether a failed diffstat fetch must stop the
// walk at its commit instead of emitting the commit without stats. Anything a
// later attempt can fix stops it — our limiter after pacing gave up, a vendor
// 429, a 5xx or network failure, an open circuit, an auth failure, the run's
// context ending — because the cursor would otherwise move past the commit for
// good. A failure that no retry can fix (a 404 for one commit, a malformed
// response) keeps the best-effort emit.
func commitEnrichmentStops(err error) bool {
	if repotally.StopsTheRun(err) {
		return true
	}
	var syncErr *sdk.SyncError
	return errors.As(err, &syncErr) && syncErr.Retryable
}

// persistFetchCommitsSettings flushes the observed vendor rate-limit cap
// into ConnectorConfig.Settings in a single read-modify-write. Mirrors
// persistFetchPRsSettings — same concurrent-write-safety reasoning. Failure is
// non-fatal: a missed write just means the next run falls back to the yml rate
// floor. The per-sha commit diffstat cache lives in connector_fetch_cache, not
// settings.
func persistFetchCommitsSettings(
	ctx context.Context,
	params *sdk.ExecutionParams,
	bucketToGroup map[ratelimiter.Bucket]string,
) {
	if params.ConfigUpdater == nil {
		return
	}

	err := params.ConfigUpdater.PatchSettings(ctx, params.Config.OrganizationID, params.Config.ConnectorID, func(current json.RawMessage) (json.RawMessage, error) {
		var live BitbucketSettings
		if len(current) > 0 {
			if err := json.Unmarshal(current, &live); err != nil {
				return nil, fmt.Errorf("parse live settings: %w", err)
			}
		}

		mergedLimits, capWritten := pacing.MergeVendorRateLimits(
			ctx, params.BudgetLimiter, params.Config.OrganizationID, bucketToGroup, live.VendorRateLimits,
		)
		if !capWritten {
			return current, nil
		}
		live.VendorRateLimits = mergedLimits
		return json.Marshal(live)
	})
	if err != nil {
		logger.WarnWithCtx(ctx, "persist fetch_commits settings", zap.Error(err))
	}
}

// enrichPRResult holds the enriched payload and per-file metadata derived from the diffstat.
type enrichPRResult struct {
	Payload      json.RawMessage
	FilesStr     string // JSON-encoded []string of file paths for files_changed metadata
	FilesStatStr string // JSON-encoded []bbPRFileLineStat for files_line_stats metadata
	Additions    int    // aggregate lines added — must be injected into payload as "additions"
	Deletions    int    // aggregate lines removed — must be injected into payload as "deletions"
	// Degraded marks aggregates that are INCOMPLETE even though enrichPR returned a
	// nil error: a diffstat entry that would not parse was skipped, or pagination hit
	// its page cap. It keeps the entry OUT of connector_fetch_cache — without that a
	// final-state cache HIT would re-serve the under-count on every later fetch, so
	// even a code fix to the parser or the page cap could never take effect.
	//
	// It deliberately does NOT hold the cursor. Both causes are deterministic, so a
	// re-fetch reproduces them exactly; retrying would burn the hold budget on
	// something no retry can fix.
	Degraded bool
}

// injectLineCounts merges the aggregate additions/deletions into a raw PR
// JSON payload. Used both by fresh enrichment and by the diffstat-cache hit
// path so cached PRs emit the same shape as freshly-enriched ones. Falls
// back to the input payload on unmarshal failure — preserves at-least-once
// emit semantics rather than dropping the record.
func injectLineCounts(prRaw json.RawMessage, additions, deletions int) json.RawMessage {
	var payloadMap map[string]json.RawMessage
	if err := json.Unmarshal(prRaw, &payloadMap); err != nil {
		logger.Warn("fetch_prs: PR payload unparseable for line-count injection; emitting un-enriched payload",
			zap.Error(err))
		return prRaw
	}
	// `json.Marshal` on a Go `int` cannot fail (encoding/json:intEncoder).
	addedBytes := strconv.AppendInt(nil, int64(additions), 10)
	removedBytes := strconv.AppendInt(nil, int64(deletions), 10)
	payloadMap["additions"] = addedBytes
	payloadMap["deletions"] = removedBytes
	enriched, err := json.Marshal(payloadMap)
	if err != nil {
		logger.Warn("fetch_prs: failed to re-marshal PR payload after line-count injection; emitting un-enriched payload",
			zap.Error(err))
		return prRaw
	}
	return enriched
}

// enrichPR fetches the diffstat for a PR and returns:
//   - a payload enriched with aggregate additions/deletions
//   - per-file path list and per-file line stats for metadata
//
// If enrichment fails (e.g. rate limit or API error), the original payload is returned
// with empty file metadata strings. Walks every page of the Bitbucket diffstat
// API (capped at maxDiffstatPages) so PRs with more than `diffstatPagelen`
// changed files still produce accurate aggregates.
func enrichPR(ctx context.Context, gw sdk.VendorGateway, paginationCfg *config.BitbucketPaginationConfig, workspace, repoSlug string, prID int, prRaw json.RawMessage, orgID string, opID sdk.OperationID, budget *pacing.PacingBudget) (enrichPRResult, error) {
	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	pagelen, maxPages := resolveDiffstatPagination(paginationCfg, opPaginationPRDiffstat)

	var totalAdded, totalRemoved int
	// degraded tracks an INCOMPLETE aggregate on an otherwise-successful call — see
	// enrichPRResult.Degraded.
	degraded := false
	filePaths := []string{}
	fileStats := []bbPRFileLineStat{}
	// A path can appear in more than one diffstat entry; files_changed counts
	// distinct paths, so a repeat adds its lines to the path's first entry.
	statIndex := map[string]int{}

	for page := int32(1); page <= maxPages; page++ {
		protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketGetPRDiffstatRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			PrId:      int64(prID),
			Page:      page,
			Pagelen:   pagelen,
		}, budget, connectorID, opID, orgID)
		if err != nil {
			return enrichPRResult{Payload: prRaw, FilesStr: "[]", FilesStatStr: "[]"},
				fmt.Errorf("fetching diffstat (page %d): %w", page, err)
		}
		resp, ok := protoResp.(*connectorpb.BitbucketGetPRDiffstatResponse)
		if !ok {
			return enrichPRResult{Payload: prRaw, FilesStr: "[]", FilesStatStr: "[]"},
				fmt.Errorf("unexpected response type %T", protoResp)
		}
		for _, entryRaw := range resp.GetDiffstatEntries() {
			var entry bbDiffstatEntry
			if err := json.Unmarshal(entryRaw, &entry); err != nil {
				logger.WarnWithCtx(ctx, "fetch_prs: skipping unparseable PR diffstat entry",
					zap.String("repo", workspace+"/"+repoSlug), zap.Int("pr_id", prID), zap.Error(err))
				degraded = true
				continue
			}
			totalAdded += entry.LinesAdded
			totalRemoved += entry.LinesRemoved

			// Use new path for added/modified files; fall back to old path for deletions.
			path := diffstatEntryPath(entry)
			if i, seen := statIndex[path]; seen {
				fileStats[i].Additions += entry.LinesAdded
				fileStats[i].Deletions += entry.LinesRemoved
			} else if path != "" {
				statIndex[path] = len(fileStats)
				filePaths = append(filePaths, path)
				fileStats = append(fileStats, bbPRFileLineStat{
					Path:      path,
					Additions: entry.LinesAdded,
					Deletions: entry.LinesRemoved,
				})
			}
		}
		if resp.GetNextPageUrl() == "" {
			break
		}
		if page == maxPages {
			logger.WarnWithCtx(ctx, "PR diffstat exceeded max pages; aggregates truncated",
				zap.String("repo", workspace+"/"+repoSlug), zap.Int("pr_id", prID), zap.Int32("pages_walked", page))
			degraded = true
		}
	}

	enriched := injectLineCounts(prRaw, totalAdded, totalRemoved)

	filesJSON, err := json.Marshal(filePaths)
	if err != nil {
		return enrichPRResult{Payload: enriched, FilesStr: "[]", FilesStatStr: "[]", Additions: totalAdded, Deletions: totalRemoved},
			fmt.Errorf("marshaling file paths: %w", err)
	}
	fileStatsJSON, err := json.Marshal(fileStats)
	if err != nil {
		return enrichPRResult{Payload: enriched, FilesStr: "[]", FilesStatStr: "[]", Additions: totalAdded, Deletions: totalRemoved},
			fmt.Errorf("marshaling file line stats: %w", err)
	}
	return enrichPRResult{
		Payload:      enriched,
		FilesStr:     string(filesJSON),
		FilesStatStr: string(fileStatsJSON),
		Additions:    totalAdded,
		Deletions:    totalRemoved,
		Degraded:     degraded,
	}, nil
}

// fetchPRsForRepo fetches all PRs for a single repo since the given timestamp.
//
// firstCommitCache memoises first_commit_at across runs (ADR-0004 §7).
// diffstatCache memoises /diffstat results for PRs in a final state
// (ADR-0004 §8). Both maps are mutated in place; the caller persists them.
// Either may be nil for tests / ad-hoc invocations — the function falls back
// to a live API call on miss.
//
// resumePage, priorWatermark, and onPage support mid-pagination resume:
//   - resumePage, when non-empty and parseable as a positive integer, names
//     the page number to start at instead of 1. Use the value already
//     persisted via onPage. The caller MUST also pass the matching `since`
//     value, otherwise pages newer than the recorded watermark could be
//     re-fetched (mostly harmless) or older pages could be early-exited
//     (which this mechanism prevents).
//   - priorWatermark, when non-zero, is the high-water mark recorded by the
//     interrupted iteration (typically state.Timestamp = page-1 max). The
//     resumed iteration walks OLDER pages whose `updated_on` values cannot
//     advance past it, so without this floor the returned cursor would
//     REGRESS below the persisted watermark — causing the next
//     run to re-fetch the page-1 time window already ingested. Initializing
//     latestUpdated to max(since, priorWatermark) prevents the regression.
//   - onPage, when non-nil, is invoked after each fully-processed page with
//     the current high-water mark and the NEXT page number to fetch (empty
//     string when the iteration just completed its final page or hit the
//     early-exit boundary, so the caller can clear PageCursor).
//
// prWithRaw pairs a parsed PR with its raw payload so a page can be unmarshaled
// once (to batch the fetch-cache prime) and still emit the original bytes.
type prWithRaw struct {
	pr  bbPullRequest
	raw json.RawMessage
}

// Fetch-cache wiring: firstCommitCache / diffstatCache
// are primed per page from connector_fetch_cache (fc); on a cache miss the
// freshly-fetched entry is recorded into the dirty maps for the end-of-run PutMany.
func fetchPRsForRepo(ctx context.Context, gw sdk.VendorGateway, paginationCfg *config.BitbucketPaginationConfig, sink sdk.DataSink, opID sdk.OperationID, orgID, workspace, repoSlug, repoFullName string, since time.Time, priorWatermark time.Time, resumePage string, firstCommitCache map[string]string, prCommitSHACache map[string]string, diffstatCache map[string]PRDiffstatCacheEntry, fc sdk.FetchCache, dirtyFirstCommit map[string]string, dirtyPRCommitSHAs map[string]string, dirtyDiffstat map[string]PRDiffstatCacheEntry, onPage pageCheckpointFn, budget *pacing.PacingBudget) (latestUpdatedOut, earliestDegradedOut time.Time, err error) {
	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	latestUpdated := since
	if priorWatermark.After(latestUpdated) {
		latestUpdated = priorWatermark
	}
	// earliestDegraded is the oldest updated_on among PRs whose enrichment
	// (diffstat or first-commit) could not be resolved this iteration. Those PRs
	// are still emitted, but the returned watermark is held at or below this
	// value so the next run re-fetches and repairs them — see applyRepairFloor.
	var earliestDegraded time.Time
	// Keyed by PR id, not incremented per failure: one PR can fail BOTH the diffstat
	// and the first-commit fetch (the common case under a throttle, since both hit
	// the same exhausted bucket), and an operator reads this count to size a repair.
	degradedPRIDs := make(map[int]struct{})
	page := int32(1)
	if resumePage != "" {
		// ParseInt with bitSize=32 already constrains the parsed value to the
		// int32 range so the cast cannot overflow — the comparison with 0 is
		// the only remaining business-rule guard (page numbers are 1-indexed).
		parsed, parseErr := strconv.ParseInt(resumePage, 10, 32)
		switch {
		case parseErr != nil:
			logger.WarnWithCtx(ctx, "fetch_prs: unparseable resume page cursor; restarting repo at page 1",
				zap.String("repo", repoFullName), zap.String("resume_page", resumePage), zap.Error(parseErr))
		case parsed > 0:
			page = int32(parsed)
		}
	}

	for {
		if page > maxListPages {
			return time.Time{}, earliestDegraded, errListPageCap("fetch PRs for " + repoFullName)
		}
		protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketListPullRequestsRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			Page:      page,
			Pagelen:   50,
		}, budget, connectorID, opID, orgID)
		if err != nil {
			return time.Time{}, earliestDegraded, fmt.Errorf("fetch PRs for %s: %w", repoFullName, err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListPullRequestsResponse)
		if !ok {
			return time.Time{}, earliestDegraded, fmt.Errorf("unexpected response type %T", protoResp)
		}

		// Pre-parse the page so we can batch one GetMany per cache_type for the
		// page's keys before serving the per-PR reads.
		pagePRs := make([]prWithRaw, 0, len(resp.GetPullRequests()))
		for _, prRaw := range resp.GetPullRequests() {
			var pr bbPullRequest
			if err := json.Unmarshal(prRaw, &pr); err != nil {
				return time.Time{}, earliestDegraded, fmt.Errorf("unmarshal PR for %s: %w", repoFullName, err)
			}
			pagePRs = append(pagePRs, prWithRaw{pr: pr, raw: prRaw})
		}
		firstCommitKeys := make([]string, 0, len(pagePRs))
		diffstatKeys := make([]string, 0, len(pagePRs))
		for _, p := range pagePRs {
			if !since.IsZero() && p.pr.UpdatedOn.Before(since) {
				break
			}
			key := fmt.Sprintf("%s/%s#%d", workspace, repoSlug, p.pr.ID)
			firstCommitKeys = append(firstCommitKeys, key)
			if isFinalPRState(p.pr.State) {
				diffstatKeys = append(diffstatKeys, key)
			}
		}
		primeFetchCache(ctx, fc, CacheTypePRFirstCommit, firstCommitKeys, firstCommitCache)
		// Same key set as first_commit_at: both are resolved by the one chained call.
		primeFetchCache(ctx, fc, CacheTypePRCommitSHAs, firstCommitKeys, prCommitSHACache)
		primeFetchCache(ctx, fc, CacheTypePRDiffstat, diffstatKeys, diffstatCache)

		done := false
		for _, p := range pagePRs {
			pr := p.pr
			prRaw := p.raw

			// Stop if we've reached PRs older than our cursor.
			if !since.IsZero() && pr.UpdatedOn.Before(since) {
				done = true
				break
			}

			// Enrich the PR payload with diffstat (additions/deletions, per-file
			// stats). ADR-0004 §8: when a PR is in a final state and we've
			// already enriched it once, the diff is immutable — skip the
			// /diffstat API call and reuse the cached values. The cache is
			// keyed by "{workspace}/{repoSlug}#{prID}" and lives in
			// connector_fetch_cache (cache_type pr_diffstat), served/written via fc.
			//
			// "Final state" means MERGED, DECLINED, or SUPERSEDED. OPEN PRs
			// can still gain commits, so we always re-enrich them.
			cacheKey := fmt.Sprintf("%s/%s#%d", workspace, repoSlug, pr.ID)
			var enriched enrichPRResult
			var enrichErr error
			cacheHit := false
			if isFinalPRState(pr.State) {
				if cached, ok := diffstatCache[cacheKey]; ok {
					cacheHit = true
					// Re-inject additions/deletions into the payload — the
					// downstream Bitbucket consumer reads them from there,
					// not from
					// the metadata fields below. Cached aggregates come from
					// PRDiffstatCacheEntry, populated on the first enrichment.
					enriched = enrichPRResult{
						Payload:      injectLineCounts(prRaw, cached.Additions, cached.Deletions),
						FilesStr:     cached.FilesStr,
						FilesStatStr: cached.FilesStatStr,
						Additions:    cached.Additions,
						Deletions:    cached.Deletions,
					}
				}
			}
			if !cacheHit {
				enriched, enrichErr = enrichPR(ctx, gw, paginationCfg, workspace, repoSlug, pr.ID, prRaw, orgID, opID, budget)
				if enrichErr != nil {
					logger.WarnWithCtx(ctx, "failed to enrich PR file metadata, using empty defaults",
						zap.String("repo", repoFullName), zap.Int("pr", pr.ID), zap.Error(enrichErr))
					// The record still goes out with empty defaults (see
					// TestFetchPRsOp_EnrichmentFailureRetainsOriginalPayload) — a PR
					// present with zeroed stats beats a PR missing entirely. But
					// those zeros are indistinguishable from a genuinely empty PR,
					// so the cursor must NOT move past this PR: hold the watermark
					// and the next run re-enriches it. Only for retry-worthy kinds
					// — a permanent per-PR failure would otherwise pin the repo and
					// re-walk it on every tick, forever.
					if isRetryableEnrichFailure(enrichErr) {
						earliestDegraded = holdWatermark(earliestDegraded, pr.UpdatedOn)
						degradedPRIDs[pr.ID] = struct{}{}
					}
				}
				// An incomplete-but-nil-error aggregate is under-counted, but it does NOT
				// hold the cursor. Both causes — an unparseable diffstat entry, and
				// diffstat pagination hitting maxDiffstatPages — are DETERMINISTIC: a
				// re-fetch returns the identical payload and degrades identically. Holding
				// would replay the window every tick until the bound gives up, then raise
				// an Error that no retry can clear. Same reason
				// isRetryableEnrichFailure refuses untyped errors: only a vendor-side
				// transient earns a retry. The warn is the signal; fixing it needs a code
				// change (a wider page cap, or a parser that handles the entry).
				if enrichErr == nil && enriched.Degraded {
					logger.WarnWithCtx(ctx, "fetch_prs: PR aggregates incomplete despite a successful enrichment; not cached, and not retryable by re-fetching",
						zap.String("repo", repoFullName), zap.Int("pr", pr.ID))
				}
				// Both guards are load-bearing. enrichErr: a failed enrichment yields
				// zeroed aggregates. Degraded: an aggregate that is INCOMPLETE despite a
				// nil error (a skipped unparseable diffstat entry, or pagination capped).
				// Caching either makes the wrong number permanent — a final-state cache
				// HIT re-serves it on every later fetch, so not even a cursor rewind can
				// repair it. Keeping it out of the cache means a later code fix takes
				// effect on the next natural re-fetch instead of being short-circuited.
				if enrichErr == nil && !enriched.Degraded && isFinalPRState(pr.State) {
					entry := PRDiffstatCacheEntry{
						FilesStr:     enriched.FilesStr,
						FilesStatStr: enriched.FilesStatStr,
						Additions:    enriched.Additions,
						Deletions:    enriched.Deletions,
					}
					if diffstatCache != nil {
						diffstatCache[cacheKey] = entry
					}
					dirtyDiffstat[cacheKey] = entry
				}
			}
			enrichedRaw := enriched.Payload

			// Fetch the earliest commit on this PR to populate
			// first_commit_at. Bitbucket has no PR payload field for this, so a
			// chained REST call is required.
			//
			// ADR-0004 §7: memoise per-PR via firstCommitCache. first_commit_at
			// is immutable once the PR opens — re-fetching wastes API budget and
			// triggers 429s. cacheKey was set above for the diffstat cache;
			// reuse the same composite "{workspace}/{repoSlug}#{prID}" for first
			// commits. Cache miss → fetch and write back; cache hit → skip the
			// API call entirely.
			var firstCommitAt *time.Time
			// The commit list is cached in its own namespace alongside first_commit_at
			// rather than re-derived, because a cache hit skips the chained call: without
			// a cached copy every re-emit of an already-seen pull request would carry no
			// list, and the record upsert assigns metadata wholesale, so that would erase
			// the links an earlier run recorded.
			//
			// Presence, not emptiness, is what marks it resolved — an empty entry is the
			// real answer "this pull request has no commits to link", and only an absent
			// entry means the call was never made.
			prCommitSHAs, shaResolved := prCommitSHACache[cacheKey]
			if cached, ok := firstCommitCache[cacheKey]; ok && cached != "" {
				if t, parseErr := time.Parse(time.RFC3339, cached); parseErr != nil {
					logger.WarnWithCtx(ctx, "fetch_prs: unparseable cached first_commit_at; refetching",
						zap.String("workspace", workspace), zap.String("repo", repoSlug),
						zap.Int("pr_id", pr.ID), zap.String("cached", cached), zap.Error(parseErr))
				} else {
					firstCommitAt = &t
				}
			}
			// One call answers both, but they are separate entries, so the call is skipped
			// only when BOTH are resolved. Gating on the date alone would strand every pull
			// request whose date was cached before a commit list was ever recorded: the date
			// never expires, so that pull request would never call again and its link would
			// be permanently absent rather than merely late.
			if firstCommitAt == nil || !shaResolved {
				fetched, fetchedSHAs, fcErr := fetchEarliestPRCommit(ctx, restGW, workspace, repoSlug, pr.ID, orgID, opID, budget)
				if fcErr != nil && !errors.Is(fcErr, errNoPRCommit) {
					logger.WarnWithCtx(ctx, "fetch earliest PR commit failed; first_commit_at will be unset",
						zap.String("workspace", workspace),
						zap.String("repo", repoSlug),
						zap.Int("pr_id", pr.ID),
						zap.Error(fcErr))
					// The PR is still emitted (see
					// TestFetchPRsOp_CommitsFetchFailureDoesNotDropPR), but
					// first_commit_at is only ever written on the first successful
					// fetch, so a miss here never self-corrects once the cursor
					// passes the PR. Hold the watermark for retry-worthy kinds.
					// errNoPRCommit is already filtered out above — that one is a
					// legitimate "this PR has no dated commit", not a failed read.
					if isRetryableEnrichFailure(fcErr) {
						earliestDegraded = holdWatermark(earliestDegraded, pr.UpdatedOn)
						degradedPRIDs[pr.ID] = struct{}{}
					}
				}
				// Only when it was not already cached: this call also runs for a pull
				// request whose date is known and whose list is not, and `fetched` is nil
				// on a failed read — assigning it unconditionally would discard a good
				// cached date.
				if firstCommitAt == nil {
					firstCommitAt = fetched
					if firstCommitAt != nil {
						ts := firstCommitAt.UTC().Format(time.RFC3339)
						if firstCommitCache != nil {
							firstCommitCache[cacheKey] = ts
						}
						dirtyFirstCommit[cacheKey] = ts
					}
				}
				// Recorded on its own condition, and on any call the provider answered —
				// including one that answered "no commits", whose empty entry is what
				// stops the refetch. A failed read records nothing, so it retries.
				if fcErr == nil || errors.Is(fcErr, errNoPRCommit) {
					prCommitSHAs = vcs.JoinCommitSHAs(fetchedSHAs)
					if prCommitSHACache != nil {
						prCommitSHACache[cacheKey] = prCommitSHAs
					}
					dirtyPRCommitSHAs[cacheKey] = prCommitSHAs
				}
			}

			destBranch := ""
			if pr.Destination != nil && pr.Destination.Branch != nil {
				destBranch = pr.Destination.Branch.Name
			}
			authorID := ""
			if pr.Author != nil {
				authorID = pr.Author.AccountID
			}
			headBranch := ""
			if pr.Source != nil && pr.Source.Branch != nil {
				headBranch = pr.Source.Branch.Name
			}
			// reviews_requested = distinct count of reviewers explicitly assigned
			// to the PR at open time. Prefer the explicit `reviewers` array; some
			// Bitbucket workspaces surface reviewers only via `participants[]`
			// entries with role="REVIEWER", so fall back when the explicit list
			// is empty. The PR list response carries these arrays because the
			// list URL adds `?fields=+values.participants,+values.reviewers`.
			reviewsRequested := countReviewersRequested(pr)
			// files_changed = distinct file paths touched by the diff. Sourced
			// from `enriched.FilesStr` (already a JSON []string from the PR
			// diffstat call).
			filesChangedCount := countFilePathsInJSON(enriched.FilesStr)
			// Inject the two counts into the emitted PR payload as
			// `files_changed` / `reviews_requested` top-level integers — the
			// downstream parser reads them from the payload the same way it
			// already reads `additions` / `deletions`. Keeps the parser
			// signature payload-only (no metadata coupling).
			enrichedRaw = injectPRCounts(enrichedRaw, filesChangedCount, reviewsRequested)
			baseMeta := map[string]string{
				"repo":                repoFullName,
				"destination_branch":  destBranch,
				"pr_author_id":        authorID,
				"files_changed":       enriched.FilesStr,
				"files_changed_count": fmt.Sprintf("%d", filesChangedCount),
				"head_branch":         headBranch,
				"reviews_requested":   fmt.Sprintf("%d", reviewsRequested),
			}
			// files_line_stats is authoritative only on a fully-successful enrichment. A
			// failed fetch (enrichErr) or a degraded aggregate (enriched.Degraded — capped
			// pagination / unparseable entry) returns "[]" or a partial list a consumer cannot
			// tell from a real empty diff, and the DAO COALESCEs a non-null value over the
			// stored stats — so stamping it would erase real file stats. Omit the key on both,
			// the same trust guard the diffstat cache uses above; a clean "[]" is a real empty
			// diff and is stamped.
			if enrichErr == nil && !enriched.Degraded {
				baseMeta["files_line_stats"] = enriched.FilesStatStr
			}
			// destination.repository.uuid is Bitbucket's stable repo ID; survives
			// repo renames and workspace transfers.
			if pr.Destination != nil && pr.Destination.Repository != nil && pr.Destination.Repository.UUID != "" {
				baseMeta["platform_repo_id"] = pr.Destination.Repository.UUID
			}
			// Detect fork PRs: source and destination repositories differ.
			if pr.Source != nil && pr.Source.Repository != nil &&
				pr.Destination != nil && pr.Destination.Repository != nil &&
				pr.Source.Repository.FullName != pr.Destination.Repository.FullName {
				baseMeta["is_cross_repository"] = "true"
				// head_repo_owner is the workspace part of the source repo full_name.
				parts := strings.SplitN(pr.Source.Repository.FullName, "/", 2)
				if len(parts) == 2 {
					baseMeta["head_repo_owner"] = parts[0]
				}
			}
			if firstCommitAt != nil {
				baseMeta["first_commit_at"] = firstCommitAt.UTC().Format(time.RFC3339)
			}
			// The commit-to-pull-request link, read back off the page fetched above or
			// its cached copy. Omitted rather than emptied when neither has it: the
			// record upsert assigns metadata wholesale, so writing an empty value would
			// erase a list an earlier run recorded.
			if prCommitSHAs != "" {
				baseMeta[vcs.MetaKeyPRCommitSHAs] = prCommitSHAs
			}
			externalIDBase := fmt.Sprintf("%s#PR-%d", repoFullName, pr.ID)

			// Emit opened record.
			if emitErr := emitRecord(ctx, sink, opID, orgID, externalIDBase+"#opened", enrichedRaw, withEventType(baseMeta, "opened"), pr.CreatedOn); emitErr != nil {
				return time.Time{}, earliestDegraded, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// Emit snapshot record. It is current state, so it has no DataDate
			// (ADR-0003 amendment).
			if emitErr := emitRecord(ctx, sink, opID, orgID, externalIDBase+"#snapshot", enrichedRaw, withEventType(baseMeta, "snapshot"), time.Time{}); emitErr != nil {
				return time.Time{}, earliestDegraded, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// Emit merged record if applicable.
			if pr.State == "MERGED" {
				if emitErr := emitRecord(ctx, sink, opID, orgID, externalIDBase+"#merged", enrichedRaw, withEventType(baseMeta, "merged"), pr.UpdatedOn); emitErr != nil {
					return time.Time{}, earliestDegraded, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}
			}

			// Emit closed record if declined or superseded.
			if pr.State == "DECLINED" || pr.State == "SUPERSEDED" {
				if emitErr := emitRecord(ctx, sink, opID, orgID, externalIDBase+"#closed", enrichedRaw, withEventType(baseMeta, "closed"), pr.UpdatedOn); emitErr != nil {
					return time.Time{}, earliestDegraded, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}
			}

			if pr.UpdatedOn.After(latestUpdated) {
				latestUpdated = pr.UpdatedOn
			}
		}

		// Decide whether another page follows BEFORE invoking onPage so the
		// checkpoint records the right next-page identifier. NextPageUrl is
		// the authoritative signal from Bitbucket; an early-exit on `since`
		// also terminates the iteration regardless of NextPageUrl. Empty
		// nextPageCursor on checkpoint means "iteration finished cleanly —
		// caller should clear PageCursor on the final cursor write".
		finished := done || resp.GetNextPageUrl() == ""
		nextPageCursor := ""
		if !finished {
			nextPageCursor = strconv.Itoa(int(page) + 1)
		}
		// Write-back this page's cache entries BEFORE the onPage cursor checkpoint
		// below — fetch_prs is the only op with a per-page checkpoint, and the
		// cursor must never advance past a page whose cache write has not landed
		// (a resume would re-fetch it). See putFetchCacheDirty for the per-page why.
		putFetchCacheDirty(ctx, fc, CacheTypePRFirstCommit, dirtyFirstCommit)
		putFetchCacheDirty(ctx, fc, CacheTypePRCommitSHAs, dirtyPRCommitSHAs)
		putFetchCacheDirty(ctx, fc, CacheTypePRDiffstat, dirtyDiffstat)
		if onPage != nil {
			if cpErr := onPage(ctx, latestUpdated, nextPageCursor); cpErr != nil {
				logger.Error(ctx, "fetch_prs: per-page checkpoint failed (continuing)",
					zap.String("repo", repoFullName),
					zap.Int32("page", page),
					zap.Error(cpErr))
			}
		}

		if finished {
			break
		}
		page++
	}

	if len(degradedPRIDs) > 0 {
		logger.WarnWithCtx(ctx, "fetch_prs: PRs persisted with incomplete enrichment; cursor will be held for retry",
			zap.String("repo", repoFullName),
			zap.Int("degraded_prs", len(degradedPRIDs)),
			zap.Time("oldest_degraded", earliestDegraded))
	}
	return latestUpdated, earliestDegraded, nil
}

// applyCursorHold decides whether to hold state.Timestamp back to the oldest PR
// whose enrichment failed, and bounds that hold across runs. Returns whether the
// state was modified.
//
// The bound is the load-bearing part. ErrKindRateLimit is raised by our own local
// token bucket, not just by a vendor 429, and a locally-throttled org does not
// self-clear — so an unbounded hold pins the repo while the re-walked window grows
// every tick, needing more budget and deepening the very throttle it is riding out.
// After maxConsecutiveCursorHolds the cursor advances instead: the degraded PRs stay
// wrong, but the repo makes progress and a cursor rewind can repair them
// once the vendor-side cause clears. That trade is deliberate — a stuck repo is
// worse than a repairable gap.
func applyCursorHold(ctx context.Context, repoFullName string, state *repoCursorState, earliestDegraded time.Time) bool {
	if earliestDegraded.IsZero() {
		// Nothing degraded — the streak is broken and any floor has served its purpose.
		if state.HoldFloor.IsZero() && state.HoldCount == 0 && state.HoldReleasedAt.IsZero() {
			return false
		}
		*state = repoCursorState{Timestamp: state.Timestamp, PageCursor: state.PageCursor, Since: state.Since}
		return true
	}
	// Already gave up on this point: do not restart a streak on it, or the counter
	// cycles 0 -> limit -> 0 forever while the same PR is re-walked every tick.
	if !state.HoldReleasedAt.IsZero() && !earliestDegraded.After(state.HoldReleasedAt) {
		return false
	}
	if state.HoldCount >= maxConsecutiveCursorHolds {
		logger.Error(ctx, "fetch_prs: giving up holding cursor after consecutive holds; PRs with incomplete enrichment are being left behind — repair with a cursor rewind once the vendor-side cause clears",
			zap.String("repo", repoFullName),
			zap.Int("consecutive_holds", state.HoldCount),
			zap.Time("abandoned_at", earliestDegraded))
		state.HoldFloor = time.Time{}
		state.HoldCount = 0
		state.HoldReleasedAt = earliestDegraded
		return true
	}
	// Count the streak on DEGRADATION, not on the floor moving: once the floor
	// already sits at the degraded PR, re-deriving it is a no-op, so keying the
	// counter off "the value changed" would leave it at 1 and the bound would never
	// fire — the pin this bound exists to prevent.
	state.HoldCount++
	state.HoldFloor = holdWatermark(state.HoldFloor, earliestDegraded)
	logger.WarnWithCtx(ctx, "fetch_prs: holding a repair floor below PRs with incomplete enrichment",
		zap.String("repo", repoFullName),
		zap.Int("consecutive_holds", state.HoldCount),
		zap.Int("hold_limit", maxConsecutiveCursorHolds),
		zap.Time("repair_floor", state.HoldFloor))
	return true
}

// isRetryableEnrichFailure reports whether a per-PR enrichment error is worth
// holding the cursor for. Deliberately NOT classifyRepoError: that helper is built
// for repo-level failures and defaults an *untyped* error to repoErrTransient, which
// inverts into a bug per-PR — a persistent untyped failure (an unexpected gateway
// response type, a malformed commits payload) would pin the cursor and replay the
// window on every tick. So only a typed, vendor-side-transient kind qualifies;
// everything else (untyped, 404, auth, config/internal) lets the cursor advance and
// leaves the PR with empty defaults for a human to fix at source.
//
// Note that ErrKindRateLimit covers our own local token bucket as well as a vendor
// 429, and a locally-throttled org does not self-clear — which is why the hold this
// gates is additionally bounded by maxConsecutiveCursorHolds.
func isRetryableEnrichFailure(err error) bool {
	var syncErr *sdk.SyncError
	if !errors.As(err, &syncErr) {
		return false
	}
	switch syncErr.Kind {
	case sdk.ErrKindRateLimit, sdk.ErrKindVendorError, sdk.ErrKindTimeout, sdk.ErrKindCtxExhausted:
		return true
	default:
		return false
	}
}

// holdWatermark folds candidate into the running "oldest degraded PR" marker,
// keeping the OLDEST updated_on seen. Zero candidates are ignored so a PR with
// no parseable updated_on cannot pin the cursor at the epoch and force a full
// re-walk of the repo on every subsequent run.
func holdWatermark(current, candidate time.Time) time.Time {
	if candidate.IsZero() {
		return current
	}
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

// applyRepairFloor pulls a run's lower bound back to the repair floor when one is
// set. It never rewrites the watermark — that separation is the point: Timestamp
// stays the true high-water mark the resume path reads as priorWatermark, while the
// floor expresses "re-walk from here to repair PRs we could not enrich".
//
// No epsilon is needed: Bitbucket pages newest->oldest and the early-exit boundary is
// `updated_on < since`, so a floor equal to the degraded PR's updated_on re-includes
// that PR.
func applyRepairFloor(since, holdFloor time.Time) time.Time {
	if holdFloor.IsZero() || !holdFloor.Before(since) {
		return since
	}
	return holdFloor
}

// errNoPRCommit signals that a PR has no commit with a usable date — a
// not-found, not an API failure. Callers treat it as "leave first_commit_at
// unset" without logging an error.
var errNoPRCommit = errors.New("no PR commit with a usable date")

// fetchEarliestPRCommit fetches a PR's commits via the chained
// GET /2.0/repositories/{workspace}/{repo}/pullrequests/{id}/commits endpoint
// and returns both the earliest date and the page's commit
// hashes. It fetches one full page and takes MIN(date) client-side so correctness
// does not depend on server ordering. Returns errNoPRCommit when the PR has no
// commit with a usable date. Returns (nil, nil, err) on API failure — the caller
// emits the PR without first_commit_at rather than dropping the record.
//
// The hashes are the commit-to-pull-request link. Bitbucket's own commit-to-pull-
// request endpoint cannot be used — its specification requires an app a human
// installs through the web interface — so reading the link back off this list is
// the only channel available here.
func fetchEarliestPRCommit(ctx context.Context, restGW sdk.VendorGateway, workspace, repoSlug string, prID int, orgID string, opID sdk.OperationID, budget *pacing.PacingBudget) (*time.Time, []string, error) {
	protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketListPRCommitsRequest{
		Workspace: workspace,
		RepoSlug:  repoSlug,
		PrId:      int64(prID),
		Pagelen:   100,
		Sort:      "date",
	}, budget, connectorID, opID, orgID)
	if err != nil {
		return nil, nil, fmt.Errorf("list PR commits: %w", err)
	}
	resp, ok := protoResp.(*connectorpb.BitbucketListPRCommitsResponse)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected response type %T", protoResp)
	}
	if len(resp.GetCommits()) == 0 {
		return nil, nil, errNoPRCommit
	}
	var earliest time.Time
	hashes := make([]string, 0, len(resp.GetCommits()))
	for _, raw := range resp.GetCommits() {
		var commit struct {
			Hash string    `json:"hash"`
			Date time.Time `json:"date"`
		}
		if err := json.Unmarshal(raw, &commit); err != nil {
			return nil, nil, fmt.Errorf("unmarshal PR commit: %w", err)
		}
		if commit.Hash != "" {
			hashes = append(hashes, commit.Hash)
		}
		if commit.Date.IsZero() {
			continue
		}
		if earliest.IsZero() || commit.Date.Before(earliest) {
			earliest = commit.Date.UTC()
		}
	}
	// A commit with no usable date still carries a usable link, so the hashes are
	// returned even when the date lookup finds nothing to report.
	if earliest.IsZero() {
		return nil, hashes, errNoPRCommit
	}
	return &earliest, hashes, nil
}

// --- FetchWorkspaceMembersOp ---

type FetchWorkspaceMembersOp struct{}

func (op *FetchWorkspaceMembersOp) OperationID() sdk.OperationID {
	return "fetch_workspace_members"
}

func (op *FetchWorkspaceMembersOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)

	// Fetch workspace groups (v1.0 API) to build account_id → group names map.
	// Best-effort: failures are logged, not propagated, so missing permissions
	// don't break identity sync.
	accountToGroups := fetchWorkspaceGroupMemberships(ctx, restGW, settings.Workspace)

	page := int32(1)
	count := 0

	for {
		if page > maxListPages {
			return nil, errListPageCap("fetch workspace members")
		}
		protoResp, err := restGW.Execute(ctx, &connectorpb.BitbucketListWorkspaceMembersRequest{
			Workspace: settings.Workspace,
			Page:      page,
			Pagelen:   100,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch workspace members: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListWorkspaceMembersResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *BitbucketListWorkspaceMembersResponse", protoResp)
		}

		for _, memberRaw := range resp.GetMembers() {
			var m bbWorkspaceMember
			if err := json.Unmarshal(memberRaw, &m); err != nil {
				logger.WarnWithCtx(ctx, "fetch_workspace_members: skipping member entry that failed to unmarshal",
					zap.String("workspace", settings.Workspace), zap.Error(err))
				continue
			}

			if m.User == nil || m.User.AccountID == "" {
				continue
			}

			// Transform the raw API response into the flat WorkspaceMemberRecord
			// shape that a consumer expects.
			record := WorkspaceMemberRecord{
				AccountID:   m.User.AccountID,
				DisplayName: m.User.DisplayName,
				Nickname:    m.User.Nickname,
				HintSource:  "workspace_members_api",
				Groups:      accountToGroups[m.User.AccountID],
			}
			recordJSON, err := json.Marshal(record)
			if err != nil {
				logger.WarnWithCtx(ctx, "fetch_workspace_members: skipping member; failed to marshal record",
					zap.String("account_id", m.User.AccountID), zap.Error(err))
				continue
			}

			externalID := fmt.Sprintf("member-%s", m.User.AccountID)

			// A member is current state: no DataDate (sdk.RawRecord.DataDate).
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, recordJSON, map[string]string{"workspace": settings.Workspace}, time.Time{}); emitErr != nil {
				return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}
			count++
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
	}

	logger.Info(ctx, "fetched workspace members", zap.String("workspace", settings.Workspace), zap.Int("count", count))
	return sdk.NewTimestampCursor(time.Now().UTC()), nil
}

// systemGroupPrefixes lists name prefixes for Atlassian-managed system groups
// (e.g. "jira-users-*", "bitbucket-admins-*") that are noisy and not useful
// as workspace team groupings.
var systemGroupPrefixes = []string{
	"bitbucket-",
	"jira-",
	"confluence-",
	"org-admins",
}

// isSystemGroup returns true if the group name is an Atlassian-managed system group.
func isSystemGroup(name string) bool {
	lower := strings.ToLower(name)
	for _, prefix := range systemGroupPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

// fetchWorkspaceGroupMemberships fetches all Bitbucket workspace groups via the
// v1.0 groups API and returns an account_id → []groupName map.
// Filters out Atlassian system groups (jira-*, bitbucket-*, confluence-*).
// Best-effort: returns an empty map on failure.
func fetchWorkspaceGroupMemberships(ctx context.Context, gw sdk.VendorGateway, workspace string) map[string][]string {
	accountToGroups := make(map[string][]string)

	protoResp, err := gw.Execute(ctx, &connectorpb.BitbucketListWorkspaceGroupsRequest{
		Workspace: workspace,
	})
	if err != nil {
		logger.WarnWithCtx(ctx, "skipping group enrichment — failed to fetch workspace groups",
			zap.String("workspace", workspace),
			zap.Error(err),
		)
		return accountToGroups
	}

	resp, ok := protoResp.(*connectorpb.BitbucketListWorkspaceGroupsResponse)
	if !ok {
		logger.WarnWithCtx(ctx, "skipping group enrichment — unexpected response type",
			zap.String(logger.Type, fmt.Sprintf("%T", protoResp)),
		)
		return accountToGroups
	}

	var totalGroups, filteredGroups int
	for _, groupRaw := range resp.GetGroups() {
		var group bbGroup
		if err := json.Unmarshal(groupRaw, &group); err != nil {
			logger.WarnWithCtx(ctx, "fetch_workspace_members: skipping group entry that failed to unmarshal",
				zap.String("workspace", workspace), zap.Error(err))
			continue
		}
		totalGroups++

		if isSystemGroup(group.Name) {
			continue
		}
		filteredGroups++

		for _, member := range group.Members {
			if member.AccountID != "" {
				accountToGroups[member.AccountID] = append(accountToGroups[member.AccountID], group.Name)
			}
		}
	}

	logger.Info(ctx, "fetched workspace group memberships",
		zap.String("workspace", workspace),
		zap.Int("total_groups", totalGroups),
		zap.Int("custom_groups", filteredGroups),
		zap.Int("members_with_groups", len(accountToGroups)),
	)

	return accountToGroups
}

// --- FetchCommitsOp ---

// commitWithRaw pairs a parsed commit with its raw payload so a page can be
// unmarshaled once (to batch the fetch-cache prime) and still emit the original bytes.
type commitWithRaw struct {
	commit bbCommit
	raw    json.RawMessage
}

type FetchCommitsOp struct {
	paginationCfg *config.BitbucketPaginationConfig
}

func (op *FetchCommitsOp) OperationID() sdk.OperationID { return "fetch_commits" }

// metaKeyRefs is the record-metadata key naming the refs a commit was reached from,
// comma-separated. A consumer side declares the same literal
// (the consumer's refs metadata key) rather than sharing a constant: a connector must not
// import its consumer, and platform_repo_id crosses the same boundary the same way. The two
// must be changed together.
const metaKeyRefs = "refs"

// refsBySHA tracks which refs a commit has been reached from so far in this repository's
// walk. Only the ref names are held — never the payloads — so the walk keeps streaming each
// page to the sink as it did before multi-ref, and memory does not grow with commit count.
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

func (op *FetchCommitsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	// One run-scoped pacing budget for this run's cursor-critical bitbucket-rest
	// calls — repo discovery, per-repo default-branch resolution, and the
	// commit-list walk. On our own limiter denying one of these it waits out the
	// window and retries rather than aborting the run and freezing the cursor (the
	// self-reinforcing re-list backlog that otherwise builds up). Diffstat
	// enrichment draws on this budget too (see enrichCommit): a commit emitted
	// without stats is never repaired. The budget bounds ONE run's cumulative
	// wait; on give-up the walk fails the run and asynq reschedules.
	budget := &pacing.PacingBudget{}
	repos, err := fetchAllRepos(ctx, params.VendorGateway, settings, op.OperationID(), params.Config.OrganizationID, budget)
	if err != nil {
		return nil, fmt.Errorf("fetching all repos: %w", err)
	}

	// Repo full name → default branch, read from the discovered-repos cache so
	// we avoid an extra workspace listing. A cache miss (explicit repo-list, or
	// first sync) resolves to empty; resolveCommitBranch then falls back to a
	// single concrete branch (never empty — see its doc for why that matters).
	defaultBranchByRepo := make(map[string]string, len(settings.DiscoveredRepos))
	for _, dr := range settings.DiscoveredRepos {
		defaultBranchByRepo[dr.FullName] = dr.DefaultBranch
	}

	// The per-sha commit diffstat cache lets us issue at most one `/diffstat/{sha}`
	// call per commit across the lifetime of the org. Commit diffs against the
	// parent are immutable — any cached hit is correct forever. The map starts
	// empty and is primed per page from connector_fetch_cache (GetMany).
	commitDiffstatCache := make(map[string]CommitDiffstatCacheEntry, 64)
	enrichEnabled := !settings.DisableCommitDiffstat

	// dirty set: commits whose diffstat was freshly fetched this run.
	dirtyCommitDiffstat := make(map[string]CommitDiffstatCacheEntry)
	defer putFetchCacheDirty(ctx, params.FetchCache, CacheTypeCommitDiffstat, dirtyCommitDiffstat)

	// persistFetchCommitsSettings flushes observed rate-limit caps; the diffstat
	// cache lives in connector_fetch_cache, not in settings.
	defer persistFetchCommitsSettings(ctx, params, bitbucketBucketToGroup)

	cursorTimestamps, droppedAgedWatermark := carryForwardCursorTimes(ctx, params.Cursor, carryableRepoKeys(settings, repos))

	// The administrator's tracked-branch set, globs already expanded server-side. Any failure
	// returns (nil, false) and every repo degrades to its default branch — never to all, which
	// would turn a settings outage into a provider-budget incident.
	trackedRefs, refsResolved := vcs.ResolveTrackedRefs(ctx, params.Host.TrackedRefs, params.Config.OrganizationID, string(connectorID))
	refsByRepo := vcs.IndexTrackedRefs(trackedRefs)

	var skipped repotally.Skipper
	var ctxErr error
repos:
	for n, repoFullName := range repos {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		if n > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, buildCompositeCursor(cursorTimestamps)); err != nil {
				logger.Error(ctx, "checkpoint failed (continuing)", zap.String("repo", repoFullName), zap.Error(err))
			}
		}
		workspace, repoSlug, err := splitFullName(repoFullName)
		if err != nil {
			logger.WarnWithCtx(ctx, "skipping invalid repo", zap.String("repo", repoFullName), zap.Error(err))
			continue
		}
		skipped.Attempt()

		highWater := getCompositeCursorTime(ctx, params.Cursor, repoFullName)
		since := firstSyncSince(highWater)
		latestDate := since
		restGW := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)

		// Commits always follow the repo's default branch, independent of the
		// tracked-branch / all-branches setting (which governs only the PR
		// read-filter). On a discovered-repos cache miss (explicit repo list /
		// first sync) resolve the real default via one BitbucketGetRepo call so
		// resolveCommitBranch's "main" stays a true last resort — guessing
		// "main" for a "master"-default repo would walk the wrong branch.
		defaultBranch, resolveErr := resolveDefaultBranch(ctx, restGW, workspace, repoSlug, defaultBranchByRepo[repoFullName], budget, op.OperationID(), params.Config.OrganizationID)
		if errors.Is(resolveErr, sdk.ErrLocalRateLimit) {
			// Our own limiter gave up before the real default branch could be
			// resolved. Skip this repo rather than fall back to "main": walking a
			// guessed branch would advance the per-repo watermark on the wrong
			// branch and permanently skip the true default's commits. The next run
			// re-resolves once capacity returns; this repo's cursor stays put.
			logger.WarnWithCtx(ctx, "fetch_commits: default-branch resolution rate-limited; skipping repo this run",
				zap.String("repo", repoFullName))
			continue
		}
		if resolveErr != nil {
			// Non-rate-limit failure (repo 404 / malformed response): fall back to
			// resolveCommitBranch's "main" last resort rather than skip.
			logger.WarnWithCtx(ctx, "fetch_commits: default-branch resolution failed; using fallback branch",
				zap.String("repo", repoFullName), zap.Error(resolveErr))
		}
		// resolveCommitBranch's last-resort "main" remains the fallback source when the
		// resolver is unavailable; the tracked set replaces it when it is.
		refs, skipRepo := vcs.CommitRefsForRepo(ctx, refsByRepo, refsResolved, repoFullName, settings.resolveCommitBranch(defaultBranch))
		if skipRepo {
			// MatchNone: the repo resolved to no countable refs, so fetching nothing is the
			// resolved answer rather than a failure.
			continue
		}

		// Ref sets only — the payloads stream straight through, as before multi-ref.
		seenRefs := make(refsBySHA)

		for _, branch := range refs {
			page := int32(1)

			for {
				if page > maxListPages {
					return nil, errListPageCap("fetch commits for " + repoFullName + " " + branch)
				}
				protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketListCommitsRequest{
					Workspace: workspace,
					RepoSlug:  repoSlug,
					Page:      page,
					Pagelen:   100,
					// Always a concrete branch — an empty Include would be
					// `git log --all` (every branch), which resolveCommitBranch
					// guards against.
					Include: branch,
				}, budget, connectorID, op.OperationID(), params.Config.OrganizationID)
				if err != nil {
					if skipped.Skip(repoFullName, err) {
						logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
						continue repos
					}
					return nil, fmt.Errorf("fetch commits for %s: %w", repoFullName, err)
				}

				resp, ok := protoResp.(*connectorpb.BitbucketListCommitsResponse)
				if !ok {
					return nil, fmt.Errorf("unexpected response type %T", protoResp)
				}

				// Pre-parse the page so the per-sha diffstat prime batches one GetMany
				// before serving the per-commit reads. Skipped when enrich is disabled.
				pageCommits := make([]commitWithRaw, 0, len(resp.GetCommits()))
				for _, commitRaw := range resp.GetCommits() {
					var commit bbCommit
					if err := json.Unmarshal(commitRaw, &commit); err != nil {
						logger.WarnWithCtx(ctx, "fetch_commits: skipping commit entry that failed to unmarshal",
							zap.String("repo", repoFullName), zap.Error(err))
						continue
					}
					pageCommits = append(pageCommits, commitWithRaw{commit: commit, raw: commitRaw})
				}
				if enrichEnabled {
					keys := make([]string, 0, len(pageCommits))
					for _, c := range pageCommits {
						if !since.IsZero() && c.commit.Date.Before(since) {
							break
						}
						if c.commit.Hash != "" {
							keys = append(keys, fmt.Sprintf("%s/%s#%s", workspace, repoSlug, c.commit.Hash))
						}
					}
					primeFetchCache(ctx, params.FetchCache, CacheTypeCommitDiffstat, keys, commitDiffstatCache)
				}

				done := false
				for _, c := range pageCommits {
					commit := c.commit
					commitRaw := c.raw

					// Stop if we've reached commits older than our cursor.
					if !since.IsZero() && commit.Date.Before(since) {
						done = true
						break
					}

					emitPayload := commitRaw
					meta := map[string]string{"repo": repoFullName}
					if enrichEnabled && commit.Hash != "" {
						cacheKey := fmt.Sprintf("%s/%s#%s", workspace, repoSlug, commit.Hash)
						if cached, ok := commitDiffstatCache[cacheKey]; ok {
							emitPayload = injectCommitLineCounts(commitRaw, cached.Additions, cached.Deletions, cached.FilesChanged)
							meta["additions"] = fmt.Sprintf("%d", cached.Additions)
							meta["deletions"] = fmt.Sprintf("%d", cached.Deletions)
							meta["files_changed"] = fmt.Sprintf("%d", cached.FilesChanged)
						} else {
							enriched, enrichErr := enrichCommit(ctx, params.VendorGateway, op.paginationCfg, workspace, repoSlug, commit.Hash, commitRaw, budget, op.OperationID(), params.Config.OrganizationID)
							if enrichErr != nil && commitEnrichmentStops(enrichErr) {
								// Stop at this commit rather than emit it without stats:
								// this repo's watermark is not advanced, so the next run
								// re-walks from here and fetches the stats.
								if err := ctx.Err(); err != nil {
									ctxErr = err
									break repos
								}
								if skipped.Skip(repoFullName, enrichErr) {
									logger.WarnWithCtx(ctx, "skipping a repository whose commit diffstat failed", zap.String("repo", repoFullName), zap.String("sha", commit.Hash), zap.Error(enrichErr))
									continue repos
								}
								return nil, fmt.Errorf("fetch commit diffstat for %s@%s: %w", repoFullName, commit.Hash, enrichErr)
							}
							if enrichErr != nil {
								logger.WarnWithCtx(ctx, "failed to enrich commit diffstat, using empty defaults",
									zap.String("repo", repoFullName), zap.String("sha", commit.Hash), zap.Error(enrichErr))
							} else {
								emitPayload = enriched.Payload
								entry := CommitDiffstatCacheEntry{
									Additions:    enriched.Additions,
									Deletions:    enriched.Deletions,
									FilesChanged: enriched.FilesChanged,
								}
								commitDiffstatCache[cacheKey] = entry
								dirtyCommitDiffstat[cacheKey] = entry
								meta["additions"] = fmt.Sprintf("%d", enriched.Additions)
								meta["deletions"] = fmt.Sprintf("%d", enriched.Deletions)
								meta["files_changed"] = fmt.Sprintf("%d", enriched.FilesChanged)
							}
						}
					}

					meta[metaKeyRefs] = seenRefs.add(commit.Hash, branch)

					// Emitted here rather than after the ref loop: the walk checkpoints as
					// it pages, and a cursor saved past commits that never reached the sink
					// would skip them permanently.
					externalID := fmt.Sprintf("%s#%s", repoFullName, commit.Hash)
					if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, emitPayload, meta, commit.Date); emitErr != nil {
						return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
					}

					if commit.Date.After(latestDate) {
						latestDate = commit.Date
					}
				}

				// Per-page write-back (see putFetchCacheDirty): a hard kill re-fetches at
				// most one page of sha-diffstats, not the whole run.
				putFetchCacheDirty(ctx, params.FetchCache, CacheTypeCommitDiffstat, dirtyCommitDiffstat)

				if done || resp.GetNextPageUrl() == "" {
					break
				}
				page++
			}
		}

		// A first sync that observed nothing records nothing (see
		// watermarkIsPinnedBeyondFloor): latestDate still equals the synthetic `since` floor, and persisting it would publish
		// a query bound as observed progress — the pin the cursor loaders drop.
		if highWater.IsZero() && latestDate.Equal(since) {
			logger.Info(ctx, "fetched commits; nothing observed, watermark left unset", zap.String("repo", repoFullName))
			continue
		}
		cursorTimestamps[repoFullName] = latestDate
		logger.Info(ctx, "fetched commits", zap.String("repo", repoFullName), zap.Time("cursor", latestDate))
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), buildCompositeCursor(cursorTimestamps))
		}
		return buildCompositeCursor(cursorTimestamps), fmt.Errorf("%s: %w", op.OperationID(), errors.Join(ctxErr, cpErr))
	}
	if err := skipped.Err(len(repos)); err != nil {
		return nil, err
	}
	if len(cursorTimestamps) == 0 && !droppedAgedWatermark {
		return params.Cursor, nil
	}
	return buildCompositeCursor(cursorTimestamps), nil
}

// --- FetchIssuesOp ---

// closedIssueStates are the Bitbucket issue states that indicate a closed/resolved issue.
var closedIssueStates = map[string]bool{
	"resolved":  true,
	"closed":    true,
	"wontfix":   true,
	"invalid":   true,
	"duplicate": true,
}

type FetchIssuesOp struct{}

func (op *FetchIssuesOp) OperationID() sdk.OperationID { return "fetch_issues" }

func (op *FetchIssuesOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	if !settings.IssueTrackerEnabled {
		logger.Info(ctx, "issue tracker disabled, skipping")
		return params.Cursor, nil
	}

	// Discovery-only pacing budget (this op does not pace its own loop). See
	// FetchCommitsOp.Execute for why repo discovery is still paced.
	repos, err := fetchAllRepos(ctx, params.VendorGateway, settings, op.OperationID(), params.Config.OrganizationID, &pacing.PacingBudget{})
	if err != nil {
		return nil, fmt.Errorf("fetching all repos: %w", err)
	}

	cursorTimestamps, droppedAgedWatermark := carryForwardCursorTimes(ctx, params.Cursor, carryableRepoKeys(settings, repos))

	var skipped repotally.Skipper
	var ctxErr error
repos:
	for n, repoFullName := range repos {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		if n > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, buildCompositeCursor(cursorTimestamps)); err != nil {
				logger.Error(ctx, "checkpoint failed (continuing)", zap.String("repo", repoFullName), zap.Error(err))
			}
		}
		workspace, repoSlug, err := splitFullName(repoFullName)
		if err != nil {
			logger.WarnWithCtx(ctx, "skipping invalid repo", zap.String("repo", repoFullName), zap.Error(err))
			continue
		}
		skipped.Attempt()

		highWater := getCompositeCursorTime(ctx, params.Cursor, repoFullName)
		since := firstSyncSince(highWater)
		latestUpdated, err := fetchIssuesForRepo(ctx, params.VendorGateway, params.Sink, op.OperationID(), params.Config.OrganizationID, workspace, repoSlug, repoFullName, since)
		if err != nil {
			if skipped.Skip(repoFullName, err) {
				logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
				continue repos
			}
			return nil, fmt.Errorf("fetching issues for repo: %w", err)
		}

		// A first sync that observed nothing records nothing — see fetch_commits.
		if highWater.IsZero() && latestUpdated.Equal(since) {
			logger.Info(ctx, "fetched issues; nothing observed, watermark left unset", zap.String("repo", repoFullName))
			continue
		}
		cursorTimestamps[repoFullName] = latestUpdated
		logger.Info(ctx, "fetched issues", zap.String("repo", repoFullName), zap.Time("cursor", latestUpdated))
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), buildCompositeCursor(cursorTimestamps))
		}
		return buildCompositeCursor(cursorTimestamps), fmt.Errorf("%s: %w", op.OperationID(), errors.Join(ctxErr, cpErr))
	}
	if err := skipped.Err(len(repos)); err != nil {
		return nil, err
	}
	if len(cursorTimestamps) == 0 && !droppedAgedWatermark {
		return params.Cursor, nil
	}
	return buildCompositeCursor(cursorTimestamps), nil
}

// fetchIssuesForRepo fetches all issues for a single repo since the given timestamp.
// Returns the since time unchanged if the repo's issue tracker is disabled (404 from API).
func fetchIssuesForRepo(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, workspace, repoSlug, repoFullName string, since time.Time) (time.Time, error) {
	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	latestUpdated := since
	page := int32(1)

	for {
		if page > maxListPages {
			return time.Time{}, errListPageCap("fetch issues for " + repoFullName)
		}
		protoResp, err := restGW.Execute(ctx, &connectorpb.BitbucketListIssuesRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			Page:      page,
			Pagelen:   50,
		})
		if err != nil {
			// Issue tracker may be disabled per-repo, so swallow a 404 as "no data" —
			// but only a 404: a blanket swallow marks the run completed while losing history.
			syncErr := &sdk.SyncError{}
			if errors.As(err, &syncErr) && syncErr.Kind == sdk.ErrKindNotFound {
				logger.Info(ctx, "issue tracker not enabled for repo, skipping", zap.String("repo", repoFullName))
				return since, nil
			}
			return time.Time{}, fmt.Errorf("fetch issues for %s: %w", repoFullName, err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListIssuesResponse)
		if !ok {
			return time.Time{}, fmt.Errorf("unexpected response type %T", protoResp)
		}

		done := false
		for _, issueRaw := range resp.GetIssues() {
			var issue bbIssue
			if err := json.Unmarshal(issueRaw, &issue); err != nil {
				logger.WarnWithCtx(ctx, "fetch_issues: skipping issue entry that failed to unmarshal",
					zap.String("repo", repoFullName), zap.Error(err))
				continue
			}

			// Stop if we've reached issues older than our cursor.
			if !since.IsZero() && issue.UpdatedOn.Before(since) {
				done = true
				break
			}

			baseMeta := map[string]string{"repo": repoFullName}
			externalID := fmt.Sprintf("%s#issue-%d", repoFullName, issue.ID)

			// Emit opened record.
			if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#opened", issueRaw, withEventType(baseMeta, "opened"), issue.CreatedOn); emitErr != nil {
				return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			// Emit closed record if issue is in a terminal state.
			if closedIssueStates[issue.State] {
				if emitErr := emitRecord(ctx, sink, opID, orgID, externalID+"#closed", issueRaw, withEventType(baseMeta, "closed"), issue.UpdatedOn); emitErr != nil {
					return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}
			}

			if issue.UpdatedOn.After(latestUpdated) {
				latestUpdated = issue.UpdatedOn
			}
		}

		if done || resp.GetNextPageUrl() == "" {
			break
		}
		page++
	}

	return latestUpdated, nil
}

// --- FetchReviewsOp ---

type FetchReviewsOp struct{}

func (op *FetchReviewsOp) OperationID() sdk.OperationID { return "fetch_reviews" }

func (op *FetchReviewsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	// One pacing budget for the whole run (repo discovery, all repos, all per-PR
	// activity calls share it) so a local-limit rejection is waited out, not failed
	// on. See the same construction in FetchPRsOp.Execute; created before repo
	// discovery so the discovery listing is paced too.
	budget := &pacing.PacingBudget{}

	repos, err := fetchAllRepos(ctx, params.VendorGateway, settings, op.OperationID(), params.Config.OrganizationID, budget)
	if err != nil {
		return nil, fmt.Errorf("fetching all repos: %w", err)
	}

	// The activity cache is primed per page from connector_fetch_cache; the dirty
	// set is the PRs whose /activity was actually re-fetched this run.
	activityCache := make(map[string]prProbeEntry)
	dirtyActivity := make(map[string]prProbeEntry)

	cursorTimestamps, droppedAgedWatermark := carryForwardCursorTimes(ctx, params.Cursor, carryableRepoKeys(settings, repos))

	var skipped repotally.Skipper
	var ctxErr error
repos:
	for n, repoFullName := range repos {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		if n > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, buildCompositeCursor(cursorTimestamps)); err != nil {
				logger.Error(ctx, "checkpoint failed (continuing)", zap.String("repo", repoFullName), zap.Error(err))
			}
		}
		workspace, repoSlug, err := splitFullName(repoFullName)
		if err != nil {
			logger.WarnWithCtx(ctx, "skipping invalid repo", zap.String("repo", repoFullName), zap.Error(err))
			continue
		}
		skipped.Attempt()

		highWater := getCompositeCursorTime(ctx, params.Cursor, repoFullName)
		since := firstSyncSince(highWater)
		latestDate, err := fetchReviewsForRepo(ctx, params.VendorGateway, params.Sink, op.OperationID(), params.Config.OrganizationID, workspace, repoSlug, repoFullName, since, highWater, activityCache, params.FetchCache, dirtyActivity, budget)
		if err != nil {
			// Flush whatever dirty entries accumulated across earlier repos before
			// propagating the error, so the next retry skips PRs we already paid for.
			flushPRActivityCache(ctx, params, dirtyActivity, CacheTypePRActivity)
			if skipped.Skip(repoFullName, err) {
				logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
				continue repos
			}
			return nil, fmt.Errorf("fetching reviews for repo: %w", err)
		}

		// See fetch_pr_comments: a zero watermark is "no activity ever observed", and
		// recording it would publish the synthetic floor in `since` as observed progress.
		if latestDate.IsZero() {
			logger.Info(ctx, "fetched reviews; no activity observed, watermark left unset", zap.String("repo", repoFullName))
		} else {
			cursorTimestamps[repoFullName] = latestDate
			logger.Info(ctx, "fetched reviews", zap.String("repo", repoFullName), zap.Time("cursor", latestDate))
		}
		// Per-repo backstop: fetchReviewsForRepo already flushed this repo's dirty
		// entries per page; this retries any a failed page-flush retained. No clear()
		// — putFetchCacheDirty clears on success, so force-clearing would drop
		// retained-for-retry entries (and diverge from fetch_prs/fetch_commits, whose
		// maps clear only on success).
		flushPRActivityCache(ctx, params, dirtyActivity, CacheTypePRActivity)
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), buildCompositeCursor(cursorTimestamps))
		}
		return buildCompositeCursor(cursorTimestamps), fmt.Errorf("%s: %w", op.OperationID(), errors.Join(ctxErr, cpErr))
	}
	if err := skipped.Err(len(repos)); err != nil {
		return nil, err
	}
	if len(cursorTimestamps) == 0 && !droppedAgedWatermark {
		return params.Cursor, nil
	}
	return buildCompositeCursor(cursorTimestamps), nil
}

// fetchReviewsForRepo fetches PR activity (approvals and comments) for a single repo.
// It iterates over PRs sorted by updated_on desc, then for each PR fetches activity entries.
//
// activityCache gates the per-PR /activity call, keyed "{workspace}/{repoSlug}#{prID}".
// The call is skipped only when the PR is unchanged since the cached probe AND that
// probe reached at least as far back as this run's window (see probeCoversWindow):
// /activity content is invariant unless the PR itself was touched, but a probe taken
// under a narrower window never read the earlier segment. A successful fetch records
// both bounds. The map is mutated in place; the caller is responsible for persisting it.
//
// Fetch-cache wiring: the page's PR keys are primed
// from connector_fetch_cache (fc) before the cache gate is consulted; a re-fetched
// PR is recorded into dirtyActivity for the end-of-run PutMany.
//
// highWater seeds the returned watermark for the same reason it does in
// fetchPRCommentsForRepo — see that function for why `since` must never be substituted.
func fetchReviewsForRepo(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, workspace, repoSlug, repoFullName string, since, highWater time.Time, activityCache map[string]prProbeEntry, fc sdk.FetchCache, dirtyActivity map[string]prProbeEntry, budget *pacing.PacingBudget) (time.Time, error) {
	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	latestDate := highWater

	// Step 1: Iterate PRs sorted by updated_on desc.
	prPage := int32(1)
	for {
		if prPage > maxListPages {
			return time.Time{}, errListPageCap("fetch PRs for reviews " + repoFullName)
		}
		protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketListPullRequestsRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			Page:      prPage,
			Pagelen:   50,
		}, budget, connectorID, opID, orgID)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch PRs for reviews %s: %w", repoFullName, err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListPullRequestsResponse)
		if !ok {
			return time.Time{}, fmt.Errorf("unexpected response type %T", protoResp)
		}

		// Pre-parse the page so the activity-cache prime batches one GetMany for
		// the page's PR keys before the cache gate is consulted. fetch_reviews
		// reads only parsed PR fields, so the raw payload is not retained here.
		pagePRs := make([]bbPullRequest, 0, len(resp.GetPullRequests()))
		for _, prRaw := range resp.GetPullRequests() {
			var pr bbPullRequest
			if err := json.Unmarshal(prRaw, &pr); err != nil {
				logger.WarnWithCtx(ctx, "fetch_reviews: skipping PR entry that failed to unmarshal",
					zap.String("repo", repoFullName), zap.Error(err))
				continue
			}
			pagePRs = append(pagePRs, pr)
		}
		keys := make([]string, 0, len(pagePRs))
		for _, pr := range pagePRs {
			if !since.IsZero() && pr.UpdatedOn.Before(since) {
				break
			}
			keys = append(keys, fmt.Sprintf("%s#%d", repoFullName, pr.ID))
		}
		primeFetchCache(ctx, fc, CacheTypePRActivity, keys, activityCache)

		prsDone := false
		for _, pr := range pagePRs {
			// Stop if we've reached PRs older than our cursor.
			if !since.IsZero() && pr.UpdatedOn.Before(since) {
				prsDone = true
				break
			}

			info := bbPRInfo{ID: pr.ID, CreatedOn: pr.CreatedOn}
			if pr.Author != nil {
				info.AuthorAccountID = pr.Author.AccountID
				info.AuthorDisplayName = pr.Author.DisplayName
			}
			if pr.Destination != nil && pr.Destination.Repository != nil {
				info.PlatformRepoID = pr.Destination.Repository.UUID
			}

			// Emit changes-requested reviews from participant state. Bitbucket
			// surfaces changes-requested only as a current participant state
			// (participants[].state), never as a timestamped /activity feed event.
			// It is therefore independent of the /activity watermark and must run
			// BEFORE the freshness gate below — otherwise a stalled review whose PR
			// stops receiving /activity (no new commits/comments) would leave its
			// changes-requested state permanently un-emitted. The record is
			// idempotent (deterministic externalID + stable dataDate), so emitting
			// it every run is a no-op upsert.
			if err := emitChangesRequestedReviews(ctx, sink, opID, orgID, repoFullName, info, pr); err != nil {
				return time.Time{}, fmt.Errorf("emitting changes-requested reviews: %w", err)
			}

			prKey := fmt.Sprintf("%s#%d", repoFullName, pr.ID)
			if cached, ok := activityCache[prKey]; ok && probeCoversWindow(ctx, cached, pr.UpdatedOn, since) {
				// PR unchanged since last /activity fetch, and that fetch already
				// covered this window — skip and save the bitbucket-rest token.
				// /activity content is invariant until pr.UpdatedOn advances.
				continue
			}

			// Step 2: Fetch activity for this PR.
			activityLatest, err := fetchPRActivity(ctx, restGW, sink, opID, orgID, workspace, repoSlug, repoFullName, info, since, budget)
			if err != nil {
				return time.Time{}, fmt.Errorf("fetching p r activity: %w", err)
			}

			probe := newPRProbeEntry(pr.UpdatedOn, since)
			activityCache[prKey] = probe
			dirtyActivity[prKey] = probe

			if activityLatest.After(latestDate) {
				latestDate = activityLatest
			}
		}

		// Per-page write-back (see putFetchCacheDirty). Matters most here:
		// fetch_reviews has no intermediate cursor checkpoint, so this watermark is
		// the sole guard against re-fetching a PR's paginated /activity next run.
		putFetchCacheDirty(ctx, fc, CacheTypePRActivity, dirtyActivity)

		if prsDone || resp.GetNextPageUrl() == "" {
			break
		}
		prPage++
	}

	return latestDate, nil
}

// watermarkPrecision is the granularity to which both the stored watermark and
// the compared UpdatedOn are floored. Store and compare MUST round to the same
// granularity or the cache silently never hits. Microsecond matches Bitbucket's
// source precision and Postgres timestamptz resolution, so the floor is lossless
// yet keeps the round-trip symmetric wherever the watermark is stored — a JSON
// string today, a timestamptz column if it ever moves there.
const watermarkPrecision = time.Microsecond

// encodeFetchWatermark renders a PR's UpdatedOn for the per-PR last-fetched cursor
// caches (pr_activity_last_fetched / pr_comments_last_fetched), floored to
// watermarkPrecision. Pairs with fetchWatermarkFresh — keep both on the same precision.
func encodeFetchWatermark(t time.Time) string {
	return t.UTC().Truncate(watermarkPrecision).Format(time.RFC3339Nano)
}

// fetchWatermarkFresh reports whether the cached last-fetched watermark is still
// current — the PR's UpdatedOn has not advanced past what we last fetched, so its
// /activity (resp. /comments) is unchanged and re-fetch can be skipped. Both sides
// are floored to watermarkPrecision so the encodeFetchWatermark round-trip is exact.
// Skipping is correct only while Bitbucket bumps pr.updated_on on every ingested PR
// activity — the same invariant fetch_prs's updated_on cursor already relies on. An
// unparseable cached value or a zero UpdatedOn returns false (refetch) so one bad
// value never poisons the cycle.
func fetchWatermarkFresh(ctx context.Context, cached string, prUpdatedOn time.Time) bool {
	if prUpdatedOn.IsZero() {
		return false
	}
	cachedAt, err := time.Parse(time.RFC3339, cached)
	if err != nil {
		logger.WarnWithCtx(ctx, "bitbucket: unparseable PR activity cache timestamp; treating as cache miss and refetching",
			zap.String("cached", cached), zap.Error(err))
		return false
	}
	return !cachedAt.Truncate(watermarkPrecision).Before(prUpdatedOn.Truncate(watermarkPrecision))
}

// flushPRActivityCache writes the run's dirty set to connector_fetch_cache
// (best-effort) for fetch_reviews and fetch_pr_comments.
// cacheType namespaces the entries in the table.
func flushPRActivityCache(ctx context.Context, params *sdk.ExecutionParams, dirty map[string]prProbeEntry, cacheType string) {
	putFetchCacheDirty(ctx, params.FetchCache, cacheType, dirty)
}

// prProbeEntry is what a per-PR probe of /activity (resp. /comments) records: the
// pr.UpdatedOn the probe was taken at, AND the lower bound of the window it actually
// captured.
//
// CoveredFrom is the load-bearing half. A probe emits only sub-resource entries at or
// after the run's `since`, so a bare watermark asserts a completeness the probe never
// had: a PR first probed under a narrow cursor yields only its recent comments, and the
// entry then suppresses every future probe until pr.UpdatedOn advances, making the
// earlier history unreachable rather than merely deferred. Recording the covered lower
// bound lets a later run whose window reaches further back detect that the earlier
// segment was never read and re-probe for it — which is also what makes a cursor rewind
// mean anything, since a rewind with a warm cache otherwise re-reads nothing.
//
// Every entry is the JSON object below. An entry that does not decode as one is
// skipped as unparseable, so the PR is re-probed.
type prProbeEntry struct {
	// Watermark is the encoded pr.UpdatedOn this probe was taken at.
	Watermark string `json:"w"`
	// CoveredFrom is the encoded lower bound of the window the probe captured.
	CoveredFrom string `json:"cf,omitempty"`
}

// newPRProbeEntry records a probe taken at prUpdatedOn that captured the window from
// since forward.
func newPRProbeEntry(prUpdatedOn, since time.Time) prProbeEntry {
	return prProbeEntry{
		Watermark:   encodeFetchWatermark(prUpdatedOn),
		CoveredFrom: encodeFetchWatermark(since),
	}
}

// probeCoversWindow reports whether a cached probe can stand in for a fresh one: the
// PR must be unchanged since the probe (fetchWatermarkFresh) AND the probe must have
// reached at least as far back as the window now being asked for. Failing either is a
// re-probe.
func probeCoversWindow(ctx context.Context, cached prProbeEntry, prUpdatedOn, since time.Time) bool {
	if !fetchWatermarkFresh(ctx, cached.Watermark, prUpdatedOn) {
		return false
	}
	coveredFrom, err := time.Parse(time.RFC3339, cached.CoveredFrom)
	if err != nil {
		logger.WarnWithCtx(ctx, "bitbucket: unparseable PR probe covered-from; treating as cache miss and refetching",
			zap.String("covered_from", cached.CoveredFrom), zap.Error(err))
		return false
	}
	// The probe is only reusable if it started at or before the window we now want. A
	// widened window (a rewind, a longer lookback) leaves an unread earlier segment.
	return !coveredFrom.After(since.Truncate(watermarkPrecision))
}

// bbPRInfo holds minimal PR metadata collected during the PR iteration phase,
// used by operations that need PR author context when processing sub-resources.
type bbPRInfo struct {
	ID                int
	AuthorAccountID   string
	AuthorDisplayName string
	// PlatformRepoID is the destination repository UUID (Bitbucket's stable repo ID).
	// Empty when the PR payload omits destination.repository.
	PlatformRepoID string
	// CreatedOn is the PR's creation time, emitted as the pr_created_at metadata key.
	CreatedOn time.Time
}

// detectIsBot returns "true" if the given Bitbucket user looks like a bot, else "false".
//
// Bitbucket's user payload exposes no clean bot flag — `type: "team"` indicates an
// organization account (a workspace/team), NOT a bot, so it must not be used here.
// Account-id pattern matching for known vendor bots is brittle. Until Atlassian
// surfaces a reliable signal, this helper conservatively returns "false" from
// vendor data alone. The org-level allow-list (BitbucketSettings.BotAccountIDs) is
// the override path, applied at a consumer layer.
//
// The signature takes *bbUser so callers can pass a possibly-nil pointer without
// extra nil checks at every call site.
func detectIsBot(_ *bbUser) string {
	return "false"
}

// fetchPRActivity fetches activity entries for a single PR and emits approval/comment records.
func fetchPRActivity(ctx context.Context, restGW sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, workspace, repoSlug, repoFullName string, prInfo bbPRInfo, since time.Time, budget *pacing.PacingBudget) (time.Time, error) {
	// Zero until an approval or comment is actually emitted — see fetchCommentsForPR for
	// why the caller needs "nothing observed" to be distinct from the query bound.
	var latestDate time.Time
	page := int32(1)
	prID := prInfo.ID

	for {
		if page > maxListPages {
			return time.Time{}, errListPageCap(fmt.Sprintf("fetch PR activity for %s#PR-%d", repoFullName, prID))
		}
		protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketListPRActivityRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			PrId:      int64(prID),
			Page:      page,
			Pagelen:   50, // Bitbucket activity API max is 50
		}, budget, connectorID, opID, orgID)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch PR activity for %s#PR-%d: %w", repoFullName, prID, err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListPRActivityResponse)
		if !ok {
			return time.Time{}, fmt.Errorf("unexpected response type %T", protoResp)
		}

		for _, activityRaw := range resp.GetActivities() {
			var activity bbActivity
			if err := json.Unmarshal(activityRaw, &activity); err != nil {
				logger.WarnWithCtx(ctx, "fetch_reviews: skipping PR activity entry that failed to unmarshal",
					zap.String("repo", repoFullName), zap.Int("pr_id", prID), zap.Error(err))
				continue
			}

			if activity.Approval != nil {
				// An approval is an event dated by when it happened. Without a date it
				// is skipped, as fetch_pr_comments skips an undated comment: dating it
				// at the fetch time would give it a new DataDate (a new row) every run.
				approvalDate, parseErr := time.Parse(time.RFC3339, activity.Approval.Date)
				if parseErr != nil || approvalDate.IsZero() {
					logger.WarnWithCtx(ctx, "fetch_reviews: skipping approval without a parseable date",
						zap.String("repo", repoFullName), zap.Int("pr_id", prID), zap.String("value", activity.Approval.Date), zap.Error(parseErr))
					continue
				}

				// Skip approvals older than cursor.
				if !since.IsZero() && approvalDate.Before(since) {
					continue
				}

				accountID := ""
				displayName := ""
				nickname := ""
				if activity.Approval.User != nil {
					accountID = activity.Approval.User.AccountID
					displayName = activity.Approval.User.DisplayName
					nickname = activity.Approval.User.Nickname
				}

				payload, marshalErr := json.Marshal(map[string]interface{}{
					"user": map[string]string{
						"account_id":   accountID,
						"display_name": displayName,
						"nickname":     nickname,
					},
					"state":      "APPROVED",
					metaKeyPRID:  fmt.Sprintf("%d", prID),
					"created_on": approvalDate.UTC().Format(time.RFC3339),
				})
				if marshalErr != nil {
					logger.WarnWithCtx(ctx, "fetch_reviews: skipping approval that failed to marshal",
						zap.String("repo", repoFullName), zap.Int("pr_id", prID), zap.Error(marshalErr))
					continue
				}

				// account_id is absent for some users (former users, app users);
				// the uuid is always present, so it keeps approvers distinct.
				approverKey := accountID
				if approverKey == "" && activity.Approval.User != nil {
					approverKey = activity.Approval.User.UUID
				}
				if approverKey == "" {
					// No identity to key on: every such approval would share one ExternalID.
					logger.WarnWithCtx(ctx, "fetch_reviews: skipping approval with no account id or uuid",
						zap.String("repo", repoFullName), zap.Int("pr_id", prID))
					continue
				}
				externalID := fmt.Sprintf("%s#PR-%d/approval-%s", repoFullName, prID, approverKey)
				meta := map[string]string{
					"repo":             repoFullName,
					"review_type":      "approval",
					metaKeyPRID:        fmt.Sprintf("%d", prID),
					"pr_key":           fmt.Sprintf("%s#PR-%d", repoFullName, prID),
					"pr_author_login":  prInfo.AuthorDisplayName,
					"pr_author_id":     prInfo.AuthorAccountID,
					"platform_repo_id": prInfo.PlatformRepoID,
					"pr_created_at":    formatPRCreatedAt(prInfo.CreatedOn),
					"reviewer_is_bot":  detectIsBot(activity.Approval.User),
				}

				if emitErr := emitRecord(ctx, sink, opID, orgID, externalID, payload, meta, approvalDate); emitErr != nil {
					return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}

				if approvalDate.After(latestDate) {
					latestDate = approvalDate
				}
			}

			if activity.Comment != nil {
				// Undated activity comments are skipped, like approvals above.
				commentDate := activity.Comment.CreatedOn
				if commentDate.IsZero() {
					logger.WarnWithCtx(ctx, "fetch_reviews: skipping activity comment without created_on",
						zap.String("repo", repoFullName), zap.Int("pr_id", prID), zap.Int64("comment_id", activity.Comment.ID))
					continue
				}

				// Skip comments older than cursor.
				if !since.IsZero() && commentDate.Before(since) {
					continue
				}

				accountID := ""
				displayName := ""
				nickname := ""
				if activity.Comment.User != nil {
					accountID = activity.Comment.User.AccountID
					displayName = activity.Comment.User.DisplayName
					nickname = activity.Comment.User.Nickname
				}

				payload, marshalErr := json.Marshal(map[string]interface{}{
					"user": map[string]string{
						"account_id":   accountID,
						"display_name": displayName,
						"nickname":     nickname,
					},
					"state":      "COMMENTED",
					metaKeyPRID:  fmt.Sprintf("%d", prID),
					"created_on": commentDate.UTC().Format(time.RFC3339),
				})
				if marshalErr != nil {
					logger.WarnWithCtx(ctx, "fetch_reviews: skipping comment that failed to marshal",
						zap.String("repo", repoFullName), zap.Int("pr_id", prID), zap.Error(marshalErr))
					continue
				}

				externalID := fmt.Sprintf("%s#PR-%d/comment-%d", repoFullName, prID, activity.Comment.ID)
				meta := map[string]string{
					"repo":             repoFullName,
					"review_type":      "comment",
					metaKeyPRID:        fmt.Sprintf("%d", prID),
					"pr_key":           fmt.Sprintf("%s#PR-%d", repoFullName, prID),
					"pr_author_login":  prInfo.AuthorDisplayName,
					"pr_author_id":     prInfo.AuthorAccountID,
					"platform_repo_id": prInfo.PlatformRepoID,
					"pr_created_at":    formatPRCreatedAt(prInfo.CreatedOn),
					"reviewer_is_bot":  detectIsBot(activity.Comment.User),
				}

				if emitErr := emitRecord(ctx, sink, opID, orgID, externalID, payload, meta, commentDate); emitErr != nil {
					return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
				}

				if commentDate.After(latestDate) {
					latestDate = commentDate
				}
			}
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
	}

	return latestDate, nil
}

// emitChangesRequestedReviews emits a CHANGES_REQUESTED review record for each PR
// participant currently in the "changes_requested" state.
//
// Unlike APPROVED / COMMENTED — which fetchPRActivity derives from timestamped
// /activity feed events — Bitbucket exposes changes-requested ONLY as a current
// participant state (participants[].state), with no per-event timestamp. The
// record therefore mirrors the APPROVED emission's payload/meta shape exactly so
// a consumer counts it: ParseReview reads payload.state,
// processReview switches on it into Reviews.ChangesRequested and appends the PR
// key to ChangesRequestedPRKeys (subject to the same bot / self-PR gates as the
// APPROVED path, which the shared meta keys satisfy).
//
// dataDate anchor: participant state carries no per-event timestamp (a Bitbucket
// API limitation), so the review is anchored to pr.CreatedOn — a STABLE per-PR
// value. pr.UpdatedOn advances on unrelated PR activity, and because data_date is
// part of the record uniqueness key, anchoring to it would insert a NEW row on
// every PR touch and double-count a single still-open changes-requested review.
// CreatedOn is invariant, so the deterministic externalID (one per account_id,
// with a /changes-requested- segment distinct from /approval-) + CreatedOn pair
// upserts to exactly one row → the review is counted once regardless of how many
// times the PR is re-synced. A PR without CreatedOn cannot be stably anchored, so
// it is skipped rather than emitted as an un-dedupable record.
//
// Accepted limitation: this dates the review to the PR's creation day, not the day
// changes were actually requested, so on a PR opened before a rolling window the
// review can fall outside "recent" views. Event-proximate dating is not achievable
// here — Bitbucket exposes no per-event timestamp for participant state, and a
// durable first-observed store is unavailable (the fetch cache is best-effort, so a
// read miss would re-stamp and double-count; connectors cannot read the record store to
// dedupe). Correctness of the count is preferred over date precision.
func emitChangesRequestedReviews(ctx context.Context, sink sdk.DataSink, opID sdk.OperationID, orgID, repoFullName string, prInfo bbPRInfo, pr bbPullRequest) error {
	prID := prInfo.ID

	if pr.CreatedOn.IsZero() {
		logger.WarnWithCtx(ctx, "fetch_reviews: skipping changes-requested emit — PR has no created_on to stably anchor dataDate",
			zap.String("repo", repoFullName), zap.Int("pr_id", prID))
		return nil
	}
	anchor := pr.CreatedOn

	for i := range pr.Participants {
		p := pr.Participants[i]
		if !strings.EqualFold(p.State, "changes_requested") {
			continue
		}

		accountID := ""
		displayName := ""
		nickname := ""
		if p.User != nil {
			accountID = p.User.AccountID
			displayName = p.User.DisplayName
			nickname = p.User.Nickname
		}

		payload, marshalErr := json.Marshal(map[string]interface{}{
			"user": map[string]string{
				"account_id":   accountID,
				"display_name": displayName,
				"nickname":     nickname,
			},
			"state":      "CHANGES_REQUESTED",
			metaKeyPRID:  fmt.Sprintf("%d", prID),
			"created_on": anchor.UTC().Format(time.RFC3339),
		})
		if marshalErr != nil {
			logger.WarnWithCtx(ctx, "fetch_reviews: skipping changes-requested participant that failed to marshal",
				zap.String("repo", repoFullName), zap.Int("pr_id", prID), zap.Error(marshalErr))
			continue
		}

		// Same fallback as approvals: account_id is absent for some users (former
		// users, app users), and the uuid keeps their reviews distinct.
		reviewerKey := accountID
		if reviewerKey == "" && p.User != nil {
			reviewerKey = p.User.UUID
		}
		if reviewerKey == "" {
			logger.WarnWithCtx(ctx, "fetch_reviews: skipping changes-requested participant with no account id or uuid",
				zap.String("repo", repoFullName), zap.Int("pr_id", prID))
			continue
		}
		externalID := fmt.Sprintf("%s#PR-%d/changes-requested-%s", repoFullName, prID, reviewerKey)
		meta := map[string]string{
			"repo":             repoFullName,
			"review_type":      "changes_requested",
			metaKeyPRID:        fmt.Sprintf("%d", prID),
			"pr_key":           fmt.Sprintf("%s#PR-%d", repoFullName, prID),
			"pr_author_login":  prInfo.AuthorDisplayName,
			"pr_author_id":     prInfo.AuthorAccountID,
			"platform_repo_id": prInfo.PlatformRepoID,
			"pr_created_at":    formatPRCreatedAt(prInfo.CreatedOn),
			"reviewer_is_bot":  detectIsBot(p.User),
		}

		if emitErr := emitRecord(ctx, sink, opID, orgID, externalID, payload, meta, anchor); emitErr != nil {
			return &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
		}
	}
	return nil
}

// --- FetchPRCommentsOp ---

const opFetchPRComments = "fetch_pr_comments"

type FetchPRCommentsOp struct{}

func (op *FetchPRCommentsOp) OperationID() sdk.OperationID { return opFetchPRComments }

func (op *FetchPRCommentsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	// One pacing budget for the whole run (repo discovery, all repos, all per-PR
	// comment calls share it) so a local-limit rejection is waited out, not failed
	// on. See the same construction in FetchPRsOp.Execute; created before repo
	// discovery so the discovery listing is paced too.
	budget := &pacing.PacingBudget{}

	repos, err := fetchAllRepos(ctx, params.VendorGateway, settings, op.OperationID(), params.Config.OrganizationID, budget)
	if err != nil {
		return nil, fmt.Errorf("fetching all repos: %w", err)
	}

	// The comments cache is primed per page from connector_fetch_cache; the dirty
	// set is the PRs whose /comments was actually re-fetched this run.
	commentsCache := make(map[string]prProbeEntry)
	dirtyComments := make(map[string]prProbeEntry)

	cursorTimestamps, droppedAgedWatermark := carryForwardCursorTimes(ctx, params.Cursor, carryableRepoKeys(settings, repos))

	var skipped repotally.Skipper
	var ctxErr error
repos:
	for n, repoFullName := range repos {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		if n > 0 && params.Checkpoint != nil {
			if err := params.Checkpoint(ctx, buildCompositeCursor(cursorTimestamps)); err != nil {
				logger.Error(ctx, "checkpoint failed (continuing)", zap.String("repo", repoFullName), zap.Error(err))
			}
		}
		workspace, repoSlug, err := splitFullName(repoFullName)
		if err != nil {
			logger.WarnWithCtx(ctx, "skipping invalid repo", zap.String("repo", repoFullName), zap.Error(err))
			continue
		}
		skipped.Attempt()

		highWater := getCompositeCursorTime(ctx, params.Cursor, repoFullName)
		since := firstSyncSince(highWater)
		latestDate, err := fetchPRCommentsForRepo(ctx, params.VendorGateway, params.Sink, op.OperationID(), params.Config.OrganizationID, workspace, repoSlug, repoFullName, since, highWater, commentsCache, params.FetchCache, dirtyComments, budget)
		if err != nil {
			flushPRActivityCache(ctx, params, dirtyComments, CacheTypePRComments)
			if skipped.Skip(repoFullName, err) {
				logger.WarnWithCtx(ctx, "skipping a repository that failed", zap.String("repo", repoFullName), zap.Error(err))
				continue repos
			}
			return nil, fmt.Errorf("fetching p r comments for repo: %w", err)
		}

		// A zero watermark means this repo has never yielded a comment, so it has no
		// observed progress to record. Leaving the entry absent is what keeps the
		// synthetic floor in `since` out of the cursor; writing it would publish a query
		// bound as an observation and pin the repo there permanently.
		if latestDate.IsZero() {
			logger.Info(ctx, "fetched pr comments; no comment observed, watermark left unset", zap.String("repo", repoFullName))
		} else {
			cursorTimestamps[repoFullName] = latestDate
			logger.Info(ctx, "fetched pr comments", zap.String("repo", repoFullName), zap.Time("cursor", latestDate))
		}
		// Per-repo backstop; no clear() — see fetch_reviews. Runs whether or not a
		// watermark was recorded: the probes this run paid for are worth keeping either way.
		flushPRActivityCache(ctx, params, dirtyComments, CacheTypePRComments)
	}

	if ctxErr != nil {
		// The context ended: save what was read, because a failed run's
		// returned cursor is not stored.
		var cpErr error
		if params.Checkpoint != nil {
			cpErr = params.Checkpoint(context.WithoutCancel(ctx), buildCompositeCursor(cursorTimestamps))
		}
		return buildCompositeCursor(cursorTimestamps), fmt.Errorf("%s: %w", op.OperationID(), errors.Join(ctxErr, cpErr))
	}
	if err := skipped.Err(len(repos)); err != nil {
		return nil, err
	}
	if len(cursorTimestamps) == 0 && !droppedAgedWatermark {
		return params.Cursor, nil
	}
	return buildCompositeCursor(cursorTimestamps), nil
}

// fetchPRCommentsForRepo fetches PR comments for a single repo using the dedicated comments API.
// It iterates over PRs sorted by updated_on desc, then calls /comments for each PR
// whose cached probe cannot stand in for a fresh one — a changed updated_on, no cache
// entry, or a probe that did not reach as far back as this run's window (see
// probeCoversWindow). commentsCache is keyed by "{workspace}/{repoSlug}#{prID}" → the
// probe entry recording both bounds. Mutated in place; the caller persists.
//
// Fetch-cache wiring: the page's PR keys are primed
// from connector_fetch_cache (fc) before the cache gate is consulted; a re-fetched
// PR is recorded into dirtyComments for the end-of-run PutMany.
//
// highWater is the caller's stored watermark and seeds the returned one; `since` is only
// the query bound and must never be substituted for it. A repo that emits no comment
// returns highWater unchanged — zero when it has never advanced — so "nothing observed"
// stays distinguishable from progress. Seeding from `since` instead persists a query
// bound as though vendor data carried it, and on a first sync that bound is a synthetic
// now-minus-lookback floor: the next run reads it back as its own floor, a warm probe
// cache skips every PR at the freshness gate so nothing re-fetches, and the floor is
// rewritten unchanged. That reproduces itself on every tick, pinning the entry a full
// lookback in the past for good.
func fetchPRCommentsForRepo(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, workspace, repoSlug, repoFullName string, since, highWater time.Time, commentsCache map[string]prProbeEntry, fc sdk.FetchCache, dirtyComments map[string]prProbeEntry, budget *pacing.PacingBudget) (time.Time, error) {
	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	latestDate := highWater

	// A page is parsed once so the comments-cache prime can batch one GetMany for
	// the page's PR keys before the cache gate is consulted. Per-PR comment
	// emission still happens inline (bounded memory).
	prPage := int32(1)
	for {
		if prPage > maxListPages {
			return time.Time{}, errListPageCap("fetch PRs for comments " + repoFullName)
		}
		protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketListPullRequestsRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			Page:      prPage,
			Pagelen:   50,
		}, budget, connectorID, opID, orgID)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch PRs for comments %s: %w", repoFullName, err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListPullRequestsResponse)
		if !ok {
			return time.Time{}, fmt.Errorf("unexpected response type %T", protoResp)
		}

		pagePRs := make([]bbPullRequest, 0, len(resp.GetPullRequests()))
		for _, prRaw := range resp.GetPullRequests() {
			var pr bbPullRequest
			if err := json.Unmarshal(prRaw, &pr); err != nil {
				logger.WarnWithCtx(ctx, "fetch_pr_comments: skipping PR entry that failed to unmarshal",
					zap.String("repo", repoFullName), zap.Error(err))
				continue
			}
			pagePRs = append(pagePRs, pr)
		}
		keys := make([]string, 0, len(pagePRs))
		for _, pr := range pagePRs {
			if !since.IsZero() && pr.UpdatedOn.Before(since) {
				break
			}
			keys = append(keys, fmt.Sprintf("%s#%d", repoFullName, pr.ID))
		}
		primeFetchCache(ctx, fc, CacheTypePRComments, keys, commentsCache)

		prsDone := false
		for _, pr := range pagePRs {
			// Stop if we've reached PRs older than our cursor.
			if !since.IsZero() && pr.UpdatedOn.Before(since) {
				prsDone = true
				break
			}

			info := bbPRInfo{ID: pr.ID, CreatedOn: pr.CreatedOn}
			if pr.Author != nil {
				info.AuthorAccountID = pr.Author.AccountID
				info.AuthorDisplayName = pr.Author.DisplayName
			}
			if pr.Destination != nil && pr.Destination.Repository != nil {
				info.PlatformRepoID = pr.Destination.Repository.UUID
			}

			prKey := fmt.Sprintf("%s#%d", repoFullName, pr.ID)
			if cached, ok := commentsCache[prKey]; ok && probeCoversWindow(ctx, cached, pr.UpdatedOn, since) {
				continue
			}

			commentLatest, err := fetchCommentsForPR(ctx, restGW, sink, opID, orgID, workspace, repoSlug, repoFullName, info, since, budget)
			if err != nil {
				return time.Time{}, fmt.Errorf("fetching comments for PR: %w", err)
			}
			probe := newPRProbeEntry(pr.UpdatedOn, since)
			commentsCache[prKey] = probe
			dirtyComments[prKey] = probe

			if commentLatest.After(latestDate) {
				latestDate = commentLatest
			}
		}

		// Per-page write-back, same rationale as fetch_reviews: no intermediate
		// cursor checkpoint, so this watermark is the sole re-fetch guard.
		putFetchCacheDirty(ctx, fc, CacheTypePRComments, dirtyComments)

		if prsDone || resp.GetNextPageUrl() == "" {
			break
		}
		prPage++
	}

	return latestDate, nil
}

// fetchCommentsForPR fetches comments for a single PR and emits one record per comment.
func fetchCommentsForPR(ctx context.Context, restGW sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, workspace, repoSlug, repoFullName string, pr bbPRInfo, since time.Time, budget *pacing.PacingBudget) (time.Time, error) {
	// Zero until a comment is actually emitted, so the zero return means "this PR
	// carried no comment in the window" rather than `since`. The caller folds this in
	// with .After() against its own watermark, and a `since`-seeded return would push
	// that watermark up to the query bound on a PR that yielded nothing.
	var latestDate time.Time
	page := int32(1)

	for {
		if page > maxListPages {
			return time.Time{}, errListPageCap(fmt.Sprintf("fetch PR comments for %s#PR-%d", repoFullName, pr.ID))
		}
		protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, restGW, &connectorpb.BitbucketListPRCommentsRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			PrId:      int64(pr.ID),
			Page:      page,
			Pagelen:   50,
		}, budget, connectorID, opID, orgID)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch PR comments for %s#PR-%d: %w", repoFullName, pr.ID, err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListPRCommentsResponse)
		if !ok {
			return time.Time{}, fmt.Errorf("unexpected response type %T", protoResp)
		}

		for _, commentRaw := range resp.GetComments() {
			var comment bbComment
			if err := json.Unmarshal(commentRaw, &comment); err != nil {
				logger.WarnWithCtx(ctx, "fetch_pr_comments: skipping comment entry that failed to unmarshal",
					zap.String("repo", repoFullName), zap.Int("pr_id", pr.ID), zap.Error(err))
				continue
			}

			commentDate := comment.CreatedOn
			// Skip comments with missing timestamps — using time.Now() would cause
			// them to be re-processed on every sync run.
			if commentDate.IsZero() {
				continue
			}

			// Skip comments older than cursor.
			if !since.IsZero() && commentDate.Before(since) {
				continue
			}

			externalID := fmt.Sprintf("%s/%s#PR-%d/COMMENT-%d", workspace, repoSlug, pr.ID, comment.ID)
			meta := map[string]string{
				"repo":                   repoFullName,
				metaKeyPRID:              strconv.FormatInt(int64(pr.ID), 10),
				"pr_author_account_id":   pr.AuthorAccountID,
				"pr_author_display_name": pr.AuthorDisplayName,
				"pr_author_login":        pr.AuthorDisplayName,
				"pr_author_id":           pr.AuthorAccountID,
				"platform_repo_id":       pr.PlatformRepoID,
				"pr_created_at":          formatPRCreatedAt(pr.CreatedOn),
				"reviewer_is_bot":        detectIsBot(comment.User),
			}

			if emitErr := emitRecord(ctx, sink, opID, orgID, externalID, commentRaw, meta, commentDate); emitErr != nil {
				return time.Time{}, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}

			if commentDate.After(latestDate) {
				latestDate = commentDate
			}
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
	}

	return latestDate, nil
}
