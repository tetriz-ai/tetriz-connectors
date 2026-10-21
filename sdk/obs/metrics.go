// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package obs

import (
	"github.com/prometheus/client_golang/prometheus"
)

// VendorPermissionDeniedTotal counts vendor responses that look like a
// per-org permission gap rather than a transient failure — typically
// HTTP 403 (forbidden) or 406 (not acceptable, which GitHub returns when
// the App installation lacks the scope required for a media type).
//
// Labels are bounded: connector_id ~ 10, operation_id ~ low tens per
// connector, status_code ~ 5, org_id ~ connected organizations. The (connector,
// operation, status) combinations show systemic gaps; the
// org_id label names the installation that needs re-consent.
var VendorPermissionDeniedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_vendor_permission_denied_total",
		Help: "Count of vendor responses indicating a per-org permission gap (403/406). Sustained non-zero rate for any (org, connector) means the installation is missing scope and the connector cannot make progress — an admin must re-consent / re-install the vendor app.",
	},
	[]string{"connector_id", "operation_id", "status_code", "org_id"},
)

// HealthStateTransitionsTotal counts connector health-state transitions
// (healthy <-> auth_revoked). Shared between orchestrator (which drives
// healthy->auth_revoked on a Terminal auth failure) and integrations (which
// drives auth_revoked->healthy on a fresh credential write or disconnect) so
// both directions of the state machine emit the same series. Labels bounded:
// connector_id ~ 10, to = 2, org_id ~ connected organizations.
var HealthStateTransitionsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_health_state_transitions_total",
		Help: "Total connector health-state transitions by connector and target state.",
	},
	[]string{"org_id", "connector_id", "to"},
)

// DisconnectsTotal counts connectors removed through the DisconnectIntegration
// RPC, once per successful config soft-delete. After a disconnect the sync and
// health series go quiet, so this is the one series that records the removal
// and which (org, connector) it was. Labels are bounded: connector_id ~ 10,
// org_id ~ connected organizations (individual tenants collapsed via
// metrictenant.OrgID, matching HealthStateTransitionsTotal).
//
// Its children are not zero-seeded: a child is born at 1 on its first
// disconnect, which increase() over a window cannot read as a rise, so a query
// on it compares totals across a window and also matches a new series.
var DisconnectsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_disconnects_total",
		Help: "Count of connectors removed via a user-initiated DisconnectIntegration (one per successful config soft-delete). A removal stops all sync runs, so no other connector series signals it — this is the only positive edge it emits.",
	},
	[]string{"org_id", "connector_id"},
)

// Direction values for HealthWriteFailuresTotal — which best-effort health-state
// write was lost. Exported because the two emit sites live in different packages
// (orchestrator drives the revoke, integrations drives the reset).
const (
	HealthWriteRevoke = "revoke"
	HealthWriteReset  = "reset"
)

// HealthWriteFailuresTotal counts best-effort connector health-state writes that
// did not land, by direction. Both directions of the state machine gate on the
// PERSISTED health state, so a lost write does not merely lose a metric — it loses
// the state change itself, and because HealthStateTransitionsTotal is only touched
// once a write succeeds, no transition is recorded and nothing else can tell a lost
// write from a landed one (the sync-error series looks identical either way).
//
// direction="revoke": a Terminal auth failure whose auth_revoked write was lost, so
// the connector is never paused and keeps burning one doomed vendor call per tick,
// ingesting nothing. direction="reset": a reconnect whose auth_revoked->healthy
// write was lost, so the connector stays gated even though the customer has already
// supplied a fresh credential, and every scheduled sync keeps being skipped.
//
// Expected value is zero in both directions, so any increment is actionable. Mirrors
// HealthStateTransitionsTotal's one-vec-per-state-machine shape. Labels bounded:
// connector_id ~ 10, direction = 2, org_id ~ connected organizations (individual tenants
// collapsed via metrictenant.OrgID).
var HealthWriteFailuresTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_health_write_failures_total",
		Help: "Count of best-effort connector health-state writes that failed, by direction. revoke = the connector was never paused after a dead credential; reset = it stayed gated after the customer reconnected. Either way that (org, connector) makes no progress until someone intervenes.",
	},
	[]string{"org_id", "connector_id", "direction"},
)

// FetchCacheOversizedSkippedTotal counts fetch-cache entries skipped because
// their serialized value exceeded the per-row size cap.
// It is the guardrail's observability signal: an oversized value is dropped
// and re-fetched next cycle (correctness preserved, only memoization lost), and
// this counter surfaces that it happened so it can be investigated rather than
// silently caching an ever-growing value. A sustained non-zero rate for a
// (connector, cache_type) flags a pathological entry (e.g. a giant-file PR) — the
// signal that the deferred pr_diffstat reshape has become worth doing. Labels are
// bounded: connector_id ~ 10, cache_type ~ low units per connector, org_id ~ connected organizations.
var FetchCacheOversizedSkippedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_fetch_cache_oversized_skipped_total",
		Help: "Count of fetch-cache entries skipped because the serialized value exceeded the per-row size cap. Nonzero rate flags a pathological cache value (a giant-file PR); the entry is re-fetched each cycle, so correctness holds but memoization is lost for it.",
	},
	[]string{"connector_id", "cache_type", "org_id"},
)

// fetch-cache read outcome label values (the "result" dimension of FetchCacheReadTotal).
const (
	fetchCacheResultHit  = "hit"
	fetchCacheResultMiss = "miss"
)

// FetchCacheReadTotal counts fetch-cache key lookups split by hit/miss — the
// connector-fetch-cache hit/miss signal. A key present in the table is a hit; a
// requested key with no row is a miss. Once a cache_type is warm the two
// `_last_fetched` cache_types run at a near-100% hit share; a sustained cold-miss
// share instead means the table is not being warmed for that cache_type. Labels are
// bounded: connector_id ~ 10, cache_type ~ low units per connector, result = 2,
// org_id ~ connected organizations (collapsed for individual tenants via metrictenant.OrgID).
var FetchCacheReadTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_fetch_cache_read_total",
		Help: "Count of fetch-cache key lookups labelled hit/miss. Once warm the two _last_fetched cache_types run at a near-100% hit share; a sustained cold-miss share means the table is not being warmed for that cache_type.",
	},
	[]string{"connector_id", "cache_type", "result", "org_id"},
)

// FetchCacheRowsWrittenTotal counts rows the cache write actually persisted — the
// DAO's reported RowsAffected, NOT the entries offered. The upsert's
// `value IS DISTINCT FROM EXCLUDED.value` guard skips a re-write of an unchanged
// entry (to preserve updated_at for age-based eviction), so this counter measures
// genuine cache churn: a warm cache trends its write rate toward zero even while
// every cycle offers the same keys. Labels bounded as above (no result label).
var FetchCacheRowsWrittenTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_fetch_cache_rows_written_total",
		Help: "Count of fetch-cache rows the cache write actually persisted (DAO RowsAffected — the value-guarded upsert skips unchanged rows). Reflects genuine churn; a warm cache trends toward zero writes even as it keeps offering the same keys.",
	},
	[]string{"connector_id", "cache_type", "org_id"},
)

// FetchCacheRows is the table-growth gauge: rows currently held per
// (org, connector, cache_type). It is refreshed on the connector's own write
// cadence (Set on each cache-write cycle), so a write-idle org's series holds its
// last value rather than disappearing — enough to see when a cache_type grows
// past the point where it should be pruned. Labels bounded as
// above. Set (not Add) per real org; individual-tenant collapse (metrictenant.OrgID)
// is a non-issue here as the connectors that populate this cache are org-scoped.
var FetchCacheRows = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "connector_fetch_cache_rows",
		Help: "Rows currently held in connector_fetch_cache per (org, connector, cache_type), refreshed on each cache-write cycle. Sustained growth past the expected working-set size is the signal to run a host's eviction job.",
	},
	[]string{"connector_id", "cache_type", "org_id"},
)

// RecordFetchCacheRead records a batch lookup as its hit and miss counts against
// FetchCacheReadTotal. Callers pass the already-collapsed org label
// (metrictenant.OrgID). Zero counts are skipped so an all-hit or all-miss batch
// does not create an idle zero-valued child series.
func RecordFetchCacheRead(connectorID, cacheType, orgID string, hits, misses int) {
	if hits > 0 {
		FetchCacheReadTotal.WithLabelValues(connectorID, cacheType, fetchCacheResultHit, orgID).Add(float64(hits))
	}
	if misses > 0 {
		FetchCacheReadTotal.WithLabelValues(connectorID, cacheType, fetchCacheResultMiss, orgID).Add(float64(misses))
	}
}

// fileContentOutcome* are the terminal-branch outcome label values of a fetch_file_content
// artifact (the "outcome" dimension of HarnessFileContentArtifactsTotal). Unexported: the
// vocabulary is not a cross-package API, so other packages reach a specific outcome only
// through its RecordFileContent* helper below, never by passing a raw string. A deferred_*
// value means the whole artifact was skipped this run (never a partial record) and the
// work-list re-offers it next run. deferred_budget counts per-vendor budget pressure; it is
// kept DISTINCT from deferred_budget_error (a budget-subsystem failure, e.g. the limiter's
// backing store) so a rare infra error never inflates the budget-pressure count.
const (
	fileContentOutcomeEmitted              = "emitted"
	fileContentOutcomeTombstone            = "tombstone"
	fileContentOutcomeDeferredBudget       = "deferred_budget"
	fileContentOutcomeDeferredBudgetErr    = "deferred_budget_error"
	fileContentOutcomeDeferredNotFound     = "deferred_not_found"
	fileContentOutcomeDeferredFetchError   = "deferred_fetch_error"
	fileContentOutcomeDeferredPathEscape   = "deferred_path_escape"
	fileContentOutcomeDeferredStagingWrite = "deferred_staging_write"
	fileContentOutcomeDeferredBadRepo      = "deferred_bad_repo"
	fileContentOutcomeDeferredEmitErr      = "deferred_emit_error"
)

// HarnessFileContentFilesFetchedTotal counts individual files whose body was fetched
// and staged by fetch_file_content — one increment per file after its staging write
// succeeds. The connector label (string(connectorID)) gives the per-vendor "files
// fetched per run" throughput signal; it mirrors treewalker's RecordTreeFetched shape
// so the two VCS content paths share a labelling convention. Labels bounded: connector
// ~ 3 (the VCS vendors).
var HarnessFileContentFilesFetchedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_harness_file_content_files_fetched_total",
		Help: "Files fetched and staged by fetch_file_content, by connector — one per file after a successful staging write.",
	},
	[]string{"connector"},
)

// HarnessFileContentArtifactsTotal counts fetch_file_content artifacts by connector and
// terminal outcome; each outcome is recorded through its RecordFileContent* helper below.
// Invariant: exactly one increment per artifact per run — every terminal branch (including
// the emit-failure paths, recorded as deferred_emit_error) records exactly one outcome. The
// connector label isolates the signal per vendor because each vendor draws on its own
// rate-limit bucket, so a shared outcome value stays legible per vendor. Labels bounded:
// connector ~ 3, outcome ~ 10.
var HarnessFileContentArtifactsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_harness_file_content_artifacts_total",
		Help: "fetch_file_content artifacts by connector and terminal outcome (emitted|tombstone|deferred_*). A rising deferred_budget share on one connector is the per-vendor budget-pressure signal.",
	},
	[]string{"connector", "outcome"},
)

// RecordFileContentFetched increments the per-file fetch counter for the connector.
func RecordFileContentFetched(connectorID string) {
	HarnessFileContentFilesFetchedTotal.WithLabelValues(connectorID).Inc()
}

// recordFileContentArtifact increments the per-artifact outcome counter. Private so the
// outcome vocabulary is reachable from other packages only via the RecordFileContent*
// helpers below — a caller cannot pass an arbitrary (forgeable) outcome string.
func recordFileContentArtifact(connectorID, outcome string) {
	HarnessFileContentArtifactsTotal.WithLabelValues(connectorID, outcome).Inc()
}

// RecordFileContentEmitted records one artifact whose record was successfully emitted.
func RecordFileContentEmitted(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeEmitted)
}

// RecordFileContentTombstone records one tombstone artifact re-emitted (no body fetch).
func RecordFileContentTombstone(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeTombstone)
}

// RecordFileContentDeferredBudget records one artifact deferred by the budget floor, the
// per-vendor budget-pressure count.
func RecordFileContentDeferredBudget(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeDeferredBudget)
}

// RecordFileContentDeferredBudgetErr records one artifact deferred by a budget-subsystem
// failure (kept distinct from deferred_budget so infra errors never inflate that count).
func RecordFileContentDeferredBudgetErr(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeDeferredBudgetErr)
}

// RecordFileContentDeferredNotFound records one artifact deferred because a file was gone
// from the ref (404 — self-heals once the tree reconciles).
func RecordFileContentDeferredNotFound(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeDeferredNotFound)
}

// RecordFileContentDeferredFetchError records one artifact deferred by a transient or
// unexpected file-fetch error.
func RecordFileContentDeferredFetchError(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeDeferredFetchError)
}

// RecordFileContentDeferredPathEscape records one artifact deferred because a resolved path
// escaped the org staging prefix (malformed input).
func RecordFileContentDeferredPathEscape(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeDeferredPathEscape)
}

// RecordFileContentDeferredStagingWrite records one artifact deferred by a staging-write
// failure.
func RecordFileContentDeferredStagingWrite(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeDeferredStagingWrite)
}

// RecordFileContentDeferredBadRepo records one artifact deferred because its repo identity
// could not be resolved (bad full name / empty platform repo id).
func RecordFileContentDeferredBadRepo(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeDeferredBadRepo)
}

// RecordFileContentDeferredEmitErr records one artifact deferred by a sink/emit failure, so
// an emit failure is visible on the outcome counter rather than silently dropped.
func RecordFileContentDeferredEmitErr(connectorID string) {
	recordFileContentArtifact(connectorID, fileContentOutcomeDeferredEmitErr)
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		VendorPermissionDeniedTotal,
		HealthStateTransitionsTotal,
		DisconnectsTotal,
		HealthWriteFailuresTotal,
		FetchCacheOversizedSkippedTotal,
		FetchCacheReadTotal,
		FetchCacheRowsWrittenTotal,
		FetchCacheRows,
		HarnessFileContentFilesFetchedTotal,
		HarnessFileContentArtifactsTotal,
		CursorRegressionsTotal,
		CursorOldestEntryAgeSeconds,
	}
}
