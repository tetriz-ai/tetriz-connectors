// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// fallbackMaxCacheValueBytes is the fail-closed per-row size cap used only when
// the wiring passes a non-positive cap (misconfig). The cap is a guardrail: an
// oversized value is dropped and counted (fetch_cache_oversized_skipped) so we
// notice and can investigate, rather than silently caching an ever-growing
// value. The ceiling is tunable (a downstream transport's message limit is one
// consideration); this conservative default just ensures the guard is never off.
const fallbackMaxCacheValueBytes = 256 * 1024

// fetchCacheAdapter adapts IFetchCacheDAO to the sdk.FetchCache capability.
// It binds the (org, connector) scope + the per-row size cap, establishes tenant
// scope at the boundary (so the DAO's tenantdb.DB sees the right app.current_tenant
// GUC — same reason configUpdaterAdapter does), and enforces the best-effort
// contract centrally so no connector has to: a store error degrades (never fails
// the op), and an oversized value is skipped + counted rather than written.
type fetchCacheAdapter struct {
	dao           store.IFetchCacheDAO
	orgID         string
	connectorID   sdk.ConnectorID
	maxValueBytes int
}

// GetMany resolves the keys from the store, degrading a store failure to a full
// miss (the op re-fetches from the vendor). Never returns a hard error — a cache
// read must not fail the sync.
func (a *fetchCacheAdapter) GetMany(ctx context.Context, cacheType string, entryKeys []string) (map[string]json.RawMessage, error) {
	var result map[string]json.RawMessage
	err := tenantdb.RunInScope(ctx, a.orgID, tenantdb.ScopeEnforced, func(sCtx context.Context) error {
		var innerErr error
		result, innerErr = a.dao.GetMany(sCtx, a.orgID, a.connectorID, cacheType, entryKeys)
		if innerErr != nil {
			return fmt.Errorf("fetch cache get: %w", innerErr)
		}
		return nil
	})
	if err != nil {
		// Warn (ctx-aware), not Error: a cache-read failure is expected best-effort
		// degradation (the op re-fetches), so it must not count as an error.
		// A degraded read is NOT counted as a miss — it is a store failure, not a
		// cold key, and counting it would inflate the cold-miss signal.
		logger.WarnWithCtx(ctx, "fetch cache read failed, degrading to miss",
			zap.String(logger.ConnectorID, string(a.connectorID)),
			zap.String("cache_type", cacheType),
			zap.Error(err),
		)
		return map[string]json.RawMessage{}, nil
	}
	// Record the batch's hit/miss split: a resolved key is a hit, a requested key
	// with no row is a miss (the connector-fetch-cache hit/miss signal).
	hits := len(result)
	obs.RecordFetchCacheRead(string(a.connectorID), cacheType, tenantlabel.OrgID(a.orgID), hits, len(entryKeys)-hits)
	return result, nil
}

// PutMany skips any entry whose serialized value exceeds the cap (counting it, so
// oversized values are surfaced for investigation rather than silently cached) and
// drops a store failure. Never returns a hard error — a cache write is best-effort.
func (a *fetchCacheAdapter) PutMany(ctx context.Context, cacheType string, entries map[string]json.RawMessage) error {
	if len(entries) == 0 {
		return nil
	}
	// Fail closed: a cap of 0 (a mis-wired/unset value) must NOT disable the guard —
	// disabling it would silently cache an unbounded value. The guard surfaces
	// oversized values (dropped + counted) so we notice and can investigate; the
	// configured cap (S3 wiring, default 256 KB) normally supersedes this fallback.
	maxBytes := a.maxValueBytes
	if maxBytes <= 0 {
		maxBytes = fallbackMaxCacheValueBytes
	}
	kept := make(map[string]json.RawMessage, len(entries))
	var oversized []string
	for key, value := range entries {
		if len(value) > maxBytes {
			// tenantlabel.OrgID collapses individual tenants to a single bucket so an
			// org_id-labelled metric cannot explode Prometheus cardinality per tenant.
			obs.FetchCacheOversizedSkippedTotal.WithLabelValues(
				string(a.connectorID), cacheType, tenantlabel.OrgID(a.orgID),
			).Inc()
			oversized = append(oversized, key)
			continue
		}
		kept[key] = value
	}
	var written int64
	if err := tenantdb.RunInScope(ctx, a.orgID, tenantdb.ScopeEnforced, func(sCtx context.Context) error {
		if len(kept) > 0 {
			n, putErr := a.dao.PutMany(sCtx, a.orgID, a.connectorID, cacheType, kept)
			if putErr != nil {
				return fmt.Errorf("fetch cache put: %w", putErr)
			}
			written = n
		}
		// An oversized value is not cached; tombstone any prior row for those keys so a
		// stale value is not served as a hit — the guard promises a re-fetch, not stale
		// memoized metadata (e.g. after the cap is lowered, or a value grows past it).
		if len(oversized) > 0 {
			if delErr := a.dao.DeleteMany(sCtx, a.orgID, a.connectorID, cacheType, oversized); delErr != nil {
				return fmt.Errorf("fetch cache delete-oversized: %w", delErr)
			}
		}
		return nil
	}); err != nil {
		// Warn (ctx-aware), not Error: a best-effort write drop must not count as
		// an error.
		logger.WarnWithCtx(ctx, "fetch cache write failed, dropping (best-effort)",
			zap.String(logger.ConnectorID, string(a.connectorID)),
			zap.String("cache_type", cacheType),
			zap.Int("entries", len(kept)),
			zap.Error(err),
		)
		return nil
	}
	orgLabel := tenantlabel.OrgID(a.orgID)
	if written > 0 {
		obs.FetchCacheRowsWrittenTotal.WithLabelValues(string(a.connectorID), cacheType, orgLabel).Add(float64(written))
	}
	// Only re-count when this PutMany actually changed rows. Per-page flushing calls
	// PutMany once per page, and a page that wrote nothing (all hits, no oversized
	// tombstone) leaves the row count unchanged — skipping the gauge refresh there
	// avoids multiplying the CountByType query across every page of a run.
	if written > 0 || len(oversized) > 0 {
		a.refreshRowsGauge(ctx, cacheType, orgLabel)
	}
	return nil
}

// refreshRowsGauge sets the table-growth gauge for one cache_type from a fresh
// row count. Called only when a PutMany actually changed rows (a write or an
// oversized tombstone), not on every per-page flush. Runs in its OWN tenant scope,
// separate from the write above, and is best-effort: a count failure must never
// roll back the committed write, so it is deliberately not folded into the write
// transaction. Callers pass the already-collapsed org label (tenantlabel.OrgID).
func (a *fetchCacheAdapter) refreshRowsGauge(ctx context.Context, cacheType, orgLabel string) {
	var count int64
	if err := tenantdb.RunInScope(ctx, a.orgID, tenantdb.ScopeEnforced, func(sCtx context.Context) error {
		var cErr error
		count, cErr = a.dao.CountByType(sCtx, a.orgID, a.connectorID, cacheType)
		if cErr != nil {
			return fmt.Errorf("fetch cache count: %w", cErr)
		}
		return nil
	}); err != nil {
		logger.WarnWithCtx(ctx, "fetch cache row-count for gauge failed, skipping gauge refresh",
			zap.String(logger.ConnectorID, string(a.connectorID)),
			zap.String("cache_type", cacheType),
			zap.Error(err),
		)
		return
	}
	obs.FetchCacheRows.WithLabelValues(string(a.connectorID), cacheType, orgLabel).Set(float64(count))
}

// Static assertion that the adapter satisfies the capability contract.
var _ sdk.FetchCache = (*fetchCacheAdapter)(nil)
