// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/metricutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchCacheUpsertChunkSize is the most rows one INSERT writes. Each row binds 6
// parameters, and Postgres rejects a statement with more than 65,535.
const FetchCacheUpsertChunkSize = 5000

// FetchCacheRepo implements IFetchCacheDAO using PostgreSQL via GORM.
type FetchCacheRepo struct {
	db *gorm.DB
}

// NewFetchCacheRepo creates a new FetchCacheRepo.
func NewFetchCacheRepo(db *gorm.DB) *FetchCacheRepo {
	return &FetchCacheRepo{db: db}
}

// PutMany upserts one cache_type's batch in a single statement. The
// `WHERE value IS DISTINCT FROM EXCLUDED.value` guard is the eviction-safety
// invariant: an unchanged entry is skipped by Postgres, so its
// updated_at is preserved and age-based eviction can still find genuinely stale
// rows. jsonb (not json) is what makes the comparison value-normalized rather
// than textual — a semantically-equal re-marshal does not count as a change.
func (r *FetchCacheRepo) PutMany(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entries map[string]json.RawMessage) (int64, error) {
	defer metricutil.TrackDuration("runner/store", "FetchCacheRepo", "PutMany", time.Now())
	// Raw Exec (not GORM OnConflict) because the DO UPDATE references EXCLUDED,
	// which GORM's clause builder cannot express cleanly.
	if len(entries) == 0 {
		return 0, nil
	}
	db := tenantdb.DB(ctx, r.db)
	now := time.Now().UTC()

	// Flatten to a stable slice so the batch can be chunked under the bind-param
	// ceiling (map order is irrelevant to the upsert).
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	var written int64
	for start := 0; start < len(keys); start += FetchCacheUpsertChunkSize {
		end := start + FetchCacheUpsertChunkSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]
		placeholders := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*6)
		for _, key := range chunk {
			// value is cast ?::jsonb — a bytea bind has no implicit cast to jsonb,
			// so bind the raw bytes as text and let Postgres parse them.
			placeholders = append(placeholders, "(?, ?, ?, ?, ?::jsonb, ?)")
			args = append(args, orgID, connectorID, cacheType, key, string(entries[key]), now)
		}

		sql := `INSERT INTO connector_fetch_cache
			(organization_id, connector_id, cache_type, entry_key, value, updated_at)
		VALUES ` + strings.Join(placeholders, ", ") + `
		ON CONFLICT (organization_id, connector_id, cache_type, entry_key) DO UPDATE
			SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
			WHERE connector_fetch_cache.value IS DISTINCT FROM EXCLUDED.value`

		res := db.Exec(sql, args...)
		if res.Error != nil {
			return written, fmt.Errorf("upserting fetch-cache batch (cache_type=%s, chunk=%d:%d): %w", cacheType, start, end, res.Error)
		}
		// RowsAffected counts inserts + genuine updates; the value-guarded DO UPDATE
		// reports 0 for an unchanged conflict, so this is the real write count.
		written += res.RowsAffected
	}
	return written, nil
}

// CountByType returns the number of rows currently held for one
// (org, connector, cache_type) — the table-growth gauge's source. A COUNT on the
// composite PK's leading columns; cheap enough to call on the connector's write
// cadence. Zero rows returns (0, nil), not an error.
func (r *FetchCacheRepo) CountByType(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string) (int64, error) {
	defer metricutil.TrackDuration("runner/store", "FetchCacheRepo", "CountByType", time.Now())
	var count int64
	err := tenantdb.DB(ctx, r.db).
		Model(&model.FetchCache{}).
		Where("organization_id = ? AND connector_id = ? AND cache_type = ?", orgID, connectorID, cacheType).
		Count(&count).Error
	if err != nil {
		return 0, fmt.Errorf("counting fetch-cache rows (cache_type=%s): %w", cacheType, err)
	}
	return count, nil
}

// DeleteMany removes the given keys of one cache_type. It tombstones an entry
// whose fresh value was skipped as oversized: leaving a stale prior row would make
// GetMany return it as a hit instead of forcing the re-fetch the size guard promises.
// Absent keys are a no-op.
func (r *FetchCacheRepo) DeleteMany(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string) error {
	defer metricutil.TrackDuration("runner/store", "FetchCacheRepo", "DeleteMany", time.Now())
	if len(entryKeys) == 0 {
		return nil
	}
	db := tenantdb.DB(ctx, r.db)
	for _, chunk := range keyChunks(entryKeys) {
		err := db.Where(
			"organization_id = ? AND connector_id = ? AND cache_type = ? AND entry_key IN ?",
			orgID, connectorID, cacheType, chunk,
		).Delete(&model.FetchCache{}).Error
		if err != nil {
			return fmt.Errorf("deleting fetch-cache entries (cache_type=%s): %w", cacheType, err)
		}
	}
	return nil
}

// DeleteManyOlderThan removes the given keys of one cache_type, but only the rows
// still older than olderThan. It is DeleteMany plus an `AND updated_at < ?` guard so
// the eviction scan→delete window cannot drop a row that was refreshed after the
// scan listed it: a concurrent PutMany that bumped updated_at past the cutoff makes
// the row survive. Returns the rows actually deleted (RowsAffected). Absent keys are
// a no-op returning 0.
func (r *FetchCacheRepo) DeleteManyOlderThan(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string, olderThan time.Time) (int64, error) {
	defer metricutil.TrackDuration("runner/store", "FetchCacheRepo", "DeleteManyOlderThan", time.Now())
	if len(entryKeys) == 0 {
		return 0, nil
	}
	db := tenantdb.DB(ctx, r.db)
	var deleted int64
	for _, chunk := range keyChunks(entryKeys) {
		res := db.Where(
			"organization_id = ? AND connector_id = ? AND cache_type = ? AND entry_key IN ? AND updated_at < ?",
			orgID, connectorID, cacheType, chunk, olderThan.UTC(),
		).Delete(&model.FetchCache{})
		if res.Error != nil {
			return deleted, fmt.Errorf("deleting aged fetch-cache entries (cache_type=%s): %w", cacheType, res.Error)
		}
		deleted += res.RowsAffected
	}
	return deleted, nil
}

// GetMany resolves the given keys by a point lookup on the PK. Keys with no row
// are simply absent from the result map — the caller reads that as a cache miss.
func (r *FetchCacheRepo) GetMany(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string) (map[string]json.RawMessage, error) {
	defer metricutil.TrackDuration("runner/store", "FetchCacheRepo", "GetMany", time.Now())
	result := make(map[string]json.RawMessage, len(entryKeys))
	if len(entryKeys) == 0 {
		return result, nil
	}
	db := tenantdb.DB(ctx, r.db)

	for _, chunk := range keyChunks(entryKeys) {
		var rows []model.FetchCache
		err := db.Where(
			"organization_id = ? AND connector_id = ? AND cache_type = ? AND entry_key IN ?",
			orgID, connectorID, cacheType, chunk,
		).Find(&rows).Error
		if err != nil {
			return nil, fmt.Errorf("point-looking up fetch-cache entries (cache_type=%s): %w", cacheType, err)
		}
		for _, row := range rows {
			result[row.EntryKey] = row.Value
		}
	}
	return result, nil
}

// keyChunks splits keys into slices of at most FetchCacheUpsertChunkSize. Each
// key binds one parameter of an IN list, and Postgres rejects a statement with
// more than 65,535.
func keyChunks(keys []string) [][]string {
	var out [][]string
	for start := 0; start < len(keys); start += FetchCacheUpsertChunkSize {
		out = append(out, keys[start:min(start+FetchCacheUpsertChunkSize, len(keys))])
	}
	return out
}

// ScanForEviction returns the rows of one cache_type whose updated_at predates
// the cutoff, ordered oldest-first — the candidate set a host's eviction job
// deletes on the updated_at-age basis.
func (r *FetchCacheRepo) ScanForEviction(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, updatedBefore time.Time) ([]*model.FetchCache, error) {
	defer metricutil.TrackDuration("runner/store", "FetchCacheRepo", "ScanForEviction", time.Now())
	db := tenantdb.DB(ctx, r.db)

	var rows []*model.FetchCache
	// Project the key columns only — NOT the JSONB value: the eviction caller deletes
	// by entry_key and never reads value, so materializing every value (up to the
	// per-row cap, thousands of rows) would balloon the eviction job's memory on exactly
	// the large aged sets the prune targets.
	err := db.
		Select("organization_id", "connector_id", "cache_type", "entry_key", "updated_at").
		Where(
			"organization_id = ? AND connector_id = ? AND cache_type = ? AND updated_at < ?",
			orgID, connectorID, cacheType, updatedBefore.UTC(),
		).Order("updated_at ASC").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("scanning fetch-cache eviction candidates (cache_type=%s): %w", cacheType, err)
	}
	return rows, nil
}
