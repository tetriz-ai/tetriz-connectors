// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	fcOrg       = "org-fc"
	fcConnector = sdk.ConnectorID("bitbucket")
	fcType      = "pr_diffstat"
	// A time far in the past used as a sentinel: force a row's updated_at to it,
	// then assert whether a subsequent PutMany moved it — clock-independent proof
	// of the "updated_at moves only on a real change" invariant.
	fcSentinelUpdatedAt = "2000-01-01T00:00:00Z"
)

// forceUpdatedAt pins one entry's updated_at to a sentinel so the invariant test
// can tell "row untouched" (preserved) from "row rewritten" (bumped to now).
func forceUpdatedAt(t *testing.T, tx *gorm.DB, key string) {
	t.Helper()
	if err := tx.Exec(
		"UPDATE connector_fetch_cache SET updated_at = ? WHERE organization_id = ? AND connector_id = ? AND cache_type = ? AND entry_key = ?",
		fcSentinelUpdatedAt, fcOrg, fcConnector, fcType, key,
	).Error; err != nil {
		t.Fatalf("force updated_at: %v", err)
	}
}

func readEntry(t *testing.T, tx *gorm.DB, key string) model.FetchCache {
	t.Helper()
	var row model.FetchCache
	if err := tx.Where(
		"organization_id = ? AND connector_id = ? AND cache_type = ? AND entry_key = ?",
		fcOrg, fcConnector, fcType, key,
	).First(&row).Error; err != nil {
		t.Fatalf("read entry %q: %v", key, err)
	}
	return row
}

func TestFetchCacheRepo_PutMany(t *testing.T) {
	t.Parallel()
	sentinel, _ := time.Parse(time.RFC3339, fcSentinelUpdatedAt)

	tests := []struct {
		name string
		run  func(t *testing.T, tx *gorm.DB, repo *store.FetchCacheRepo)
	}{
		{
			name: "new key inserts a row",
			run: func(t *testing.T, tx *gorm.DB, repo *store.FetchCacheRepo) {
				_, err := repo.PutMany(context.Background(), fcOrg, fcConnector, fcType,
					map[string]json.RawMessage{"ws/repo#1": json.RawMessage(`{"files":3}`)})
				if err != nil {
					t.Fatalf("PutMany insert: %v", err)
				}
				row := readEntry(t, tx, "ws/repo#1")
				if string(row.Value) != `{"files": 3}` && string(row.Value) != `{"files":3}` {
					t.Errorf("value = %s, want the inserted JSON", row.Value)
				}
				if row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() {
					t.Error("created_at/updated_at should be set by DB defaults")
				}
			},
		},
		{
			name: "byte-equal re-write is a no-op that PRESERVES updated_at (the eviction-safety invariant)",
			run: func(t *testing.T, tx *gorm.DB, repo *store.FetchCacheRepo) {
				ctx := context.Background()
				v := json.RawMessage(`{"files":3,"additions":10}`)
				if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{"ws/repo#2": v}); err != nil {
					t.Fatalf("first put: %v", err)
				}
				forceUpdatedAt(t, tx, "ws/repo#2")
				// Re-write the SAME value — the WHERE IS DISTINCT FROM guard must skip it.
				if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{"ws/repo#2": v}); err != nil {
					t.Fatalf("no-op put: %v", err)
				}
				row := readEntry(t, tx, "ws/repo#2")
				if !row.UpdatedAt.UTC().Truncate(time.Second).Equal(sentinel.UTC()) {
					t.Errorf("updated_at moved on a byte-equal write: got %v, want preserved %v", row.UpdatedAt, sentinel)
				}
			},
		},
		{
			name: "semantically-equal re-marshal is also a no-op (jsonb-normalized compare)",
			run: func(t *testing.T, tx *gorm.DB, repo *store.FetchCacheRepo) {
				ctx := context.Background()
				if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{"ws/repo#3": json.RawMessage(`{"a":1,"b":2}`)}); err != nil {
					t.Fatalf("first put: %v", err)
				}
				forceUpdatedAt(t, tx, "ws/repo#3")
				// Same value, different key order + whitespace — jsonb IS DISTINCT FROM
				// compares by normalized value, so this must NOT count as a change.
				if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{"ws/repo#3": json.RawMessage(`{"b": 2, "a": 1}`)}); err != nil {
					t.Fatalf("re-marshal put: %v", err)
				}
				row := readEntry(t, tx, "ws/repo#3")
				if !row.UpdatedAt.UTC().Truncate(time.Second).Equal(sentinel.UTC()) {
					t.Errorf("updated_at moved on a semantically-equal re-marshal: got %v, want preserved", row.UpdatedAt)
				}
			},
		},
		{
			name: "changed value BUMPS updated_at and stores the new value",
			run: func(t *testing.T, tx *gorm.DB, repo *store.FetchCacheRepo) {
				ctx := context.Background()
				if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{"ws/repo#4": json.RawMessage(`{"files":1}`)}); err != nil {
					t.Fatalf("first put: %v", err)
				}
				forceUpdatedAt(t, tx, "ws/repo#4")
				if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{"ws/repo#4": json.RawMessage(`{"files":9}`)}); err != nil {
					t.Fatalf("changed put: %v", err)
				}
				row := readEntry(t, tx, "ws/repo#4")
				if row.UpdatedAt.UTC().Truncate(time.Second).Equal(sentinel.UTC()) {
					t.Error("updated_at was NOT bumped on a genuine value change")
				}
				if string(row.Value) != `{"files": 9}` && string(row.Value) != `{"files":9}` {
					t.Errorf("value = %s, want the changed JSON", row.Value)
				}
			},
		},
		{
			name: "empty batch is a no-op",
			run: func(t *testing.T, tx *gorm.DB, repo *store.FetchCacheRepo) {
				if _, err := repo.PutMany(context.Background(), fcOrg, fcConnector, fcType, map[string]json.RawMessage{}); err != nil {
					t.Errorf("empty PutMany should be a no-op, got %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := newTestTx(t)
			repo := store.NewFetchCacheRepo(tx)
			tt.run(t, tx, repo)
		})
	}
}

func TestFetchCacheRepo_GetMany(t *testing.T) {
	t.Parallel()
	tx := newTestTx(t)
	repo := store.NewFetchCacheRepo(tx)
	ctx := context.Background()

	if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"ws/repo#10": json.RawMessage(`{"files":1}`),
		"ws/repo#11": json.RawMessage(`{"files":2}`),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Ask for one present + one absent key: only the hit is returned, the miss
	// is simply absent from the map.
	got, err := repo.GetMany(ctx, fcOrg, fcConnector, fcType, []string{"ws/repo#10", "ws/repo#absent"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if _, ok := got["ws/repo#10"]; !ok {
		t.Error("expected hit ws/repo#10 to be present")
	}
	if _, ok := got["ws/repo#absent"]; ok {
		t.Error("absent key must not be in the result (a miss)")
	}
	if len(got) != 1 {
		t.Errorf("got %d entries, want 1", len(got))
	}

	// Empty key list returns an empty map, not an error.
	empty, err := repo.GetMany(ctx, fcOrg, fcConnector, fcType, nil)
	if err != nil || len(empty) != 0 {
		t.Errorf("GetMany(nil) = (%v, %v), want (empty, nil)", empty, err)
	}
}

func TestFetchCacheRepo_ScanForEviction(t *testing.T) {
	t.Parallel()
	tx := newTestTx(t)
	repo := store.NewFetchCacheRepo(tx)
	ctx := context.Background()

	if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"old-1": json.RawMessage(`{"n":1}`),
		"old-2": json.RawMessage(`{"n":2}`),
		"fresh": json.RawMessage(`{"n":3}`),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Age the two "old" rows past the cutoff, leave "fresh" recent.
	forceUpdatedAt(t, tx, "old-1")
	forceUpdatedAt(t, tx, "old-2")

	cutoff := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := repo.ScanForEviction(ctx, fcOrg, fcConnector, fcType, cutoff)
	if err != nil {
		t.Fatalf("ScanForEviction: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d eviction candidates, want 2 (the aged rows)", len(got))
	}
	for _, row := range got {
		if row.EntryKey == "fresh" {
			t.Error("fresh row (updated_at after cutoff) must not be an eviction candidate")
		}
	}
}

func TestFetchCacheRepo_DeleteMany(t *testing.T) {
	t.Parallel()
	tx := newTestTx(t)
	repo := store.NewFetchCacheRepo(tx)
	ctx := context.Background()

	if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"del-1": json.RawMessage(`{"n":1}`),
		"del-2": json.RawMessage(`{"n":2}`),
		"keep":  json.RawMessage(`{"n":3}`),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A mixed-in absent key is a no-op, not an error.
	if err := repo.DeleteMany(ctx, fcOrg, fcConnector, fcType, []string{"del-1", "del-2", "never-existed"}); err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	got, err := repo.GetMany(ctx, fcOrg, fcConnector, fcType, []string{"del-1", "del-2", "keep"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if _, ok := got["del-1"]; ok {
		t.Error("del-1 should be deleted")
	}
	if _, ok := got["del-2"]; ok {
		t.Error("del-2 should be deleted")
	}
	if _, ok := got["keep"]; !ok {
		t.Error("keep should remain")
	}
	if err := repo.DeleteMany(ctx, fcOrg, fcConnector, fcType, nil); err != nil {
		t.Errorf("DeleteMany(nil) should be a no-op, got %v", err)
	}
}

// TestFetchCacheRepo_DeleteManyOlderThan pins the eviction scan→delete TOCTOU fix:
// a candidate that was refreshed (its updated_at bumped past the cutoff by a PutMany)
// between the scan and the delete must SURVIVE, while a genuinely-still-old key is
// deleted. The AND updated_at < cutoff guard on the delete is the whole point.
func TestFetchCacheRepo_DeleteManyOlderThan(t *testing.T) {
	t.Parallel()
	tx := newTestTx(t)
	repo := store.NewFetchCacheRepo(tx)
	ctx := context.Background()

	if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"aged-keep": json.RawMessage(`{"n":1}`),
		"aged-del":  json.RawMessage(`{"n":2}`),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Both rows start aged past the cutoff — the scan would list both as candidates.
	forceUpdatedAt(t, tx, "aged-keep")
	forceUpdatedAt(t, tx, "aged-del")

	cutoff := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	candidates, err := repo.ScanForEviction(ctx, fcOrg, fcConnector, fcType, cutoff)
	if err != nil {
		t.Fatalf("ScanForEviction: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("got %d candidates, want 2", len(candidates))
	}

	// Simulate a concurrent refresh AFTER the scan: a PutMany with a genuinely new
	// value bumps aged-keep's updated_at to now (past the cutoff). aged-del stays old.
	if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"aged-keep": json.RawMessage(`{"n":99}`),
	}); err != nil {
		t.Fatalf("refresh put: %v", err)
	}

	// The cutoff-guarded delete over BOTH candidate keys must remove only the still-old
	// aged-del — aged-keep was refreshed past the cutoff, so it survives.
	deleted, err := repo.DeleteManyOlderThan(ctx, fcOrg, fcConnector, fcType, []string{"aged-keep", "aged-del"}, cutoff)
	if err != nil {
		t.Fatalf("DeleteManyOlderThan: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1 (only the still-old row; the refreshed one survives)", deleted)
	}
	got, err := repo.GetMany(ctx, fcOrg, fcConnector, fcType, []string{"aged-keep", "aged-del"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if _, ok := got["aged-keep"]; !ok {
		t.Error("aged-keep was refreshed past the cutoff and must NOT be deleted")
	}
	if _, ok := got["aged-del"]; ok {
		t.Error("aged-del was still older than the cutoff and must be deleted")
	}

	// Empty keys is a no-op returning 0.
	if n, err := repo.DeleteManyOlderThan(ctx, fcOrg, fcConnector, fcType, nil, cutoff); err != nil || n != 0 {
		t.Errorf("DeleteManyOlderThan(nil) = (%d, %v), want (0, nil)", n, err)
	}
}

func TestFetchCacheRepo_PutMany_LargeBatchChunks(t *testing.T) {
	t.Parallel()
	tx := newTestTx(t)
	repo := store.NewFetchCacheRepo(tx)
	ctx := context.Background()

	// 12,000 entries exceeds the 65,535/6 (~10.9k) single-statement bind-param
	// ceiling — proves the upsert is chunked rather than failing as one statement.
	const n = 12000
	entries := make(map[string]json.RawMessage, n)
	for i := 0; i < n; i++ {
		entries[fmt.Sprintf("k-%d", i)] = json.RawMessage(`{"i":` + strconv.Itoa(i) + `}`)
	}
	if _, err := repo.PutMany(ctx, fcOrg, fcConnector, "large_batch", entries); err != nil {
		t.Fatalf("PutMany of %d entries must succeed via chunking, got %v", n, err)
	}
	var count int64
	tx.Raw("SELECT count(*) FROM connector_fetch_cache WHERE organization_id = ? AND cache_type = 'large_batch'", fcOrg).Scan(&count)
	if count != n {
		t.Errorf("row count = %d, want %d (all chunks committed)", count, n)
	}
}

// TestFetchCacheRepo_PutMany_RowsWritten pins the returned count to rows ACTUALLY
// written — the churn signal the rows-written metric reports. The value-guarded
// DO UPDATE reports 0 for an unchanged conflict, so a warm re-write must return 0
// even though it "put" the same key.
func TestFetchCacheRepo_PutMany_RowsWritten(t *testing.T) {
	t.Parallel()
	tx := newTestTx(t)
	repo := store.NewFetchCacheRepo(tx)
	ctx := context.Background()

	n, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"rw-1": json.RawMessage(`{"n":1}`),
		"rw-2": json.RawMessage(`{"n":2}`),
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n != 2 {
		t.Errorf("fresh insert rows-written = %d, want 2", n)
	}

	// Re-writing the SAME values is a value-guarded no-op — zero rows written.
	n, err = repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"rw-1": json.RawMessage(`{"n":1}`),
		"rw-2": json.RawMessage(`{"n":2}`),
	})
	if err != nil {
		t.Fatalf("no-op re-write: %v", err)
	}
	if n != 0 {
		t.Errorf("no-op re-write rows-written = %d, want 0 (value-guarded skip)", n)
	}

	// One genuine change in the batch counts exactly one row written.
	n, err = repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"rw-1": json.RawMessage(`{"n":1}`),  // unchanged
		"rw-2": json.RawMessage(`{"n":99}`), // changed
	})
	if err != nil {
		t.Fatalf("mixed change: %v", err)
	}
	if n != 1 {
		t.Errorf("one-changed batch rows-written = %d, want 1", n)
	}

	// Empty batch writes nothing.
	if n, err = repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{}); err != nil || n != 0 {
		t.Errorf("empty PutMany = (%d, %v), want (0, nil)", n, err)
	}
}

// TestFetchCacheRepo_CountByType pins the table-growth gauge's source: a count
// scoped to one (org, connector, cache_type), excluding other cache_types and
// other orgs, and 0 (not an error) when empty.
func TestFetchCacheRepo_CountByType(t *testing.T) {
	t.Parallel()
	tx := newTestTx(t)
	repo := store.NewFetchCacheRepo(tx)
	ctx := context.Background()

	// Empty is (0, nil).
	got, err := repo.CountByType(ctx, fcOrg, fcConnector, fcType)
	if err != nil {
		t.Fatalf("CountByType empty: %v", err)
	}
	if got != 0 {
		t.Errorf("empty count = %d, want 0", got)
	}

	if _, err := repo.PutMany(ctx, fcOrg, fcConnector, fcType, map[string]json.RawMessage{
		"c-1": json.RawMessage(`{"n":1}`),
		"c-2": json.RawMessage(`{"n":2}`),
	}); err != nil {
		t.Fatalf("seed fcType: %v", err)
	}
	// A different cache_type for the same org must NOT be counted.
	if _, err := repo.PutMany(ctx, fcOrg, fcConnector, "other_type", map[string]json.RawMessage{
		"o-1": json.RawMessage(`{"n":1}`),
	}); err != nil {
		t.Fatalf("seed other_type: %v", err)
	}
	// A different ORG's rows for the SAME cache_type must NOT be counted — the
	// org dimension of CountByType's WHERE clause (RLS is defense-in-depth on top).
	const otherOrg = "org-fc-other"
	if _, err := repo.PutMany(ctx, otherOrg, fcConnector, fcType, map[string]json.RawMessage{
		"x-1": json.RawMessage(`{"n":1}`),
		"x-2": json.RawMessage(`{"n":2}`),
		"x-3": json.RawMessage(`{"n":3}`),
	}); err != nil {
		t.Fatalf("seed other org: %v", err)
	}

	got, err = repo.CountByType(ctx, fcOrg, fcConnector, fcType)
	if err != nil {
		t.Fatalf("CountByType: %v", err)
	}
	if got != 2 {
		t.Errorf("count = %d, want 2 (scoped to cache_type + org; other_type and other-org rows excluded)", got)
	}
}
