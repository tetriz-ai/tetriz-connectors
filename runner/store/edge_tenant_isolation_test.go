// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Connected as the table owner (no superuser, no BYPASSRLS), a scope for one
// organization neither reads, changes nor writes another organization's
// cursors, runs or cache rows, even when the DAO call names the other
// organization explicitly.
func TestEdge_ScopeForOneOrgCannotReachAnother(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("tenant isolation as the table owner", err)
		return
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	defer cleanup()

	const orgA, orgB = "00000000-0000-4000-8000-00000000e0a1", "00000000-0000-4000-8000-00000000e0b1"
	states, runs, cache := store.NewSyncStateRepo(db), store.NewSyncRunRepo(db), store.NewFetchCacheRepo(db)
	tx := store.PostgresTxRunner{}
	started := time.Now().UTC().Add(-time.Minute)
	done := started.Add(time.Second)

	// Seed B's rows in B's own scope.
	require.NoError(t, tx.RunInScope(ctx, orgB, store.ScopeEnforced, func(c context.Context) error {
		if err := states.SaveCursor(c, orgB, "github", "fetch_prs", &sdk.Cursor{Data: json.RawMessage(`{"b":1}`)}); err != nil {
			return err
		}
		if err := runs.Create(c, &model.SyncRun{ID: uuid.NewString(), OrganizationID: orgB, ConnectorID: "github", OperationID: "fetch_prs", Status: model.SyncRunStatusCompleted, StartedAt: started, CompletedAt: &done, Trigger: "test"}); err != nil {
			return err
		}
		_, err := cache.PutMany(c, orgB, "github", "t", map[string]json.RawMessage{"k": json.RawMessage(`1`)})
		return err
	}))

	// Reads in A's scope that name B see nothing.
	require.NoError(t, tx.RunInScope(ctx, orgA, store.ScopeEnforced, func(c context.Context) error {
		_, err := states.GetCursor(c, orgB, "github", "fetch_prs")
		assert.ErrorIs(t, err, store.ErrCursorNotFound, "A's scope cannot read B's cursor")
		_, err = runs.GetLatest(c, orgB, "github", "fetch_prs")
		assert.ErrorIs(t, err, store.ErrSyncRunNotFound, "A's scope cannot read B's runs")
		list, err := runs.ListByOperation(c, orgB, "github", "fetch_prs", 10)
		assert.NoError(t, err)
		assert.Empty(t, list)
		status, err := runs.GetConnectorSyncStatusByOrg(c, orgB)
		assert.NoError(t, err)
		assert.Empty(t, status)
		got, err := cache.GetMany(c, orgB, "github", "t", []string{"k"})
		assert.NoError(t, err)
		assert.Empty(t, got, "A's scope cannot read B's cache")
		n, err := cache.CountByType(c, orgB, "github", "t")
		assert.NoError(t, err)
		assert.Zero(t, n)
		// Deletes in A's scope that name B change nothing.
		assert.NoError(t, states.DeleteByConnector(c, orgB, "github"))
		assert.NoError(t, runs.DeleteRunsByConnector(c, orgB, "github"))
		assert.NoError(t, cache.DeleteMany(c, orgB, "github", "t", []string{"k"}))
		return nil
	}))

	// A write in A's scope that names B is refused by the policy's WITH CHECK.
	err = tx.RunInScope(ctx, orgA, store.ScopeEnforced, func(c context.Context) error {
		return states.SaveCursor(c, orgB, "github", "fetch_prs", &sdk.Cursor{Data: json.RawMessage(`{"forged":true}`)})
	})
	assert.Error(t, err, "A's scope cannot write a cursor for B")
	err = tx.RunInScope(ctx, orgA, store.ScopeEnforced, func(c context.Context) error {
		_, err := cache.PutMany(c, orgB, "github", "t", map[string]json.RawMessage{"k": json.RawMessage(`2`)})
		return err
	})
	assert.Error(t, err, "A's scope cannot write B's cache")

	// B's rows are intact.
	require.NoError(t, tx.RunInScope(ctx, orgB, store.ScopeEnforced, func(c context.Context) error {
		cur, err := states.GetCursor(c, orgB, "github", "fetch_prs")
		require.NoError(t, err)
		assert.JSONEq(t, `{"b":1}`, string(cur.Data))
		_, err = runs.GetLatest(c, orgB, "github", "fetch_prs")
		assert.NoError(t, err)
		got, err := cache.GetMany(c, orgB, "github", "t", []string{"k"})
		require.NoError(t, err)
		assert.JSONEq(t, `1`, string(got["k"]))
		return nil
	}))

	// Outside any scope the owner sees nothing at all.
	_, err = states.GetCursor(ctx, orgB, "github", "fetch_prs")
	assert.ErrorIs(t, err, store.ErrCursorNotFound, "an unscoped read sees no rows")
}

// Cursors round-trip through jsonb: a large one, nested unicode, numbers
// beyond float64 precision and a cursor whose Data is nil all come back as
// the same JSON value.
func TestEdge_CursorRoundTripsThroughJSONB(t *testing.T) {
	if testDB == nil {
		t.Skip("no test database")
	}
	tx := newTestTx(t)
	repo := store.NewSyncStateRepo(tx)
	ctx := context.Background()
	org := "00000000-0000-4000-8000-00000000c0c1"

	big := make([]string, 0, 20000)
	for i := range 20000 {
		big = append(big, fmt.Sprintf(`"repo-%d":"2024-01-01T00:00:00Z"`, i))
	}
	cases := map[string]json.RawMessage{
		"large":     json.RawMessage(`{` + strings.Join(big, ",") + `}`),
		"unicode":   json.RawMessage(`{"ветка":"日本語","emoji":"😀","escaped":" \t\"q\""}`),
		"bigint":    json.RawMessage(`{"since_id":123456789012345678901234567890,"f":0.1}`),
		"array":     json.RawMessage(`[1,"two",null,{"three":[]}]`),
		"scalar":    json.RawMessage(`"opaque-token"`),
		"empty-obj": json.RawMessage(`{}`),
	}
	for name, data := range cases {
		op := sdk.OperationID("op-" + name)
		require.NoError(t, repo.SaveCursor(ctx, org, "c", op, &sdk.Cursor{Data: data}), name)
		got, err := repo.GetCursor(ctx, org, "c", op)
		require.NoError(t, err, name)
		assert.JSONEq(t, string(data), string(got.Data), name)
	}
	// The exact digits of a number beyond float64 survive.
	got, err := repo.GetCursor(ctx, org, "c", "op-bigint")
	require.NoError(t, err)
	assert.Contains(t, string(got.Data), "123456789012345678901234567890")
}

// GetMany, DeleteMany and DeleteManyOlderThan take one bind parameter per
// key. Postgres caps a statement at 65535 parameters, so a call with more keys
// than that must still work (PutMany already chunks for the same reason). The
// eviction path hits it first: ScanForEviction returns every aged row, and a
// host deletes them with DeleteManyOlderThan.
func TestEdge_FetchCacheLookupBeyondTheParameterLimit(t *testing.T) {
	if testDB == nil {
		t.Skip("no test database")
	}
	tx := newTestTx(t)
	repo := store.NewFetchCacheRepo(tx)
	ctx := context.Background()
	org := "00000000-0000-4000-8000-00000000fc01"

	keys := make([]string, 70000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
	}
	_, err := repo.PutMany(ctx, org, "bitbucket", "t", map[string]json.RawMessage{keys[0]: json.RawMessage(`1`), keys[69999]: json.RawMessage(`2`)})
	require.NoError(t, err)

	got, err := repo.GetMany(ctx, org, "bitbucket", "t", keys)
	assert.NoError(t, err, "a lookup of 70000 keys")
	assert.Len(t, got, 2)

	deleted, err := repo.DeleteManyOlderThan(ctx, org, "bitbucket", "t", keys, time.Now().Add(time.Hour))
	assert.NoError(t, err, "an eviction of 70000 keys")
	assert.Equal(t, int64(2), deleted)
	_, err = repo.PutMany(ctx, org, "bitbucket", "t", map[string]json.RawMessage{keys[0]: json.RawMessage(`1`)})
	require.NoError(t, err)

	require.NoError(t, repo.DeleteMany(ctx, org, "bitbucket", "t", keys), "a delete of 70000 keys")
	n, err := repo.CountByType(ctx, org, "bitbucket", "t")
	require.NoError(t, err)
	assert.Zero(t, n)
}
