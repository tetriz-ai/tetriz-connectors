// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package memstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// impl is one store implementation under the shared contract.
type impl struct {
	configs store.IConnectorConfigDAO
	states  store.ISyncStateDAO
	runs    store.ISyncRunDAO
	cache   store.IFetchCacheDAO
	sched   store.IScheduleDAO
	tx      store.TxRunner
}

var postgres *impl

// pgDB is the Postgres half's database, for rows only a raw write makes.
var pgDB *gorm.DB

func TestMain(m *testing.M) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("store contract (postgres half)", err)
		os.Exit(testguard.Run(m))
	}
	db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "store contract: %v\n", err)
		os.Exit(1)
	}
	pgDB = db
	postgres = &impl{store.NewConnectorConfigRepo(db, nil), store.NewSyncStateRepo(db), store.NewSyncRunRepo(db), store.NewFetchCacheRepo(db), store.NewScheduleRepo(db), store.PostgresTxRunner{}}
	code := testguard.Run(m)
	cleanup()
	os.Exit(code)
}

var seq int

func org() string {
	seq++
	return fmt.Sprintf("00000000-0000-4000-9000-%012d", seq)
}

// both runs fn against the memory store and, when reachable, Postgres, each
// call inside one tenant scope for org.
func both(t *testing.T, fn func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error))) {
	run := func(t *testing.T, s impl) {
		o := org()
		in := func(f func(ctx context.Context) error) {
			t.Helper()
			require.NoError(t, s.tx.RunInScope(context.Background(), o, store.ScopeEnforced, f))
		}
		fn(t, s, o, in)
	}
	t.Run("memory", func(t *testing.T) {
		m := memstore.New()
		run(t, impl{m, m, m, m, m, store.NoopTxRunner{}})
	})
	t.Run("file", func(t *testing.T) {
		f, err := memstore.OpenFile(filepath.Join(t.TempDir(), "store.json"), testEncryptor(t))
		require.NoError(t, err)
		run(t, impl{f, f, f, f, f, store.NoopTxRunner{}})
	})
	if postgres != nil {
		t.Run("postgres", func(t *testing.T) { run(t, *postgres) })
	}
}

func cfg(o string, enabled bool, settings string) *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{OrganizationID: o, ConnectorID: "github", Credentials: json.RawMessage(`{"token":"t"}`), Settings: json.RawMessage(settings), Enabled: enabled}
}

func TestContract_ConfigLifecycle(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error {
			_, err := s.configs.Get(ctx, o, "github")
			assert.True(t, store.IsNotFound(err), "missing config: %v", err)
			return nil
		})
		in(func(ctx context.Context) error { return s.configs.Save(ctx, cfg(o, true, `{"a":1}`)) })
		var first *sdk.ConnectorConfig
		in(func(ctx context.Context) error {
			var err error
			first, err = s.configs.Get(ctx, o, "github")
			return err
		})
		assert.JSONEq(t, `{"a":1}`, string(first.Settings))
		assert.Equal(t, sdk.HealthStateHealthy, first.HealthState)

		// Delete is soft: the row disappears from reads; Save revives it.
		in(func(ctx context.Context) error { return s.configs.Delete(ctx, o, "github") })
		in(func(ctx context.Context) error {
			_, err := s.configs.Get(ctx, o, "github")
			assert.True(t, store.IsNotFound(err))
			l, err := s.configs.ListByOrg(ctx, o)
			assert.Empty(t, l)
			return err
		})
		in(func(ctx context.Context) error { return s.configs.Save(ctx, cfg(o, true, `{"a":2}`)) })
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.JSONEq(t, `{"a":2}`, string(got.Settings))
			return nil
		})
	})
}

func TestContract_AuthRevokeFence(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error { return s.configs.Save(ctx, cfg(o, true, `{}`)) })
		var c *sdk.ConnectorConfig
		in(func(ctx context.Context) error {
			var err error
			c, err = s.configs.Get(ctx, o, "github")
			return err
		})
		in(func(ctx context.Context) error {
			ok, err := s.configs.MarkAuthRevoked(ctx, o, "github", c.CredentialsUpdatedAt.Add(-time.Hour))
			assert.False(t, ok, "a stale fence must not revoke")
			return err
		})
		in(func(ctx context.Context) error {
			ok, err := s.configs.MarkAuthRevoked(ctx, o, "github", c.CredentialsUpdatedAt)
			assert.True(t, ok)
			return err
		})
		in(func(ctx context.Context) error {
			ok, err := s.configs.MarkAuthRevoked(ctx, o, "github", c.CredentialsUpdatedAt)
			assert.False(t, ok, "already revoked")
			return err
		})
		// The Postgres store stamps an inserted row with the database clock and
		// a reset with the application clock; the pause keeps a small skew
		// between the two from reordering them.
		time.Sleep(time.Second)
		in(func(ctx context.Context) error {
			ok, err := s.configs.ResetHealthState(ctx, o, "github")
			assert.True(t, ok)
			return err
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.Equal(t, sdk.HealthStateHealthy, got.HealthState)
			assert.True(t, got.CredentialsUpdatedAt.After(c.CredentialsUpdatedAt), "a credential write advances the fence")
			// A revoke fenced on the old timestamp now loses.
			ok, err := s.configs.MarkAuthRevoked(ctx, o, "github", c.CredentialsUpdatedAt)
			assert.False(t, ok)
			return err
		})
	})
}

func TestContract_PreserveEnabledAndSettings(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error {
			return s.configs.SaveCredentialsPreserveEnabled(ctx, cfg(o, true, `{"s":1}`))
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.False(t, got.Enabled, "a first credential lands disabled")
			return nil
		})
		in(func(ctx context.Context) error {
			return s.configs.EnableConnectorWithSettings(ctx, o, "github", func(json.RawMessage) (json.RawMessage, error) {
				return json.RawMessage(`{"s":2}`), nil
			})
		})
		in(func(ctx context.Context) error {
			return s.configs.SaveCredentialsPreserveEnabled(ctx, cfg(o, false, `{"s":3}`))
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.True(t, got.Enabled, "a rotation keeps the live enabled bit")
			assert.JSONEq(t, `{"s":3}`, string(got.Settings))
			return nil
		})
		in(func(ctx context.Context) error {
			return s.configs.SaveCredentialsEnablePreservingSettings(ctx, cfg(o, false, `{"s":4}`))
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.JSONEq(t, `{"s":3}`, string(got.Settings), "a live row keeps its settings")
			return nil
		})
		in(func(ctx context.Context) error { return s.configs.Delete(ctx, o, "github") })
		in(func(ctx context.Context) error {
			return s.configs.SaveCredentialsEnablePreservingSettings(ctx, cfg(o, false, `{"s":5}`))
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.JSONEq(t, `{"s":5}`, string(got.Settings), "a revived row takes the incoming settings")
			assert.True(t, got.Enabled)
			return nil
		})
		in(func(ctx context.Context) error {
			err := s.configs.PatchSettings(ctx, o, "github", func(json.RawMessage) (json.RawMessage, error) { return nil, nil })
			require.NoError(t, err, "an empty patch writes nothing")
			got, _ := s.configs.Get(ctx, o, "github")
			assert.JSONEq(t, `{"s":5}`, string(got.Settings))
			n, err := s.configs.DisableConfigsForOrg(ctx, o)
			assert.Equal(t, int64(1), n)
			return err
		})
	})
}

// SaveCursor is a compare-and-swap on the version the writer read: a writer
// that read an older version cannot overwrite a newer cursor, and gets a
// typed conflict.
func TestContract_Cursors(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error {
			_, err := s.states.GetCursor(ctx, o, "github", "fetch_prs")
			assert.True(t, errors.Is(err, store.ErrCursorNotFound))
			return nil
		})
		// Two runs both start from no cursor (version 0).
		first := &sdk.Cursor{Data: json.RawMessage(`{"n":1}`)}
		stale := &sdk.Cursor{Data: json.RawMessage(`{"n":0}`)}
		in(func(ctx context.Context) error {
			require.NoError(t, s.states.SaveCursor(ctx, o, "github", "fetch_prs", first))
			assert.Equal(t, int64(1), first.Version, "a save sets the new version")
			return nil
		})
		in(func(ctx context.Context) error {
			err := s.states.SaveCursor(ctx, o, "github", "fetch_prs", stale)
			require.ErrorIs(t, err, store.ErrCursorConflict)
			var conflict *store.CursorConflictError
			require.ErrorAs(t, err, &conflict)
			assert.Equal(t, int64(0), conflict.ReadVersion)
			return nil
		})
		in(func(ctx context.Context) error {
			c, err := s.states.GetCursor(ctx, o, "github", "fetch_prs")
			require.NoError(t, err)
			assert.JSONEq(t, `{"n":1}`, string(c.Data), "the stale writer did not overwrite the newer cursor")
			assert.Equal(t, int64(1), c.Version)
			c.Data = json.RawMessage(`{"n":2}`)
			require.NoError(t, s.states.SaveCursor(ctx, o, "github", "fetch_prs", c), "a writer at the stored version saves")
			assert.Equal(t, int64(2), c.Version)
			first.Data = json.RawMessage(`{"n":9}`)
			require.ErrorIs(t, s.states.SaveCursor(ctx, o, "github", "fetch_prs", first), store.ErrCursorConflict, "version 1 is stale now")
			return nil
		})
		in(func(ctx context.Context) error {
			c, err := s.states.GetCursor(ctx, o, "github", "fetch_prs")
			require.NoError(t, err)
			assert.JSONEq(t, `{"n":2}`, string(c.Data))
			require.NoError(t, s.states.DeleteByConnector(ctx, o, "github"))
			_, err = s.states.GetCursor(ctx, o, "github", "fetch_prs")
			assert.True(t, errors.Is(err, store.ErrCursorNotFound))
			require.ErrorIs(t, s.states.SaveCursor(ctx, o, "github", "fetch_prs", c), store.ErrCursorConflict,
				"a run that read a cursor since reset cannot write it back")
			return nil
		})
	})
}

// A running run whose lease expired is closed as failed, kind abandoned; a
// renewed lease, a lease that expires exactly now, a run without a lease and a
// finished run are left alone. Leases are on the store's clock: the
// database's now() for Postgres, the injected clock for memstore. A closed run
// is not renewed and not overwritten. With no organization, CloseAbandoned
// spans every one, in the system scope.
// Rows GetCursor reads as no cursor take a first save like a fresh cursor:
// a live row at version zero (written outside SaveCursor) and a soft-deleted
// row, which the save revives.
func TestPostgres_CursorRowsReadAsNoneTakeAFirstSave(t *testing.T) {
	if postgres == nil {
		t.Skip("no test database")
	}
	o := org()
	in := func(f func(ctx context.Context) error) {
		t.Helper()
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(context.Background(), o, store.ScopeEnforced, f))
	}
	in(func(ctx context.Context) error {
		db := tenantdb.DB(ctx, pgDB)
		require.NoError(t, db.Exec(`INSERT INTO connector_sync_states (organization_id, connector_id, operation_id, cursor_data, version)
VALUES (?, 'github', 'live_v0', '{"data":{"n":1}}', 0)`, o).Error)
		return db.Exec(`INSERT INTO connector_sync_states (organization_id, connector_id, operation_id, cursor_data, version, deleted_at)
VALUES (?, 'github', 'deleted', '{"data":{"n":1}}', 3, now())`, o).Error
	})
	for _, op := range []sdk.OperationID{"live_v0", "deleted"} {
		in(func(ctx context.Context) error {
			c, err := postgres.states.GetCursor(ctx, o, "github", op)
			if op == "deleted" {
				require.ErrorIs(t, err, store.ErrCursorNotFound)
				c = &sdk.Cursor{}
			} else {
				require.NoError(t, err)
				assert.Zero(t, c.Version)
			}
			c.Data = json.RawMessage(`{"n":2}`)
			require.NoError(t, postgres.states.SaveCursor(ctx, o, "github", op, c), op)
			got, err := postgres.states.GetCursor(ctx, o, "github", op)
			require.NoError(t, err, "%s: the saved cursor reads back", op)
			assert.JSONEq(t, `{"n":2}`, string(got.Data))
			assert.Equal(t, c.Version, got.Version)
			return nil
		})
	}
}

func TestContract_RunLeases(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		now := time.Now().UTC().Truncate(time.Second)
		if c, ok := s.runs.(interface{ SetClock(func() time.Time) }); ok {
			c.SetClock(func() time.Time { return now })
		}
		past := now.Add(-time.Minute)
		mk := func(status model.SyncRunStatus, lease *time.Time) *model.SyncRun {
			return &model.SyncRun{ID: uuid.NewString(), OrganizationID: o, ConnectorID: "github", OperationID: "fetch_prs",
				Status: status, StartedAt: now.Add(-time.Hour), Trigger: "test", LeaseExpiresAt: lease}
		}
		expired, renewed, atNow, noLease, done := mk(model.SyncRunStatusRunning, &past), mk(model.SyncRunStatusRunning, &past),
			mk(model.SyncRunStatusRunning, &past), mk(model.SyncRunStatusRunning, nil), mk(model.SyncRunStatusCompleted, &past)
		other := mk(model.SyncRunStatusRunning, &past)
		other.OperationID = "fetch_issues"
		in(func(ctx context.Context) error {
			for _, r := range []*model.SyncRun{expired, renewed, atNow, noLease, done, other} {
				require.NoError(t, s.runs.Create(ctx, r))
			}
			require.NoError(t, s.runs.RenewLease(ctx, o, renewed.ID, time.Minute))
			require.ErrorIs(t, s.runs.RenewLease(ctx, o, done.ID, time.Minute), store.ErrRunClosed, "a finished run is not renewed")
			// One transaction: Postgres's now() is the same for both statements,
			// so this lease expires exactly at the comparison's now.
			require.NoError(t, s.runs.RenewLease(ctx, o, atNow.ID, 0))
			n, err := s.runs.CloseAbandoned(ctx, o, "github", "fetch_prs")
			require.NoError(t, err)
			assert.Equal(t, int64(1), n, "only the lease that expired before now")
			return nil
		})
		byID := func() map[string]*model.SyncRun {
			out := map[string]*model.SyncRun{}
			in(func(ctx context.Context) error {
				for _, op := range []sdk.OperationID{"fetch_prs", "fetch_issues"} {
					rs, err := s.runs.ListByOperation(ctx, o, "github", op, -1)
					require.NoError(t, err)
					for _, r := range rs {
						out[r.ID] = r
					}
				}
				return nil
			})
			return out
		}
		got := byID()
		assert.Equal(t, model.SyncRunStatusFailed, got[expired.ID].Status)
		assert.Equal(t, "abandoned", got[expired.ID].ErrorKind)
		assert.Equal(t, store.AbandonedRunMessage(past), got[expired.ID].ErrorMessage)
		assert.NotNil(t, got[expired.ID].CompletedAt)
		assert.Equal(t, model.SyncRunStatusRunning, got[renewed.ID].Status)
		assert.Equal(t, model.SyncRunStatusRunning, got[atNow.ID].Status, "a lease expiring exactly now is not yet expired")
		assert.Equal(t, model.SyncRunStatusRunning, got[noLease.ID].Status)
		assert.Equal(t, model.SyncRunStatusCompleted, got[done.ID].Status)
		assert.Equal(t, model.SyncRunStatusRunning, got[other.ID].Status, "another operation is not this operation's to close")

		// The run that was closed finishes after all: its final write is refused.
		late := *got[expired.ID]
		late.Status, late.ErrorKind, late.ErrorMessage = model.SyncRunStatusCompleted, "", ""
		in(func(ctx context.Context) error {
			require.ErrorIs(t, s.runs.Update(ctx, &late), store.ErrRunClosed)
			require.ErrorIs(t, s.runs.RenewLease(ctx, o, expired.ID, time.Minute), store.ErrRunClosed)
			return nil
		})
		assert.Equal(t, "abandoned", byID()[expired.ID].ErrorKind, "the closed row keeps its abandoned status")

		require.NoError(t, s.tx.RunInScope(context.Background(), tenantdb.SystemTenantID, store.ScopeSystem, func(ctx context.Context) error {
			n, err := s.runs.CloseAbandoned(ctx, "", "", "")
			require.NoError(t, err)
			assert.GreaterOrEqual(t, n, int64(1))
			return nil
		}))
		assert.Equal(t, model.SyncRunStatusFailed, byID()[other.ID].Status, "the system-wide pass closes every organization's")
	})
}

func TestContract_Runs(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		base := time.Now().UTC().Truncate(time.Second)
		ids := []string{"11111111-0000-4000-8000-000000000001", "11111111-0000-4000-8000-000000000002"}
		if seq > 0 {
			ids = []string{fmt.Sprintf("11111111-0000-4000-8000-%012d", seq*10+1), fmt.Sprintf("11111111-0000-4000-8000-%012d", seq*10+2)}
		}
		in(func(ctx context.Context) error {
			for i, id := range ids {
				if err := s.runs.Create(ctx, &model.SyncRun{ID: id, OrganizationID: o, ConnectorID: "github", OperationID: "fetch_prs", Status: model.SyncRunStatusRunning, StartedAt: base.Add(time.Duration(i) * time.Minute), Trigger: "test"}); err != nil {
					return err
				}
			}
			return nil
		})
		in(func(ctx context.Context) error {
			r, err := s.runs.GetLatest(ctx, o, "github", "fetch_prs")
			require.NoError(t, err)
			assert.Equal(t, ids[1], r.ID, "latest is the newest start")
			r.Status = model.SyncRunStatusCompleted
			require.NoError(t, s.runs.Update(ctx, r))
			// DeleteRunsByConnector removes the rows outright.
			require.NoError(t, s.runs.DeleteRunsByConnector(ctx, o, "github"))
			_, err = s.runs.GetLatest(ctx, o, "github", "fetch_prs")
			assert.True(t, store.IsNotFound(err))
			return nil
		})
	})
}

func TestContract_FetchCache(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error {
			n, err := s.cache.PutMany(ctx, o, "github", "c", map[string]json.RawMessage{"a": json.RawMessage(`{"x":1,"y":2}`), "b": json.RawMessage(`1`)})
			require.NoError(t, err)
			assert.Equal(t, int64(2), n)
			n, err = s.cache.PutMany(ctx, o, "github", "c", map[string]json.RawMessage{"a": json.RawMessage(`{"y":2, "x":1}`), "b": json.RawMessage(`2`)})
			require.NoError(t, err)
			assert.Equal(t, int64(1), n, "an equal JSON value is not a write")
			got, err := s.cache.GetMany(ctx, o, "github", "c", []string{"a", "b", "missing"})
			require.NoError(t, err)
			assert.Len(t, got, 2)
			assert.JSONEq(t, `2`, string(got["b"]))
			c, err := s.cache.CountByType(ctx, o, "github", "c")
			assert.Equal(t, int64(2), c)
			return err
		})
	})
}

func TestContract_Schedules(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error {
			require.NoError(t, s.sched.Upsert(ctx, &model.ScheduleOverride{OrganizationID: o, ConnectorID: "github", OperationID: "fetch_prs", CronExpression: "5 * * * *", Enabled: true}))
			require.NoError(t, s.sched.Upsert(ctx, &model.ScheduleOverride{OrganizationID: o, ConnectorID: "github", OperationID: "fetch_commits", CronExpression: "6 * * * *", Enabled: false}))
			all, err := s.sched.ListAll(ctx)
			require.NoError(t, err)
			enabled := map[string]bool{}
			for _, x := range all {
				if x.OrganizationID == o {
					enabled[string(x.OperationID)] = x.Enabled
				}
			}
			assert.Equal(t, map[string]bool{"fetch_prs": true, "fetch_commits": false}, enabled, "ListAll lists disabled overrides too")
			return nil
		})
	})
}

// A credential swap made with a context from WithCredentialSwaps records the
// fence it wrote, exactly as stored, once its scope commits, so a later
// MarkAuthRevoked fenced on it applies. A swap whose scope rolls back records
// nothing (Postgres; the memory stores have no transactions).
func TestContract_CredentialSwapRecordsItsFence(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error { return s.configs.Save(ctx, cfg(o, true, `{}`)) })
		if _, pg := s.tx.(store.PostgresTxRunner); pg {
			rolledBack := store.WithCredentialSwaps(context.Background())
			err := s.tx.RunInScope(rolledBack, o, store.ScopeEnforced, func(ctx context.Context) error {
				c, err := s.configs.Get(ctx, o, "github")
				require.NoError(t, err)
				ok, err := s.configs.CompareAndSwapCredentials(ctx, o, "github", c.CredentialsUpdatedAt, json.RawMessage(`{"token":"never"}`))
				require.NoError(t, err)
				require.True(t, ok)
				return errors.New("roll back")
			})
			require.Error(t, err)
			_, recorded := store.CredentialSwap(rolledBack, o, "github")
			assert.False(t, recorded, "a rolled-back swap is not a fence")
		}
		swaps := store.WithCredentialSwaps(context.Background())
		require.NoError(t, s.tx.RunInScope(swaps, o, store.ScopeEnforced, func(ctx context.Context) error {
			c, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			ok, err := s.configs.CompareAndSwapCredentials(ctx, o, "github", c.CredentialsUpdatedAt, json.RawMessage(`{"token":"rotated"}`))
			require.NoError(t, err)
			require.True(t, ok)
			return nil
		}))
		fence, recorded := store.CredentialSwap(swaps, o, "github")
		require.True(t, recorded)
		in(func(ctx context.Context) error {
			after, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.True(t, fence.Equal(after.CredentialsUpdatedAt), "recorded %v, stored %v", fence, after.CredentialsUpdatedAt)
			revoked, err := s.configs.MarkAuthRevoked(ctx, o, "github", fence)
			require.NoError(t, err)
			assert.True(t, revoked)
			return nil
		})
	})
}

func TestContract_CompareAndSwapCredentials(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error { return s.configs.Save(ctx, cfg(o, true, `{"keep":1}`)) })
		var c *sdk.ConnectorConfig
		in(func(ctx context.Context) error {
			var err error
			c, err = s.configs.Get(ctx, o, "github")
			return err
		})
		in(func(ctx context.Context) error {
			ok, err := s.configs.CompareAndSwapCredentials(ctx, o, "github", c.CredentialsUpdatedAt.Add(-time.Hour), json.RawMessage(`{"token":"stale"}`))
			assert.False(t, ok, "a stale fence does not swap")
			return err
		})
		in(func(ctx context.Context) error {
			ok, err := s.configs.CompareAndSwapCredentials(ctx, o, "github", c.CredentialsUpdatedAt, json.RawMessage(`{"token":"t2"}`))
			assert.True(t, ok)
			return err
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.JSONEq(t, `{"token":"t2"}`, string(got.Credentials))
			assert.JSONEq(t, `{"keep":1}`, string(got.Settings), "settings are not written")
			assert.True(t, got.Enabled)
			ok, err := s.configs.CompareAndSwapCredentials(ctx, o, "github", c.CredentialsUpdatedAt, json.RawMessage(`{"token":"t3"}`))
			assert.False(t, ok, "the fence advanced with the first swap")
			c = got
			return err
		})
		// A disconnect between the read and the swap wins: the row stays deleted.
		in(func(ctx context.Context) error { return s.configs.Delete(ctx, o, "github") })
		in(func(ctx context.Context) error {
			ok, err := s.configs.CompareAndSwapCredentials(ctx, o, "github", c.CredentialsUpdatedAt, json.RawMessage(`{"token":"t4"}`))
			assert.False(t, ok, "a deleted row is not swapped")
			if err != nil {
				return err
			}
			_, err = s.configs.Get(ctx, o, "github")
			assert.True(t, store.IsNotFound(err), "the connector stays disconnected")
			return nil
		})
	})
}

func testEncryptor(t *testing.T) cryptutil.Encryptor {
	t.Helper()
	enc, err := cryptutil.NewAESGCMEncryptor(make([]byte, 32))
	require.NoError(t, err)
	return enc
}

// A file store reopened from its file holds what was written, with the
// credentials sealed on disk.
func TestFileStore_SurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.json")
	f, err := memstore.OpenFile(path, testEncryptor(t))
	require.NoError(t, err)
	o := org()
	require.NoError(t, f.Save(ctx, cfg(o, true, `{"a":1}`)))
	require.NoError(t, f.SaveCursor(ctx, o, "github", "fetch_prs", &sdk.Cursor{Data: json.RawMessage(`{"n":7}`)}))
	require.NoError(t, f.Create(ctx, &model.SyncRun{ID: "11111111-0000-4000-8000-0000000000aa", OrganizationID: o, ConnectorID: "github", OperationID: "fetch_prs", Status: model.SyncRunStatusRunning, StartedAt: time.Now().UTC(), Trigger: "test"}))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), `"token":"t"`, "credentials are encrypted on disk")
	st, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), st.Mode().Perm())

	g, err := memstore.OpenFile(path, testEncryptor(t))
	require.NoError(t, err)
	c, err := g.Get(ctx, o, "github")
	require.NoError(t, err)
	assert.JSONEq(t, `{"token":"t"}`, string(c.Credentials))
	cur, err := g.GetCursor(ctx, o, "github", "fetch_prs")
	require.NoError(t, err)
	assert.JSONEq(t, `{"n":7}`, string(cur.Data))
	run, err := g.GetLatest(ctx, o, "github", "fetch_prs")
	require.NoError(t, err)
	assert.Equal(t, "11111111-0000-4000-8000-0000000000aa", run.ID)
}

// A first connect, with no row at all, inserts an enabled row with empty
// operation overrides.
func TestContract_FirstConnectInsertsEmptyOverrides(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error {
			return s.configs.SaveCredentialsEnableWithSettings(ctx, cfg(o, false, `{}`), workspaceSettings)
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.True(t, got.Enabled, "a first connect enables the row")
			assert.JSONEq(t, `{"workspace_id":"w"}`, string(got.Settings))
			assert.Empty(t, got.OperationOverrides)
			return nil
		})
	})
}

// A reconnect after a disconnect revives the soft-deleted row and clears the
// operation overrides it carried.
func TestContract_ReconnectClearsOverrides(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error {
			c := cfg(o, true, `{"workspace_id":"w"}`)
			c.OperationOverrides = map[sdk.OperationID]json.RawMessage{"fetch_repos": json.RawMessage(`{"enabled":false}`)}
			return s.configs.Save(ctx, c)
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			require.NotEmpty(t, got.OperationOverrides, "the row starts with an override")
			return s.configs.Delete(ctx, o, "github")
		})
		in(func(ctx context.Context) error {
			return s.configs.SaveCredentialsEnableWithSettings(ctx, cfg(o, false, `{}`), workspaceSettings)
		})
		in(func(ctx context.Context) error {
			got, err := s.configs.Get(ctx, o, "github")
			require.NoError(t, err)
			assert.True(t, got.Enabled)
			assert.Empty(t, got.OperationOverrides, "a revival clears the old row's overrides")
			return nil
		})
	})
}

func workspaceSettings(json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"workspace_id":"w"}`), nil
}
