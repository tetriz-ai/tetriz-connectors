// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// updaterRace lands a concurrent write on the first store call the settings
// write makes, before that call runs.
type updaterRace struct {
	store.IConnectorConfigDAO
	between func()
}

func (r *updaterRace) land() {
	if f := r.between; f != nil {
		r.between = nil
		f()
	}
}

func (r *updaterRace) Get(ctx context.Context, org string, c sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	cfg, err := r.IConnectorConfigDAO.Get(ctx, org, c)
	if err == nil {
		r.land()
	}
	return cfg, err //nolint:wrapcheck // test double
}

func (r *updaterRace) PatchSettings(ctx context.Context, org string, c sdk.ConnectorID, m func(json.RawMessage) (json.RawMessage, error)) error {
	r.land()
	return r.IConnectorConfigDAO.PatchSettings(ctx, org, c, m) //nolint:wrapcheck // test double
}

// sdk.ConfigUpdater.UpdateSettings writes only the settings: a credential swap
// that lands during it keeps the rotated credentials, and a disconnect that
// lands during it stays a disconnect.
func testConfigUpdaterKeepsConcurrentWrites(t *testing.T, repo store.IConnectorConfigDAO, tx store.TxRunner, org string) {
	ctx := context.Background()
	in := func(fn func(context.Context) error) error {
		return tx.RunInScope(context.Background(), org, store.ScopeEnforced, fn) //nolint:wrapcheck // test helper
	}
	seed := func() *sdk.ConnectorConfig {
		var got *sdk.ConnectorConfig
		require.NoError(t, in(func(s context.Context) error {
			if err := repo.Save(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira", Enabled: true,
				Settings: json.RawMessage(`{"cloud_id":"a"}`), Credentials: json.RawMessage(`{"refresh_token":"old"}`)}); err != nil {
				return err
			}
			if _, err := repo.ResetHealthState(s, org, "jira"); err != nil {
				return err
			}
			var err error
			got, err = repo.Get(s, org, "jira")
			return err
		}))
		return got
	}

	t.Run("credential_swap", func(t *testing.T) {
		loaded := seed()
		racer := &updaterRace{IConnectorConfigDAO: repo, between: func() {
			require.NoError(t, in(func(s context.Context) error {
				ok, e := repo.CompareAndSwapCredentials(s, org, "jira", loaded.CredentialsUpdatedAt, json.RawMessage(`{"refresh_token":"rotated"}`))
				require.True(t, ok)
				return e
			}))
		}}
		u := &configUpdaterAdapter{dao: racer, tx: tx}
		require.NoError(t, u.UpdateSettings(ctx, org, "jira", json.RawMessage(`{"cloud_id":"b"}`)))
		var got *sdk.ConnectorConfig
		require.NoError(t, in(func(s context.Context) (err error) { got, err = repo.Get(s, org, "jira"); return err }))
		assert.JSONEq(t, `{"refresh_token":"rotated"}`, string(got.Credentials), "the swapped credentials survive")
		assert.JSONEq(t, `{"cloud_id":"b"}`, string(got.Settings))
	})

	t.Run("disconnect", func(t *testing.T) {
		seed()
		racer := &updaterRace{IConnectorConfigDAO: repo, between: func() {
			require.NoError(t, in(func(s context.Context) error { return repo.Delete(s, org, "jira") }))
		}}
		u := &configUpdaterAdapter{dao: racer, tx: tx}
		err := u.UpdateSettings(ctx, org, "jira", json.RawMessage(`{"cloud_id":"b"}`))
		if err != nil {
			assert.True(t, store.IsNotFound(err), "a disconnected row is not found, got %v", err)
		}
		getErr := in(func(s context.Context) error { _, e := repo.Get(s, org, "jira"); return e })
		assert.True(t, store.IsNotFound(getErr), "the row stays disconnected, got %v", getErr)
	})
}

func TestConfigUpdaterUpdateSettingsKeepsConcurrentWrites(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		if err := testdb.Ping(); err != nil {
			testdb.SkipOrFail("config updater as the table owner", err)
			t.Skip("no test database")
		}
		db, _, cleanup, err := testdb.SetupAsOwner(context.Background(), testdb.Connector)
		require.NoError(t, err)
		t.Cleanup(cleanup)
		testConfigUpdaterKeepsConcurrentWrites(t, store.NewConnectorConfigRepo(db, nil), store.PostgresTxRunner{}, "00000000-0000-4000-8000-0000000c1a11")
	})
	t.Run("memstore", func(t *testing.T) {
		testConfigUpdaterKeepsConcurrentWrites(t, memstore.New(), store.NoopTxRunner{}, "00000000-0000-4000-8000-0000000c1a12")
	})
}
