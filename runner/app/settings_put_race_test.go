// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

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

// racingConfigs lands a concurrent write on the first store call the
// settings put makes, before that call runs: the window between a read and
// a write, where a token refresh or a disconnect can land.
type racingConfigs struct {
	store.IConnectorConfigDAO
	between func()
}

func (r *racingConfigs) land() {
	if f := r.between; f != nil {
		r.between = nil
		f()
	}
}

func (r *racingConfigs) Get(ctx context.Context, org string, c sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	cfg, err := r.IConnectorConfigDAO.Get(ctx, org, c)
	if err == nil {
		r.land()
	}
	return cfg, err //nolint:wrapcheck // test double
}

func (r *racingConfigs) PatchSettings(ctx context.Context, org string, c sdk.ConnectorID, m func(json.RawMessage) (json.RawMessage, error)) error {
	r.land()
	return r.IConnectorConfigDAO.PatchSettings(ctx, org, c, m) //nolint:wrapcheck // test double
}

func (r *racingConfigs) SetEnabled(ctx context.Context, org string, c sdk.ConnectorID, enabled bool) error {
	r.land()
	return r.IConnectorConfigDAO.SetEnabled(ctx, org, c, enabled) //nolint:wrapcheck // test double
}

// A settings-only `config put` writes settings and enabled and nothing else: a
// token refresh that lands during it keeps its rotated credentials, and a
// disconnect that lands during it stays a disconnect.
func testSettingsPutKeepsConcurrentWrites(t *testing.T, repo store.IConnectorConfigDAO, tx store.TxRunner, org string) {
	ctx := context.Background()
	in := func(fn func(context.Context) error) {
		require.NoError(t, tx.RunInScope(context.Background(), org, store.ScopeEnforced, fn))
	}
	seed := func() *sdk.ConnectorConfig {
		require.NoError(t, WriteConfig(ctx, tx, repo, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
			Settings: json.RawMessage(`{"cloud_id":"a"}`), Credentials: json.RawMessage(`{"refresh_token":"old"}`), Enabled: true,
			OperationOverrides: map[sdk.OperationID]json.RawMessage{"fetch_issues": json.RawMessage(`{"schedule":"0 * * * *"}`)}}))
		var got *sdk.ConnectorConfig
		in(func(s context.Context) (err error) { got, err = repo.Get(s, org, "jira"); return err })
		return got
	}

	t.Run("token_refresh", func(t *testing.T) {
		loaded := seed()
		racer := &racingConfigs{IConnectorConfigDAO: repo, between: func() {
			in(func(s context.Context) error {
				ok, e := repo.CompareAndSwapCredentials(s, org, "jira", loaded.CredentialsUpdatedAt, json.RawMessage(`{"refresh_token":"rotated"}`))
				require.True(t, ok)
				return e
			})
		}}
		require.NoError(t, UpdateSettings(ctx, tx, racer, org, "jira", json.RawMessage(`{"cloud_id":"b"}`), true))
		var got *sdk.ConnectorConfig
		in(func(s context.Context) (err error) { got, err = repo.Get(s, org, "jira"); return err })
		assert.JSONEq(t, `{"refresh_token":"rotated"}`, string(got.Credentials), "the refreshed token was overwritten")
		assert.JSONEq(t, `{"cloud_id":"b"}`, string(got.Settings))
		assert.True(t, got.Enabled)
		assert.Contains(t, got.OperationOverrides, sdk.OperationID("fetch_issues"), "the override is kept")
	})

	t.Run("disconnect", func(t *testing.T) {
		seed()
		racer := &racingConfigs{IConnectorConfigDAO: repo, between: func() {
			in(func(s context.Context) error { return repo.Delete(s, org, "jira") })
		}}
		err := UpdateSettings(ctx, tx, racer, org, "jira", json.RawMessage(`{"cloud_id":"b"}`), true)
		if err != nil {
			assert.ErrorIs(t, err, ErrNoStoredConfig)
		}
		getErr := tx.RunInScope(context.Background(), org, store.ScopeEnforced, func(s context.Context) error {
			got, e := repo.Get(s, org, "jira")
			if e == nil {
				t.Errorf("a disconnect was undone: row live again, enabled=%t credentials=%s", got.Enabled, got.Credentials)
			}
			return e
		})
		assert.True(t, store.IsNotFound(getErr), "the row stays disconnected, got %v", getErr)
	})
}

func TestSettingsPutKeepsConcurrentWrites(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		if err := testdb.Ping(); err != nil {
			testdb.SkipOrFail("settings put as the table owner", err)
			t.Skip("no test database")
		}
		db, _, cleanup, err := testdb.SetupAsOwner(context.Background(), testdb.Connector)
		require.NoError(t, err)
		t.Cleanup(cleanup)
		testSettingsPutKeepsConcurrentWrites(t, store.NewConnectorConfigRepo(db, nil), store.PostgresTxRunner{}, "00000000-0000-4000-8000-0000000c1a01")
	})
	t.Run("memstore", func(t *testing.T) {
		testSettingsPutKeepsConcurrentWrites(t, memstore.New(), store.NoopTxRunner{}, "00000000-0000-4000-8000-0000000c1a02")
	})
}
