// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

type writeStores struct {
	configs store.IConnectorConfigDAO
	states  store.ISyncStateDAO
	runs    store.ISyncRunDAO
	tx      store.TxRunner
}

func eachWriteStore(t *testing.T, fn func(t *testing.T, s writeStores)) {
	t.Run("memory", func(t *testing.T) {
		m := memstore.New()
		fn(t, writeStores{m, m, m, store.NoopTxRunner{}})
	})
	t.Run("postgres", func(t *testing.T) {
		if err := testdb.Ping(); err != nil {
			testdb.SkipOrFail("write config (postgres)", err)
			t.Skip("no test database")
		}
		db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
		require.NoError(t, err)
		t.Cleanup(cleanup)
		fn(t, writeStores{store.NewConnectorConfigRepo(db, nil), store.NewSyncStateRepo(db), store.NewSyncRunRepo(db), store.PostgresTxRunner{}})
	})
}

// Writing credentials with `config put` unpauses a connector that a terminal
// auth failure paused, and the next run executes.
func TestWriteConfigUnpausesARevokedConnector(t *testing.T) {
	eachWriteStore(t, func(t *testing.T, s writeStores) {
		ctx := context.Background()
		const org = "00000000-0000-4000-8000-0000000c0f1a"
		vendor := fakevendor.New(fakevendor.Item{ID: "a", Title: "t", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
		defer vendor.Close()
		settings, _ := json.Marshal(fakevendor.Settings{BaseURL: vendor.URL})
		cfg := func(token string) *sdk.ConnectorConfig {
			return &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: fakevendor.ConnectorID, Settings: settings,
				Credentials: json.RawMessage(`{"token":"` + token + `"}`), Enabled: true, HealthState: sdk.HealthStateHealthy}
		}
		require.NoError(t, WriteConfig(ctx, s.tx, s.configs, cfg("old")))

		var loaded *sdk.ConnectorConfig
		require.NoError(t, s.tx.RunInScope(ctx, org, store.ScopeEnforced, func(sctx context.Context) error {
			var err error
			if loaded, err = s.configs.Get(sctx, org, fakevendor.ConnectorID); err != nil {
				return err
			}
			_, err = s.configs.MarkAuthRevoked(sctx, org, fakevendor.ConnectorID, loaded.CredentialsUpdatedAt)
			return err
		}))

		require.NoError(t, WriteConfig(ctx, s.tx, s.configs, cfg("fresh")))

		reg := sdk.NewConnectorRegistry()
		reg.Register(fakevendor.Connector{})
		orch := runner.NewOrchestrator(reg, s.configs, s.states, s.runs, sink.NewChannelSink(), fakevendor.GatewayFactory(nil)).WithTxRunner(s.tx)
		require.NoError(t, orch.RunOperation(ctx, org, fakevendor.ConnectorID, fakevendor.OpFetchItems, sdk.TenantKindOrganization, "test"))
		assert.Greater(t, vendor.Calls(), 0, "the unpaused connector called its vendor")

		require.NoError(t, s.tx.RunInScope(ctx, org, store.ScopeEnforced, func(sctx context.Context) error {
			got, err := s.configs.Get(sctx, org, fakevendor.ConnectorID)
			if err != nil {
				return err
			}
			assert.Equal(t, sdk.HealthStateHealthy, got.HealthState)
			assert.JSONEq(t, `{"token":"fresh"}`, string(got.Credentials))
			assert.True(t, got.CredentialsUpdatedAt.After(loaded.CredentialsUpdatedAt) || !got.CredentialsUpdatedAt.Equal(loaded.CredentialsUpdatedAt),
				"the credential write advances the fence")
			return nil
		}))
	})
}

// A credential write that passes no operation overrides keeps the stored ones,
// as it keeps the stored settings.
func TestWriteConfigKeepsStoredOperationOverrides(t *testing.T) {
	eachWriteStore(t, func(t *testing.T, s writeStores) {
		ctx := context.Background()
		const org = "00000000-0000-4000-8000-0000000c0f1b"
		overrides := map[sdk.OperationID]json.RawMessage{fakevendor.OpFetchItems: json.RawMessage(`{"schedule":"0 3 * * *"}`)}
		require.NoError(t, WriteConfig(ctx, s.tx, s.configs, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: fakevendor.ConnectorID,
			Credentials: json.RawMessage(`{"token":"old"}`), OperationOverrides: overrides, Enabled: true, HealthState: sdk.HealthStateHealthy}))
		require.NoError(t, WriteConfig(ctx, s.tx, s.configs, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: fakevendor.ConnectorID,
			Credentials: json.RawMessage(`{"token":"new"}`), Enabled: true, HealthState: sdk.HealthStateHealthy}))

		var got *sdk.ConnectorConfig
		require.NoError(t, s.tx.RunInScope(ctx, org, store.ScopeEnforced, func(sctx context.Context) error {
			var err error
			got, err = s.configs.Get(sctx, org, fakevendor.ConnectorID)
			return err
		}))
		require.Contains(t, got.OperationOverrides, fakevendor.OpFetchItems)
		assert.JSONEq(t, `{"schedule":"0 3 * * *"}`, string(got.OperationOverrides[fakevendor.OpFetchItems]))
		assert.JSONEq(t, `{"token":"new"}`, string(got.Credentials))
	})
}

// resetFails is a store whose health reset fails.
type resetFails struct {
	*memstore.Store
	err error
}

func (r resetFails) ResetHealthState(context.Context, string, sdk.ConnectorID) (bool, error) {
	return false, r.err
}

// A credential write whose health reset fails is an error: the connector could
// otherwise stay paused while the command reports success.
func TestWriteConfigFailsWhenTheHealthResetFails(t *testing.T) {
	boom := errors.New("reset failed")
	err := WriteConfig(context.Background(), store.NoopTxRunner{}, resetFails{Store: memstore.New(), err: boom}, &sdk.ConnectorConfig{
		OrganizationID: "00000000-0000-4000-8000-0000000c0f1c", ConnectorID: fakevendor.ConnectorID,
		Credentials: json.RawMessage(`{"token":"t"}`), Enabled: true, HealthState: sdk.HealthStateHealthy})
	require.ErrorIs(t, err, boom)
}
