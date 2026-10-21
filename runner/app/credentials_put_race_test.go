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
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// `config put --credentials` without --settings keeps the stored settings in
// its own write statement, so a settings put that lands just before that
// statement, or between a read and the write, stands.
func TestCredentialsPutKeepsAConcurrentSettingsPut(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("credentials put as the table owner", err)
		t.Skip("no test database")
	}
	db, _, cleanup, err := testdb.SetupAsOwner(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	repo := store.NewConnectorConfigRepo(db, nil)
	tx := store.PostgresTxRunner{}
	org := "00000000-0000-4000-8000-0000000c1b01"
	ctx := context.Background()
	require.NoError(t, WriteConfig(ctx, tx, repo, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
		Settings: json.RawMessage(`{"cloud_id":"a"}`), Credentials: json.RawMessage(`{"refresh_token":"old"}`), Enabled: true}))

	racer := &racingConfigs{IConnectorConfigDAO: repo, between: func() {
		require.NoError(t, UpdateSettings(context.Background(), tx, repo, org, "jira", json.RawMessage(`{"cloud_id":"b"}`), true))
	}}
	require.NoError(t, WriteConfig(ctx, tx, racer, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
		Credentials: json.RawMessage(`{"refresh_token":"new"}`), Enabled: true}))

	var got *sdk.ConnectorConfig
	require.NoError(t, tx.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) (e error) { got, e = repo.Get(s, org, "jira"); return e }))
	t.Logf("settings after both writes: %s", got.Settings)
	assert.JSONEq(t, `{"cloud_id":"b"}`, string(got.Settings), "the concurrent settings put was overwritten")
	assert.JSONEq(t, `{"refresh_token":"new"}`, string(got.Credentials), "the credentials put landed")
}

// SaveKeepingUnset lands the concurrent write just before the credential
// write's own statement.
func (r *racingConfigs) SaveKeepingUnset(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	r.land()
	return r.IConnectorConfigDAO.SaveKeepingUnset(ctx, cfg) //nolint:wrapcheck // test double
}

// A credentials put without --settings over a disconnected row does not carry
// the torn-down settings or overrides across: a revived row gets {}.
func TestCredentialsPutOverADisconnectedRowStartsFromEmptySettings(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("credentials put as the table owner", err)
		t.Skip("no test database")
	}
	db, _, cleanup, err := testdb.SetupAsOwner(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	repo := store.NewConnectorConfigRepo(db, nil)
	tx := store.PostgresTxRunner{}
	org := "00000000-0000-4000-8000-0000000c1b02"
	ctx := context.Background()
	require.NoError(t, WriteConfig(ctx, tx, repo, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
		Settings: json.RawMessage(`{"cloud_id":"a"}`), Credentials: json.RawMessage(`{"refresh_token":"old"}`), Enabled: true,
		OperationOverrides: map[sdk.OperationID]json.RawMessage{"fetch_issues": json.RawMessage(`{"schedule":"0 * * * *"}`)}}))
	require.NoError(t, tx.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error { return repo.Delete(s, org, "jira") }))

	require.NoError(t, WriteConfig(ctx, tx, repo, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
		Credentials: json.RawMessage(`{"refresh_token":"new"}`), Enabled: true}))
	var got *sdk.ConnectorConfig
	require.NoError(t, tx.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) (e error) { got, e = repo.Get(s, org, "jira"); return e }))
	assert.JSONEq(t, `{}`, string(got.Settings))
	assert.Empty(t, got.OperationOverrides)
}
