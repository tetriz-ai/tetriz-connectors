// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// One row whose operation_overrides is not a JSON object is left out of every
// list read and named; the other organizations' rows are returned. The schema
// refuses to write such a row.
func TestOneUndecodableRowDoesNotFailTheLists(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("unreadable rows as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	repo := store.NewConnectorConfigRepo(db, nil)
	tx := store.PostgresTxRunner{}
	const good, bad = "00000000-0000-4000-8000-0000000f0001", "00000000-0000-4000-8000-0000000f0002"
	for _, org := range []string{good, bad} {
		require.NoError(t, tx.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
			return repo.Save(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github",
				Credentials: json.RawMessage(`{"token":"t"}`), Settings: json.RawMessage(`{}`), Enabled: true})
		}))
	}

	// The constraint refuses a non-object.
	err = tx.RunInScope(ctx, bad, store.ScopeEnforced, func(s context.Context) error {
		return tenantdb.DB(s, db).Exec(`UPDATE connector_configs SET operation_overrides = '[]' WHERE organization_id = ?`, bad).Error
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connector_configs_operation_overrides_is_object")
	err = tx.RunInScope(ctx, bad, store.ScopeEnforced, func(s context.Context) error {
		return tenantdb.DB(s, db).Exec(`UPDATE connector_configs SET settings = '"x"' WHERE organization_id = ?`, bad).Error
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connector_configs_settings_is_object")

	// A row stored before the constraint: drop it for this database only.
	require.NoError(t, db.Exec(`ALTER TABLE connector_configs DROP CONSTRAINT connector_configs_operation_overrides_is_object`).Error)
	require.NoError(t, tx.RunInScope(ctx, bad, store.ScopeEnforced, func(s context.Context) error {
		return tenantdb.DB(s, db).Exec(`UPDATE connector_configs SET operation_overrides = '[]' WHERE organization_id = ?`, bad).Error
	}))

	check := func(t *testing.T, cfgs []*sdk.ConnectorConfig, err error) {
		t.Helper()
		var unreadable *store.UndecryptableRowsError
		require.True(t, errors.As(err, &unreadable), "err=%v", err)
		assert.Equal(t, []string{bad}, unreadable.OrgIDs)
		require.Len(t, cfgs, 1)
		assert.Equal(t, good, cfgs[0].OrganizationID)
	}
	t.Run("ListByConnector", func(t *testing.T) {
		var cfgs []*sdk.ConnectorConfig
		err := tx.RunInScope(ctx, "", store.ScopeSystem, func(s context.Context) (e error) { cfgs, e = repo.ListByConnector(s, "github"); return e })
		check(t, cfgs, err)
	})
	t.Run("ListEnabledByConnector", func(t *testing.T) {
		var cfgs []*sdk.ConnectorConfig
		err := tx.RunInScope(ctx, "", store.ScopeSystem, func(s context.Context) (e error) { cfgs, e = repo.ListEnabledByConnector(s, "github"); return e })
		check(t, cfgs, err)
	})
	t.Run("ListByOrg", func(t *testing.T) {
		var cfgs []*sdk.ConnectorConfig
		err := tx.RunInScope(ctx, bad, store.ScopeEnforced, func(s context.Context) (e error) { cfgs, e = repo.ListByOrg(s, bad); return e })
		var unreadable *store.UndecryptableRowsError
		require.True(t, errors.As(err, &unreadable), "the organization's own unreadable row is reported")
		assert.Empty(t, cfgs)
	})
	t.Run("ListEnabledOrganizations", func(t *testing.T) {
		var orgs []string
		err := tx.RunInScope(ctx, "", store.ScopeSystem, func(s context.Context) (e error) { orgs, e = repo.ListEnabledOrganizations(s, "github"); return e })
		require.NoError(t, err, "it decodes nothing")
		assert.Equal(t, []string{good, bad}, orgs)
	})
}
