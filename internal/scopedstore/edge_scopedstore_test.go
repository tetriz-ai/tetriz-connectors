// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package scopedstore_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/scopedstore"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The package had no tests, so it had no guard; this file adds one.
func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

// ownerDB is a database the test connects to as its owner, so forced
// row-level security applies to every query.
func ownerDB(t *testing.T) *gorm.DB {
	t.Helper()
	if err := testdb.Ping(); err != nil {
		if v := os.Getenv(testdb.RequireEnv); v != "" && v != "0" {
			t.Fatalf("no test database: %v", err)
		}
		t.Skipf("no test database: %v", err)
	}
	db, _, cleanup, err := testdb.SetupAsOwner(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	return db
}

const (
	orgA = "0000000a-0000-4000-8000-00000000000a"
	orgB = "0000000b-0000-4000-8000-00000000000b"
)

// An organization id spelled in upper case or in braces reaches the same rows
// as its canonical spelling, through every wrapped store.
func TestEdge_OrgSpellingsReachTheSameRows(t *testing.T) {
	db := ownerDB(t)
	tx := store.PostgresTxRunner{}
	configs := scopedstore.Configs(tx, store.NewConnectorConfigRepo(db, nil))
	states := scopedstore.States(tx, store.NewSyncStateRepo(db))
	ctx := context.Background()
	upper := strings.ToUpper(orgA)
	braces := "{" + orgA + "}"

	require.NoError(t, configs.Save(ctx, &sdk.ConnectorConfig{OrganizationID: upper, ConnectorID: "github", Enabled: true, Settings: json.RawMessage(`{}`), Credentials: json.RawMessage(`{}`)}))
	for _, spelling := range []string{orgA, upper, braces} {
		got, err := configs.Get(ctx, spelling, "github")
		require.NoError(t, err, spelling)
		assert.Equal(t, orgA, got.OrganizationID, "stored under the canonical id")
	}

	require.NoError(t, states.SaveCursor(ctx, braces, "github", "fetch_prs", &sdk.Cursor{Data: json.RawMessage(`{"n":1}`)}))
	c, err := states.GetCursor(ctx, upper, "github", "fetch_prs")
	require.NoError(t, err)
	assert.JSONEq(t, `{"n":1}`, string(c.Data))
}

// A call made with a context already scoped to one organization runs in that
// scope, so naming another organization reaches none of its rows; a write
// naming the other organization fails instead of landing.
func TestEdge_AlreadyScopedContextDoesNotWidenToAnotherOrg(t *testing.T) {
	db := ownerDB(t)
	tx := store.PostgresTxRunner{}
	states := scopedstore.States(tx, store.NewSyncStateRepo(db))
	ctx := context.Background()
	require.NoError(t, states.SaveCursor(ctx, orgB, "github", "fetch_prs", &sdk.Cursor{Data: json.RawMessage(`{"b":1}`)}))

	require.NoError(t, tenantdb.RunInScope(ctx, orgA, tenantdb.ScopeEnforced, func(sctx context.Context) error {
		_, err := states.GetCursor(sctx, orgB, "github", "fetch_prs")
		assert.ErrorIs(t, err, store.ErrCursorNotFound, "A's scope cannot read B's cursor through the wrapper")
		return nil
	}))
	err := tenantdb.RunInScope(ctx, orgA, tenantdb.ScopeEnforced, func(sctx context.Context) error {
		return states.SaveCursor(sctx, orgB, "github", "fetch_prs", &sdk.Cursor{Data: json.RawMessage(`{"forged":1}`)})
	})
	assert.Error(t, err, "A's scope cannot overwrite B's cursor through the wrapper")

	c, err := states.GetCursor(ctx, orgB, "github", "fetch_prs")
	require.NoError(t, err)
	assert.JSONEq(t, `{"b":1}`, string(c.Data))
}

// The cross-organization lists run in the system scope and see every
// organization; a per-organization list sees only its own.
func TestEdge_ListsSpanOrganizationsOnlyWhereTheyShould(t *testing.T) {
	db := ownerDB(t)
	tx := store.PostgresTxRunner{}
	configs := scopedstore.Configs(tx, store.NewConnectorConfigRepo(db, nil))
	ctx := context.Background()
	for _, o := range []string{orgA, orgB} {
		require.NoError(t, configs.Save(ctx, &sdk.ConnectorConfig{OrganizationID: o, ConnectorID: "github", Enabled: true, Settings: json.RawMessage(`{}`), Credentials: json.RawMessage(`{}`)}))
	}
	all, err := configs.ListEnabledByConnector(ctx, "github")
	require.NoError(t, err)
	assert.Len(t, all, 2)
	orgs, err := configs.ListEnabledOrganizations(ctx, "github")
	require.NoError(t, err)
	assert.Equal(t, []string{orgA, orgB}, orgs)
	mine, err := configs.ListByOrg(ctx, orgA)
	require.NoError(t, err)
	require.Len(t, mine, 1)
	assert.Equal(t, orgA, mine[0].OrganizationID)
}

// A nil store stays nil, and a nil runner leaves calls unscoped, which under
// forced row-level security sees no rows.
func TestEdge_NilStoreAndNilRunner(t *testing.T) {
	assert.Nil(t, scopedstore.Configs(store.PostgresTxRunner{}, nil))
	assert.Nil(t, scopedstore.States(store.PostgresTxRunner{}, nil))
	assert.Nil(t, scopedstore.Runs(store.PostgresTxRunner{}, nil))
	assert.Nil(t, scopedstore.Schedules(store.PostgresTxRunner{}, nil))

	db := ownerDB(t)
	scoped := scopedstore.States(store.PostgresTxRunner{}, store.NewSyncStateRepo(db))
	require.NoError(t, scoped.SaveCursor(context.Background(), orgA, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{}`)}))
	unscoped := scopedstore.States(nil, store.NewSyncStateRepo(db))
	_, err := unscoped.GetCursor(context.Background(), orgA, "c", "op")
	assert.ErrorIs(t, err, store.ErrCursorNotFound)
}
