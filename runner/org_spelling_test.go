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
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// RunOperation accepts any spelling of the organization's UUID: it runs under
// the canonical id the config was stored with, as the tables' owner under
// forced row-level security.
func TestRunOperationCanonicalizesTheOrg(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("run operation org spelling", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	configs, states, runs := store.NewConnectorConfigRepo(db, nil), store.NewSyncStateRepo(db), store.NewSyncRunRepo(db)
	const canonical = "aaaaaaaa-0000-4000-8000-0000000000f2"
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, canonical, store.ScopeEnforced, func(sctx context.Context) error {
		return configs.Save(sctx, &sdk.ConnectorConfig{OrganizationID: canonical, ConnectorID: "c", Enabled: true,
			Settings: json.RawMessage(`{}`), Credentials: json.RawMessage(`{}`)})
	}))
	reg := setupRegistry("c", "op", func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		return &sdk.Cursor{Data: json.RawMessage(`{"page":2}`)}, nil
	})
	orch := NewOrchestrator(reg, configs, states, runs, sink.NewChannelSink(), noopGatewayFactory).WithTxRunner(store.PostgresTxRunner{})
	for _, spelling := range []string{"AAAAAAAA-0000-4000-8000-0000000000F2", "urn:uuid:" + canonical, "{" + canonical + "}"} {
		t.Run(spelling, func(t *testing.T) {
			require.NoError(t, orch.RunOperation(ctx, spelling, "c", "op", sdk.TenantKindOrganization, "test"))
			var cur *sdk.Cursor
			require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, canonical, store.ScopeEnforced, func(sctx context.Context) error {
				var err error
				cur, err = states.GetCursor(sctx, canonical, "c", "op")
				return err
			}))
			assert.JSONEq(t, `{"page":2}`, string(cur.Data), "the cursor is stored under the canonical id")
		})
	}
}
