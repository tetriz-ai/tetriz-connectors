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
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// A run that fails before its row exists (here: no stored config) reports
// "failed" with the error, not "skipped", and no cursor: the stored one is
// another run's.
func TestSyncOnceFailingBeforeItsRowReportsFailed(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("sync result", err)
		t.Skip("no test database")
	}
	db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	ctx := context.Background()
	const org = "00000000-0000-4000-8000-0000000d1e0b"
	reg := sdk.NewConnectorRegistry()
	reg.Register(fakevendor.Connector{})
	configs, states, runs := store.NewConnectorConfigRepo(db, nil), store.NewSyncStateRepo(db), store.NewSyncRunRepo(db)
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(sctx context.Context) error {
		return states.SaveCursor(sctx, org, fakevendor.ConnectorID, fakevendor.OpFetchItems, &sdk.Cursor{Data: json.RawMessage(`{"another":"run"}`)})
	}))
	a := &App{
		Registry: reg, Configs: configs, States: states, Runs: runs,
		Orchestrator: runner.NewOrchestrator(reg, configs, states, runs, sink.NewChannelSink(), fakevendor.GatewayFactory(nil)).WithTxRunner(store.PostgresTxRunner{}),
	}

	res, err := a.SyncOnce(ctx, org, fakevendor.ConnectorID, fakevendor.OpFetchItems, sdk.TenantKindOrganization)
	require.Error(t, err)
	assert.Equal(t, "failed", res.Status)
	assert.Contains(t, res.Error, "load config")
	assert.Empty(t, res.RunID)
	assert.Empty(t, res.Cursor, "no other run's cursor is reported")
}
