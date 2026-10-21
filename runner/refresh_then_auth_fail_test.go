// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// A run that refreshes its token and then fails with a terminal auth error
// pauses the connector: the failure belongs to the refreshed credentials, so
// the pause is fenced on what the refresh wrote, not on the credentials the
// run started with.
func TestRefreshThenTerminalAuthPausesThatRun(t *testing.T) {
	var mem *memstore.Store
	reg := setupRegistry("c", "op", func(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
		// An in-run OAuth refresh stores the rotated token.
		cur, err := mem.Get(ctx, limOrg, "c")
		require.NoError(t, err)
		ok, err := mem.CompareAndSwapCredentials(ctx, limOrg, "c", cur.CredentialsUpdatedAt, json.RawMessage(`{"refresh_token":"rotated"}`))
		require.NoError(t, err)
		require.True(t, ok)
		return nil, &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401, Terminal: true, Cause: errors.New("HTTP 401")}
	})
	orch, m := memOrchestrator(t, reg, sink.NewChannelSink())
	mem = m

	require.Error(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	cfg, err := mem.Get(context.Background(), limOrg, "c")
	require.NoError(t, err)
	assert.Equal(t, sdk.HealthStateAuthRevoked, cfg.HealthState, "the run that refreshed and then failed pauses the connector")
}

// A credential write from outside the run (a reconnect) that lands during it
// still wins: the run's terminal auth failure belongs to the replaced
// credentials, and the connector is not paused.
func TestReconnectDuringTheRunIsNotPaused(t *testing.T) {
	var mem *memstore.Store
	reg := setupRegistry("c", "op", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
		// A reconnect, outside this run's context, stores new credentials.
		cur, err := mem.Get(context.Background(), limOrg, "c")
		require.NoError(t, err)
		ok, err := mem.CompareAndSwapCredentials(context.Background(), limOrg, "c", cur.CredentialsUpdatedAt, json.RawMessage(`{"refresh_token":"reconnected"}`))
		require.NoError(t, err)
		require.True(t, ok)
		return nil, &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401, Terminal: true, Cause: errors.New("HTTP 401")}
	})
	orch, m := memOrchestrator(t, reg, sink.NewChannelSink())
	mem = m

	require.Error(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	cfg, err := mem.Get(context.Background(), limOrg, "c")
	require.NoError(t, err)
	assert.Equal(t, sdk.HealthStateHealthy, cfg.HealthState, "the reconnect's credentials are not paused for the old ones' failure")
}
