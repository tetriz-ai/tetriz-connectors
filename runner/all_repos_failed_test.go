// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// When every repository an operation attempted failed with a terminal auth
// error, the run fails as auth and the connector is paused.
func TestEveryRepositoryFailingWithAuthPausesTheConnector(t *testing.T) {
	reg := setupRegistry("c", "op", func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		var tally repotally.Tally
		for _, repo := range []string{"acme/a", "acme/b"} {
			tally.Attempt()
			tally.Fail(repo, &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401, Terminal: true, Cause: errors.New("HTTP 401")})
		}
		return nil, tally.Err(2)
	})
	orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
	err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	cfg, err := mem.Get(context.Background(), limOrg, "c")
	require.NoError(t, err)
	assert.Equal(t, sdk.HealthStateAuthRevoked, cfg.HealthState, "the connector is paused")
}
