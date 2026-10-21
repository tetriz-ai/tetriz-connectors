// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// A connector that panics, or passes a nil cursor to Checkpoint, fails its run:
// RunOperation returns an error instead of panicking, and the run row is
// `failed` with error kind internal and the panic value in its message.
func TestRunOperation_RecoversAPanickingOperation(t *testing.T) {
	for name, tc := range map[string]struct {
		exec    func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error)
		message string
	}{
		"execute panics": {
			exec:    func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) { panic("connector bug") },
			message: "operation panicked: connector bug",
		},
		"nil checkpoint": {
			exec: func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
				return nil, p.Checkpoint(ctx, nil)
			},
			message: "checkpoint: nil cursor",
		},
	} {
		t.Run(name, func(t *testing.T) {
			orch, mem := memOrchestrator(t, setupRegistry("c", "op", tc.exec), sink.NewChannelSink())
			var err error
			require.NotPanics(t, func() {
				err = orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
			})
			require.Error(t, err)
			run, getErr := mem.GetLatest(context.Background(), limOrg, "c", "op")
			require.NoError(t, getErr)
			assert.Equal(t, model.SyncRunStatusFailed, run.Status)
			assert.Equal(t, sdk.ErrKindInternal.String(), run.ErrorKind)
			assert.Contains(t, run.ErrorMessage, tc.message)
			assert.NotContains(t, run.ErrorMessage, "goroutine", "the stack goes to the log, not the run row")
		})
	}
}
