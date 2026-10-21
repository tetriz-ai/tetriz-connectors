// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

type panickingConfigConnector struct{ *mockConnector }

func (*panickingConfigConnector) ValidateConfig(context.Context, *sdk.ConnectorConfig) error {
	panic("validator bug")
}

// A panic in ValidateConfig, an operation's factory or the gateway factory
// fails the run as internal instead of crashing the process.
func TestRunOperation_RecoversAPanickingValidateConfig(t *testing.T) {
	okOp := func() sdk.Operation {
		return &mockFetchOp{opID: "op", executeFn: func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) { return nil, nil }}
	}
	for name, tc := range map[string]struct {
		conn    sdk.Connector
		gateway sdk.VendorGatewayFactory
		message string
	}{
		"ValidateConfig": {
			conn:    &panickingConfigConnector{&mockConnector{id: "c", ops: []sdk.OperationDescriptor{{ID: "op", IngestionType: sdk.IngestionPull, Factory: okOp}}}},
			message: "ValidateConfig panicked: validator bug",
		},
		"operation factory": {
			conn: &mockConnector{id: "c", ops: []sdk.OperationDescriptor{{ID: "op", IngestionType: sdk.IngestionPull,
				Factory: func() sdk.Operation { panic("factory bug") }}}},
			message: "operation factory panicked: factory bug",
		},
		"gateway factory": {
			conn:    &mockConnector{id: "c", ops: []sdk.OperationDescriptor{{ID: "op", IngestionType: sdk.IngestionPull, Factory: okOp}}},
			gateway: func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) { panic("gateway bug") },
			message: "gateway factory panicked: gateway bug",
		},
	} {
		t.Run(name, func(t *testing.T) {
			reg := sdk.NewConnectorRegistry()
			reg.Register(tc.conn)
			orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
			if tc.gateway != nil {
				orch.gatewayFactory = tc.gateway
			}
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
		})
	}
}

// A lease below MinRunLease is raised to it, so the heartbeat's ticker
// (a third of the lease) never gets a non-positive interval.
func TestWithRunLease_TinyLeaseDoesNotPanic(t *testing.T) {
	reg := setupRegistry("c", "op", func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) { return nil, nil })
	orch, _ := memOrchestrator(t, reg, sink.NewChannelSink())
	orch.WithRunLease(time.Nanosecond)
	assert.Equal(t, MinRunLease, orch.runLease)
	require.NotPanics(t, func() {
		require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	})
}

// The heartbeat renews the lease every third of it: two thirds into the
// first lease, the stored lease has already moved forward.
func TestHeartbeatRenewsWithinAThirdOfTheLease(t *testing.T) {
	var mem *memstore.Store
	var first, later time.Time
	reg := setupRegistry("c", "op", func(ctx context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
		r, err := mem.GetLatest(ctx, limOrg, "c", "op")
		require.NoError(t, err)
		first = *r.LeaseExpiresAt
		time.Sleep(MinRunLease * 2 / 3)
		r, err = mem.GetLatest(ctx, limOrg, "c", "op")
		require.NoError(t, err)
		later = *r.LeaseExpiresAt
		return nil, nil
	})
	var orch *Orchestrator
	orch, mem = memOrchestrator(t, reg, sink.NewChannelSink())
	orch.WithRunLease(MinRunLease)
	require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	assert.True(t, later.After(first), "the lease moved forward before two thirds of it passed (first %v, later %v)", first, later)
}

// closeOwnRow expires the running run's lease and closes it as abandoned, as
// a reconciler would after the lease ran out.
func closeOwnRow(ctx context.Context, t *testing.T, runs store.ISyncRunDAO, tx store.TxRunner) {
	t.Helper()
	runID := sdk.SyncRunIDFromContext(ctx)
	require.NoError(t, tx.RunInScope(context.WithoutCancel(ctx), limOrg, store.ScopeEnforced, func(sCtx context.Context) error {
		require.NoError(t, runs.RenewLease(sCtx, limOrg, runID, -time.Hour))
		n, err := runs.CloseAbandoned(sCtx, limOrg, "c", "op")
		require.Equal(t, int64(1), n)
		return err
	}))
}

// A run whose row was closed as abandoned while it ran does not overwrite the
// closed row when it finishes, saves no cursor (the lease is fenced before
// the cursor, which matters for stores without transactions), and reports
// that it lost its lease.
func TestClosedRunDoesNotOverwriteItsAbandonedRow(t *testing.T) {
	for name, open := range map[string]func(t *testing.T) (*memstore.Store, sdk.DataSink, func() store.ISyncRunDAO){
		"memory": func(t *testing.T) (*memstore.Store, sdk.DataSink, func() store.ISyncRunDAO) {
			m := memstore.New()
			return m, nil, func() store.ISyncRunDAO { return m }
		},
		"file": func(t *testing.T) (*memstore.Store, sdk.DataSink, func() store.ISyncRunDAO) {
			enc, err := cryptutil.NewAESGCMEncryptor(make([]byte, 32))
			require.NoError(t, err)
			f, err := memstore.OpenFile(filepath.Join(t.TempDir(), "store.json"), enc)
			require.NoError(t, err)
			return f.Store, nil, func() store.ISyncRunDAO { return f }
		},
	} {
		t.Run(name, func(t *testing.T) {
			mem, _, runsOf := open(t)
			runs := runsOf()
			reg := setupRegistry("c", "op", func(ctx context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
				closeOwnRow(ctx, t, runs, store.NoopTxRunner{})
				return &sdk.Cursor{Data: json.RawMessage(`{"n":2}`)}, nil
			})
			require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: limOrg, ConnectorID: "c", Enabled: true, Settings: json.RawMessage(`{}`)}))
			states, _ := runs.(store.ISyncStateDAO)
			require.NoError(t, states.SaveCursor(context.Background(), limOrg, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{"n":1}`)}))
			orch := NewOrchestrator(reg, runs.(store.IConnectorConfigDAO), states, runs, sink.NewChannelSink(), noopGatewayFactory).WithTxRunner(store.NoopTxRunner{})
			err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
			require.ErrorIs(t, err, store.ErrRunClosed)
			run, getErr := mem.GetLatest(context.Background(), limOrg, "c", "op")
			require.NoError(t, getErr)
			assert.Equal(t, model.SyncRunStatusFailed, run.Status)
			assert.Equal(t, sdk.ErrKindAbandoned.String(), run.ErrorKind, "the closed row is kept")
			c, getErr := states.GetCursor(context.Background(), limOrg, "c", "op")
			require.NoError(t, getErr)
			assert.JSONEq(t, `{"n":1}`, string(c.Data), "the run that lost its lease saved no cursor")
		})
	}
}

// Once the heartbeat stops a run (its row was closed while it ran), the sink
// takes no more records and Checkpoint persists nothing, even when the
// connector calls them on a context that is not cancelled.
func TestLeaseLostRunPublishesAndCheckpointsNothing(t *testing.T) {
	var mem *memstore.Store
	var emitErr, cpErr error
	reg := setupRegistry("c", "op", func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		closeOwnRow(ctx, t, mem, store.NoopTxRunner{})
		select {
		case <-ctx.Done():
		case <-time.After(3 * MinRunLease):
			return nil, errors.New("the run was not stopped")
		}
		late := context.WithoutCancel(ctx)
		emitErr = p.Sink.Emit(late, &sdk.RawRecord{OrganizationID: limOrg, ConnectorID: "c", OperationID: "op", ExternalID: "late"})
		cpErr = p.Checkpoint(late, &sdk.Cursor{Data: json.RawMessage(`{"n":2}`)})
		return nil, ctx.Err()
	})
	records := sink.NewChannelSink()
	var orch *Orchestrator
	orch, mem = memOrchestrator(t, reg, records)
	orch.WithRunLease(MinRunLease)
	require.NoError(t, mem.SaveCursor(context.Background(), limOrg, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{"n":1}`)}))
	err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.ErrorIs(t, err, errLeaseLost)
	require.ErrorIs(t, emitErr, errLeaseLost)
	require.ErrorIs(t, cpErr, errLeaseLost)
	assert.Empty(t, records.Records(), "nothing is published after the lease is lost")
	c, getErr := mem.GetCursor(context.Background(), limOrg, "c", "op")
	require.NoError(t, getErr)
	assert.JSONEq(t, `{"n":1}`, string(c.Data))
}

// failingRenewals fails every lease renewal after the first (the one that
// starts the lease when the row is created), or only the second when once.
type failingRenewals struct {
	*memstore.Store
	once  bool
	calls int
}

func (f *failingRenewals) RenewLease(ctx context.Context, orgID, runID string, lease time.Duration) error {
	f.calls++
	if f.calls == 1 || (f.once && f.calls > 2) {
		return f.Store.RenewLease(ctx, orgID, runID, lease)
	}
	return errors.New("database unreachable")
}

// A renewal that fails is retried on the next tick, and the run goes on. When
// no renewal lands for a whole lease, the run is stopped and fails as
// abandoned, since a reconciler may already be closing its row.
func TestHeartbeatRenewalFailures(t *testing.T) {
	t.Run("one failure is retried", func(t *testing.T) {
		reg := setupRegistry("c", "op", func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
			time.Sleep(MinRunLease * 5 / 4)
			return nil, nil
		})
		orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
		orch.runDAO = &failingRenewals{Store: mem, once: true}
		orch.WithRunLease(MinRunLease)
		require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	})
	t.Run("a whole lease of failures stops the run", func(t *testing.T) {
		reg := setupRegistry("c", "op", func(ctx context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * MinRunLease):
				return nil, errors.New("the run was not stopped")
			}
		})
		orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
		orch.runDAO = &failingRenewals{Store: mem}
		orch.WithRunLease(MinRunLease)
		err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
		stopped := time.Now()
		require.ErrorIs(t, err, errLeaseLost)
		run, getErr := mem.GetLatest(context.Background(), limOrg, "c", "op")
		require.NoError(t, getErr)
		assert.Equal(t, sdk.ErrKindAbandoned.String(), run.ErrorKind)
		require.NotNil(t, run.LeaseExpiresAt)
		assert.True(t, stopped.Before(*run.LeaseExpiresAt), "the run stopped (%v) before its stored lease expired (%v)", stopped, *run.LeaseExpiresAt)
	})
}

type panickingFlush struct{ sdk.DataSink }

func (panickingFlush) Flush(context.Context) error { panic("sink bug") }

// A panic inside Checkpoint (here the sink's Flush) fails that checkpoint
// with an error instead of escaping the run.
func TestCheckpointRecoversAPanic(t *testing.T) {
	var cpErr error
	reg := setupRegistry("c", "op", func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		cpErr = p.Checkpoint(ctx, &sdk.Cursor{Data: json.RawMessage(`{"n":1}`)})
		return nil, cpErr
	})
	orch, _ := memOrchestrator(t, reg, panickingFlush{sink.NewChannelSink()})
	var err error
	require.NotPanics(t, func() {
		err = orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	})
	require.Error(t, err)
	require.Error(t, cpErr)
	assert.Contains(t, cpErr.Error(), "checkpoint panicked: sink bug")
}

// A Checkpoint panic fails the run as internal even when the operation
// ignores Checkpoint's error and returns success.
func TestSwallowedCheckpointPanicFailsTheRun(t *testing.T) {
	reg := setupRegistry("c", "op", func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		_ = p.Checkpoint(ctx, &sdk.Cursor{Data: json.RawMessage(`{"n":1}`)})
		return nil, nil
	})
	orch, mem := memOrchestrator(t, reg, panickingFlush{sink.NewChannelSink()})
	err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.Error(t, err)
	run, getErr := mem.GetLatest(context.Background(), limOrg, "c", "op")
	require.NoError(t, getErr)
	assert.Equal(t, model.SyncRunStatusFailed, run.Status)
	assert.Equal(t, sdk.ErrKindInternal.String(), run.ErrorKind)
	assert.Contains(t, run.ErrorMessage, "checkpoint panicked: sink bug")
}

// A panic in the final sink flush, after an operation that never
// checkpoints, fails the run as internal instead of crashing the process.
func TestFinalFlushPanicFailsTheRun(t *testing.T) {
	reg := setupRegistry("c", "op", func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		return &sdk.Cursor{Data: json.RawMessage(`{"n":1}`)}, nil
	})
	orch, mem := memOrchestrator(t, reg, panickingFlush{sink.NewChannelSink()})
	var err error
	require.NotPanics(t, func() {
		err = orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	})
	require.Error(t, err)
	run, getErr := mem.GetLatest(context.Background(), limOrg, "c", "op")
	require.NoError(t, getErr)
	assert.Equal(t, model.SyncRunStatusFailed, run.Status)
	assert.Equal(t, sdk.ErrKindInternal.String(), run.ErrorKind)
	assert.Contains(t, run.ErrorMessage, "final sink flush panicked: sink bug")
}
