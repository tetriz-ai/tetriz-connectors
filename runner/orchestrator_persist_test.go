// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// flushFailingSink accepts records and fails every Flush.
type flushFailingSink struct {
	mu sync.Mutex
	n  int
}

func (s *flushFailingSink) Emit(context.Context, *sdk.RawRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return nil
}
func (s *flushFailingSink) EmitBatch(ctx context.Context, rs []*sdk.RawRecord) error {
	for _, r := range rs {
		if err := s.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}
func (s *flushFailingSink) Flush(context.Context) error { return errors.New("sink unavailable") }

// ctxCheckingStates refuses a cursor write on a cancelled context, as a real
// database driver does.
type ctxCheckingStates struct{ *memstore.Store }

func (s ctxCheckingStates) SaveCursor(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, c *sdk.Cursor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.SaveCursor(ctx, orgID, connectorID, opID, c)
}

// recordingTx records the scope of every transaction.
type recordingTx struct {
	mu     sync.Mutex
	scopes []store.Scope
}

func (r *recordingTx) RunInScope(ctx context.Context, _ string, scope store.Scope, fn func(context.Context) error) error {
	r.mu.Lock()
	r.scopes = append(r.scopes, scope)
	r.mu.Unlock()
	return fn(ctx)
}

func persistOrchestrator(t *testing.T, sink sdk.DataSink, states store.ISyncStateDAO, tx store.TxRunner, exec func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error)) (*Orchestrator, *memstore.Store) {
	t.Helper()
	mem := memstore.New()
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: limOrg, ConnectorID: "c", Enabled: true, Settings: json.RawMessage(`{}`)}))
	if states == nil {
		states = mem
	}
	if tx == nil {
		tx = store.NoopTxRunner{}
	}
	return NewOrchestrator(setupRegistry("c", "op", exec), mem, states, mem, sink, noopGatewayFactory).WithTxRunner(tx), mem
}

func storedCursor(t *testing.T, mem *memstore.Store) *sdk.Cursor {
	t.Helper()
	c, err := mem.GetCursor(context.Background(), limOrg, "c", "op")
	if store.IsNotFound(err) {
		return nil
	}
	require.NoError(t, err)
	return c
}

// A checkpoint flushes the sink before it saves the cursor: when the flush
// fails, the checkpoint fails and no cursor is stored, so the cursor never
// runs ahead of records the sink has not accepted.
func TestCheckpointFlushesBeforeSavingTheCursor(t *testing.T) {
	var cpErr error
	orch, mem := persistOrchestrator(t, &flushFailingSink{}, nil, nil, func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		_ = p.Sink.Emit(ctx, &sdk.RawRecord{ExternalID: "a"})
		cpErr = p.Checkpoint(ctx, &sdk.Cursor{Data: json.RawMessage(`{"page":1}`)})
		return nil, cpErr
	})
	_ = orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.Error(t, cpErr)
	assert.Contains(t, cpErr.Error(), "checkpoint flush")
	assert.Nil(t, storedCursor(t, mem), "no cursor is saved past an unflushed record")
}

// After Execute succeeds the runner flushes the sink; a failing flush fails
// the run as sink_failure and leaves the cursor where it was.
func TestFailedFinalFlushFailsTheRunAndKeepsTheCursor(t *testing.T) {
	orch, mem := persistOrchestrator(t, &flushFailingSink{}, nil, nil, func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		_ = p.Sink.Emit(ctx, &sdk.RawRecord{ExternalID: "a"})
		return &sdk.Cursor{Data: json.RawMessage(`{"page":9}`)}, nil
	})
	require.NoError(t, mem.SaveCursor(context.Background(), limOrg, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{"page":1}`)}))
	err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.Error(t, err)
	run, err := mem.GetLatest(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.Equal(t, model.SyncRunStatusFailed, run.Status)
	assert.Equal(t, "sink_failure", run.ErrorKind)
	assert.JSONEq(t, `{"page":1}`, string(storedCursor(t, mem).Data))
}

// A checkpoint called with a context that is already cancelled still saves
// the cursor: the write runs on a detached context.
func TestCheckpointSavesOnACancelledContext(t *testing.T) {
	var cpErr error
	mem2 := memstore.New()
	states := ctxCheckingStates{mem2}
	orch, _ := persistOrchestrator(t, &flushSink{}, states, nil, func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		cpErr = p.Checkpoint(cctx, &sdk.Cursor{Data: json.RawMessage(`{"page":1}`)})
		return nil, errors.New("stop after the checkpoint")
	})
	_ = orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.NoError(t, cpErr)
	c, err := mem2.GetCursor(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.JSONEq(t, `{"page":1}`, string(c.Data))
}

// Every transaction a run opens is bound to the organization's tenant scope.
func TestRunTransactionsUseTheTenantScope(t *testing.T) {
	tx := &recordingTx{}
	orch, _ := persistOrchestrator(t, &flushSink{}, nil, tx, func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		if err := p.Checkpoint(ctx, &sdk.Cursor{Data: json.RawMessage(`{"page":1}`)}); err != nil {
			return nil, err
		}
		return &sdk.Cursor{Data: json.RawMessage(`{}`)}, nil
	})
	require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	require.GreaterOrEqual(t, len(tx.scopes), 3, "setup, checkpoint and finish transactions")
	for i, sc := range tx.scopes {
		assert.Equal(t, store.ScopeEnforced, sc, "transaction %d", i)
	}
}

// flushSink accepts everything.
type flushSink struct{}

func (flushSink) Emit(context.Context, *sdk.RawRecord) error        { return nil }
func (flushSink) EmitBatch(context.Context, []*sdk.RawRecord) error { return nil }
func (flushSink) Flush(context.Context) error                       { return nil }

// saveFailingStates fails every cursor write.
type saveFailingStates struct{ *memstore.Store }

func (saveFailingStates) SaveCursor(context.Context, string, sdk.ConnectorID, sdk.OperationID, *sdk.Cursor) error {
	return errors.New("state store unavailable")
}

// A checkpoint whose cursor write fails returns the error, so the connector
// does not believe its progress was saved.
func TestCheckpointReportsACursorSaveFailure(t *testing.T) {
	var cpErr error
	orch, _ := persistOrchestrator(t, &flushSink{}, saveFailingStates{memstore.New()}, nil, func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		cpErr = p.Checkpoint(ctx, &sdk.Cursor{Data: json.RawMessage(`{"page":1}`)})
		return nil, cpErr
	})
	_ = orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.Error(t, cpErr)
	assert.Contains(t, cpErr.Error(), "checkpoint save cursor")
}

// The cursor a successful run returns is saved even when the caller's
// context is cancelled after Execute: the write runs on a detached context.
func TestFinalCursorSavesAfterTheCallerCancels(t *testing.T) {
	mem2 := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	orch, _ := persistOrchestrator(t, &flushSink{}, ctxCheckingStates{mem2}, nil, func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		cancel()
		return &sdk.Cursor{Data: json.RawMessage(`{"page":2}`)}, nil
	})
	_ = orch.RunOperation(ctx, limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	c, err := mem2.GetCursor(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.JSONEq(t, `{"page":2}`, string(c.Data))
}

// Only a terminal auth error pauses the connector; a retryable one leaves it
// healthy.
func TestNonTerminalAuthErrorDoesNotPause(t *testing.T) {
	orch, mem := persistOrchestrator(t, &flushSink{}, nil, nil, func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		return nil, &sdk.SyncError{Kind: sdk.ErrKindAuth, Cause: errors.New("401 once")}
	})
	require.Error(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	got, err := mem.Get(context.Background(), limOrg, "c")
	require.NoError(t, err)
	assert.NotEqual(t, sdk.HealthStateAuthRevoked, got.HealthState)
}

// cursorLoadFails fails every cursor read with an error that is not
// "not found", such as an undecodable row or a database error.
type cursorLoadFails struct{ *memstore.Store }

func (cursorLoadFails) GetCursor(context.Context, string, sdk.ConnectorID, sdk.OperationID) (*sdk.Cursor, error) {
	return nil, errors.New("cursor_data does not decode")
}

// A cursor that cannot be loaded fails the run before the operation executes;
// the run does not restart from scratch, and the stored cursor is untouched.
func TestCursorLoadErrorFailsTheRun(t *testing.T) {
	mem2 := memstore.New()
	require.NoError(t, mem2.SaveCursor(context.Background(), limOrg, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{"page":7}`)}))
	executed := false
	orch, mem := persistOrchestrator(t, &flushSink{}, cursorLoadFails{mem2}, nil, func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		executed = true
		return &sdk.Cursor{Data: json.RawMessage(`{"page":1}`)}, nil
	})
	err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load cursor")
	assert.False(t, executed, "the operation does not run from a nil cursor")
	_, err = mem.GetLatest(context.Background(), limOrg, "c", "op")
	assert.True(t, store.IsNotFound(err), "no run row is started")
	c, err := mem2.GetCursor(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.JSONEq(t, `{"page":7}`, string(c.Data))
}
