// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

// Tests of the run guards: what the orchestrator keeps, refuses or resets
// around an operation's cursor, config, records and registration.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

const limOrg = "00000000-0000-4000-8000-00000000aaaa"

func memOrchestrator(t *testing.T, reg *sdk.ConnectorRegistry, sink sdk.DataSink) (*Orchestrator, *memstore.Store) {
	t.Helper()
	mem := memstore.New()
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: limOrg, ConnectorID: "c", Enabled: true, Settings: json.RawMessage(`{}`)}))
	return NewOrchestrator(reg, mem, mem, mem, sink, noopGatewayFactory).WithTxRunner(store.NoopTxRunner{}), mem
}

// An operation that returns a nil cursor on success keeps the previous
// cursor: the orchestrator saves nothing. An operation that wants to start
// over returns a cursor holding its own initial state.
func TestNilCursorKeepsPreviousCursor(t *testing.T) {
	reg := setupRegistry("c", "op", func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) { return nil, nil })
	orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
	require.NoError(t, mem.SaveCursor(context.Background(), limOrg, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{"page":"last"}`)}))
	require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	got, err := mem.GetCursor(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.JSONEq(t, `{"page":"last"}`, string(got.Data))
}

type rejectingConnector struct{ *mockConnector }

func (*rejectingConnector) ValidateConfig(context.Context, *sdk.ConnectorConfig) error {
	return errors.New("config rejected")
}

// A connector whose ValidateConfig rejects the stored config does not run:
// the run fails, terminal and config_invalid, before Execute.
func TestRejectedConfigFailsTheRunBeforeExecute(t *testing.T) {
	ran := false
	reg := sdk.NewConnectorRegistry()
	reg.Register(&rejectingConnector{&mockConnector{id: "c", ops: []sdk.OperationDescriptor{{
		ID: "op", IngestionType: sdk.IngestionPull,
		Factory: func() sdk.Operation {
			return &mockFetchOp{opID: "op", executeFn: func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
				ran = true
				return &sdk.Cursor{Data: json.RawMessage(`{}`)}, nil
			}}
		},
	}}}})
	orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
	err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.Error(t, err)
	assert.False(t, ran, "the operation must not run on a rejected config")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.True(t, se.Terminal)
	assert.Equal(t, sdk.ErrKindConfigInvalid, se.Kind)
	assert.Contains(t, err.Error(), "config rejected")
	run, err := mem.GetLatest(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.Equal(t, "failed", string(run.Status))
	assert.Equal(t, "config_invalid", run.ErrorKind)
}

// A simulated organization's config is not validated: the simulator ignores
// credentials, so a config without them still runs.
func TestSimulatedOrgSkipsConfigValidation(t *testing.T) {
	ran := false
	reg := sdk.NewConnectorRegistry()
	reg.Register(&rejectingConnector{&mockConnector{id: "c", ops: []sdk.OperationDescriptor{{
		ID: "op", IngestionType: sdk.IngestionPull,
		Factory: func() sdk.Operation {
			return &mockFetchOp{opID: "op", executeFn: func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
				ran = true
				return nil, nil
			}}
		},
	}}}})
	orch, _ := memOrchestrator(t, reg, sink.NewChannelSink())
	orch.WithSimulatedOrgs(func(id string) bool { return id == limOrg })
	require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	assert.True(t, ran)
}

type streamOp struct{}

func (streamOp) OperationID() sdk.OperationID { return "stream" }
func (streamOp) Start(context.Context, *sdk.ExecutionParams) error {
	return nil
}

// Only pull operations run, and the host receives webhooks. Register refuses
// a stream or bulk-import operation, which nothing would run.
func TestRegisterRefusesUnhostedIngestionTypes(t *testing.T) {
	for _, it := range []sdk.IngestionType{sdk.IngestionSSE, sdk.IngestionWebSocket, sdk.IngestionGraphQLSub, sdk.IngestionBulkImport} {
		t.Run(it.String(), func(t *testing.T) {
			reg := sdk.NewConnectorRegistry()
			assert.PanicsWithValue(t,
				"connector c: operation stream declares ingestion type "+it.String()+", which nothing runs; only pull and webhook operations are supported",
				func() {
					reg.Register(&mockConnector{id: "c", ops: []sdk.OperationDescriptor{{
						ID: "stream", IngestionType: it, Factory: func() sdk.Operation { return streamOp{} },
					}}})
				})
			_, err := reg.Get("c")
			assert.Error(t, err, "the refused connector is not registered")
		})
	}
	reg := sdk.NewConnectorRegistry()
	assert.NotPanics(t, func() {
		reg.Register(&mockConnector{id: "c", ops: []sdk.OperationDescriptor{
			{ID: "pull", IngestionType: sdk.IngestionPull},
			{ID: "hook", IngestionType: sdk.IngestionWebhook},
		}})
	})
}

type failSecond struct{ n int }

func (f *failSecond) Emit(context.Context, *sdk.RawRecord) error {
	f.n++
	if f.n == 2 {
		return errors.New("queue full")
	}
	return nil
}

func (f *failSecond) EmitBatch(ctx context.Context, rs []*sdk.RawRecord) error {
	for _, r := range rs {
		if err := f.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (f *failSecond) Flush(context.Context) error { return nil }

// records_emitted counts what was published: when a batch fails part way,
// the records the sink took before the failure are counted.
func TestPartialBatchCountsWhatWasPublished(t *testing.T) {
	inner := &failSecond{}
	c := newCountingSink(inner, "")
	err := c.EmitBatch(context.Background(), []*sdk.RawRecord{{ExternalID: "a"}, {ExternalID: "b"}, {ExternalID: "c"}})
	require.Error(t, err)
	assert.Equal(t, 2, inner.n, "the batch stops at the failing record")
	assert.Equal(t, int64(1), c.Count(), "the published record is counted")
}

// An operation that returns an empty cursor (&sdk.Cursor{}) resets it: the
// next run receives a nil cursor, as on a first run.
func TestEmptyCursorResets(t *testing.T) {
	var seen []*sdk.Cursor
	reg := setupRegistry("c", "op", func(_ context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		seen = append(seen, p.Cursor)
		return &sdk.Cursor{}, nil
	})
	orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
	require.NoError(t, mem.SaveCursor(context.Background(), limOrg, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{"page":"last"}`)}))
	for i := 0; i < 2; i++ {
		require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	}
	require.Len(t, seen, 2)
	require.NotNil(t, seen[0], "the first run resumes from the stored cursor")
	assert.Nil(t, seen[1], "the run after a reset starts as a first run")
}

// The runner checks each record's organization against the run's: a record
// naming another organization, none, or the run's own in another spelling
// (the run's is canonical, lower case, and the sinks key on it as written) is
// refused, nothing is published, and the run fails as internal.
func TestRecordFromAnotherOrgFailsTheRun(t *testing.T) {
	for name, recordOrg := range map[string]string{
		"another organization": "00000000-0000-4000-8000-00000000bbbb",
		"no organization":      "",
		"upper-case spelling":  strings.ToUpper(limOrg),
	} {
		t.Run(name, func(t *testing.T) {
			reg := setupRegistry("c", "op", func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
				return nil, p.Sink.Emit(ctx, &sdk.RawRecord{OrganizationID: recordOrg, ConnectorID: "c", OperationID: "op", ExternalID: "x"})
			})
			records := sink.NewChannelSink()
			orch, mem := memOrchestrator(t, reg, records)
			err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("names organization %q", recordOrg))
			run, err := mem.GetLatest(context.Background(), limOrg, "c", "op")
			require.NoError(t, err)
			assert.Equal(t, "failed", string(run.Status))
			assert.Equal(t, "internal", run.ErrorKind)
			assert.Empty(t, records.Records(), "nothing is published")
			assert.Zero(t, run.RecordsEmitted)
		})
	}
}

// A run whose cursor another writer saved after this run read it fails with
// a retryable cursor conflict, and the newer cursor is kept.
func TestCursorConflictKeepsTheNewerCursor(t *testing.T) {
	var mem *memstore.Store
	reg := setupRegistry("c", "op", func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		newer, err := mem.GetCursor(ctx, limOrg, "c", "op")
		require.NoError(t, err)
		newer.Data = json.RawMessage(`{"page":"newer"}`)
		require.NoError(t, mem.SaveCursor(ctx, limOrg, "c", "op", newer))
		return &sdk.Cursor{Data: json.RawMessage(`{"page":"older"}`)}, nil
	})
	var orch *Orchestrator
	orch, mem = memOrchestrator(t, reg, sink.NewChannelSink())
	require.NoError(t, mem.SaveCursor(context.Background(), limOrg, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{"page":"first"}`)}))
	err := orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.ErrorIs(t, err, store.ErrCursorConflict)
	var se *sdk.SyncError
	assert.False(t, errors.As(err, &se) && se.Terminal, "a conflict is retryable")
	got, err := mem.GetCursor(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.JSONEq(t, `{"page":"newer"}`, string(got.Data))
	run, err := mem.GetLatest(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.Equal(t, "failed", string(run.Status))
}

// A run renews its lease while it executes, so a run that outlives one lease
// is not closed as abandoned while it is still running.
func TestRunningRunRenewsItsLease(t *testing.T) {
	const lease = MinRunLease
	var mem *memstore.Store
	var closed int64
	reg := setupRegistry("c", "op", func(ctx context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
		time.Sleep(lease * 3 / 2)
		n, err := mem.CloseAbandoned(ctx, limOrg, "", "")
		closed = n
		return nil, err
	})
	var orch *Orchestrator
	orch, mem = memOrchestrator(t, reg, sink.NewChannelSink())
	orch.WithRunLease(lease)
	require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	assert.Zero(t, closed, "the heartbeat kept the lease ahead of the clock")
	run, err := mem.GetLatest(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.Equal(t, "completed", string(run.Status))
}
