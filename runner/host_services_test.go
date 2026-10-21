// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	lineagemocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk/lineage"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// hostOrchestrator runs one op "github/fetch_pr_diffs" that requires the
// work queue and the blame sink, against the memory store.
func hostOrchestrator(t *testing.T, exec func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error)) (*Orchestrator, *memstore.Store) {
	t.Helper()
	mem := memstore.New()
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: limOrg, ConnectorID: "github", Enabled: true, Settings: json.RawMessage(`{}`)}))
	registry := sdk.NewConnectorRegistry()
	registry.Register(&mockConnector{id: "github", ops: []sdk.OperationDescriptor{{
		ID:            "fetch_pr_diffs",
		IngestionType: sdk.IngestionPull,
		RequiresHost:  []sdk.HostService{sdk.HostWorkQueue, sdk.HostBlameSink},
		Factory: func() sdk.Operation {
			return &mockFetchOp{opID: "fetch_pr_diffs", executeFn: exec}
		},
	}}})
	return NewOrchestrator(registry, mem, mem, mem, sink.NewChannelSink(), noopGatewayFactory).WithTxRunner(store.NoopTxRunner{}), mem
}

// An operation whose RequiresHost names a service the orchestrator was not
// given fails before Execute, terminal and config_invalid, naming every
// missing service, and its run row says so.
func TestOrchestrator_RefusesAnOperationMissingHostServices(t *testing.T) {
	orch, mem := hostOrchestrator(t, func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		t.Fatal("Execute must not run without the host services it requires")
		return nil, nil
	})
	orch.WithHostServices(sdk.HostServices{BlameSink: lineagemocks.NewMockBlameSink(gomock.NewController(t))})

	err := orch.RunOperation(context.Background(), limOrg, "github", "fetch_pr_diffs", sdk.TenantKindOrganization, "test")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, sdk.ErrKindConfigInvalid, se.Kind)
	assert.True(t, se.Terminal)
	assert.ErrorContains(t, err, "operation github/fetch_pr_diffs needs host services: work_queue")

	run, err := mem.GetLatest(context.Background(), limOrg, "github", "fetch_pr_diffs")
	require.NoError(t, err)
	assert.Equal(t, model.SyncRunStatusFailed, run.Status)
	assert.Equal(t, "config_invalid", run.ErrorKind)
	assert.Contains(t, run.ErrorMessage, "needs host services: work_queue")
}

// With no host services at all, the error lists each required one.
func TestOrchestrator_NamesEveryMissingHostService(t *testing.T) {
	orch, _ := hostOrchestrator(t, func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		t.Fatal("Execute must not run without the host services it requires")
		return nil, nil
	})
	err := orch.RunOperation(context.Background(), limOrg, "github", "fetch_pr_diffs", sdk.TenantKindOrganization, "test")
	assert.ErrorContains(t, err, "needs host services: work_queue, blame_sink")
}

// With every required service, the operation runs and receives them, and
// the blob store and budget observer the generic options set.
func TestOrchestrator_HandsHostServicesToTheOperation(t *testing.T) {
	ctrl := gomock.NewController(t)
	host := sdk.HostServices{WorkQueue: sdkmocks.NewMockWorkQueue(ctrl), BlameSink: lineagemocks.NewMockBlameSink(ctrl)}
	blobs := sdkmocks.NewMockBlobStore(ctrl)
	budget := sdkmocks.NewMockBudgetObserver(ctrl)
	var got *sdk.ExecutionParams
	orch, _ := hostOrchestrator(t, func(_ context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		got = p
		return nil, nil
	})
	orch.WithHostServices(host).WithBlobStore(blobs).WithBudgetObservers(map[sdk.ConnectorID]sdk.BudgetObserver{"github": budget})

	require.NoError(t, orch.RunOperation(context.Background(), limOrg, "github", "fetch_pr_diffs", sdk.TenantKindOrganization, "test"))
	require.NotNil(t, got)
	assert.Equal(t, host, got.Host)
	assert.Equal(t, sdk.BlobStore(blobs), got.BlobStore)
	assert.Equal(t, sdk.BudgetObserver(budget), got.BudgetLimiter)
}
