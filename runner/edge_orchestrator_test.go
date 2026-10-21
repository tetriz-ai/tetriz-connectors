// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

const edgeOrg = "00000000-0000-4000-8000-00000000ed9e"

// A paused connector clears its freshness gauge, so it does not read as
// stale. The gauge is written under the org's metric label
// (tenantlabel.OrgID), so the clear must match that label too. With a host
// label rule in place, a delete keyed by the raw org id matches nothing and
// the stale series stays.
func TestEdge_PausedConnectorClearsFreshnessGaugeUnderAHostLabelRule(t *testing.T) {
	restore := tenantlabel.SetLabelFunc(func(org string) string { return "tenant:" + org })
	defer restore()

	const org = "00000000-0000-4000-8000-0000000fe5e1"
	label := tenantlabel.OrgID(org)
	lastSuccessfulSyncTimestampSeconds.WithLabelValues(label, "c", "op").Set(1234)

	mem := memstore.New()
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "c", Enabled: false, Settings: json.RawMessage(`{}`)}))
	reg := setupRegistry("c", "op", func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
		t.Fatal("a paused connector must not execute")
		return nil, nil
	})
	orch := NewOrchestrator(reg, mem, mem, mem, sink.NewChannelSink(), noopGatewayFactory).WithTxRunner(store.NoopTxRunner{})
	require.NoError(t, orch.RunOperation(context.Background(), org, "c", "op", sdk.TenantKindOrganization, "test"))

	assert.False(t, hasSeries(t, label, "c", "op"), "the stale freshness gauge for the paused connector must be cleared")
}

func hasSeries(t *testing.T, org, conn, op string) bool {
	t.Helper()
	out := testutil.ToFloat64(lastSuccessfulSyncTimestampSeconds.WithLabelValues(org, conn, op))
	// WithLabelValues re-creates a deleted child at 0, so a non-zero value means
	// the original child survived. Clean up the probe either way.
	lastSuccessfulSyncTimestampSeconds.DeleteLabelValues(org, conn, op)
	return out != 0
}

// Two runs of the same operation at once each create their own row, and each
// row counts only the records its own run emitted. Both read no cursor, so the
// run that saves second meets a cursor conflict: it fails, retryable, and does
// not overwrite the first run's cursor.
func TestEdge_ConcurrentRunsOfOneOperationCountTheirOwnRecords(t *testing.T) {
	var mu sync.Mutex
	next := []int{3, 5}
	start := make(chan struct{})
	reg := setupRegistry("c", "op", func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		mu.Lock()
		n := next[0]
		next = next[1:]
		mu.Unlock()
		<-start // both runs are inside Execute before either emits
		for i := range n {
			if err := p.Sink.Emit(ctx, &sdk.RawRecord{ConnectorID: "c", OperationID: "op", OrganizationID: edgeOrg, ExternalID: fmt.Sprint(n, "-", i)}); err != nil {
				return nil, err
			}
		}
		return &sdk.Cursor{Data: json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))}, nil
	})
	mem := memstore.New()
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: edgeOrg, ConnectorID: "c", Enabled: true, Settings: json.RawMessage(`{}`)}))
	out := sink.NewChannelSink()
	orch := NewOrchestrator(reg, mem, mem, mem, out, noopGatewayFactory).WithTxRunner(store.NoopTxRunner{})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = orch.RunOperation(context.Background(), edgeOrg, "c", "op", sdk.TenantKindOrganization, "test")
		}()
	}
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(next) == 0 }, 5*time.Second, time.Millisecond)
	close(start)
	wg.Wait()
	var failed int
	for _, err := range errs {
		if err != nil {
			failed++
			require.ErrorIs(t, err, store.ErrCursorConflict)
		}
	}
	require.Equal(t, 1, failed, "exactly one run loses the cursor race")

	runs, err := mem.ListByOperation(context.Background(), edgeOrg, "c", "op", -1)
	require.NoError(t, err)
	require.Len(t, runs, 2, "each run has its own row")
	got := []int64{runs[0].RecordsEmitted, runs[1].RecordsEmitted}
	assert.ElementsMatch(t, []int64{3, 5}, got, "each row counts its own records")
	status := map[model.SyncRunStatus]int{}
	var winner *model.SyncRun
	for _, r := range runs {
		status[r.Status]++
		if r.Status == model.SyncRunStatusCompleted {
			winner = r
		}
	}
	assert.Equal(t, map[model.SyncRunStatus]int{model.SyncRunStatusCompleted: 1, model.SyncRunStatusFailed: 1}, status)
	require.NotNil(t, winner)
	c, err := mem.GetCursor(context.Background(), edgeOrg, "c", "op")
	require.NoError(t, err)
	assert.JSONEq(t, fmt.Sprintf(`{"n":%d}`, winner.RecordsEmitted), string(c.Data), "the stored cursor is the completed run's")
	assert.Len(t, out.Records(), 8)
	ids := map[string]bool{}
	for _, r := range out.Records() {
		ids[r.ExternalID] = true
	}
	assert.Len(t, ids, 8)
}

// A parent deadline that fires during Execute fails the run as ctx_exhausted,
// closes its row (the close uses a detached context) and leaves the cursor.
func TestEdge_ParentDeadlineDuringExecuteIsCtxExhausted(t *testing.T) {
	reg := setupRegistry("c", "op", func(ctx context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
		<-ctx.Done()
		return &sdk.Cursor{Data: json.RawMessage(`{"page":"never"}`)}, fmt.Errorf("listing: %w", ctx.Err())
	})
	orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
	require.NoError(t, mem.SaveCursor(context.Background(), limOrg, "c", "op", &sdk.Cursor{Data: json.RawMessage(`{"page":"before"}`)}))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := orch.RunOperation(ctx, limOrg, "c", "op", sdk.TenantKindOrganization, "test")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	run, err := mem.GetLatest(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.Equal(t, model.SyncRunStatusFailed, run.Status)
	assert.Equal(t, sdk.ErrKindCtxExhausted.String(), run.ErrorKind)
	assert.NotNil(t, run.CompletedAt)
	c, err := mem.GetCursor(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.JSONEq(t, `{"page":"before"}`, string(c.Data), "a failed run never saves the cursor it returned")
}

// A checkpointed cursor survives an operation that then returns a nil cursor:
// nil means "nothing new", not "clear the position".
func TestEdge_CheckpointSurvivesANilFinalCursor(t *testing.T) {
	reg := setupRegistry("c", "op", func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		if err := p.Checkpoint(ctx, &sdk.Cursor{Data: json.RawMessage(`{"page":7}`)}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	orch, mem := memOrchestrator(t, reg, sink.NewChannelSink())
	require.NoError(t, orch.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "test"))
	c, err := mem.GetCursor(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.JSONEq(t, `{"page":7}`, string(c.Data))
	run, err := mem.GetLatest(context.Background(), limOrg, "c", "op")
	require.NoError(t, err)
	assert.Equal(t, model.SyncRunStatusCompleted, run.Status)
}

// The fetch-cache size cap is inclusive: a value of exactly the cap is
// cached, one byte more is skipped and its key tombstoned.
func TestEdge_FetchCacheCapBoundaryIsInclusive(t *testing.T) {
	const capBytes = 16
	atCap := json.RawMessage(`"` + strings.Repeat("a", capBytes-2) + `"`)
	overCap := json.RawMessage(`"` + strings.Repeat("a", capBytes-1) + `"`)
	require.Len(t, atCap, capBytes)
	require.Len(t, overCap, capBytes+1)

	ctrl := gomock.NewController(t)
	dao := mocks.NewMockIFetchCacheDAO(ctrl)
	const org = "org-edge-cap"
	dao.EXPECT().PutMany(gomock.Any(), org, fcaConnector, fcaType, map[string]json.RawMessage{"at": atCap}).Return(int64(1), nil)
	dao.EXPECT().DeleteMany(gomock.Any(), org, fcaConnector, fcaType, []string{"over"}).Return(nil)
	dao.EXPECT().CountByType(gomock.Any(), org, fcaConnector, fcaType).Return(int64(1), nil)

	before := oversizedSkipped(org)
	require.NoError(t, newAdapter(dao, org, capBytes).PutMany(context.Background(), fcaType, map[string]json.RawMessage{"at": atCap, "over": overCap}))
	assert.Equal(t, before+1, oversizedSkipped(org))
}
