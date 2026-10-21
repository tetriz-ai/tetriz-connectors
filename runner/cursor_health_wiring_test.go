// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	prommetrics "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// compositeOf builds the per-repo composite cursor shape the VCS ops write.
func compositeOf(t *testing.T, watermarks map[string]time.Time) *sdk.Cursor {
	t.Helper()
	parts := make(map[string]json.RawMessage, len(watermarks))
	for repo, ts := range watermarks {
		raw, err := json.Marshal(ts.UTC())
		require.NoError(t, err)
		parts[repo] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// TestCursorHealthNotRecordedWhenTransactionRollsBack pins that a cursor is measured only
// once it has LANDED. SaveCursor succeeding is not the cursor landing: the run-row update
// shares its transaction, so a failure after the save rolls the cursor write back. Recording
// inside that transaction would advance the tracker to a state that never existed, and the
// next run's comparison would read the real cursor as a regression — a false increment on a
// counter whose whole premise is that any increment is a defect.
func TestCursorHealthNotRecordedWhenTransactionRollsBack(t *testing.T) {
	now := time.Now().UTC()
	const orgID = "org_cursor_rollback"
	const repo = "ws/repo"

	mocks := NewMockedDependencies(t)
	sink := sink.NewChannelSink()

	// The run returns a REGRESSED cursor: older than the one it started from. If health
	// were recorded before the commit, this would increment the counter.
	registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
		return compositeOf(t, map[string]time.Time{repo: now.Add(-300 * 24 * time.Hour)}), nil
	})

	mocks.MockConfigDAO.EXPECT().
		Get(gomock.Any(), orgID, sdk.ConnectorID("github")).
		Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: orgID, Enabled: true}, nil)
	mocks.MockStateDAO.EXPECT().
		GetCursor(gomock.Any(), orgID, sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
		Return(compositeOf(t, map[string]time.Time{repo: now.Add(-time.Hour)}), nil)
	mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	// The cursor write itself succeeds...
	mocks.MockStateDAO.EXPECT().
		SaveCursor(gomock.Any(), orgID, sdk.ConnectorID("github"), sdk.OperationID("fetch_prs"), gomock.Any()).
		Return(nil)
	// ...and the run-row update in the SAME transaction then fails, so the cursor write
	// is rolled back. Update is also called again by failRun, which must not change the
	// verdict.
	mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(fmt.Errorf("update failed")).Times(1)
	mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	before := prommetrics.ToFloat64(
		obs.CursorRegressionsTotal.WithLabelValues("github", "fetch_prs", orgID),
	)

	orch := NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
	err := orch.RunOperation(context.Background(), orgID, "github", "fetch_prs", sdk.TenantKindOrganization, "test")
	require.Error(t, err, "a failed post-execution transaction must surface as an error")

	after := prommetrics.ToFloat64(
		obs.CursorRegressionsTotal.WithLabelValues("github", "fetch_prs", orgID),
	)
	assert.Equal(t, before, after,
		"a cursor whose transaction rolled back must not be recorded: the write did not land, so there is no regression to report")
}

// TestPausedConnectorClearsCursorAgeGauge pins that a paused connector stops holding the
// age gauge open. A disabled or auth_revoked connector returns before any cursor save, so
// without this the last value ages indefinitely and reports a connector nobody expects to
// advance — the gauge is a level, and nothing else would ever clear it.
func TestPausedConnectorClearsCursorAgeGauge(t *testing.T) {
	const orgID = "org_cursor_paused"

	// Seed a child for this (org, connector) as a live run would have.
	obs.CursorOldestEntryAgeSeconds.WithLabelValues("github", "fetch_prs", orgID).Set(42)
	require.Equal(t, float64(42), prommetrics.ToFloat64(
		obs.CursorOldestEntryAgeSeconds.WithLabelValues("github", "fetch_prs", orgID),
	),
		"precondition: the gauge child exists before the paused run")

	mocks := NewMockedDependencies(t)
	sink := sink.NewChannelSink()
	registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
		t.Fatal("a paused connector must not execute the operation")
		return nil, nil
	})

	mocks.MockConfigDAO.EXPECT().
		Get(gomock.Any(), orgID, sdk.ConnectorID("github")).
		Return(&sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: orgID,
			Enabled:        false, // the paused state
		}, nil)

	orch := NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
	require.NoError(t, orch.RunOperation(context.Background(), orgID, "github", "fetch_prs", sdk.TenantKindOrganization, "test"))

	// DeletePartialMatch removes the child; ToFloat64 on a fresh child reads 0.
	assert.Zero(t, prommetrics.ToFloat64(
		obs.CursorOldestEntryAgeSeconds.WithLabelValues("github", "fetch_prs", orgID),
	),
		"a paused connector must not leave a stale cursor-age gauge to alert on")
}

// TestCheckpointRecordsCursorRegression pins that a checkpoint measures the cursor it
// saved. The run below regresses its cursor at a checkpoint and then fails, so the
// post-Execute path never sees that cursor; only the checkpoint can count it.
func TestCheckpointRecordsCursorRegression(t *testing.T) {
	now := time.Now().UTC()
	const orgID = "org_cursor_checkpoint"
	const repo = "ws/repo"

	mocks := NewMockedDependencies(t)
	registry := setupRegistry("github", "fetch_prs", func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		require.NoError(t, p.Checkpoint(ctx, compositeOf(t, map[string]time.Time{repo: now.Add(-300 * 24 * time.Hour)})))
		return nil, fmt.Errorf("the run fails after its checkpoint")
	})
	mocks.MockConfigDAO.EXPECT().
		Get(gomock.Any(), orgID, sdk.ConnectorID("github")).
		Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: orgID, Enabled: true}, nil)
	mocks.MockStateDAO.EXPECT().
		GetCursor(gomock.Any(), orgID, sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
		Return(compositeOf(t, map[string]time.Time{repo: now.Add(-time.Hour)}), nil)
	mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mocks.MockStateDAO.EXPECT().
		SaveCursor(gomock.Any(), orgID, sdk.ConnectorID("github"), sdk.OperationID("fetch_prs"), gomock.Any()).
		Return(nil).Times(1)
	mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	counter := obs.CursorRegressionsTotal.WithLabelValues("github", "fetch_prs", orgID)
	before := prommetrics.ToFloat64(counter)
	orch := NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink.NewChannelSink(), noopGatewayFactory)
	require.Error(t, orch.RunOperation(context.Background(), orgID, "github", "fetch_prs", sdk.TenantKindOrganization, "test"))
	assert.Equal(t, before+1, prommetrics.ToFloat64(counter), "the checkpoint's regressed cursor is counted")
}
