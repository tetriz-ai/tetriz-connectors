// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// As a non-superuser owner of the tables, under forced row-level security,
// the cross-organization lists and the per-organization queries each see the
// stored rows.
func TestConnectorRPCsAsTableOwner(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("connectorsvc as owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	configs, runs := store.NewConnectorConfigRepo(db, nil), store.NewSyncRunRepo(db)

	const org = "00000000-0000-4000-8000-0000000c0b01"
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(sctx context.Context) error {
		if err := configs.Save(sctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github",
			Settings: []byte(`{"organization":"acme"}`), Credentials: []byte(`{"personal_access_token":"x"}`), Enabled: true}); err != nil {
			return err
		}
		done := time.Now().UTC()
		return runs.Create(sctx, &model.SyncRun{ID: uuid.NewString(), OrganizationID: org, ConnectorID: "github", OperationID: "fetch_prs",
			Status: model.SyncRunStatusCompleted, StartedAt: done.Add(-time.Minute), CompletedAt: &done, Trigger: "test"})
	}))
	svc := NewService(store.PostgresTxRunner{}, configs, runs, store.NewScheduleRepo(db), app.NewRegistry(nil), nil, nil, nil)

	listed, err := svc.ListEnabledConfigs(ctx, &connectorpb.ListEnabledConfigsRequest{ConnectorId: "github"})
	require.NoError(t, err)
	assert.Len(t, listed.GetConfigs(), 1, "the cross-organization list runs in the system scope")

	orgs, err := svc.ListEnabledOrganizations(ctx, &connectorpb.ListEnabledOrganizationsRequest{ConnectorId: "github"})
	require.NoError(t, err)
	assert.Equal(t, []string{org}, orgs.GetOrganizationIds())

	vcs, err := svc.GetActiveVCSConnector(ctx, &connectorpb.GetActiveVCSConnectorRequest{OrgId: org})
	require.NoError(t, err)
	assert.Equal(t, "github", vcs.GetConnectorId())

	status, err := svc.GetConnectorSyncStatus(ctx, &connectorpb.GetConnectorSyncStatusRequest{OrgId: org})
	require.NoError(t, err)
	assert.NotEmpty(t, status.String(), "the sync-run row is visible")
	assert.Contains(t, status.String(), "github")

	conn, err := svc.GetConnectionStatus(ctx, &connectorpb.GetConnectionStatusRequest{OrganizationId: org, ConnectorId: "github"})
	require.NoError(t, err)
	assert.True(t, conn.GetConnected())
}
