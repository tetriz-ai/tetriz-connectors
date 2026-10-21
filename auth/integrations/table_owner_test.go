// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// As the README's recommended deployment, a non-superuser owner of the tables,
// under forced row-level security: a token connect, the integration list, the
// VCS settings read, the org-wide disable and a disconnect each reach the row.
func TestIntegrationRPCsAsTableOwner(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("integrations as owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(proto.Message(&vgpb.GetAuthenticatedUserResponse{Login: "octocat", Id: 583231}), nil).AnyTimes()
	tenants := fwmocks.NewMockTenantDirectory(ctrl)
	tenants.EXPECT().TenantType(gomock.Any(), gomock.Any()).Return(sdk.TenantKindOrganization, nil).AnyTimes()
	configs := store.NewConnectorConfigRepo(db, nil)
	svc := NewService(Deps{
		Tx: store.PostgresTxRunner{}, Cfg: testConfig(), Registry: testRegistry(), Tenants: tenants,
		ConnectorDAO: configs, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db),
		GatewayFactory: func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) { return gw, nil },
	})
	const org = "00000000-0000-4000-8000-0000000c0a01"

	_, err = svc.SubmitConnectorToken(ctx, &connintv1.SubmitConnectorTokenRequest{OrgId: org, ConnectorId: "github", Token: "ghp_x"})
	require.NoError(t, err, "connect")

	resp, err := svc.GetIntegrations(ctx, &connintv1.GetIntegrationsRequest{OrgId: org})
	require.NoError(t, err, "list")
	github := connintv1.IntegrationStatus_INTEGRATION_STATUS_UNSPECIFIED
	for _, cat := range resp.GetCategories() {
		for _, it := range cat.GetIntegrations() {
			if it.GetId() == "github" {
				github = it.GetStatus()
			}
		}
	}
	// A token connect stores the config disabled until repositories are
	// chosen; a list that cannot see the row reports NOT_CONNECTED.
	assert.NotContains(t, []connintv1.IntegrationStatus{
		connintv1.IntegrationStatus_INTEGRATION_STATUS_UNSPECIFIED,
		connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
	}, github, "the list sees the stored config")

	_, err = svc.GetVCSSettings(ctx, &connintv1.GetVCSSettingsRequest{OrgId: org, ConnectorId: "github"})
	require.NoError(t, err, "settings read")

	_, err = svc.DisconnectIntegration(ctx, &connintv1.DisconnectIntegrationRequest{OrgId: org, ConnectorId: "github"})
	require.NoError(t, err, "disconnect")
	var enabled bool
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(sctx context.Context) error {
		cfg, err := configs.Get(sctx, org, "github")
		if store.IsNotFound(err) {
			return nil
		}
		if err == nil {
			enabled = cfg.Enabled
		}
		return err
	}))
	assert.False(t, enabled, "the disconnected connector no longer syncs")

	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(sctx context.Context) error {
		return configs.Save(sctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira", Settings: []byte(`{}`), Credentials: []byte(`{}`), Enabled: true})
	}))
	disabled, err := svc.DisableConnectorConfigsForOrg(ctx, &connintv1.DisableConnectorConfigsForOrgRequest{OrgId: org})
	require.NoError(t, err, "org-wide disable")
	assert.Equal(t, int64(1), disabled.GetDisabledCount())
}

// The gRPC services accept any spelling of the organization's UUID: the
// store calls run under, and store, the canonical lower-case id.
func TestIntegrationRPCsCanonicalizeTheOrg(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("integrations org spelling", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(proto.Message(&vgpb.GetAuthenticatedUserResponse{Login: "octocat", Id: 583231}), nil).AnyTimes()
	configs := store.NewConnectorConfigRepo(db, nil)
	svc := NewService(Deps{
		Tx: store.PostgresTxRunner{}, Cfg: testConfig(), Registry: testRegistry(),
		ConnectorDAO: configs, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db),
		GatewayFactory: func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) { return gw, nil },
	})
	const canonical = "aaaaaaaa-0000-4000-8000-0000000000f3"
	for _, spelling := range []string{"AAAAAAAA-0000-4000-8000-0000000000F3", "urn:uuid:" + canonical, "{" + canonical + "}"} {
		t.Run(spelling, func(t *testing.T) {
			_, err := svc.SubmitConnectorToken(ctx, &connintv1.SubmitConnectorTokenRequest{OrgId: spelling, ConnectorId: "github", Token: "ghp_x"})
			require.NoError(t, err)
			require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, canonical, store.ScopeEnforced, func(sctx context.Context) error {
				cfg, err := configs.Get(sctx, canonical, "github")
				if err == nil {
					assert.Equal(t, canonical, cfg.OrganizationID)
				}
				return err
			}))
		})
	}
}

// As the table owner, an installation "suspend" webhook disables only the
// organization whose installation it names.
func TestHandleWebhookSuspendAsTableOwner(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("webhook as owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	configs := store.NewConnectorConfigRepo(db, nil)
	svc := NewService(Deps{Tx: store.PostgresTxRunner{}, Cfg: webhookConfig(), Registry: testRegistry(),
		ConnectorDAO: configs, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db)})
	const orgA, orgB = "00000000-0000-4000-8000-0000000c0b0a", "00000000-0000-4000-8000-0000000c0b0b"
	save := func(org, inst string) {
		c := configWithInstallation(inst, true)
		c.OrganizationID = org
		c.Settings = []byte(`{}`)
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error { return configs.Save(s, c) }))
	}
	enabled := func(org string) bool {
		var e bool
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
			c, err := configs.Get(s, org, "github")
			if err == nil {
				e = c.Enabled
			}
			return err
		}))
		return e
	}
	save(orgA, "1111")
	save(orgB, "2222")

	p := installationPayload("suspend", 2222)
	_, err = svc.HandleWebhook(ctx, &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	require.NoError(t, err)
	assert.False(t, enabled(orgB), "the suspended installation's organization is disabled")
	assert.True(t, enabled(orgA), "the other organization is untouched")
}
