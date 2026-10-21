// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"fmt"
	"testing"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// findIntegrationStatus locates an integration by ID in the response and returns its status.
func findIntegrationStatus(resp *connintv1.GetIntegrationsResponse, id string) connintv1.IntegrationStatus {
	for _, cat := range resp.Categories {
		for _, integration := range cat.Integrations {
			if integration.Id == id {
				return integration.Status
			}
		}
	}
	return connintv1.IntegrationStatus_INTEGRATION_STATUS_UNSPECIFIED
}

func TestGetIntegrations(t *testing.T) {
	tests := []struct {
		name string
		// tenantType is what the (lazily-invoked) tenant directory returns. It is
		// consulted only to gate PENDING_REPO_SELECTION to INDIVIDUAL tenants —
		// defaults to ORGANIZATION via the mock unless a case sets it.
		tenantType   commonv1.TenantType
		daoConfigs   []*sdk.ConnectorConfig
		daoErr       error
		wantErr      bool
		wantCode     codes.Code
		wantStatuses map[string]connintv1.IntegrationStatus
	}{
		{
			name:       "NoConnectors",
			daoConfigs: nil,
			daoErr:     nil,
			wantErr:    false,
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
				"jira":   connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
				"linear": connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
			},
		},
		{
			name: "GitHubConnected",
			daoConfigs: []*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: testOrgID, Enabled: true},
			},
			wantErr: false,
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_CONNECTED,
				"jira":   connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
			},
		},
		{
			// INDIVIDUAL tenant: a disabled GitHub config with no elected repos is
			// mid-connect (the individual-accounts resume state), not suspended.
			name:       "GitHubPendingRepoSelectionIndividual",
			tenantType: commonv1.TenantType_TENANT_TYPE_INDIVIDUAL,
			daoConfigs: []*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: testOrgID, Enabled: false},
			},
			wantErr: false,
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_PENDING_REPO_SELECTION,
			},
		},
		{
			// ORG tenant: a disabled GitHub App config ALSO has no elected repos
			// (orgs auto-discover), but it is a genuine suspension — it must NOT be
			// classified pending, because ConfirmConnectorRepos rejects orgs and
			// would route the admin to a broken "Finish setup". Gating PENDING on
			// tenant_type=INDIVIDUAL keeps the org on SUSPENDED (reconnect).
			name:       "GitHubDisabledOrgIsSuspendedNotPending",
			tenantType: commonv1.TenantType_TENANT_TYPE_ORGANIZATION,
			daoConfigs: []*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: testOrgID, Enabled: false},
			},
			wantErr: false,
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED,
			},
		},
		{
			// Disabled GitHub config that already elected repos is a genuine
			// suspension (e.g. expired/revoked auth), not a mid-connect state —
			// regardless of tenant kind (hasElectedRepos short-circuits it before
			// the tenant-type lookup even matters).
			name: "GitHubSuspended",
			daoConfigs: []*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: testOrgID, Enabled: false, Settings: []byte(`{"repositories":["org/repo"]}`)},
			},
			wantErr: false,
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED,
			},
		},
		{
			// An enabled connector whose credential was confirmed dead (the
			// health-state machine's auth_revoked) must surface as SUSPENDED — the
			// same reconnect-required status as a disabled connector — not CONNECTED,
			// or an admin has no signal that scheduled syncs have silently stopped.
			name: "GitHubEnabledButAuthRevokedIsSuspended",
			daoConfigs: []*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: testOrgID, Enabled: true, HealthState: sdk.HealthStateAuthRevoked},
			},
			wantErr: false,
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED,
			},
		},
		{
			// The zero value of HealthState (unset on older rows / configs built
			// without it) must not be misread as auth_revoked — only the explicit
			// auth_revoked enum value suspends an otherwise-enabled connector.
			name: "GitHubEnabledHealthStateUnsetIsConnected",
			daoConfigs: []*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: testOrgID, Enabled: true},
			},
			wantErr: false,
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_CONNECTED,
			},
		},
		{
			name: "MultipleConnectors",
			daoConfigs: []*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: testOrgID, Enabled: true},
				{ConnectorID: "linear", OrganizationID: testOrgID, Enabled: false},
				{ConnectorID: "jira", OrganizationID: testOrgID, Enabled: true},
			},
			wantErr: false,
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_CONNECTED,
				"linear": connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED,
				"jira":   connintv1.IntegrationStatus_INTEGRATION_STATUS_CONNECTED,
				"gitlab": connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
			},
		},
		{
			name:     "DAOError",
			daoErr:   fmt.Errorf("db connection refused"),
			wantErr:  true,
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(tt.daoConfigs, tt.daoErr)
			// linkGitHubIssues calls Get when GitHub is enabled — allow it for any test.
			dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
				Return(&sdk.ConnectorConfig{Settings: []byte(`{}`)}, nil).AnyTimes()

			// The tenant directory is consulted lazily — only when a disabled GitHub config
			// with no elected repos exists, to gate PENDING on INDIVIDUAL. Allow it any
			// number of times (incl. zero) so cases with no such candidate don't fail.
			mgmt := sdkmocks.NewMockTenantDirectory(ctrl)
			mgmt.EXPECT().TenantType(gomock.Any(), gomock.Any()).
				Return(sdk.TenantKind(tt.tenantType), nil).AnyTimes()

			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Tenants: mgmt, Registry: testRegistry()})

			resp, err := svc.GetIntegrations(context.Background(), &connintv1.GetIntegrationsRequest{OrgId: testOrgID})

			if tt.wantErr {
				require.Error(t, err)
				st, _ := status.FromError(err)
				assert.Equal(t, tt.wantCode, st.Code())
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)

			for id, wantStatus := range tt.wantStatuses {
				assert.Equal(t, wantStatus, findIntegrationStatus(resp, id))
			}
		})
	}
}

// TestGetIntegrations_GitHubIssuesInheritsAuthRevoked guards
// linkGitHubIssues: github-issues piggybacks on the GitHub connection for
// data, so if GitHub's credential is confirmed dead, github-issues cannot
// work either and must not report CONNECTED while its parent reports
// SUSPENDED.
func TestGetIntegrations_GitHubIssuesInheritsAuthRevoked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "github", OrganizationID: testOrgID, Enabled: true, HealthState: sdk.HealthStateAuthRevoked},
	}, nil)
	dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
		Return(&sdk.ConnectorConfig{Settings: []byte(`{"github_issues_enabled":true}`)}, nil).AnyTimes()

	cfg := testConfig()
	cfg.EnabledConnectors["github-issues"] = true
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Registry: testRegistry()})

	resp, err := svc.GetIntegrations(context.Background(), &connintv1.GetIntegrationsRequest{OrgId: testOrgID})
	require.NoError(t, err)

	assert.Equal(t, connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED, findIntegrationStatus(resp, "github"))
	assert.Equal(t, connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED, findIntegrationStatus(resp, "github-issues"),
		"github-issues must mirror its parent's SUSPENDED status, not claim CONNECTED off a dead credential")
}

// TestGetIntegrations_GitHubIssuesDisabledStaysDefaultRegardlessOfHealth
// pins that an org that never turned on the github_issues_enabled toggle must see no status change on that entry
// from GitHub's health state — the toggle gate must still short-circuit
// before the health check, not after.
func TestGetIntegrations_GitHubIssuesDisabledStaysDefaultRegardlessOfHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "github", OrganizationID: testOrgID, Enabled: true, HealthState: sdk.HealthStateAuthRevoked},
	}, nil)
	dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
		Return(&sdk.ConnectorConfig{Settings: []byte(`{}`)}, nil).AnyTimes()

	cfg := testConfig()
	cfg.EnabledConnectors["github-issues"] = true
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Registry: testRegistry()})

	resp, err := svc.GetIntegrations(context.Background(), &connintv1.GetIntegrationsRequest{OrgId: testOrgID})
	require.NoError(t, err)

	assert.Equal(t, connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED, findIntegrationStatus(resp, "github"))
	assert.NotEqual(t, connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED, findIntegrationStatus(resp, "github-issues"),
		"github-issues was never enabled for this org — GitHub's auth_revoked state must not change its status")
}
