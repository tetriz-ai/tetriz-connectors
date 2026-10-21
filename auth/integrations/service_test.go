// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intgithubmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/github"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestConnectIntegration(t *testing.T) {
	tests := []struct {
		name            string
		connectorID     string
		wantErr         bool
		wantCode        codes.Code
		orgID           string
		cfg             *connectorconfig.Config                     // nil → testConfig()
		setupDAO        func(dao *daomocks.MockIConnectorConfigDAO) // nil means no mock needed
		wantURL         string
		wantURLContains []string
	}{
		{
			name:        "GitHub",
			connectorID: "github",
			orgID:       testOrgID,
			wantErr:     false,
			// The install URL now carries a CSRF state token GitHub echoes back
			// to the setup callback for verification.
			wantURLContains: []string{
				"https://github.com/apps/acme-app/installations/new",
				"state=",
			},
			setupDAO: func(dao *daomocks.MockIConnectorConfigDAO) {
				dao.EXPECT().ListByOrg(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			// The Slack data connector is not shipped: "slack" is not an
			// enabled connector, so its connect is refused.
			name:        "SlackDataConnector",
			connectorID: "slack",
			orgID:       testOrgID,
			wantErr:     true,
			wantCode:    codes.FailedPrecondition,
		},
		{
			// Off-switch guard: a disabled connector is rejected before its
			// OAuth flow starts.
			name:        "GitHubDisabled",
			connectorID: "github",
			cfg:         testConfigWithDisabledConnector("github"),
			wantErr:     true,
			wantCode:    codes.FailedPrecondition,
		},
		{
			name:        "UnsupportedConnector",
			connectorID: "jira",
			wantErr:     true,
			wantCode:    codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var dao *daomocks.MockIConnectorConfigDAO
			if tt.setupDAO != nil {
				dao = daomocks.NewMockIConnectorConfigDAO(ctrl)
				tt.setupDAO(dao)
			}

			cfg := tt.cfg
			if cfg == nil {
				cfg = testConfig()
			}

			// OAuth connect flows write CSRF state to Redis — provide miniredis.
			mr := miniredis.RunT(t)
			rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

			resp, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
				ConnectorId: tt.connectorID,
				OrgId:       tt.orgID,
			})

			if tt.wantErr {
				require.Error(t, err)
				st, _ := status.FromError(err)
				assert.Equal(t, tt.wantCode, st.Code())
			} else {
				require.NoError(t, err)
				if tt.wantURL != "" {
					assert.Equal(t, tt.wantURL, resp.GetRedirectUrl())
				}
				for _, substr := range tt.wantURLContains {
					assert.Contains(t, resp.GetRedirectUrl(), substr)
				}
			}
		})
	}
}

func TestDisconnectIntegration_GitHub_DeletePath(t *testing.T) {
	tests := []struct {
		name          string
		getReturn     *sdk.ConnectorConfig
		getErr        error
		setupGHClient func(c *intgithubmocks.MockGitHubAppClient)
		deleteErr     error
		wantErr       bool
		wantCode      codes.Code
	}{
		{
			name: "HappyPath_DeleteAndRevoke",
			getReturn: &sdk.ConnectorConfig{
				ConnectorID:    "github",
				OrganizationID: testOrgID,
				Enabled:        true,
				Credentials:    githubCreds("12345"),
			},
			setupGHClient: func(c *intgithubmocks.MockGitHubAppClient) {
				c.EXPECT().DeleteInstallation(gomock.Any(), "12345").Return(nil)
			},
			wantErr: false,
		},
		{
			name: "RevocationFails_StillDeletes",
			getReturn: &sdk.ConnectorConfig{
				ConnectorID:    "github",
				OrganizationID: testOrgID,
				Enabled:        true,
				Credentials:    githubCreds("12345"),
			},
			setupGHClient: func(c *intgithubmocks.MockGitHubAppClient) {
				c.EXPECT().DeleteInstallation(gomock.Any(), "12345").Return(fmt.Errorf("network error"))
			},
			wantErr: false,
		},
		{
			name: "DeleteFails",
			getReturn: &sdk.ConnectorConfig{
				ConnectorID:    "github",
				OrganizationID: testOrgID,
				Enabled:        true,
				Credentials:    githubCreds("12345"),
			},
			setupGHClient: func(c *intgithubmocks.MockGitHubAppClient) {
				c.EXPECT().DeleteInstallation(gomock.Any(), "12345").Return(nil)
			},
			deleteErr: fmt.Errorf("db connection lost"),
			wantErr:   true,
			wantCode:  codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(tt.getReturn, tt.getErr)

			var ghClient *intgithubmocks.MockGitHubAppClient
			if tt.setupGHClient != nil {
				ghClient = intgithubmocks.NewMockGitHubAppClient(ctrl)
				tt.setupGHClient(ghClient)
			}

			if tt.getErr == nil {
				dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(tt.deleteErr)
				// ResetHealthState only runs after a successful Delete.
				if tt.deleteErr == nil {
					dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(true, nil)
				}
			}

			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, GHClient: ghClient, Registry: testRegistry()})

			_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
				OrgId:       testOrgID,
				ConnectorId: "github",
			})

			if tt.wantErr {
				require.Error(t, err)
				st, _ := status.FromError(err)
				assert.Equal(t, tt.wantCode, st.Code())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetIntegrations_FeatureFlag(t *testing.T) {
	tests := []struct {
		name              string
		disabledConnector string
		configs           []*sdk.ConnectorConfig
		wantStatuses      map[string]connintv1.IntegrationStatus
	}{
		{
			name:              "LinearDisabled_ShowsOpsDisabled",
			disabledConnector: "linear",
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"linear": connintv1.IntegrationStatus_INTEGRATION_STATUS_OPS_DISABLED,
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
			},
		},
		{
			name:              "LinearDisabled_OverridesConnectedStatus",
			disabledConnector: "linear",
			configs: []*sdk.ConnectorConfig{
				{ConnectorID: "linear", OrganizationID: testOrgID, Enabled: true},
			},
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"linear": connintv1.IntegrationStatus_INTEGRATION_STATUS_OPS_DISABLED,
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
			},
		},
		{
			name:              "GitHubDisabled_ShowsOpsDisabled",
			disabledConnector: "github",
			configs: []*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: testOrgID, Enabled: true},
			},
			wantStatuses: map[string]connintv1.IntegrationStatus{
				"github": connintv1.IntegrationStatus_INTEGRATION_STATUS_OPS_DISABLED,
				"linear": connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(tt.configs, nil)
			dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
				Return(&sdk.ConnectorConfig{Settings: []byte(`{}`)}, nil).AnyTimes()

			svc := NewService(Deps{Cfg: testConfigWithDisabledConnector(tt.disabledConnector), ConnectorDAO: dao, Registry: testRegistry()})

			resp, err := svc.GetIntegrations(context.Background(), &connintv1.GetIntegrationsRequest{
				OrgId: testOrgID,
			})

			require.NoError(t, err)
			statusMap := make(map[string]connintv1.IntegrationStatus)
			for _, cat := range resp.Categories {
				for _, intg := range cat.Integrations {
					statusMap[intg.Id] = intg.Status
				}
			}

			for id, wantStatus := range tt.wantStatuses {
				assert.Equal(t, wantStatus, statusMap[id], "unexpected status for integration %q", id)
			}
		})
	}
}
