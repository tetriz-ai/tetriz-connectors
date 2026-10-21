// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	ghconfig "github.com/tetriz-ai/tetriz-connectors/connectors/github"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intgithubmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/github"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func githubCreds(installationID string) json.RawMessage {
	creds := ghconfig.GitHubCredentials{InstallationID: installationID}
	b, _ := json.Marshal(creds)
	return b
}

func TestDisconnectIntegration(t *testing.T) {
	tests := []struct {
		name          string
		connectorID   string
		setupDAO      func(dao *daomocks.MockIConnectorConfigDAO)
		setupGHClient func(c *intgithubmocks.MockGitHubAppClient)
		wantErr       bool
		wantCode      codes.Code
	}{
		{
			name:        "HappyPath",
			connectorID: "github",
			setupDAO: func(dao *daomocks.MockIConnectorConfigDAO) {
				dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(&sdk.ConnectorConfig{
					ConnectorID:    "github",
					OrganizationID: testOrgID,
					Enabled:        true,
					Credentials:    githubCreds("12345"),
				}, nil)
				dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(true, nil)
				dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(nil)
			},
			setupGHClient: func(c *intgithubmocks.MockGitHubAppClient) {
				c.EXPECT().DeleteInstallation(gomock.Any(), "12345").Return(nil)
			},
			wantErr: false,
		},
		{
			name:        "ConnectorNotFound",
			connectorID: "github",
			setupDAO: func(dao *daomocks.MockIConnectorConfigDAO) {
				dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(nil, fmt.Errorf("record not found"))
			},
			wantErr:  true,
			wantCode: codes.NotFound,
		},
		{
			name:        "RevocationFails_StillSucceeds",
			connectorID: "github",
			setupDAO: func(dao *daomocks.MockIConnectorConfigDAO) {
				dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(&sdk.ConnectorConfig{
					ConnectorID:    "github",
					OrganizationID: testOrgID,
					Enabled:        true,
					Credentials:    githubCreds("12345"),
				}, nil)
				dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(true, nil)
				dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(nil)
			},
			setupGHClient: func(c *intgithubmocks.MockGitHubAppClient) {
				c.EXPECT().DeleteInstallation(gomock.Any(), "12345").Return(fmt.Errorf("network timeout"))
			},
			// Must succeed despite revocation failure (best-effort).
			wantErr: false,
		},
		{
			name:        "DeleteFails",
			connectorID: "github",
			setupDAO: func(dao *daomocks.MockIConnectorConfigDAO) {
				dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(&sdk.ConnectorConfig{
					ConnectorID:    "github",
					OrganizationID: testOrgID,
					Enabled:        true,
					Credentials:    githubCreds("12345"),
				}, nil)
				// ResetHealthState must NOT be called — it only runs after a successful Delete.
				dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(fmt.Errorf("db connection lost"))
			},
			setupGHClient: func(c *intgithubmocks.MockGitHubAppClient) {
				c.EXPECT().DeleteInstallation(gomock.Any(), "12345").Return(nil)
			},
			wantErr:  true,
			wantCode: codes.Internal,
		},
		{
			name:        "NonGitHubConnector_NoProvider",
			connectorID: "linear",
			setupDAO: func(dao *daomocks.MockIConnectorConfigDAO) {
				dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("linear")).Return(&sdk.ConnectorConfig{
					ConnectorID:    "linear",
					OrganizationID: testOrgID,
					Enabled:        true,
					Credentials:    json.RawMessage(`{}`),
				}, nil)
				dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("linear")).Return(true, nil)
				dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("linear")).Return(nil)
			},
			// No ghClient — DeleteInstallation should not be called.
			setupGHClient: nil,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			tt.setupDAO(dao)

			var ghClient *intgithubmocks.MockGitHubAppClient
			if tt.setupGHClient != nil {
				ghClient = intgithubmocks.NewMockGitHubAppClient(ctrl)
				tt.setupGHClient(ghClient)
			}

			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, GHClient: ghClient, Registry: testRegistry()})

			_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
				OrgId: testOrgID, ConnectorId: tt.connectorID,
			})

			if tt.wantErr {
				require.Error(t, err)
				st, _ := status.FromError(err)
				require.Equal(t, tt.wantCode, st.Code())
				if tt.name == "ConnectorNotFound" {
					require.Contains(t, st.Message(), "github")
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDisconnectBitbucket_ClearsRedisAndCredentials(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	// Seed cached access token in Redis.
	_ = mr.Set(bbconfig.AccessTokenKey(testOrgID), "cached-access-token")

	bbCreds, _ := json.Marshal(bbconfig.BitbucketCredentials{
		OAuthRefreshToken: "refresh-token-to-clear",
	})

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("bitbucket")).Return(&sdk.ConnectorConfig{
		ConnectorID:    "bitbucket",
		OrganizationID: testOrgID,
		Enabled:        true,
		Credentials:    bbCreds,
	}, nil)
	// Expect a credentials-only wipe before Delete.
	dao.EXPECT().CompareAndSwapCredentials(gomock.Any(), testOrgID, gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, _ time.Time, creds json.RawMessage) (bool, error) {
			assert.JSONEq(t, "{}", string(creds))
			return true, nil
		})
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("bitbucket")).Return(true, nil)
	dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("bitbucket")).Return(nil)

	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
		OrgId: testOrgID, ConnectorId: "bitbucket",
	})
	require.NoError(t, err)

	// Verify cached access token was deleted from Redis.
	assert.False(t, mr.Exists(bbconfig.AccessTokenKey(testOrgID)))
}
