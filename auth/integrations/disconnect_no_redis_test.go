// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intjiramocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/jira"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// recordingGitLab records RevokeToken; disconnect calls nothing else here.
type recordingGitLab struct {
	GitLabOAuthClient
	revoked []string
}

func (r *recordingGitLab) RevokeToken(_ context.Context, token string) error {
	r.revoked = append(r.revoked, token)
	return nil
}

// With no Redis configured, a disconnect still revokes the vendor token where
// the vendor has a revoke endpoint and still wipes the stored credentials;
// only the access-token cache clear needs Redis.
func TestDisconnectWithoutRedisRevokesAndWipes(t *testing.T) {
	for _, tc := range []struct {
		connector string
		creds     string
	}{
		{"jira", `{"oauth_refresh_token":"jira-refresh"}`},
		{"bitbucket", `{"oauth_refresh_token":"bb-refresh"}`},
		{"gitlab", `{"oauth_refresh_token":"gl-refresh"}`},
	} {
		t.Run(tc.connector, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID(tc.connector)).Return(&sdk.ConnectorConfig{
				ConnectorID: sdk.ConnectorID(tc.connector), OrganizationID: testOrgID, Enabled: true,
				Credentials: json.RawMessage(tc.creds), Settings: json.RawMessage(`{}`),
			}, nil)
			wiped := false
			dao.EXPECT().CompareAndSwapCredentials(gomock.Any(), testOrgID, gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, _ time.Time, creds json.RawMessage) (bool, error) {
					assert.JSONEq(t, "{}", string(creds))
					wiped = true
					return true, nil
				})
			dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, gomock.Any()).Return(true, nil).AnyTimes()
			dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID(tc.connector)).Return(nil)

			jira := intjiramocks.NewMockOAuthClient(ctrl)
			gl := &recordingGitLab{}
			var revokedJira []string
			jira.EXPECT().RevokeToken(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, tok string) error { revokedJira = append(revokedJira, tok); return nil }).AnyTimes()

			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Registry: testRegistry(), JiraClient: jira, GLClient: gl})
			_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{OrgId: testOrgID, ConnectorId: tc.connector})
			require.NoError(t, err)
			assert.True(t, wiped, "the stored credentials are wiped without Redis")
			switch tc.connector {
			case "jira":
				assert.Equal(t, []string{"jira-refresh"}, revokedJira, "the token is revoked without Redis")
			case "gitlab":
				assert.Equal(t, []string{"gl-refresh"}, gl.revoked, "the token is revoked without Redis")
			}
		})
	}
}
