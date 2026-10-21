// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	intjira "github.com/tetriz-ai/tetriz-connectors/auth/integrations/jira"
	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	jiramocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/jira"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func jiraTestConfig() *connectorconfig.Config {
	cfg := testConfig()
	cfg.JiraOAuth = &connectorconfig.JiraOAuthConfig{
		ClientID:    "test-jira-client-id",
		CallbackURL: "http://localhost:8080/integrations/jira/callback",
	}
	return cfg
}

// --- connectJira tests ---

func TestConnectJira_ReturnsAuthorizeURL(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
		ConnectorId: "jira",
		OrgId:       testOrgID,
	})
	require.NoError(t, err)
	assert.Contains(t, resp.GetRedirectUrl(), "https://auth.atlassian.com/authorize")
	assert.Contains(t, resp.GetRedirectUrl(), "client_id=test-jira-client-id")
	assert.Contains(t, resp.GetRedirectUrl(), "response_type=code")
	assert.Contains(t, resp.GetRedirectUrl(), "state=")
}

func TestConnectJira_StoresCSRFStateInRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
		ConnectorId: "jira",
		OrgId:       testOrgID,
	})
	require.NoError(t, err)

	// Extract state from URL.
	parsedURL, err := url.Parse(resp.GetRedirectUrl())
	require.NoError(t, err)
	state := parsedURL.Query().Get("state")
	require.NotEmpty(t, state)

	// State must be stored in Redis with org_id.
	raw, err := rc.Get(context.Background(), jiraconfig.OAuthStateKey(state)).Result()
	require.NoError(t, err)
	var data struct {
		OrgID string `json:"org_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &data))
	assert.Equal(t, testOrgID, data.OrgID)
}

func TestConnectJira_MissingOrgID(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
		ConnectorId: "jira",
		OrgId:       "",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestConnectJira_OAuthNotConfigured(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	cfg := testConfig() // No JiraOAuth configured.
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
		ConnectorId: "jira",
		OrgId:       testOrgID,
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// --- ValidateAndSaveJiraConnection tests ---

func TestValidateAndSaveJiraConnection_SingleSite_AutoSaves(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	// Store a valid CSRF state.
	stateData, err := json.Marshal(map[string]string{"org_id": testOrgID})
	require.NoError(t, err)
	_ = mr.Set(jiraconfig.OAuthStateKey("valid-state"), string(stateData))

	mockJira := jiramocks.NewMockOAuthClient(ctrl)
	mockJira.EXPECT().
		ExchangeAuthCode(gomock.Any(), "auth-code-123").
		Return(&intjira.TokenResponse{
			AccessToken:  "access-token",
			RefreshToken: "refresh-token",
		}, nil)
	mockJira.EXPECT().
		FetchAccessibleResources(gomock.Any(), "access-token").
		Return([]intjira.CloudSite{
			{ID: "cloud-abc", Name: "My Jira", URL: "https://myorg.atlassian.net"},
		}, nil)
	mockJira.EXPECT().
		FetchCurrentUser(gomock.Any(), "access-token").
		Return("Admin User", nil)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().
		SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, cfg *sdk.ConnectorConfig, _ func(json.RawMessage) (json.RawMessage, error)) error {
			assert.Equal(t, sdk.ConnectorID("jira"), cfg.ConnectorID)
			assert.Equal(t, testOrgID, cfg.OrganizationID)
			assert.True(t, cfg.Enabled)

			var creds jiraconfig.JiraCredentials
			require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
			assert.Equal(t, "refresh-token", creds.OAuthRefreshToken)

			var settings jiraconfig.JiraSettings
			require.NoError(t, json.Unmarshal(cfg.Settings, &settings))
			assert.Equal(t, "cloud-abc", settings.CloudID)
			assert.Equal(t, "My Jira", settings.CloudName)
			assert.Equal(t, "Admin User", settings.ConnectedByName)
			return nil
		})
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("jira")).Return(true, nil)

	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, JiraClient: mockJira, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ValidateAndSaveJiraConnection(context.Background(), &connintv1.ValidateAndSaveJiraConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "auth-code-123",
		State:    "valid-state",
	})
	require.NoError(t, err)
	assert.Empty(t, resp.GetSelectionKey(), "single site should not return selection_key")

	// State token must be consumed (deleted).
	_, err = rc.Get(context.Background(), jiraconfig.OAuthStateKey("valid-state")).Result()
	assert.Error(t, err, "state token should be deleted after use")
}

func TestValidateAndSaveJiraConnection_MultipleSites_ReturnsSiteList(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	stateData, err := json.Marshal(map[string]string{"org_id": testOrgID})
	require.NoError(t, err)
	_ = mr.Set(jiraconfig.OAuthStateKey("valid-state"), string(stateData))

	mockJira := jiramocks.NewMockOAuthClient(ctrl)
	mockJira.EXPECT().
		ExchangeAuthCode(gomock.Any(), "auth-code-123").
		Return(&intjira.TokenResponse{AccessToken: "access-token", RefreshToken: "refresh-token"}, nil)
	mockJira.EXPECT().
		FetchAccessibleResources(gomock.Any(), "access-token").
		Return([]intjira.CloudSite{
			{ID: "cloud-1", Name: "Site One", URL: "https://siteone.atlassian.net"},
			{ID: "cloud-2", Name: "Site Two", URL: "https://sitetwo.atlassian.net"},
		}, nil)
	mockJira.EXPECT().
		FetchCurrentUser(gomock.Any(), "access-token").
		Return("Admin User", nil)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, JiraClient: mockJira, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ValidateAndSaveJiraConnection(context.Background(), &connintv1.ValidateAndSaveJiraConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "auth-code-123",
		State:    "valid-state",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetSelectionKey(), "multiple sites should return selection_key")
	assert.Len(t, resp.GetSites(), 2)
	assert.Equal(t, "cloud-1", resp.GetSites()[0].GetId())
	assert.Equal(t, "cloud-2", resp.GetSites()[1].GetId())
}

func TestValidateAndSaveJiraConnection_ExpiredState(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	// State not stored in Redis (expired or never set).
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveJiraConnection(context.Background(), &connintv1.ValidateAndSaveJiraConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "auth-code-123",
		State:    "missing-state",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestValidateAndSaveJiraConnection_StateMismatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	// State belongs to a different org.
	stateData, err := json.Marshal(map[string]string{"org_id": "different-org"})
	require.NoError(t, err)
	_ = mr.Set(jiraconfig.OAuthStateKey("valid-state"), string(stateData))

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err = svc.ValidateAndSaveJiraConnection(context.Background(), &connintv1.ValidateAndSaveJiraConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "auth-code-123",
		State:    "valid-state",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestValidateAndSaveJiraConnection_NoSitesFound(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	stateData, err := json.Marshal(map[string]string{"org_id": testOrgID})
	require.NoError(t, err)
	_ = mr.Set(jiraconfig.OAuthStateKey("valid-state"), string(stateData))

	mockJira := jiramocks.NewMockOAuthClient(ctrl)
	mockJira.EXPECT().
		ExchangeAuthCode(gomock.Any(), "auth-code-123").
		Return(&intjira.TokenResponse{AccessToken: "access-token", RefreshToken: "refresh-token"}, nil)
	mockJira.EXPECT().
		FetchAccessibleResources(gomock.Any(), "access-token").
		Return([]intjira.CloudSite{}, nil)
	// FetchCurrentUser is NOT called when there are no sites (early return before step 4).

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, JiraClient: mockJira, Redis: rc, Registry: testRegistry()})

	_, err = svc.ValidateAndSaveJiraConnection(context.Background(), &connintv1.ValidateAndSaveJiraConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "auth-code-123",
		State:    "valid-state",
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// --- SelectJiraCloudSite tests ---

func seedJiraSiteSelection(t *testing.T, mr *miniredis.Miniredis, selectionKey, orgID string, sites []intjira.CloudSite) {
	t.Helper()
	data, err := json.Marshal(jiraSiteSelectionData{
		OrgID:           orgID,
		RefreshToken:    "refresh-token",
		Sites:           sites,
		ConnectedByName: "Admin User",
	})
	require.NoError(t, err)
	_ = mr.Set(jiraconfig.SiteSelectionKey(selectionKey), string(data))
}

func TestSelectJiraCloudSite_SavesSelectedSite(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	sites := []intjira.CloudSite{
		{ID: "cloud-1", Name: "Site One", URL: "https://siteone.atlassian.net"},
		{ID: "cloud-2", Name: "Site Two", URL: "https://sitetwo.atlassian.net"},
	}
	seedJiraSiteSelection(t, mr, "sel-key-123", testOrgID, sites)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().
		SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, cfg *sdk.ConnectorConfig, _ func(json.RawMessage) (json.RawMessage, error)) error {
			var settings jiraconfig.JiraSettings
			require.NoError(t, json.Unmarshal(cfg.Settings, &settings))
			assert.Equal(t, "cloud-2", settings.CloudID)
			assert.Equal(t, "Site Two", settings.CloudName)
			assert.Equal(t, "Admin User", settings.ConnectedByName)
			return nil
		})
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("jira")).Return(true, nil)

	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectJiraCloudSite(context.Background(), &connintv1.SelectJiraCloudSiteRequest{
		OrgId:        testOrgID,
		SelectionKey: "sel-key-123",
		CloudId:      "cloud-2",
	})
	require.NoError(t, err)
}

func TestSelectJiraCloudSite_DeletesSelectionKey(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	sites := []intjira.CloudSite{{ID: "cloud-1", Name: "Site One", URL: "https://siteone.atlassian.net"}}
	seedJiraSiteSelection(t, mr, "sel-key-123", testOrgID, sites)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("jira")).Return(true, nil)

	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectJiraCloudSite(context.Background(), &connintv1.SelectJiraCloudSiteRequest{
		OrgId:        testOrgID,
		SelectionKey: "sel-key-123",
		CloudId:      "cloud-1",
	})
	require.NoError(t, err)

	// Session key must be deleted.
	_, err = rc.Get(context.Background(), jiraconfig.SiteSelectionKey("sel-key-123")).Result()
	assert.Error(t, err, "selection key should be deleted after use")
}

func TestSelectJiraCloudSite_InvalidCloudID(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	sites := []intjira.CloudSite{{ID: "cloud-1", Name: "Site One", URL: "https://siteone.atlassian.net"}}
	seedJiraSiteSelection(t, mr, "sel-key-123", testOrgID, sites)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectJiraCloudSite(context.Background(), &connintv1.SelectJiraCloudSiteRequest{
		OrgId:        testOrgID,
		SelectionKey: "sel-key-123",
		CloudId:      "cloud-nonexistent",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSelectJiraCloudSite_OrgIDMismatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	sites := []intjira.CloudSite{{ID: "cloud-1", Name: "Site One", URL: "https://siteone.atlassian.net"}}
	seedJiraSiteSelection(t, mr, "sel-key-123", "other-org", sites)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectJiraCloudSite(context.Background(), &connintv1.SelectJiraCloudSiteRequest{
		OrgId:        testOrgID, // different from session's org
		SelectionKey: "sel-key-123",
		CloudId:      "cloud-1",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSelectJiraCloudSite_ExpiredSession(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectJiraCloudSite(context.Background(), &connintv1.SelectJiraCloudSiteRequest{
		OrgId:        testOrgID,
		SelectionKey: "nonexistent-key",
		CloudId:      "cloud-1",
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}
