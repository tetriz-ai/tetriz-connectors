// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	neturl "net/url"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func bitbucketTestConfig() *connectorconfig.Config {
	cfg := testConfig()
	cfg.BitbucketOAuth = &connectorconfig.BitbucketOAuthConfig{
		ClientID:    "test-bb-client-id",
		CallbackURL: "http://localhost:8080/integrations/bitbucket/callback",
	}
	cfg.BitbucketOAuthClientSecret = "test-bb-client-secret"
	return cfg
}

func TestConnectBitbucket_ReturnsAuthorizeURL(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)

	cfg := bitbucketTestConfig()
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
		ConnectorId: "bitbucket",
		OrgId:       testOrgID,
	})
	require.NoError(t, err)
	assert.Contains(t, resp.GetRedirectUrl(), "https://bitbucket.org/site/oauth2/authorize")
	assert.Contains(t, resp.GetRedirectUrl(), "client_id=test-bb-client-id")
	assert.Contains(t, resp.GetRedirectUrl(), "response_type=code")
	assert.Contains(t, resp.GetRedirectUrl(), "state=")

	// redirect_uri must be pinned explicitly (RFC 6749 §4.1.1) so the code
	// can only be delivered to our registered callback.
	parsed, err := neturl.Parse(resp.GetRedirectUrl())
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8080/integrations/bitbucket/callback", parsed.Query().Get("redirect_uri"))
}

func TestConnectBitbucket_StoresCSRFState(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)

	cfg := bitbucketTestConfig()
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
		ConnectorId: "bitbucket",
		OrgId:       testOrgID,
	})
	require.NoError(t, err)

	// Extract state from the redirect URL.
	parsed, err := neturl.Parse(resp.GetRedirectUrl())
	require.NoError(t, err)
	state := parsed.Query().Get("state")
	require.NotEmpty(t, state)

	// Verify state is stored in Redis.
	key := bbconfig.OAuthStateKey(state)
	val, err := rc.Get(context.Background(), key).Result()
	require.NoError(t, err)

	var stateData struct {
		OrgID string `json:"org_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(val), &stateData))
	assert.Equal(t, testOrgID, stateData.OrgID)

	// Verify TTL is set.
	ttl := mr.TTL(key)
	assert.Positive(t, ttl, "expected TTL > 0, got %v", ttl)
}

func TestValidateAndSaveBitbucketConnection_SingleWorkspace(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	// Seed CSRF state in Redis.
	stateData, _ := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: testOrgID})
	_ = mr.Set(bbconfig.OAuthStateKey("valid-state"), string(stateData))

	// Mock BitbucketOAuthClient.
	bbClient := NewMockBitbucketOAuthClient(ctrl)
	bbClient.EXPECT().ExchangeAuthCode(gomock.Any(), "valid-code").
		Return(&BitbucketTokenResponse{
			AccessToken:  "access-123",
			RefreshToken: "refresh-456",
			ExpiresIn:    7200,
		}, nil)
	bbClient.EXPECT().FetchCurrentUser(gomock.Any(), "access-123").
		Return(&BitbucketUser{UUID: "{user-uuid}", DisplayName: "Alex Doe"}, nil)
	bbClient.EXPECT().FetchWorkspaces(gomock.Any(), "access-123").
		Return([]BitbucketWorkspace{
			{UUID: "{ws-uuid}", Slug: "acme", Name: "Acme Corp"},
		}, nil)

	// Mock DAO — expect VCS guard check, then Save with correct config.
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
	dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(_ context.Context, cfg *sdk.ConnectorConfig, _ func(json.RawMessage) (json.RawMessage, error)) {
		assert.Equal(t, sdk.ConnectorID("bitbucket"), cfg.ConnectorID)
		assert.Equal(t, testOrgID, cfg.OrganizationID)
		assert.True(t, cfg.Enabled)

		var creds bbconfig.BitbucketCredentials
		require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
		assert.Equal(t, "refresh-456", creds.OAuthRefreshToken)

		var settings bbconfig.BitbucketSettings
		require.NoError(t, json.Unmarshal(cfg.Settings, &settings))
		assert.Equal(t, "acme", settings.Workspace)
		assert.Equal(t, "{ws-uuid}", settings.WorkspaceUUID)
		assert.Equal(t, "Acme Corp", settings.WorkspaceName)
		assert.Equal(t, "{user-uuid}", settings.AuthorizingUserUUID)
		assert.Equal(t, "Alex Doe", settings.AuthorizingUserDisplayName)
	}).Return(nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("bitbucket")).Return(true, nil)

	cfg := bitbucketTestConfig()
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, BBClient: bbClient, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ValidateAndSaveBitbucketConnection(context.Background(), &connintv1.ValidateAndSaveBitbucketConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "valid-code",
		State:    "valid-state",
	})
	require.NoError(t, err)

	// Single workspace: auto-saved, no selection key or workspace list returned.
	assert.Empty(t, resp.GetSelectionKey())
	assert.Empty(t, resp.GetWorkspaces())

	// Verify state was deleted (single-use).
	assert.False(t, mr.Exists(bbconfig.OAuthStateKey("valid-state")))
}

func TestValidateAndSaveBitbucketConnection_MultipleWorkspaces(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	// Seed CSRF state.
	stateData, _ := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: testOrgID})
	_ = mr.Set(bbconfig.OAuthStateKey("valid-state"), string(stateData))

	bbClient := NewMockBitbucketOAuthClient(ctrl)
	bbClient.EXPECT().ExchangeAuthCode(gomock.Any(), "valid-code").
		Return(&BitbucketTokenResponse{
			AccessToken:  "access-123",
			RefreshToken: "refresh-456",
			ExpiresIn:    7200,
		}, nil)
	bbClient.EXPECT().FetchCurrentUser(gomock.Any(), "access-123").
		Return(&BitbucketUser{UUID: "{user-uuid}", DisplayName: "Alex Doe"}, nil)
	bbClient.EXPECT().FetchWorkspaces(gomock.Any(), "access-123").
		Return([]BitbucketWorkspace{
			{UUID: "{ws-1}", Slug: "acme", Name: "Acme Corp"},
			{UUID: "{ws-2}", Slug: "other-team", Name: "Other Team"},
		}, nil)

	// DAO should NOT be called — no auto-save when multiple workspaces.
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)

	cfg := bitbucketTestConfig()
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, BBClient: bbClient, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ValidateAndSaveBitbucketConnection(context.Background(),
		&connintv1.ValidateAndSaveBitbucketConnectionRequest{
			OrgId: testOrgID, AuthCode: "valid-code", State: "valid-state",
		})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetSelectionKey())
	assert.Len(t, resp.GetWorkspaces(), 2)
	assert.Equal(t, "acme", resp.GetWorkspaces()[0].GetId())
	assert.Equal(t, "Acme Corp", resp.GetWorkspaces()[0].GetName())

	// Verify tokens stored in Redis.
	stored, err := rc.Get(context.Background(), bbconfig.WorkspaceSelectionKey(resp.GetSelectionKey())).Result()
	require.NoError(t, err)
	assert.NotEmpty(t, stored)

	// Verify CSRF state was deleted.
	assert.False(t, mr.Exists(bbconfig.OAuthStateKey("valid-state")))
}

func TestValidateAndSaveBitbucketConnection_InvalidState(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	cfg := bitbucketTestConfig()
	svc := NewService(Deps{Cfg: cfg, Redis: rc, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveBitbucketConnection(context.Background(), &connintv1.ValidateAndSaveBitbucketConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "code",
		State:    "nonexistent-state",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid or expired state")
}

func TestValidateAndSaveBitbucketConnection_MismatchedOrgID(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Seed state with different org_id.
	stateData, _ := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: "other-org"})
	_ = mr.Set(bbconfig.OAuthStateKey("mismatched-state"), string(stateData))

	cfg := bitbucketTestConfig()
	svc := NewService(Deps{Cfg: cfg, Redis: rc, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveBitbucketConnection(context.Background(), &connintv1.ValidateAndSaveBitbucketConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "code",
		State:    "mismatched-state",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "state mismatch")
}

func TestValidateAndSaveBitbucketConnection_TokenExchangeFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	stateData, _ := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: testOrgID})
	_ = mr.Set(bbconfig.OAuthStateKey("valid-state"), string(stateData))

	bbClient := NewMockBitbucketOAuthClient(ctrl)
	bbClient.EXPECT().ExchangeAuthCode(gomock.Any(), "bad-code").
		Return(nil, assert.AnError)

	cfg := bitbucketTestConfig()
	svc := NewService(Deps{Cfg: cfg, BBClient: bbClient, Redis: rc, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveBitbucketConnection(context.Background(), &connintv1.ValidateAndSaveBitbucketConnectionRequest{
		OrgId:    testOrgID,
		AuthCode: "bad-code",
		State:    "valid-state",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token exchange")
}

// ── GetBitbucketWorkspaces ──────────────────────────────────────────────

func seedWorkspaceSelection(t *testing.T, mr *miniredis.Miniredis, selectionKey string) {
	t.Helper()
	data, err := json.Marshal(workspaceSelectionData{
		OrgID:        testOrgID,
		RefreshToken: "refresh-456",
		Workspaces: []BitbucketWorkspace{
			{UUID: "{ws-1}", Slug: "acme", Name: "Acme Corp"},
			{UUID: "{ws-2}", Slug: "other", Name: "Other Team"},
		},
		User: &BitbucketUser{UUID: "{user-uuid}", DisplayName: "Alex Doe"},
	})
	require.NoError(t, err)
	_ = mr.Set(bbconfig.WorkspaceSelectionKey(selectionKey), string(data))
}

func TestGetBitbucketWorkspaces_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	seedWorkspaceSelection(t, mr, "sess-key")

	svc := NewService(Deps{Cfg: bitbucketTestConfig(), Redis: rc, Registry: testRegistry()})

	resp, err := svc.GetBitbucketWorkspaces(context.Background(),
		&connintv1.GetBitbucketWorkspacesRequest{SelectionKey: "sess-key"})
	require.NoError(t, err)
	assert.Len(t, resp.GetWorkspaces(), 2)
	assert.Equal(t, "acme", resp.GetWorkspaces()[0].GetId())
	assert.Equal(t, "Other Team", resp.GetWorkspaces()[1].GetName())
}

func TestGetBitbucketWorkspaces_ExpiredSession(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	_ = mr // no data seeded

	svc := NewService(Deps{Cfg: bitbucketTestConfig(), Redis: rc, Registry: testRegistry()})

	_, err := svc.GetBitbucketWorkspaces(context.Background(),
		&connintv1.GetBitbucketWorkspacesRequest{SelectionKey: "expired-key"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// ── SelectBitbucketWorkspace ────────────────────────────────────────────

func TestSelectBitbucketWorkspace_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)
	seedWorkspaceSelection(t, mr, "sess-key")

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
	dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(_ context.Context, cfg *sdk.ConnectorConfig, _ func(json.RawMessage) (json.RawMessage, error)) {
		var settings bbconfig.BitbucketSettings
		require.NoError(t, json.Unmarshal(cfg.Settings, &settings))
		assert.Equal(t, "other", settings.Workspace)
		assert.Equal(t, "Other Team", settings.WorkspaceName)
		assert.Equal(t, "{ws-2}", settings.WorkspaceUUID)
		assert.Equal(t, "{user-uuid}", settings.AuthorizingUserUUID)
		assert.Equal(t, "Alex Doe", settings.AuthorizingUserDisplayName)
	}).Return(nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("bitbucket")).Return(true, nil)

	svc := NewService(Deps{Cfg: bitbucketTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectBitbucketWorkspace(context.Background(),
		&connintv1.SelectBitbucketWorkspaceRequest{
			OrgId: testOrgID, SelectionKey: "sess-key", WorkspaceSlug: "other",
		})
	require.NoError(t, err)

	// Verify Redis key was deleted (single-use).
	assert.False(t, mr.Exists(bbconfig.WorkspaceSelectionKey("sess-key")))
}

func TestSelectBitbucketWorkspace_InvalidSlug(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	seedWorkspaceSelection(t, mr, "sess-key")

	svc := NewService(Deps{Cfg: bitbucketTestConfig(), Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectBitbucketWorkspace(context.Background(),
		&connintv1.SelectBitbucketWorkspaceRequest{
			OrgId: testOrgID, SelectionKey: "sess-key", WorkspaceSlug: "nonexistent",
		})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
