// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func gitlabTestConfig() *connectorconfig.Config {
	cfg := testConfig()
	cfg.GitLabOAuth = &connectorconfig.GitLabOAuthConfig{
		ClientID:    "test-gl-client-id",
		CallbackURL: "http://localhost:8080/integrations/gitlab/callback",
		WebhookURL:  "", // empty → webhook registration skipped in tests
	}
	cfg.GitLabOAuthClientSecret = "test-gl-client-secret"
	return cfg
}

// testGroups returns two groups for use in multi-group test cases.
func testGroups() []GitLabGroup {
	return []GitLabGroup{
		{ID: 100, Path: "acme", FullPath: "acme", Name: "Acme Corp"},
		{ID: 200, Path: "other", FullPath: "other", Name: "Other Team"},
	}
}

func testGitLabUser() *GitLabUser {
	return &GitLabUser{ID: 42, Username: "alexdoe", Name: "Alex Doe"}
}

// seedGitLabState stores a CSRF state JSON entry in miniredis.
func seedGitLabState(t *testing.T, mr *miniredis.Miniredis, state string) {
	t.Helper()
	data, err := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: testOrgID})
	require.NoError(t, err)
	_ = mr.Set(glconfig.OAuthStateKey(state), string(data))
}

// seedGroupSelection stores a group selection session in miniredis.
func seedGroupSelection(t *testing.T, mr *miniredis.Miniredis, key string) {
	t.Helper()
	data, err := json.Marshal(groupSelectionData{
		OrgID:        testOrgID,
		RefreshToken: "refresh-456",
		AccessToken:  "access-123",
		Groups:       testGroups(),
		User:         testGitLabUser(),
	})
	require.NoError(t, err)
	_ = mr.Set(glconfig.GroupSelectionKey(key), string(data))
}

// ── ConnectGitLab ──────────────────────────────────────────────────────

func TestConnectGitLab_ReturnsAuthorizeURL(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)

	svc := NewService(Deps{Cfg: gitlabTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
		ConnectorId: "gitlab",
		OrgId:       testOrgID,
	})
	require.NoError(t, err)
	assert.Contains(t, resp.GetRedirectUrl(), "https://gitlab.com/oauth/authorize")
	assert.Contains(t, resp.GetRedirectUrl(), "client_id=test-gl-client-id")
	assert.Contains(t, resp.GetRedirectUrl(), "response_type=code")
	assert.Contains(t, resp.GetRedirectUrl(), "state=")
}

func TestConnectGitLab_StoresCSRFState(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)

	svc := NewService(Deps{Cfg: gitlabTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
		ConnectorId: "gitlab",
		OrgId:       testOrgID,
	})
	require.NoError(t, err)

	// Extract state from redirect URL via net/url.
	parsed, err := url.Parse(resp.GetRedirectUrl())
	require.NoError(t, err)
	state := parsed.Query().Get("state")
	require.NotEmpty(t, state)

	// Verify state stored in Redis.
	val, redisErr := rc.Get(context.Background(), glconfig.OAuthStateKey(state)).Result()
	require.NoError(t, redisErr)
	var stateData struct {
		OrgID string `json:"org_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(val), &stateData))
	assert.Equal(t, testOrgID, stateData.OrgID)

	// Verify TTL is set.
	ttl := mr.TTL(glconfig.OAuthStateKey(state))
	assert.Positive(t, ttl, "expected TTL > 0, got %v", ttl)
}

// ── ValidateAndSaveGitLabConnection ────────────────────────────────────

func TestValidateAndSaveGitLabConnection_SingleGroup(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	seedGitLabState(t, mr, "valid-state")

	glClient := NewMockGitLabOAuthClient(ctrl)
	glClient.EXPECT().ExchangeAuthCode(gomock.Any(), "valid-code").
		Return(&GitLabTokenResponse{
			AccessToken:  "access-123",
			RefreshToken: "refresh-456",
		}, nil)
	glClient.EXPECT().FetchCurrentUser(gomock.Any(), "access-123").
		Return(testGitLabUser(), nil)
	glClient.EXPECT().FetchGroups(gomock.Any(), "access-123").
		Return([]GitLabGroup{testGroups()[0]}, nil) // single group

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
	dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(_ context.Context, cfg *sdk.ConnectorConfig, _ func(json.RawMessage) (json.RawMessage, error)) {
		assert.Equal(t, sdk.ConnectorID("gitlab"), cfg.ConnectorID)
		assert.Equal(t, testOrgID, cfg.OrganizationID)
		assert.True(t, cfg.Enabled)

		var creds glconfig.GitLabCredentials
		require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
		assert.Equal(t, "refresh-456", creds.OAuthRefreshToken)
		assert.NotEmpty(t, creds.WebhookSecret)

		var settings glconfig.GitLabSettings
		require.NoError(t, json.Unmarshal(cfg.Settings, &settings))
		assert.Equal(t, int64(100), settings.GroupID)
		assert.Equal(t, "acme", settings.GroupPath)
		assert.Equal(t, "Acme Corp", settings.GroupName)
		assert.Equal(t, "Alex Doe", settings.AuthorizingUserDisplayName)
	}).Return(nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(true, nil)

	svc := NewService(Deps{Cfg: gitlabTestConfig(), ConnectorDAO: dao, GLClient: glClient, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ValidateAndSaveGitLabConnection(context.Background(),
		&connintv1.ValidateAndSaveGitLabConnectionRequest{
			OrgId:    testOrgID,
			AuthCode: "valid-code",
			State:    "valid-state",
		})
	require.NoError(t, err)

	// Single group: auto-saved, no selection key or group list returned.
	assert.Empty(t, resp.GetSelectionKey())
	assert.Empty(t, resp.GetGroups())

	// State token must be deleted (single-use).
	assert.False(t, mr.Exists(glconfig.OAuthStateKey("valid-state")))
}

func TestValidateAndSaveGitLabConnection_MultipleGroups(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	seedGitLabState(t, mr, "valid-state")

	glClient := NewMockGitLabOAuthClient(ctrl)
	glClient.EXPECT().ExchangeAuthCode(gomock.Any(), "valid-code").
		Return(&GitLabTokenResponse{
			AccessToken:  "access-123",
			RefreshToken: "refresh-456",
		}, nil)
	glClient.EXPECT().FetchCurrentUser(gomock.Any(), "access-123").
		Return(testGitLabUser(), nil)
	glClient.EXPECT().FetchGroups(gomock.Any(), "access-123").
		Return(testGroups(), nil) // two groups

	// DAO should NOT be called — no auto-save when multiple groups.
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)

	svc := NewService(Deps{Cfg: gitlabTestConfig(), ConnectorDAO: dao, GLClient: glClient, Redis: rc, Registry: testRegistry()})

	resp, err := svc.ValidateAndSaveGitLabConnection(context.Background(),
		&connintv1.ValidateAndSaveGitLabConnectionRequest{
			OrgId:    testOrgID,
			AuthCode: "valid-code",
			State:    "valid-state",
		})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetSelectionKey())
	assert.Len(t, resp.GetGroups(), 2)
	assert.Equal(t, int64(100), resp.GetGroups()[0].GetId())
	assert.Equal(t, "acme", resp.GetGroups()[0].GetPath())
	assert.Equal(t, "Acme Corp", resp.GetGroups()[0].GetName())

	// Verify tokens + groups stored in Redis.
	stored, err := rc.Get(context.Background(), glconfig.GroupSelectionKey(resp.GetSelectionKey())).Result()
	require.NoError(t, err)
	assert.NotEmpty(t, stored)

	// State token must be deleted.
	assert.False(t, mr.Exists(glconfig.OAuthStateKey("valid-state")))
}

// TestGitLabGroupSelection_TokensEncryptedAtRest guards that tokens are encrypted at rest:
// when an encryptor is configured, the OAuth access/refresh tokens parked in
// Redis during the multi-group picker flow must not appear as plaintext, and a
// store→load round-trip (ValidateAndSave → SelectGitLabGroup) must still recover
// the original tokens. With no encryptor the blob is plaintext; that is
// asserted to document the contract, not to recommend running without a key.
func TestGitLabGroupSelection_TokensEncryptedAtRest(t *testing.T) {
	const (
		accessToken  = "access-secret-abc"
		refreshToken = "refresh-secret-xyz"
	)

	tests := []struct {
		name                 string
		withEncryptor        bool
		wantPlaintextInRedis bool
	}{
		{name: "encrypted", withEncryptor: true, wantPlaintextInRedis: false},
		{name: "plaintext_dev_mode", withEncryptor: false, wantPlaintextInRedis: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			ctrl := gomock.NewController(t)

			seedGitLabState(t, mr, "valid-state")

			glClient := NewMockGitLabOAuthClient(ctrl)
			glClient.EXPECT().ExchangeAuthCode(gomock.Any(), "valid-code").
				Return(&GitLabTokenResponse{AccessToken: accessToken, RefreshToken: refreshToken}, nil)
			glClient.EXPECT().FetchCurrentUser(gomock.Any(), accessToken).Return(testGitLabUser(), nil)
			glClient.EXPECT().FetchGroups(gomock.Any(), accessToken).Return(testGroups(), nil) // two → picker

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)

			var enc cryptutil.Encryptor
			if tt.withEncryptor {
				enc = testEncryptor(t)
			}
			svc := NewService(Deps{Cfg: gitlabTestConfig(), ConnectorDAO: dao, GLClient: glClient, Redis: rc, Registry: testRegistry(), Enc: enc})

			resp, err := svc.ValidateAndSaveGitLabConnection(context.Background(),
				&connintv1.ValidateAndSaveGitLabConnectionRequest{
					OrgId: testOrgID, AuthCode: "valid-code", State: "valid-state",
				})
			require.NoError(t, err)
			require.NotEmpty(t, resp.GetSelectionKey())

			// Inspect the raw bytes persisted to Redis.
			rawStored, err := rc.Get(context.Background(), glconfig.GroupSelectionKey(resp.GetSelectionKey())).Result()
			require.NoError(t, err)

			if tt.wantPlaintextInRedis {
				assert.Contains(t, rawStored, accessToken, "dev plaintext mode: tokens stored as-is")
				assert.Contains(t, rawStored, refreshToken)
			} else {
				assert.NotContains(t, rawStored, accessToken, "access token must not be plaintext in Redis")
				assert.NotContains(t, rawStored, refreshToken, "refresh token must not be plaintext in Redis")
				assert.NotContains(t, rawStored, testOrgID, "encrypted blob leaks no plaintext field")
			}

			// Round-trip: the picker finalize path must recover the original tokens.
			dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
			dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(_ context.Context, cfg *sdk.ConnectorConfig, _ func(json.RawMessage) (json.RawMessage, error)) {
				var creds glconfig.GitLabCredentials
				require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
				assert.Equal(t, refreshToken, creds.OAuthRefreshToken, "round-trip recovers refresh token")
			}).Return(nil)
			dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(true, nil)

			_, err = svc.SelectGitLabGroup(context.Background(),
				&connintv1.SelectGitLabGroupRequest{OrgId: testOrgID, SelectionKey: resp.GetSelectionKey(), GroupId: 100})
			require.NoError(t, err)
		})
	}
}

func TestValidateAndSaveGitLabConnection_InvalidState(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	svc := NewService(Deps{Cfg: gitlabTestConfig(), Redis: rc, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveGitLabConnection(context.Background(),
		&connintv1.ValidateAndSaveGitLabConnectionRequest{
			OrgId:    testOrgID,
			AuthCode: "code",
			State:    "nonexistent-state",
		})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "invalid or expired state")
}

func TestValidateAndSaveGitLabConnection_MismatchedOrgID(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Seed state with a different org.
	data, _ := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: "other-org"})
	_ = mr.Set(glconfig.OAuthStateKey("mismatched-state"), string(data))

	svc := NewService(Deps{Cfg: gitlabTestConfig(), Redis: rc, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveGitLabConnection(context.Background(),
		&connintv1.ValidateAndSaveGitLabConnectionRequest{
			OrgId:    testOrgID,
			AuthCode: "code",
			State:    "mismatched-state",
		})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "state mismatch")
}

func TestValidateAndSaveGitLabConnection_TokenExchangeFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	seedGitLabState(t, mr, "valid-state")

	glClient := NewMockGitLabOAuthClient(ctrl)
	glClient.EXPECT().ExchangeAuthCode(gomock.Any(), "bad-code").
		Return(nil, fmt.Errorf("network timeout"))

	svc := NewService(Deps{Cfg: gitlabTestConfig(), GLClient: glClient, Redis: rc, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveGitLabConnection(context.Background(),
		&connintv1.ValidateAndSaveGitLabConnectionRequest{
			OrgId:    testOrgID,
			AuthCode: "bad-code",
			State:    "valid-state",
		})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "token exchange")
}

func TestValidateAndSaveGitLabConnection_NoGroups(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	seedGitLabState(t, mr, "valid-state")

	glClient := NewMockGitLabOAuthClient(ctrl)
	glClient.EXPECT().ExchangeAuthCode(gomock.Any(), "code").
		Return(&GitLabTokenResponse{AccessToken: "tok", RefreshToken: "ref"}, nil)
	glClient.EXPECT().FetchCurrentUser(gomock.Any(), "tok").Return(testGitLabUser(), nil)
	glClient.EXPECT().FetchGroups(gomock.Any(), "tok").Return([]GitLabGroup{}, nil)

	svc := NewService(Deps{Cfg: gitlabTestConfig(), GLClient: glClient, Redis: rc, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveGitLabConnection(context.Background(),
		&connintv1.ValidateAndSaveGitLabConnectionRequest{
			OrgId:    testOrgID,
			AuthCode: "code",
			State:    "valid-state",
		})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// ── GetGitLabGroups ────────────────────────────────────────────────────

func TestGetGitLabGroups_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	seedGroupSelection(t, mr, "sess-key")

	svc := NewService(Deps{Cfg: gitlabTestConfig(), Redis: rc, Registry: testRegistry()})

	resp, err := svc.GetGitLabGroups(context.Background(),
		&connintv1.GetGitLabGroupsRequest{SelectionKey: "sess-key"})
	require.NoError(t, err)
	assert.Len(t, resp.GetGroups(), 2)
	assert.Equal(t, int64(100), resp.GetGroups()[0].GetId())
	assert.Equal(t, "acme", resp.GetGroups()[0].GetPath())
	assert.Equal(t, int64(200), resp.GetGroups()[1].GetId())
	assert.Equal(t, "Other Team", resp.GetGroups()[1].GetName())
}

func TestGetGitLabGroups_ExpiredSession(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	svc := NewService(Deps{Cfg: gitlabTestConfig(), Redis: rc, Registry: testRegistry()})

	_, err := svc.GetGitLabGroups(context.Background(),
		&connintv1.GetGitLabGroupsRequest{SelectionKey: "expired-key"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// ── SelectGitLabGroup ──────────────────────────────────────────────────

func TestSelectGitLabGroup_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)
	seedGroupSelection(t, mr, "sess-key")

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
	dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(_ context.Context, cfg *sdk.ConnectorConfig, _ func(json.RawMessage) (json.RawMessage, error)) {
		var settings glconfig.GitLabSettings
		require.NoError(t, json.Unmarshal(cfg.Settings, &settings))
		assert.Equal(t, int64(200), settings.GroupID)
		assert.Equal(t, "other", settings.GroupPath)
		assert.Equal(t, "Other Team", settings.GroupName)
		assert.Equal(t, "Alex Doe", settings.AuthorizingUserDisplayName)

		var creds glconfig.GitLabCredentials
		require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
		assert.Equal(t, "refresh-456", creds.OAuthRefreshToken)
		assert.NotEmpty(t, creds.WebhookSecret)
	}).Return(nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(true, nil)

	svc := NewService(Deps{Cfg: gitlabTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectGitLabGroup(context.Background(),
		&connintv1.SelectGitLabGroupRequest{
			OrgId: testOrgID, SelectionKey: "sess-key", GroupId: 200,
		})
	require.NoError(t, err)

	// Verify Redis key was deleted (single-use).
	assert.False(t, mr.Exists(glconfig.GroupSelectionKey("sess-key")))
}

func TestSelectGitLabGroup_InvalidGroupID(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	seedGroupSelection(t, mr, "sess-key")

	svc := NewService(Deps{Cfg: gitlabTestConfig(), Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectGitLabGroup(context.Background(),
		&connintv1.SelectGitLabGroupRequest{
			OrgId: testOrgID, SelectionKey: "sess-key", GroupId: 999,
		})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSelectGitLabGroup_ExpiredSession(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	svc := NewService(Deps{Cfg: gitlabTestConfig(), Redis: rc, Registry: testRegistry()})

	_, err := svc.SelectGitLabGroup(context.Background(),
		&connintv1.SelectGitLabGroupRequest{
			OrgId: testOrgID, SelectionKey: "expired-key", GroupId: 100,
		})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// ── DisconnectIntegration (GitLab) ─────────────────────────────────────

func gitlabCreds(refreshToken, webhookSecret string) json.RawMessage {
	b, _ := json.Marshal(glconfig.GitLabCredentials{
		OAuthRefreshToken: refreshToken,
		WebhookSecret:     webhookSecret,
	})
	return b
}

func gitlabSettings(groupID int64, hookID int64) json.RawMessage {
	b, _ := json.Marshal(glconfig.GitLabSettings{
		GroupID:       groupID,
		GroupPath:     "acme",
		WebhookHookID: hookID,
	})
	return b
}

func TestDisconnectGitLab_ClearsRedisAndCredentials(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	// Seed cached access token.
	_ = mr.Set(glconfig.AccessTokenKey(testOrgID), "cached-access-token")

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(&sdk.ConnectorConfig{
		ConnectorID:    "gitlab",
		OrganizationID: testOrgID,
		Enabled:        true,
		Credentials:    gitlabCreds("refresh-token", "webhook-secret"),
		Settings:       gitlabSettings(100, 0), // hookID=0 → no webhook to delete
	}, nil)
	// Expect Save with cleared credentials before Delete.
	dao.EXPECT().CompareAndSwapCredentials(gomock.Any(), testOrgID, gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, _ time.Time, creds json.RawMessage) (bool, error) {
			assert.JSONEq(t, "{}", string(creds))
			return true, nil
		})
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(true, nil)
	dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(nil)

	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
		OrgId: testOrgID, ConnectorId: "gitlab",
	})
	require.NoError(t, err)

	// Cached access token must be evicted from Redis.
	assert.False(t, mr.Exists(glconfig.AccessTokenKey(testOrgID)))
}

func TestDisconnectGitLab_WebhookAndTokenRevocation(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(&sdk.ConnectorConfig{
		ConnectorID:    "gitlab",
		OrganizationID: testOrgID,
		Enabled:        true,
		Credentials:    gitlabCreds("refresh-token", "webhook-secret"),
		Settings:       gitlabSettings(100, 777), // hookID=777 → webhook deletion attempted
	}, nil)
	dao.EXPECT().CompareAndSwapCredentials(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab"), gomock.Any(), json.RawMessage("{}")).Return(true, nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(true, nil)
	dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(nil)

	glClient := NewMockGitLabOAuthClient(ctrl)
	// Access token exchange for webhook deletion.
	glClient.EXPECT().ExchangeRefreshToken(gomock.Any(), "refresh-token").Return("new-access-token", nil)
	// Webhook deletion (best-effort).
	glClient.EXPECT().DeleteGroupWebhook(gomock.Any(), "new-access-token", int64(100), int64(777)).Return(nil)
	// Token revocation (best-effort).
	glClient.EXPECT().RevokeToken(gomock.Any(), "refresh-token").Return(nil)

	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, GLClient: glClient, Redis: rc, Registry: testRegistry()})

	_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
		OrgId: testOrgID, ConnectorId: "gitlab",
	})
	require.NoError(t, err)
}

func TestDisconnectGitLab_WebhookFails_StillSucceeds(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(&sdk.ConnectorConfig{
		ConnectorID:    "gitlab",
		OrganizationID: testOrgID,
		Enabled:        true,
		Credentials:    gitlabCreds("refresh-token", "webhook-secret"),
		Settings:       gitlabSettings(100, 777),
	}, nil)
	dao.EXPECT().CompareAndSwapCredentials(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab"), gomock.Any(), json.RawMessage("{}")).Return(true, nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(true, nil)
	dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(nil)

	glClient := NewMockGitLabOAuthClient(ctrl)
	glClient.EXPECT().ExchangeRefreshToken(gomock.Any(), "refresh-token").Return("new-access-token", nil)
	// Webhook deletion fails (best-effort — must not affect overall success).
	glClient.EXPECT().DeleteGroupWebhook(gomock.Any(), "new-access-token", int64(100), int64(777)).
		Return(fmt.Errorf("network timeout"))
	// Token revocation still attempted.
	glClient.EXPECT().RevokeToken(gomock.Any(), "refresh-token").Return(nil)

	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, GLClient: glClient, Redis: rc, Registry: testRegistry()})

	_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
		OrgId: testOrgID, ConnectorId: "gitlab",
	})
	require.NoError(t, err)
}

func TestDisconnectGitLab_TokenRevocationFails_StillSucceeds(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(&sdk.ConnectorConfig{
		ConnectorID:    "gitlab",
		OrganizationID: testOrgID,
		Enabled:        true,
		Credentials:    gitlabCreds("refresh-token", "webhook-secret"),
		Settings:       gitlabSettings(100, 0), // no webhook to delete
	}, nil)
	dao.EXPECT().CompareAndSwapCredentials(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab"), gomock.Any(), json.RawMessage("{}")).Return(true, nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(true, nil)
	dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(nil)

	glClient := NewMockGitLabOAuthClient(ctrl)
	// Token revocation fails (best-effort).
	glClient.EXPECT().RevokeToken(gomock.Any(), "refresh-token").Return(fmt.Errorf("revoke failed"))

	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, GLClient: glClient, Redis: rc, Registry: testRegistry()})

	_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
		OrgId: testOrgID, ConnectorId: "gitlab",
	})
	require.NoError(t, err)
}

func TestDisconnectGitLab_DeleteFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctrl := gomock.NewController(t)

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).Return(&sdk.ConnectorConfig{
		ConnectorID:    "gitlab",
		OrganizationID: testOrgID,
		Enabled:        true,
		Credentials:    gitlabCreds("refresh-token", ""),
		Settings:       gitlabSettings(100, 0),
	}, nil)
	dao.EXPECT().CompareAndSwapCredentials(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab"), gomock.Any(), json.RawMessage("{}")).Return(true, nil)
	// ResetHealthState must NOT be called — it only runs after a successful Delete.
	dao.EXPECT().Delete(gomock.Any(), testOrgID, sdk.ConnectorID("gitlab")).
		Return(fmt.Errorf("db connection lost"))

	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
		OrgId: testOrgID, ConnectorId: "gitlab",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}
