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

	intlinear "github.com/tetriz-ai/tetriz-connectors/auth/integrations/linear"
	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intlinearmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/linear"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const testLinearStateToken = "test-linear-state-abcdef5678"

func testLinearConfig() *connectorconfig.Config {
	cfg := testConfig()
	cfg.EnabledConnectors["linear"] = true
	cfg.LinearOAuth = &connectorconfig.LinearOAuthConfig{
		ClientID:    "test-linear-client-id",
		CallbackURL: "http://localhost:9998/integrations/linear/callback",
	}
	return cfg
}

func seedLinearState(t *testing.T, rc *redis.Client, state, orgID string) {
	t.Helper()
	data, err := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: orgID})
	require.NoError(t, err)
	require.NoError(t, rc.Set(context.Background(), linearconfig.OAuthStateKey(state), data, time.Minute).Err())
}

func TestValidateAndSaveLinearConnection_Disabled(t *testing.T) {
	// testLinearConfig() has linear: true; override to false to test the off-switch.
	cfg := testLinearConfig()
	cfg.EnabledConnectors["linear"] = false
	svc := NewService(Deps{Cfg: cfg, Registry: testRegistry()})

	_, err := svc.ValidateAndSaveLinearConnection(context.Background(), &connintv1.ValidateAndSaveLinearConnectionRequest{
		OrgId: testOrgID,
		Code:  "linear-code-123",
		State: testLinearStateToken,
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestValidateAndSaveLinearConnection(t *testing.T) {
	workspaceOK := &intlinear.WorkspaceInfo{
		ID:     "ws-uuid-abc",
		Name:   "Acme Corp",
		URLKey: "acme",
	}
	tokenOK := &intlinear.TokenResponse{
		AccessToken:  "lin_api_access_token",
		RefreshToken: "lin_api_refresh_token",
		TokenType:    "Bearer",
		Scope:        "read,admin",
	}

	tests := []struct {
		name          string
		orgID         string
		code          string
		state         string
		seedOrg       string // orgID stored in Redis for this state; empty = not seeded
		tokenResp     *intlinear.TokenResponse
		tokenErr      error
		workspaceResp *intlinear.WorkspaceInfo
		workspaceErr  error
		saveErr       error
		wantErr       bool
		wantCode      codes.Code
	}{
		{
			name:          "HappyPath",
			orgID:         testOrgID,
			code:          "linear-code-123",
			state:         testLinearStateToken,
			seedOrg:       testOrgID,
			tokenResp:     tokenOK,
			workspaceResp: workspaceOK,
			wantErr:       false,
		},
		{
			name:     "MissingOrgID",
			orgID:    "",
			code:     "linear-code-123",
			state:    testLinearStateToken,
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "MissingCode",
			orgID:    testOrgID,
			code:     "",
			state:    testLinearStateToken,
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "MissingState",
			orgID:    testOrgID,
			code:     "linear-code-123",
			state:    "",
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "InvalidState_NotInRedis",
			orgID:    testOrgID,
			code:     "linear-code-123",
			state:    testLinearStateToken,
			seedOrg:  "", // not seeded
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "InvalidState_OrgMismatch",
			orgID:    testOrgID,
			code:     "linear-code-123",
			state:    testLinearStateToken,
			seedOrg:  "other-org-id",
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "TokenExchangeFails",
			orgID:    testOrgID,
			code:     "bad-code",
			state:    testLinearStateToken,
			seedOrg:  testOrgID,
			tokenErr: fmt.Errorf("oauth error"),
			wantErr:  true,
			wantCode: codes.Internal,
		},
		{
			name:         "FetchWorkspaceFails",
			orgID:        testOrgID,
			code:         "linear-code-123",
			state:        testLinearStateToken,
			seedOrg:      testOrgID,
			tokenResp:    tokenOK,
			workspaceErr: fmt.Errorf("graphql error"),
			wantErr:      true,
			wantCode:     codes.Internal,
		},
		{
			name:          "SaveFails",
			orgID:         testOrgID,
			code:          "linear-code-123",
			state:         testLinearStateToken,
			seedOrg:       testOrgID,
			tokenResp:     tokenOK,
			workspaceResp: workspaceOK,
			saveErr:       fmt.Errorf("db connection lost"),
			wantErr:       true,
			wantCode:      codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mr := miniredis.RunT(t)
			rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

			if tt.seedOrg != "" {
				seedLinearState(t, rc, tt.state, tt.seedOrg)
			}

			linearClient := intlinearmocks.NewMockOAuthClient(ctrl)
			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)

			stateValid := tt.seedOrg == testOrgID
			codeStateOK := tt.code != "" && tt.orgID != "" && tt.state != "" && stateValid

			if codeStateOK {
				linearClient.EXPECT().ExchangeAuthCode(gomock.Any(), tt.code).
					Return(tt.tokenResp, tt.tokenErr)
			}

			if codeStateOK && tt.tokenErr == nil {
				linearClient.EXPECT().FetchWorkspaceInfo(gomock.Any(), tt.tokenResp.AccessToken).
					Return(tt.workspaceResp, tt.workspaceErr)
				// FetchCurrentUser is best-effort; always called when token exchange succeeds.
				linearClient.EXPECT().FetchCurrentUser(gomock.Any(), tt.tokenResp.AccessToken).
					Return("", fmt.Errorf("skipped")).AnyTimes()
			}

			if codeStateOK && tt.tokenErr == nil && tt.workspaceErr == nil {
				dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.saveErr)
				if tt.saveErr == nil {
					dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("linear")).Return(true, nil)
				}
			}

			svc := NewService(Deps{Cfg: testLinearConfig(), ConnectorDAO: dao, LinearClient: linearClient, Redis: rc, Registry: testRegistry()})

			_, err := svc.ValidateAndSaveLinearConnection(context.Background(), &connintv1.ValidateAndSaveLinearConnectionRequest{
				OrgId: tt.orgID,
				Code:  tt.code,
				State: tt.state,
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

// TestValidateAndSaveLinearConnection_StateConsumedAfterUse asserts the CSRF
// state token is deleted from Redis after a successful OAuth flow so it cannot
// be replayed.
func TestValidateAndSaveLinearConnection_StateConsumedAfterUse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	seedLinearState(t, rc, testLinearStateToken, testOrgID)

	linearClient := intlinearmocks.NewMockOAuthClient(ctrl)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)

	tokenResp := &intlinear.TokenResponse{AccessToken: "lin_api_abc", RefreshToken: "lin_ref_abc"}
	workspaceInfo := &intlinear.WorkspaceInfo{ID: "ws-111", Name: "Acme", URLKey: "acme"}

	linearClient.EXPECT().ExchangeAuthCode(gomock.Any(), "code-abc").Return(tokenResp, nil)
	linearClient.EXPECT().FetchWorkspaceInfo(gomock.Any(), tokenResp.AccessToken).Return(workspaceInfo, nil)
	linearClient.EXPECT().FetchCurrentUser(gomock.Any(), tokenResp.AccessToken).Return("Alice", nil)
	dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("linear")).Return(true, nil)

	svc := NewService(Deps{Cfg: testLinearConfig(), ConnectorDAO: dao, LinearClient: linearClient, Redis: rc, Registry: testRegistry()})
	_, err := svc.ValidateAndSaveLinearConnection(context.Background(), &connintv1.ValidateAndSaveLinearConnectionRequest{
		OrgId: testOrgID, Code: "code-abc", State: testLinearStateToken,
	})
	require.NoError(t, err)

	exists := rc.Exists(context.Background(), linearconfig.OAuthStateKey(testLinearStateToken)).Val()
	assert.Equal(t, int64(0), exists, "state token must be consumed after use to prevent replay")
}
