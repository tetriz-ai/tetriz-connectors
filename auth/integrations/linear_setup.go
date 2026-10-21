// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	intlinear "github.com/tetriz-ai/tetriz-connectors/auth/integrations/linear"
	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	linearAuthorizeURL = "https://linear.app/oauth/authorize"
	// linearOAuthScopes requests read-only access. V1 ingests Linear data via
	// polling only; without webhookCreate there is no need for write/admin, so a
	// non-admin can complete the OAuth flow.
	linearOAuthScopes   = "read"
	linearOAuthStateTTL = linearconfig.OAuthStateTTL
)

// connectLinear generates a CSRF state token, stores it in Redis, and
// returns the Linear OAuth authorize URL.
func (s *Service) connectLinear(ctx context.Context, req *connintv1.ConnectIntegrationRequest) (*connintv1.ConnectIntegrationResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if orgID == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id is required")
	}
	if s.cfg.LinearOAuth == nil {
		return nil, status.Error(codes.FailedPrecondition, "linear oauth not configured")
	}
	if s.linearClient == nil {
		return nil, status.Error(codes.FailedPrecondition, "linear oauth client not configured")
	}

	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		logger.Error(ctx, "connectLinear: failed to generate state", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "generate state: %v", err)
	}
	state := hex.EncodeToString(stateBytes)

	stateData, err := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: orgID})
	if err != nil {
		logger.Error(ctx, "connectLinear: failed to marshal state data",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal state data: %v", err)
	}

	if err := s.redis.Set(ctx, linearconfig.OAuthStateKey(state), stateData, linearOAuthStateTTL).Err(); err != nil {
		logger.Error(ctx, "connectLinear: failed to store state in Redis",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "store state: %v", err)
	}

	params := url.Values{}
	params.Set("client_id", s.cfg.LinearOAuth.ClientID)
	params.Set("redirect_uri", s.cfg.LinearOAuth.CallbackURL)
	params.Set("state", state)
	params.Set("response_type", "code")
	params.Set("scope", linearOAuthScopes)
	authorizeURL := linearAuthorizeURL + "?" + params.Encode()

	logger.Info(ctx, "linear setup: authorize URL generated", zap.String(logger.OrgID, orgID))
	return &connintv1.ConnectIntegrationResponse{RedirectUrl: authorizeURL}, nil
}

// ValidateAndSaveLinearConnection validates the CSRF state, exchanges the
// authorization code for an access token, fetches the Linear workspace, and
// persists the connector config. Data is ingested via polling; no webhook is
// registered.
func (s *Service) ValidateAndSaveLinearConnection(ctx context.Context, req *connintv1.ValidateAndSaveLinearConnectionRequest) (*connintv1.ValidateAndSaveLinearConnectionResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	authCode := req.GetCode()
	stateToken := req.GetState()

	if orgID == "" || authCode == "" || stateToken == "" {
		logger.Error(ctx, "ValidateAndSaveLinearConnection: missing required fields",
			zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.InvalidArgument, "org_id, code, and state are required")
	}
	if !s.connectorEnabled(connectorIDLinear) {
		logger.Error(ctx, "ValidateAndSaveLinearConnection: linear connector not enabled",
			zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.FailedPrecondition, "linear connector is not enabled")
	}
	if s.cfg.LinearOAuth == nil {
		logger.Error(ctx, "ValidateAndSaveLinearConnection: linear oauth not configured",
			zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.FailedPrecondition, "linear oauth not configured")
	}
	if s.linearClient == nil {
		logger.Error(ctx, "ValidateAndSaveLinearConnection: linear oauth client not configured",
			zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.FailedPrecondition, "linear oauth client not configured")
	}

	// Validate CSRF state (single-use).
	key := linearconfig.OAuthStateKey(stateToken)
	// GETDEL reads and consumes the single-use state atomically, so two
	// concurrent callbacks cannot both pass, and a mismatched org burns it.
	storedJSON, err := s.redis.GetDel(ctx, key).Result()
	if err != nil {
		logger.Error(ctx, "linear setup: invalid or expired state",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, "invalid or expired state")
	}
	var stateData struct {
		OrgID string `json:"org_id"`
	}
	if err := json.Unmarshal([]byte(storedJSON), &stateData); err != nil {
		logger.Error(ctx, "linear setup: failed to decode stored state",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.Internal, "decode state data")
	}
	if stateData.OrgID != orgID {
		logger.Error(ctx, "linear setup: state org_id mismatch",
			zap.String("expected", stateData.OrgID), zap.String("got", orgID))
		return nil, status.Error(codes.InvalidArgument, "state mismatch")
	}

	// Exchange auth code for access token.
	tokenResp, err := s.linearClient.ExchangeAuthCode(ctx, authCode)
	if err != nil {
		logger.Error(ctx, "linear setup: token exchange failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "token exchange: %v", err)
	}
	// Require a refresh token: Linear user-actor access tokens expire ~24h, so a
	// connection without a refresh token cannot sustain sync (every run would fail
	// auth after the first day). Reject the connect rather than persist a config
	// that looks connected but can never refresh. Since the Apr-2026 OAuth
	// migration Linear always issues one; an empty value signals a misconfigured
	// OAuth app or an unexpected vendor response.
	if tokenResp.RefreshToken == "" {
		logger.Error(ctx, "linear setup: token exchange returned no refresh token",
			zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.FailedPrecondition,
			"linear did not return a refresh token; reconnect or check the OAuth app configuration")
	}

	// Fetch workspace info.
	workspace, err := s.linearClient.FetchWorkspaceInfo(ctx, tokenResp.AccessToken)
	if err != nil {
		logger.Error(ctx, "linear setup: fetch workspace info failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "fetch workspace info: %v", err)
	}

	// Fetch connecting user's display name (best-effort).
	connectedByName := ""
	if name, fetchErr := s.linearClient.FetchCurrentUser(ctx, tokenResp.AccessToken); fetchErr != nil {
		logger.Error(ctx, "linear setup: failed to fetch current user (non-fatal)",
			zap.String(logger.OrgID, orgID), zap.Error(fetchErr))
	} else {
		connectedByName = name
	}

	if err := s.saveLinearConfig(ctx, orgID, tokenResp.AccessToken, tokenResp.RefreshToken, workspace, connectedByName); err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	return &connintv1.ValidateAndSaveLinearConnectionResponse{}, nil
}

// saveLinearConfig persists a Linear connector config.
func (s *Service) saveLinearConfig(ctx context.Context, orgID, accessToken, refreshToken string, workspace *intlinear.WorkspaceInfo, connectedByName string) error {
	creds, err := json.Marshal(linearconfig.LinearCredentials{ //nolint:gosec // G117: credentials are persisted encrypted at rest; marshaling the token is intentional
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	})
	if err != nil {
		logger.Error(ctx, "saveLinearConfig: failed to marshal credentials",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}

	settings, err := json.Marshal(linearconfig.LinearSettings{
		WorkspaceID:     workspace.ID,
		OrgSlug:         workspace.URLKey,
		ConnectedByName: connectedByName,
	})
	if err != nil {
		logger.Error(ctx, "saveLinearConfig: failed to marshal settings",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "marshal settings: %v", err)
	}

	connCfg := &sdk.ConnectorConfig{
		ConnectorID:    connectorIDLinear,
		OrganizationID: orgID,
		Credentials:    creds,
		Settings:       settings,
		Enabled:        true,
	}
	// A reconnect over a live row keeps every settings key this flow does not
	// produce, and the operation overrides.
	if err := s.connectorDAO.SaveCredentialsEnableWithSettings(ctx, connCfg, mergeProducedSettings(settings)); err != nil {
		logger.Error(ctx, "linear setup: failed to save connector config",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "save connector config: %v", err)
	}
	s.resetConnectorHealth(ctx, orgID, connectorIDLinear)

	logger.Info(ctx, "linear setup: connection saved",
		zap.String(logger.OrgID, orgID),
		zap.String("workspace_id", workspace.ID),
		zap.String("org_slug", workspace.URLKey))

	return nil
}
