// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	intslack "github.com/tetriz-ai/tetriz-connectors/auth/integrations/slack"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorIDSlackComms is the comms connector's id: a Slack grant that holds a
// bot token for posting, which the secret broker's GetSecret resolves.
const connectorIDSlackComms = "slack_comms"

// slackOAuthStateKey returns the Redis key for a Slack CSRF state token.
func slackOAuthStateKey(state string) string {
	return "oauth_state:slack:" + state
}

// slackOAuthState is the CSRF state payload stored in Redis during a Slack
// connect. ConnectorID must be connectorIDSlackComms; the
// /integrations/slack/setup callback refuses any other.
type slackOAuthState struct {
	OrgID       string `json:"org_id"`
	ConnectorID string `json:"connector_id,omitempty"`
}

// connectSlackComms starts the OAuth connect for the notifications app: it mints
// a CSRF state (carrying the connector id so the shared /integrations/slack/setup
// callback can route the exchange), stores it in Redis, and returns the Slack
// bot-scope authorize URL built from the SlackCommsApp config.
func (s *Service) connectSlackComms(ctx context.Context, req *connintv1.ConnectIntegrationRequest) (*connintv1.ConnectIntegrationResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if orgID == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id is required")
	}
	app := s.cfg.SlackCommsApp
	if app == nil || app.ClientID == "" {
		return nil, status.Error(codes.FailedPrecondition, connectorIDSlackComms+" not configured: set SlackCommsApp + SlackCommsAppClientSecret")
	}

	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		logger.Error(ctx, "connectSlackComms: generate state", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "generate state: %v", err)
	}
	state := hex.EncodeToString(stateBytes)

	stateData, err := json.Marshal(slackOAuthState{OrgID: orgID, ConnectorID: connectorIDSlackComms})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal state data: %v", err)
	}
	if err := s.redis.Set(ctx, slackOAuthStateKey(state), stateData, oauthStateTTL).Err(); err != nil {
		logger.Error(ctx, "connectSlackComms: store state", zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "store state: %v", err)
	}

	// Bot-token flow uses scope= (not user_scope=); the app requests bot scopes
	// (chat:write, files:write, …) defined in the SlackCommsApp manifest/config.
	redirectURL := fmt.Sprintf(
		"https://slack.com/oauth/v2/authorize?client_id=%s&scope=%s&redirect_uri=%s&state=%s",
		url.QueryEscape(app.ClientID),
		url.QueryEscape(app.Scopes),
		url.QueryEscape(app.RedirectURI),
		url.QueryEscape(state),
	)
	logger.Info(ctx, connectorIDSlackComms+" setup: authorize URL generated", zap.String(logger.OrgID, orgID))
	return &connintv1.ConnectIntegrationResponse{RedirectUrl: redirectURL}, nil
}

// slackCommsClient builds the notifications-app OAuth client on demand from
// config (no wire dependency — the connect flow is the only caller). Returns nil
// when unconfigured (local/dev without the secret), matching NewSlackAppClient.
func (s *Service) slackCommsClient() intslack.SlackAppClient {
	app := s.cfg.SlackCommsApp
	if app == nil {
		return nil
	}
	return intslack.NewSlackAppClient(&intslack.SlackAppConfig{
		ClientID:     app.ClientID,
		ClientSecret: s.cfg.SlackCommsAppClientSecret,
		RedirectURI:  app.RedirectURI,
		Scopes:       app.Scopes,
		APIBaseURL:   app.APIBaseURL,
	})
}

// saveSlackCommsConnection completes the comms connector's OAuth callback: it
// exchanges the code for a BOT token, verifies the authorizing user is a
// workspace admin, and persists the grant under connectorIDSlackComms with the
// bot token under "bot_token" (the key the secret broker reads). Called from
// the shared ValidateAndSaveSlackConnection callback when the state names the
// comms connector.
func (s *Service) saveSlackCommsConnection(ctx context.Context, orgID, code string) error {
	if !s.connectorEnabled(connectorIDSlackComms) {
		return status.Error(codes.FailedPrecondition, connectorIDSlackComms+" integration is not enabled")
	}
	client := s.slackCommsClient()
	if client == nil {
		return status.Error(codes.FailedPrecondition, connectorIDSlackComms+" not configured: set SlackCommsAppClientSecret")
	}

	oauthResp, err := client.ExchangeOAuthCode(ctx, code)
	if err != nil {
		logger.Error(ctx, "saveSlackCommsConnection: exchange failed", zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "exchange oauth code: %v", err)
	}
	if oauthResp.BotToken == "" {
		return status.Error(codes.Internal, "slack did not return a bot token")
	}
	// Verify the installing user is a Slack workspace admin. This is a bot-only
	// install (scope=, no user_scope), so Slack returns no user token — the check
	// runs against the BOT token + the installer's user id (authed_user.id, which
	// Slack does return), which is why the app requests the users:read bot scope.
	// Fail closed: if the installer id is absent or the lookup fails, reject rather
	// than silently connect.
	if oauthResp.UserID == "" {
		logger.Error(ctx, "saveSlackCommsConnection: slack omitted installing user id; cannot verify workspace admin",
			zap.String(logger.OrgID, orgID))
		return status.Error(codes.FailedPrecondition, "could not verify Slack workspace admin")
	}
	userInfo, uerr := client.GetUserInfo(ctx, oauthResp.BotToken, oauthResp.UserID)
	if uerr != nil {
		logger.Error(ctx, "saveSlackCommsConnection: fetch user info", zap.String(logger.OrgID, orgID), zap.Error(uerr))
		return status.Errorf(codes.Internal, "fetch user info: %v", uerr)
	}
	if !userInfo.IsAdmin {
		logger.Info(ctx, "saveSlackCommsConnection: rejected non-admin installer",
			zap.String(logger.OrgID, orgID), zap.String(logger.UserID, oauthResp.UserID))
		return status.Error(codes.FailedPrecondition, "you are not a Slack workspace admin — ask your admin to connect notifications")
	}

	creds, err := json.Marshal(map[string]string{
		"bot_token": oauthResp.BotToken,
		"team_id":   oauthResp.TeamID,
		"scope":     oauthResp.BotScope,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "marshal creds: %v", err)
	}
	settings, err := json.Marshal(map[string]string{"team_name": oauthResp.TeamName})
	if err != nil {
		return status.Errorf(codes.Internal, "marshal settings: %v", err)
	}

	cfg := &sdk.ConnectorConfig{
		ConnectorID:    connectorIDSlackComms,
		OrganizationID: orgID,
		Credentials:    creds,
		Settings:       settings,
		Enabled:        true,
	}
	// A reconnect over a live row keeps every settings key this flow does not
	// produce, and the operation overrides.
	if err := s.connectorDAO.SaveCredentialsEnableWithSettings(ctx, cfg, mergeProducedSettings(settings)); err != nil {
		logger.Error(ctx, "saveSlackCommsConnection: save grant", zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "save connector config: %v", err)
	}
	s.resetConnectorHealth(ctx, orgID, connectorIDSlackComms)
	logger.Info(ctx, connectorIDSlackComms+" connection saved", zap.String(logger.OrgID, orgID), zap.String("team", oauthResp.TeamName))
	return nil
}
