// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"net/http"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// HandleGitLabWebhook verifies the X-Gitlab-Token against the stored webhook
// secret for the matching org config, then emits the event as a RawRecord via
// the connector framework WebhookOperation.
func (s *Service) HandleGitLabWebhook(ctx context.Context, req *connintv1.HandleGitLabWebhookRequest) (*emptypb.Empty, error) {
	tokenHeader := req.GetTokenHeader()
	eventType := req.GetEventType()

	if tokenHeader == "" {
		logger.Error(ctx, "gitlab webhook: missing X-Gitlab-Token header")
		return nil, status.Error(codes.PermissionDenied, "missing X-Gitlab-Token header")
	}

	// List all GitLab connector configs and find the one whose webhook secret
	// matches the token. GitLab webhooks do not carry an org identifier, so
	// we perform a linear scan across all active configs.
	// A row that does not decrypt is skipped and logged, and the rest are
	// still matched: one tenant's row cannot fail another tenant's hook.
	configs, err := s.connectorDAO.ListByConnector(ctx, sdk.ConnectorID(connectorIDGitLab))
	var undecryptable *store.UndecryptableRowsError
	switch {
	case errors.As(err, &undecryptable):
		for _, org := range undecryptable.OrgIDs {
			logger.Error(ctx, "gitlab webhook: skipping gitlab config whose credentials do not decrypt",
				zap.String(logger.OrgID, org), zap.Error(undecryptable.Err))
		}
		logger.Error(ctx, "gitlab webhook: skipped gitlab configs that do not decrypt", zap.Int("skipped", len(undecryptable.OrgIDs)))
	case err != nil:
		logger.Error(ctx, "gitlab webhook: failed to list gitlab configs", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to list gitlab configs")
	}

	var matchedConfig *sdk.ConnectorConfig
	for _, cfg := range configs {
		var creds glconfig.GitLabCredentials
		if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
			logger.WarnWithCtx(ctx, "gitlab webhook: skipping config with unparseable credentials",
				zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
			continue
		}
		if creds.WebhookSecret == "" {
			continue
		}
		// Constant-time comparison to prevent timing attacks.
		if hmac.Equal([]byte(creds.WebhookSecret), []byte(tokenHeader)) {
			matchedConfig = cfg
			break
		}
	}

	if matchedConfig == nil {
		logger.Error(ctx, "gitlab webhook: no matching config for token",
			zap.String("event_type", eventType))
		return nil, status.Error(codes.PermissionDenied, "invalid X-Gitlab-Token")
	}

	// Build headers map so WebhookEventsOp can read X-Gitlab-Event and X-Gitlab-Event-UUID.
	// A disabled connector (paused, suspended, awaiting confirmation) ingests
	// nothing. The delivery is still authentic, so it is acknowledged rather
	// than refused: a refusal would count toward GitLab auto-disabling the hook.
	if !matchedConfig.Enabled {
		logger.Info(ctx, "gitlab webhook: connector disabled, dropping delivery",
			zap.String(logger.OrgID, matchedConfig.OrganizationID),
			zap.String("event_type", eventType))
		return &emptypb.Empty{}, nil
	}

	headers := make(http.Header)
	headers.Set("X-Gitlab-Token", tokenHeader)
	headers.Set("X-Gitlab-Event", eventType)
	headers.Set("X-Gitlab-Event-UUID", req.GetEventUuid())

	op := glconfig.WebhookEventsOp{}
	result, err := op.HandleWebhook(ctx, &sdk.WebhookParams{
		Config:  matchedConfig,
		Headers: headers,
		Body:    req.GetBody(),
		Sink:    s.sink,
	})
	if err != nil {
		logger.Error(ctx, "gitlab webhook: emit failed",
			zap.String(logger.OrgID, matchedConfig.OrganizationID),
			zap.String("event_type", eventType),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "webhook emit: %v", err)
	}

	logger.Info(ctx, "gitlab webhook: processed",
		zap.String(logger.OrgID, matchedConfig.OrganizationID),
		zap.String("event_type", eventType),
		zap.Bool("acknowledged", result.Acknowledged))

	return &emptypb.Empty{}, nil
}
