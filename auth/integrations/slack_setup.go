// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// ValidateAndSaveSlackConnection is the Slack OAuth callback of the slack_comms
// connector: it validates the CSRF state, then exchanges the code for a bot
// token and saves the grant (saveSlackCommsConnection).
func (s *Service) ValidateAndSaveSlackConnection(ctx context.Context, req *connintv1.ValidateAndSaveSlackConnectionRequest) (*connintv1.ValidateAndSaveSlackConnectionResponse, error) {
	code := req.GetCode()
	orgID := orgid.Canonical(req.GetOrgId())
	stateToken := req.GetState()

	if code == "" {
		return nil, status.Error(codes.InvalidArgument, "missing code")
	}
	if orgID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing org_id")
	}
	if stateToken == "" {
		return nil, status.Error(codes.InvalidArgument, "missing state")
	}

	// 1. Validate CSRF state.
	key := slackOAuthStateKey(stateToken)
	// GETDEL reads and consumes the single-use state atomically, so two
	// concurrent callbacks cannot both pass.
	storedJSON, err := s.redis.GetDel(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		logger.Error(ctx, "ValidateAndSaveSlackConnection: invalid or expired state",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, "invalid or expired state token")
	}
	if err != nil {
		logger.Error(ctx, "ValidateAndSaveSlackConnection: redis error reading state",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "read state: %v", err)
	}

	var stateData slackOAuthState
	if err := json.Unmarshal([]byte(storedJSON), &stateData); err != nil {
		return nil, status.Errorf(codes.Internal, "parse state data: %v", err)
	}
	if stateData.OrgID != orgID {
		logger.Error(ctx, "ValidateAndSaveSlackConnection: state org_id mismatch",
			zap.String("expected", stateData.OrgID), zap.String("got", orgID))
		return nil, status.Error(codes.InvalidArgument, "state org_id mismatch")
	}

	// Only the notifications app (bot token) connects through Slack OAuth.
	if stateData.ConnectorID != connectorIDSlackComms {
		return nil, status.Error(codes.InvalidArgument, "state is not for a "+connectorIDSlackComms+" connection")
	}
	if err := s.saveSlackCommsConnection(ctx, orgID, code); err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	return &connintv1.ValidateAndSaveSlackConnectionResponse{ConnectorId: connectorIDSlackComms}, nil
}
