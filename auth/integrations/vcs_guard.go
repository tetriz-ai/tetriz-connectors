// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// isVCSConnector reports whether the given connector ID belongs to a VCS connector
// by looking it up in the registry and checking its category.
func (s *Service) isVCSConnector(id sdk.ConnectorID) bool {
	c, err := s.registry.Get(id)
	if err != nil {
		// Unregistered ID — by definition not a VCS connector. Expected for any
		// non-connector or non-VCS id this predicate is asked about.
		logger.InfoNoCtx("isVCSConnector: connector not in registry", zap.String(logger.ConnectorID, string(id)), zap.Error(err))
		return false
	}
	return sdk.HasCategory(c, sdk.CategoryVCS)
}

// checkSingleVCSGuard verifies that no other VCS connector is already enabled
// for the given org before allowing a new VCS connector to be activated.
// Non-VCS connectors pass through without any check.
func (s *Service) checkSingleVCSGuard(ctx context.Context, orgID string, newConnectorID sdk.ConnectorID) error {
	if !s.isVCSConnector(newConnectorID) {
		return nil
	}

	configs, err := s.connectorDAO.ListByOrg(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "checkSingleVCSGuard: failed to list org connectors",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		// Return a gRPC status (not a plain wrapped error) so every checkSingleVCSGuard
		// failure is a status the caller passes through unchanged — see the
		// status-pass-through nolint at its call sites.
		return status.Errorf(codes.Internal, "list org connectors: %v", err)
	}

	for _, cfg := range configs {
		if cfg.Enabled && s.isVCSConnector(cfg.ConnectorID) && cfg.ConnectorID != newConnectorID {
			return status.Errorf(codes.FailedPrecondition,
				"VCS connector %s is already active; disconnect it before enabling %s",
				cfg.ConnectorID, newConnectorID)
		}
	}
	return nil
}
