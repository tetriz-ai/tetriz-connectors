// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

//go:generate go tool mockgen -source=handler.go -destination=../../../internal/mocks/runner/schedule/connectorsource/mock_handler.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// OperationRunner is the interface satisfied by runner.Orchestrator.
// Sink and VendorGateway are domain concerns owned by the runner implementation,
// not the scheduling layer.
type OperationRunner interface {
	RunOperation(ctx context.Context, orgID string, connectorID sdk.ConnectorID,
		opID sdk.OperationID, tenantType sdk.TenantKind, trigger string) error
}

// ConnectorSyncHandler processes enqueued connector sync tasks.
// It deserializes the payload and delegates to the OperationRunner.
type ConnectorSyncHandler struct {
	runner OperationRunner
}

// NewConnectorSyncHandler creates a new ConnectorSyncHandler.
func NewConnectorSyncHandler(runner OperationRunner) *ConnectorSyncHandler {
	return &ConnectorSyncHandler{
		runner: runner,
	}
}

// ProcessTask implements asynq.Handler.
//
// Sets org_id in context for RLS tenant scoping but does NOT wrap RunOperation
// in a single transaction — the orchestrator manages its own short-lived scopes
// so vendor API calls don't hold a DB transaction open.
func (h *ConnectorSyncHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var payload ConnectorSyncPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		// The payload is fixed in the task, so no retry can decode it: fail
		// once rather than burn MaxRetry on the same bytes.
		logger.Error(ctx, "failed to unmarshal payload; skipping retry", zap.String("task_type", t.Type()), zap.Error(err))
		return fmt.Errorf("scheduleradapter: unmarshal payload: %w", errors.Join(err, asynq.SkipRetry))
	}

	ctx = reqctx.WithOrgID(ctx, payload.OrgID)
	if err := h.runner.RunOperation(ctx, payload.OrgID, payload.ConnectorID, payload.OperationID, payload.TenantType, "scheduled"); err != nil {
		logger.Error(ctx, "failed to run connector operation",
			zap.String(logger.OrgID, payload.OrgID),
			zap.String(logger.ConnectorID, string(payload.ConnectorID)),
			zap.String(logger.OperationID, string(payload.OperationID)),
			zap.Error(err))
		// A Terminal SyncError means re-running this exact task cannot succeed —
		// the fault is in the payload. Join asynq.SkipRetry so asynq fails it once
		// instead of stamping duplicate failed SyncRun rows across MaxRetry. Keyed
		// on the explicit Terminal flag, not Kind/Retryable, so the many transient
		// ErrKindConfigInvalid paths connectors raise keep asynq's retry.
		var syncErr *sdk.SyncError
		if errors.As(err, &syncErr) && syncErr.Terminal {
			// Distinguishing signal for the SkipRetry fork: without it neither the
			// error log above nor failRun's error_kind metric tells ops whether a
			// run was SkipRetried or is still burning MaxRetry (a Terminal and a
			// non-terminal auth error carry the same Kind). Searchable so an operator
			// can answer "did this dead-credential run stop retrying?".
			logger.WarnWithCtx(ctx, "connector sync terminal failure: skipping retry until next scheduled run",
				zap.String(logger.OrgID, payload.OrgID),
				zap.String(logger.ConnectorID, string(payload.ConnectorID)),
				zap.String(logger.OperationID, string(payload.OperationID)),
				zap.String(logger.ErrorKind, syncErr.Kind.String()))
			return fmt.Errorf("running connector operation: %w", errors.Join(err, asynq.SkipRetry))
		}
		return fmt.Errorf("running connector operation: %w", err)
	}
	return nil
}
