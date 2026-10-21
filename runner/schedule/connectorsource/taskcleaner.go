// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"context"
	"errors"
	"fmt"

	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// SyncTaskCleaner removes stuck asynq tasks for a connector's operations.
// When a connector is disconnected, any tasks stuck in the retry queue block
// new enqueues (asynq.TaskID uniqueness). This cleaner removes them so a
// reconnect can start fresh immediately.
type SyncTaskCleaner struct {
	inspector *asynq.Inspector
	registry  *sdk.ConnectorRegistry
	namespace string
}

// NewSyncTaskCleaner creates a new SyncTaskCleaner.
func NewSyncTaskCleaner(inspector *asynq.Inspector, registry *sdk.ConnectorRegistry) *SyncTaskCleaner {
	return &SyncTaskCleaner{inspector: inspector, registry: registry}
}

// WithNamespace makes the cleaner delete from the connector lanes of
// namespace ns, the schedule.Options.Namespace of the scheduler that
// enqueued the tasks.
func (c *SyncTaskCleaner) WithNamespace(ns string) *SyncTaskCleaner {
	c.namespace = ns
	return c
}

// DeleteTasksForConnector removes all queued tasks (pending, retry, scheduled)
// for the given connector's pull operations. Errors on individual deletions are
// logged but do not stop the cleanup — best-effort.
func (c *SyncTaskCleaner) DeleteTasksForConnector(ctx context.Context, orgID string, connectorID sdk.ConnectorID) {
	conn, err := c.registry.Get(connectorID)
	if err != nil {
		logger.Debug(ctx, "taskcleaner: connector not in registry, skipping queue cleanup",
			zap.String(logger.ConnectorID, string(connectorID)),
			zap.Error(err),
		)
		return
	}

	for _, op := range conn.Operations() {
		if op.IngestionType != sdk.IngestionPull {
			continue
		}
		taskID := fmt.Sprintf("%s/%s/%s", orgID, connectorID, op.ID)
		// A connector.sync for the same (org, connector, op) lands in the org lane
		// or the low-weight individual lane depending on the tenant's live type,
		// and a tenant can flip type between enqueue and disconnect. Delete from
		// BOTH connector lanes unconditionally rather than re-derive the lane from
		// a possibly-stale individual set — a stale lookup would miss the lane the
		// task actually landed in and leave a stuck task that blocks reconnect via
		// the same TaskID. The task lives in at most one lane, so the other delete
		// is an expected not-found no-op.
		for _, queue := range []string{
			schedule.Namespaced(c.namespace, schedule.QueueConnectorSync),
			schedule.Namespaced(c.namespace, schedule.QueueIndividualConnectorSync),
		} {
			delErr := c.inspector.DeleteTask(queue, taskID)
			switch {
			case delErr == nil:
				logger.Info(ctx, "taskcleaner: deleted stuck task from queue",
					zap.String("queue", queue),
					zap.String("task_id", taskID),
				)
			case errors.Is(delErr, asynq.ErrTaskNotFound), errors.Is(delErr, asynq.ErrQueueNotFound):
				// Expected: the task already completed, was never enqueued, or lives
				// in the other lane. One of the two lanes always reports this — not an
				// error. Logging it at Error would fire on every disconnect.
				logger.Debug(ctx, "taskcleaner: no stuck task in lane (expected)",
					zap.String("queue", queue),
					zap.String("task_id", taskID),
				)
			default:
				logger.Error(ctx, "taskcleaner: could not delete task",
					zap.String("queue", queue),
					zap.String("task_id", taskID),
					zap.Error(delErr),
				)
			}
		}
	}
}
