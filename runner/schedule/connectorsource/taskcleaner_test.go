// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// queuePending counts pending tasks in a queue, treating a not-yet-created
// queue as zero — the strongest form of "absent".
func queuePending(t *testing.T, inspector *asynq.Inspector, queue string) int {
	t.Helper()
	tasks, err := inspector.ListPendingTasks(queue)
	if err != nil {
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return 0
		}
		require.NoError(t, err)
	}
	return len(tasks)
}

// TestDeleteTasksForConnector_BothConnectorLanes asserts the disconnect cleaner
// removes a connector's stuck sync task from BOTH connector lanes. A connector.sync
// for an individual tenant lives in the individual lane, so cleaning only the
// standard lane would leave the task stuck and block reconnect via the same TaskID.
func TestDeleteTasksForConnector_BothConnectorLanes(t *testing.T) {
	t.Parallel()
	const (
		orgID       = "org-1"
		connectorID = sdk.ConnectorID("github")
		opID        = sdk.OperationID("fetch_prs")
	)
	taskID := fmt.Sprintf("%s/%s/%s", orgID, connectorID, opID)

	tests := []struct {
		name          string
		enqueueQueues []string // lanes a stuck task pre-exists in
	}{
		{
			name:          "task in standard lane is deleted",
			enqueueQueues: []string{schedule.QueueConnectorSync},
		},
		{
			name:          "task in individual lane is deleted",
			enqueueQueues: []string{schedule.QueueIndividualConnectorSync},
		},
		{
			name:          "task in both lanes is deleted from both",
			enqueueQueues: []string{schedule.QueueConnectorSync, schedule.QueueIndividualConnectorSync},
		},
		{
			name:          "no task in either lane is a no-op (best-effort)",
			enqueueQueues: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mr := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = redisClient.Close() })

			client := asynq.NewClientFromRedisClient(redisClient)
			t.Cleanup(func() { _ = client.Close() })

			for _, q := range tt.enqueueQueues {
				_, err := client.Enqueue(
					asynq.NewTask(schedule.JobTypeConnectorSync, []byte("{}")),
					asynq.Queue(q),
					asynq.TaskID(taskID),
				)
				require.NoError(t, err)
			}

			inspector := asynq.NewInspectorFromRedisClient(redisClient)
			t.Cleanup(func() { _ = inspector.Close() })

			ctrl := gomock.NewController(t)
			reg := sdk.NewConnectorRegistry()
			reg.Register(newMockConnector(ctrl, connectorID, []sdk.OperationDescriptor{pullOp(opID, "*/15 * * * *")}))

			cleaner := NewSyncTaskCleaner(inspector, reg)
			cleaner.DeleteTasksForConnector(context.Background(), orgID, connectorID)

			// After cleanup, both connector lanes must be empty regardless of where
			// the stuck task lived.
			assert.Zero(t, queuePending(t, inspector, schedule.QueueConnectorSync),
				"standard lane must be empty after cleanup")
			assert.Zero(t, queuePending(t, inspector, schedule.QueueIndividualConnectorSync),
				"individual lane must be empty after cleanup")
		})
	}
}

// TestDeleteTasksForConnector_SkipsNonPullOps confirms only Pull ops are
// targeted — a webhook op carries no scheduled task to clean, so the cleaner
// must not attempt a delete for it (and must not panic when the connector mixes
// op types).
func TestDeleteTasksForConnector_SkipsNonPullOps(t *testing.T) {
	t.Parallel()
	const (
		orgID       = "org-1"
		connectorID = sdk.ConnectorID("slack")
		pullOpID    = sdk.OperationID("fetch_messages")
	)

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	client := asynq.NewClientFromRedisClient(redisClient)
	t.Cleanup(func() { _ = client.Close() })

	pullTaskID := fmt.Sprintf("%s/%s/%s", orgID, connectorID, pullOpID)
	_, err := client.Enqueue(
		asynq.NewTask(schedule.JobTypeConnectorSync, []byte("{}")),
		asynq.Queue(schedule.QueueIndividualConnectorSync),
		asynq.TaskID(pullTaskID),
	)
	require.NoError(t, err)

	inspector := asynq.NewInspectorFromRedisClient(redisClient)
	t.Cleanup(func() { _ = inspector.Close() })

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, connectorID, []sdk.OperationDescriptor{
		pullOp(pullOpID, "*/5 * * * *"),
		{ID: "receive_events", IngestionType: sdk.IngestionWebhook},
	}))

	cleaner := NewSyncTaskCleaner(inspector, reg)
	cleaner.DeleteTasksForConnector(context.Background(), orgID, connectorID)

	assert.Zero(t, queuePending(t, inspector, schedule.QueueIndividualConnectorSync),
		"pull-op task must be deleted")
}

// TestDeleteTasksForConnector_UnregisteredConnector is a no-op when the
// connector is not in the registry (cannot enumerate its ops) — best-effort.
func TestDeleteTasksForConnector_UnregisteredConnector(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	inspector := asynq.NewInspectorFromRedisClient(redisClient)
	t.Cleanup(func() { _ = inspector.Close() })

	cleaner := NewSyncTaskCleaner(inspector, sdk.NewConnectorRegistry())
	// Must not panic; connector absent from registry → debug-log and return.
	cleaner.DeleteTasksForConnector(context.Background(), "org-1", "unknown")
}
