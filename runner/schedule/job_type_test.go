// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
)

// TestDefaultQueuesRegisterEveryConnectorLane pins that both connector lanes
// and the default lane are drained by the main worker: asynq has no run-time
// queue add, so a lane missing here would drop its tasks.
func TestDefaultQueuesRegisterEveryConnectorLane(t *testing.T) {
	t.Parallel()
	q := DefaultQueues()
	for _, lane := range []string{QueueConnectorSync, QueueIndividualConnectorSync, QueueDefault} {
		w, ok := q[lane]
		assert.True(t, ok, "lane %q must be registered", lane)
		assert.Positive(t, w, "lane %q must have a positive weight", lane)
	}
	assert.Greater(t, q[QueueConnectorSync], q[QueueIndividualConnectorSync],
		"organizations take the larger share of connector work")
}

// TestRegisteredQueuesCoverLeaderWorkers pins that archived-task cleanup
// visits a queue that only a leader worker drains: a lane it skipped would
// keep archived TaskIDs and block its periodic re-enqueue for good.
func TestRegisteredQueuesCoverLeaderWorkers(t *testing.T) {
	t.Parallel()
	s := &Scheduler{
		opts: Options{Queues: map[string]int{QueueConnectorSync: 1}},
		leaderWorkers: []LeaderWorker{
			{Name: "bounded", Mux: asynq.NewServeMux(), Queues: map[string]int{"bounded-lane": 1}},
		},
	}
	assert.ElementsMatch(t, []string{QueueConnectorSync, "bounded-lane"}, s.registeredQueues())
}

// TestLeaderWorkerConcurrencyClamp pins the clamp: asynq reads a concurrency
// of zero as "one per CPU", which would defeat a worker that exists to bound
// work globally.
func TestLeaderWorkerConcurrencyClamp(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1, LeaderWorker{}.concurrency())
	assert.Equal(t, 1, LeaderWorker{Concurrency: func() int { return 0 }}.concurrency())
	assert.Equal(t, 1, LeaderWorker{Concurrency: func() int { return -3 }}.concurrency())
	assert.Equal(t, 4, LeaderWorker{Concurrency: func() int { return 4 }}.concurrency())
}
