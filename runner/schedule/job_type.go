// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

// JobTypeConnectorSync is the task type of one scheduled connector operation
// run.
const JobTypeConnectorSync = "connector.sync"

const (
	// QueueConnectorSync carries organization connector runs.
	QueueConnectorSync = "connector-sync"
	// QueueIndividualConnectorSync is the low-weight lane for individual
	// tenants, so an individual ingestion burst cannot starve organization
	// work while organizations still take the larger share.
	QueueIndividualConnectorSync = "individual-connector-sync"
	// QueueDefault carries every task that names no queue.
	QueueDefault = "default"
)

// DefaultQueues is the queue-to-weight map the runner registers with the
// main worker. asynq cannot add a queue at run time, so a lane a schedule
// source routes to must appear in the worker's map or its tasks are never
// processed.
func DefaultQueues() map[string]int {
	return map[string]int{
		QueueConnectorSync:           6,
		QueueDefault:                 3,
		QueueIndividualConnectorSync: 1,
	}
}
