// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"time"
)

// ScheduleSource produces schedule definitions from a domain.
// The connector source and any job of your own implement it.
//
// FetchSchedules must honour ctx: it gets a deadline (30 s, less on a new
// leader's first read), and a fetch that has not returned shortly after it is
// abandoned. Its schedules are then left out of that sync, the source counts
// as failed, and it is not read again until the abandoned fetch returns. An
// abandoned fetch's goroutine is not reclaimed until it returns.
type ScheduleSource interface {
	Name() string
	FetchSchedules(ctx context.Context) ([]ScheduleDefinition, error)
}

// ScheduleDefinition is a domain-agnostic schedule entry.
type ScheduleDefinition struct {
	ID             string        // Unique key, e.g. "org1/github/pull_commits"
	JobType        string        // e.g. "connector.sync"
	CronExpression string        // 5-field cron
	Payload        []byte        // JSON, opaque to scheduler
	Queue          string        // Target asynq queue
	MaxRetry       int           // 0 = use handler default
	Timeout        time.Duration // 0 = use handler default
}
