// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package heartbeat_test

import (
	"context"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/examples/heartbeat"
)

// A ScheduleSource returns the jobs the scheduler registers. This one
// returns one job that fires every ten seconds.
func ExampleSource() {
	defs, err := heartbeat.NewSource("@every 10s").FetchSchedules(context.Background())
	if err != nil {
		panic(err)
	}
	for _, d := range defs {
		fmt.Println(d.ID, d.JobType, d.CronExpression, d.Queue)
	}
	// Output: heartbeat/ping example.heartbeat @every 10s default
}
