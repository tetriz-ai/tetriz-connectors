// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two sources emitting the same ID, where the later copy is invalid: the
// invalid copy is dropped before deduplication, so the valid schedule stays
// registered instead of the key vanishing.
func TestEdge_InvalidDuplicateDoesNotEvictTheValidOne(t *testing.T) {
	t.Parallel()
	good := &mockScheduleSource{name: "good", defs: []ScheduleDefinition{{ID: "org/x/op", JobType: "x", CronExpression: "*/5 * * * *"}}}
	bad := &mockScheduleSource{name: "bad", defs: []ScheduleDefinition{{ID: "org/x/op", JobType: "x", CronExpression: "*/5 * * *"}}}
	for _, order := range [][]ScheduleSource{{good, bad}, {bad, good}} {
		cfgs, err := NewConfigProvider(order, 0, nil).GetConfigs()
		require.NoError(t, err)
		require.Len(t, cfgs, 1)
		assert.Equal(t, "*/5 * * * *", cfgs[0].Cronspec)
	}
}

// The cron forms asynq runs (descriptors, @every, a CRON_TZ prefix) are kept;
// a seconds-field cron, which asynq's parser rejects, is dropped alone.
func TestEdge_CronFormsAsynqAcceptsAreKept(t *testing.T) {
	t.Parallel()
	src := &mockScheduleSource{name: "forms", defs: []ScheduleDefinition{
		{ID: "a", JobType: "x", CronExpression: "@every 30s"},
		{ID: "b", JobType: "x", CronExpression: "@hourly"},
		{ID: "c", JobType: "x", CronExpression: "CRON_TZ=America/New_York 0 9 * * *"},
		{ID: "d", JobType: "x", CronExpression: "0 0 31 * *"},
		{ID: "e", JobType: "x", CronExpression: "0 */5 * * * *"},
		{ID: "f", JobType: "x", CronExpression: "*/0 * * * *"},
	}}
	cfgs, err := NewConfigProvider([]ScheduleSource{src}, 0, nil).GetConfigs()
	require.NoError(t, err)
	got := map[string]bool{}
	for _, c := range cfgs {
		got[c.Cronspec] = true
	}
	assert.Equal(t, map[string]bool{
		"@every 30s": true, "@hourly": true, "CRON_TZ=America/New_York 0 9 * * *": true, "0 0 31 * *": true,
	}, got)
}
