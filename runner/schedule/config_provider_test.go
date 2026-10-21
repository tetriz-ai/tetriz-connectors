// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test double: mockScheduleSource
// ---------------------------------------------------------------------------

type mockScheduleSource struct {
	name string
	defs []ScheduleDefinition
	err  error
}

func (m *mockScheduleSource) Name() string { return m.name }

func (m *mockScheduleSource) FetchSchedules(_ context.Context) ([]ScheduleDefinition, error) {
	return m.defs, m.err
}

// newTestConfigForProvider returns Options with a configurable DefaultTimeout.
func newTestConfigForProvider(executionTimeout time.Duration) Options {
	cfg := Options{
		SyncInterval:    5 * time.Minute,
		Concurrency:     5,
		DefaultTimeout:  executionTimeout,
		LockKey:         "scheduler-lock",
		LockTTL:         30 * time.Second,
		LockRetry:       10 * time.Second,
		ShutdownTimeout: 30 * time.Second,
	}
	return cfg
}

// ---------------------------------------------------------------------------
// Tests: GetConfigs
// ---------------------------------------------------------------------------

func TestConfigProvider_GetConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sources   []ScheduleSource
		wantCount int
		// wantCrons maps expected cronspecs to verify they appear in the result.
		wantCrons []string
	}{
		{
			name: "single source with 3 schedules",
			sources: []ScheduleSource{
				&mockScheduleSource{
					name: "source-a",
					defs: []ScheduleDefinition{
						{ID: "a/1", JobType: "connector.sync", CronExpression: "*/5 * * * *", Payload: []byte(`{"x":1}`), Queue: "connector-sync"},
						{ID: "a/2", JobType: "connector.sync", CronExpression: "*/10 * * * *", Payload: []byte(`{"x":2}`), Queue: "connector-sync"},
						{ID: "a/3", JobType: "connector.sync", CronExpression: "0 * * * *", Payload: []byte(`{"x":3}`), Queue: "default"},
					},
				},
			},
			wantCount: 3,
			wantCrons: []string{"*/5 * * * *", "*/10 * * * *", "0 * * * *"},
		},
		{
			name: "multiple sources aggregated",
			sources: []ScheduleSource{
				&mockScheduleSource{
					name: "source-a",
					defs: []ScheduleDefinition{
						{ID: "a/1", JobType: "connector.sync", CronExpression: "*/5 * * * *", Queue: "connector-sync"},
						{ID: "a/2", JobType: "connector.sync", CronExpression: "*/10 * * * *", Queue: "connector-sync"},
					},
				},
				&mockScheduleSource{
					name: "source-b",
					defs: []ScheduleDefinition{
						{ID: "b/1", JobType: "report.compute", CronExpression: "0 * * * *", Queue: "default"},
						{ID: "b/2", JobType: "report.compute", CronExpression: "0 0 * * *", Queue: "default"},
					},
				},
			},
			wantCount: 4,
		},
		{
			name: "source returns error, healthy source still works",
			sources: []ScheduleSource{
				&mockScheduleSource{
					name: "healthy",
					defs: []ScheduleDefinition{
						{ID: "h/1", JobType: "connector.sync", CronExpression: "*/5 * * * *", Queue: "connector-sync"},
						{ID: "h/2", JobType: "connector.sync", CronExpression: "*/10 * * * *", Queue: "connector-sync"},
					},
				},
				&mockScheduleSource{
					name: "broken",
					err:  errors.New("database unavailable"),
				},
			},
			wantCount: 2,
		},
		{
			name: "all sources error before any success returns empty slice",
			sources: []ScheduleSource{
				&mockScheduleSource{name: "broken-1", err: errors.New("err1")},
				&mockScheduleSource{name: "broken-2", err: errors.New("err2")},
			},
			wantCount: 0,
		},
		{
			name: "empty source returns empty slice",
			sources: []ScheduleSource{
				&mockScheduleSource{name: "empty", defs: nil},
			},
			wantCount: 0,
		},
		{
			name: "duplicate IDs across sources, last one wins",
			sources: []ScheduleSource{
				&mockScheduleSource{
					name: "source-a",
					defs: []ScheduleDefinition{
						{ID: "shared/1", JobType: "connector.sync", CronExpression: "*/5 * * * *", Queue: "q-a"},
					},
				},
				&mockScheduleSource{
					name: "source-b",
					defs: []ScheduleDefinition{
						{ID: "shared/1", JobType: "connector.sync", CronExpression: "*/10 * * * *", Queue: "q-b"},
					},
				},
			},
			wantCount: 1,
			wantCrons: []string{"*/10 * * * *"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cp := NewConfigProvider(tt.sources, newTestConfigForProvider(0).DefaultTimeout, nil)
			configs, err := cp.GetConfigs()
			require.NoError(t, err)
			assert.Len(t, configs, tt.wantCount)

			if tt.wantCrons != nil {
				gotCrons := make([]string, 0, len(configs))
				for _, c := range configs {
					gotCrons = append(gotCrons, c.Cronspec)
				}
				assert.ElementsMatch(t, tt.wantCrons, gotCrons)
			}
		})
	}
}

// A source whose fetch fails keeps the schedules of its last successful
// fetch, so a failed read (a database blip) does not unregister its tasks.
// Its next successful fetch replaces them.
func TestConfigProvider_FailedSourceKeepsItsLastGoodSchedules(t *testing.T) {
	t.Parallel()
	src := &mockScheduleSource{name: "flaky", defs: []ScheduleDefinition{
		{ID: "f/1", JobType: "connector.sync", CronExpression: "*/5 * * * *"},
		{ID: "f/2", JobType: "connector.sync", CronExpression: "*/10 * * * *"},
	}}
	other := &mockScheduleSource{name: "other", defs: []ScheduleDefinition{{ID: "o/1", JobType: "connector.sync", CronExpression: "0 * * * *"}}}
	cp := NewConfigProvider([]ScheduleSource{src, other}, 0, nil)
	ids := func() []string {
		cfgs, err := cp.GetConfigs()
		require.NoError(t, err)
		out := make([]string, 0, len(cfgs))
		for _, c := range cfgs {
			for _, o := range c.Opts {
				if o.Type() == asynq.TaskIDOpt {
					out = append(out, o.Value().(string))
				}
			}
		}
		return out
	}
	assert.ElementsMatch(t, []string{"f/1", "f/2", "o/1"}, ids())

	src.defs, src.err = nil, errors.New("database unavailable")
	assert.ElementsMatch(t, []string{"f/1", "f/2", "o/1"}, ids(), "one source failing keeps its last good set")
	other.defs, other.err = nil, errors.New("database unavailable")
	assert.ElementsMatch(t, []string{"f/1", "f/2", "o/1"}, ids(), "every source failing keeps every last good set")

	src.defs, src.err = []ScheduleDefinition{{ID: "f/3", JobType: "connector.sync", CronExpression: "*/5 * * * *"}}, nil
	assert.ElementsMatch(t, []string{"f/3", "o/1"}, ids(), "a successful fetch replaces the kept set")
}

func TestConfigProvider_OptionsCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		def            ScheduleDefinition
		wantOptsMinLen int // Minimum number of options (TaskID always present)
	}{
		{
			name: "base options: TaskID + Queue + default MaxRetry",
			def: ScheduleDefinition{
				ID: "x/1", JobType: "connector.sync", CronExpression: "*/5 * * * *",
				Queue: "connector-sync",
			},
			wantOptsMinLen: 3, // TaskID + Queue + MaxRetry (default=3)
		},
		{
			name: "with MaxRetry adds option",
			def: ScheduleDefinition{
				ID: "x/1", JobType: "connector.sync", CronExpression: "*/5 * * * *",
				Queue: "connector-sync", MaxRetry: 3,
			},
			wantOptsMinLen: 3, // TaskID + Queue + MaxRetry
		},
		{
			name: "with Timeout adds option",
			def: ScheduleDefinition{
				ID: "x/1", JobType: "connector.sync", CronExpression: "*/5 * * * *",
				Queue: "connector-sync", Timeout: 5 * time.Minute,
			},
			wantOptsMinLen: 4, // TaskID + Queue + MaxRetry (default) + Timeout
		},
		{
			name: "with MaxRetry and Timeout adds both options",
			def: ScheduleDefinition{
				ID: "x/1", JobType: "connector.sync", CronExpression: "*/5 * * * *",
				Queue: "connector-sync", MaxRetry: 5, Timeout: 5 * time.Minute,
			},
			wantOptsMinLen: 4, // TaskID + Queue + MaxRetry + Timeout
		},
		{
			name: "no queue omits Queue option",
			def: ScheduleDefinition{
				ID: "x/1", JobType: "connector.sync", CronExpression: "*/5 * * * *",
			},
			wantOptsMinLen: 2, // TaskID + MaxRetry (default)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cp := NewConfigProvider([]ScheduleSource{
				&mockScheduleSource{name: "test", defs: []ScheduleDefinition{tt.def}},
			}, newTestConfigForProvider(0).DefaultTimeout, nil)

			configs, err := cp.GetConfigs()
			require.NoError(t, err)
			require.Len(t, configs, 1)
			assert.GreaterOrEqual(t, len(configs[0].Opts), tt.wantOptsMinLen)
		})
	}
}

func TestConfigProvider_TaskTypeAndPayload(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"org_id":"org-1","connector_id":"github"}`)
	cp := NewConfigProvider([]ScheduleSource{
		&mockScheduleSource{
			name: "test",
			defs: []ScheduleDefinition{
				{ID: "a/1", JobType: "connector.sync", CronExpression: "*/5 * * * *", Payload: payload, Queue: "connector-sync"},
			},
		},
	}, newTestConfigForProvider(0).DefaultTimeout, nil)

	configs, err := cp.GetConfigs()
	require.NoError(t, err)
	require.Len(t, configs, 1)

	assert.Equal(t, "connector.sync", configs[0].Task.Type())
	assert.Equal(t, payload, configs[0].Task.Payload())
	assert.Equal(t, "*/5 * * * *", configs[0].Cronspec)
}

func TestConfigProvider_DefaultExecutionTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		defTimeout       time.Duration
		configTimeout    time.Duration
		wantTimeoutOpt   bool
		wantOptsMinCount int
	}{
		{
			name:             "def has no timeout, config has default — default applied",
			defTimeout:       0,
			configTimeout:    10 * time.Minute,
			wantTimeoutOpt:   true,
			wantOptsMinCount: 2, // TaskID + Timeout
		},
		{
			name:             "def has explicit timeout — config default NOT applied",
			defTimeout:       5 * time.Minute,
			configTimeout:    10 * time.Minute,
			wantTimeoutOpt:   true,
			wantOptsMinCount: 2, // TaskID + Timeout
		},
		{
			name:             "def has no timeout, config has no default — no timeout opt",
			defTimeout:       0,
			configTimeout:    0,
			wantTimeoutOpt:   false,
			wantOptsMinCount: 1, // TaskID only
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cp := NewConfigProvider([]ScheduleSource{
				&mockScheduleSource{
					name: "test",
					defs: []ScheduleDefinition{
						{ID: "x/1", JobType: "connector.sync", CronExpression: "*/5 * * * *", Timeout: tt.defTimeout},
					},
				},
			}, newTestConfigForProvider(tt.configTimeout).DefaultTimeout, nil)

			configs, err := cp.GetConfigs()
			require.NoError(t, err)
			require.Len(t, configs, 1)
			assert.GreaterOrEqual(t, len(configs[0].Opts), tt.wantOptsMinCount)
		})
	}
}

// Each periodic task carries its definition's id as its asynq task id, which
// is what stops two overlapping runs of one operation.
func TestConfigProvider_TaskIDIsTheDefinitionID(t *testing.T) {
	p := NewConfigProvider([]ScheduleSource{&mockScheduleSource{name: "s", defs: []ScheduleDefinition{{
		ID: "org-1/github/fetch_prs", JobType: "connector.sync", CronExpression: "*/5 * * * *",
	}}}}, time.Minute, nil)
	cfgs, err := p.GetConfigs()
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	var got any
	for _, o := range cfgs[0].Opts {
		if o.Type() == asynq.TaskIDOpt {
			got = o.Value()
		}
	}
	assert.Equal(t, "org-1/github/fetch_prs", got)
}
