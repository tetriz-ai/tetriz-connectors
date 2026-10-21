// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two schedulers in different namespaces on one Redis both lead (their lock
// keys differ) and each runs only its own tasks, although both schedule the
// same task type on the same queue name and on asynq's default queue.
func TestNamespacedSchedulersShareARedisWithoutCrossing(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	var mu sync.Mutex
	ran := map[string][]string{} // namespace -> payloads it ran
	newScheduler := func(ns string) *Scheduler {
		mux := asynq.NewServeMux()
		mux.HandleFunc("tick", func(_ context.Context, task *asynq.Task) error {
			mu.Lock()
			defer mu.Unlock()
			ran[ns] = append(ran[ns], string(task.Payload()))
			return nil
		})
		src := &countingSource{name: "tick-" + ns, defs: []ScheduleDefinition{
			{ID: "tick/lane", JobType: "tick", CronExpression: "@every 1s", Queue: QueueConnectorSync, Payload: []byte(ns + "/lane")},
			{ID: "tick/default", JobType: "tick", CronExpression: "@every 1s", Payload: []byte(ns + "/default")},
		}}
		cfg := newTestConfig()
		cfg.Namespace = ns
		cfg.ShutdownTimeout = 2 * time.Second
		s, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{src}, mux, nil, NewDistLockFactory(rc))
		require.NoError(t, err)
		require.NoError(t, s.Start(context.Background()))
		t.Cleanup(func() { _ = s.Stop(context.Background()) })
		return s
	}
	a, b := newScheduler("team-a"), newScheduler("team-b")
	require.True(t, waitFor(t, 10*time.Second, a.isLeading), "team-a never led")
	require.True(t, waitFor(t, 10*time.Second, b.isLeading), "team-b never led: the namespaces share a lock")

	require.True(t, waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		seen := map[string]bool{}
		for _, ns := range []string{"team-a", "team-b"} {
			for _, p := range ran[ns] {
				seen[p] = true
			}
		}
		return seen["team-a/lane"] && seen["team-a/default"] && seen["team-b/lane"] && seen["team-b/default"]
	}), "each namespace runs both of its tasks")

	mu.Lock()
	for ns, payloads := range ran {
		for _, p := range payloads {
			assert.True(t, strings.HasPrefix(p, ns+"/"), "%s ran %s, a task of another namespace", ns, p)
		}
	}
	mu.Unlock()

	assert.True(t, mr.Exists("team-a:scheduler-lock"), "team-a's lock key carries its namespace")
	assert.True(t, mr.Exists("team-b:scheduler-lock"), "team-b's lock key carries its namespace")
	assert.False(t, mr.Exists("scheduler-lock"))
	queues, err := asynq.NewInspectorFromRedisClient(rc).Queues()
	require.NoError(t, err)
	for _, q := range queues {
		assert.True(t, strings.HasPrefix(q, "team-a:") || strings.HasPrefix(q, "team-b:"), "queue %q carries no namespace", q)
	}
}

// With no namespace the lock key, the queue names and each definition's
// queue are exactly the unprefixed ones.
func TestEmptyNamespaceKeepsTheNames(t *testing.T) {
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	lockFactory, _ := newSuccessLockFactory()
	s, err := NewScheduler(context.Background(), Options{}, rc, nil, asynq.NewServeMux(),
		[]LeaderWorker{{Name: "lw", Mux: asynq.NewServeMux(), Queues: map[string]int{"leader-q": 1}}}, lockFactory)
	require.NoError(t, err)
	t.Cleanup(func() { unregisterForStart(s) })
	assert.Equal(t, "scheduler-lock", s.opts.LockKey)
	assert.Equal(t, DefaultQueues(), s.opts.Queues)
	got := s.registeredQueues()
	sort.Strings(got)
	assert.Equal(t, []string{"connector-sync", "default", "individual-connector-sync", "leader-q"}, got)

	cp := NewConfigProvider([]ScheduleSource{&countingSource{name: "s", defs: []ScheduleDefinition{
		{ID: "x/lane", JobType: "x", CronExpression: "@every 1m", Queue: QueueConnectorSync},
		{ID: "x/none", JobType: "x", CronExpression: "@every 1m"},
	}}}, 0, nil).WithNamespace("")
	cfgs, err := cp.GetConfigs()
	require.NoError(t, err)
	queueOf := map[string]string{}
	for _, c := range cfgs {
		queueOf[c.Task.Type()+"|"+c.Cronspec+"|"+optQueue(c.Opts)] = optQueue(c.Opts)
	}
	assert.ElementsMatch(t, []string{"connector-sync", ""}, values(queueOf), "the definitions keep their queue, and no queue stays asynq's default")
}

// Namespaced prefixes a name with "ns:" and leaves it alone for an empty ns.
func TestNamespaced(t *testing.T) {
	assert.Equal(t, "scheduler-lock", Namespaced("", "scheduler-lock"))
	assert.Equal(t, "prod:scheduler-lock", Namespaced("prod", "scheduler-lock"))
}

func optQueue(opts []asynq.Option) string {
	for _, o := range opts {
		if o.Type() == asynq.QueueOpt {
			return o.Value().(string)
		}
	}
	return ""
}

func values(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// The periodic task manager's own connection carries the shared client's
// credentials, database and TLS settings, so a password-protected or TLS
// Redis works for the leader too.
func TestOwnConnOptKeepsCredentialsAndTLS(t *testing.T) {
	o, err := redis.ParseURL("rediss://alice:pw@redis.internal:6380/3")
	require.NoError(t, err)
	got := ownConnOpt(redis.NewClient(o))
	assert.Equal(t, "redis.internal:6380", got.Addr)
	assert.Equal(t, "alice", got.Username)
	assert.Equal(t, "pw", got.Password)
	assert.Equal(t, 3, got.DB)
	assert.NotNil(t, got.TLSConfig)
}
