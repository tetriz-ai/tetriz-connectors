// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// A LeaderWorker slice drains its own queues once this process leads.
func TestLeaderWorker_DrainsItsQueueWhileLeading(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	ran := make(chan struct{}, 1)
	lmux := asynq.NewServeMux()
	lmux.HandleFunc("leader.task", func(context.Context, *asynq.Task) error {
		ran <- struct{}{}
		return nil
	})
	lockFactory, _ := newSuccessLockFactory()
	s, err := NewScheduler(context.Background(), newTestConfig(), rc, nil, asynq.NewServeMux(),
		[]LeaderWorker{{Name: "test", Mux: lmux, Queues: map[string]int{"leader-q": 1}}}, lockFactory)
	require.NoError(t, err)
	require.NoError(t, s.Start(context.Background()))
	defer func() { _ = s.Stop(context.Background()) }()

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()
	_, err = client.Enqueue(asynq.NewTask("leader.task", nil), asynq.Queue("leader-q"))
	require.NoError(t, err)

	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("the leader worker did not process its queue")
	}
}
