// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command heartbeat runs the heartbeat source and handler on a bare asynq scheduler against a local Redis.
//
// It needs Redis on localhost:6379, or REDIS_ADDR naming another:
//
//	go run ./examples/heartbeat/cmd
//
// The job fires every 10 seconds, and each fire is logged. Press Ctrl+C to stop.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/examples/heartbeat"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/handler"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("creating logger: %v", err))
	}
	defer func() {
		// Sync errors on stderr/stdout are expected on some platforms; log and move on.
		if syncErr := logger.Sync(); syncErr != nil {
			logger.Warn("syncing logger", zap.Error(syncErr))
		}
	}()

	redisAddr := "localhost:6379"
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		redisAddr = addr
	}

	redisOpt := asynq.RedisClientOpt{Addr: redisAddr}

	// 1. Create the heartbeat source and handler.
	src := heartbeat.NewSource("@every 10s")
	h := heartbeat.NewHandler()

	// 2. Wrap the source in the scheduler's config provider.
	//    This bridges ScheduleSource → asynq.PeriodicTaskConfigProvider.
	cp := schedule.NewConfigProvider([]schedule.ScheduleSource{src}, 30*time.Second, nil)

	// 3. Create the asynq worker (processes tasks from Redis queues).
	worker := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: 2,
		Queues: map[string]int{
			schedule.QueueDefault: 1,
		},
	})

	mux := asynq.NewServeMux()
	mux.Use(handler.LoggingMiddleware(logger))
	mux.Use(handler.RecoveryMiddleware(logger))
	mux.Handle(heartbeat.JobType, h)

	// 4. Create the periodic task manager (enqueues tasks on cron schedule).
	manager, err := asynq.NewPeriodicTaskManager(asynq.PeriodicTaskManagerOpts{
		RedisConnOpt:               redisOpt,
		PeriodicTaskConfigProvider: cp,
		SyncInterval:               30 * time.Second,
	})
	if err != nil {
		logger.Fatal("failed to create periodic task manager", zap.Error(err))
	}

	// 5. Start worker + manager.
	if err := worker.Start(mux); err != nil {
		logger.Fatal("failed to start worker", zap.Error(err))
	}
	if err := manager.Start(); err != nil {
		logger.Fatal("failed to start periodic task manager", zap.Error(err))
	}

	logger.Info("heartbeat example running — a task fires every 10s",
		zap.String("redis", redisAddr),
	)
	logger.Info("press Ctrl+C to stop")

	// 6. Wait for interrupt, then shut down gracefully.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("shutting down...")
	manager.Shutdown()
	worker.Shutdown()
	logger.Info("stopped")
}
