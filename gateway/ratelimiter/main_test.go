// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter_test

import (
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/store"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

var redisClient *redis.Client

// TestMain runs the suite against the Redis at $TETRIZ_TEST_REDIS_ADDR, or an
// in-process miniredis when the variable is empty.
func TestMain(m *testing.M) {
	logger.Init("test", nil)
	addr := os.Getenv("TETRIZ_TEST_REDIS_ADDR")
	var mini *miniredis.Miniredis
	if addr == "" {
		var err error
		mini, err = miniredis.Run()
		if err != nil {
			panic(err)
		}
		addr = mini.Addr()
	}
	redisClient = redis.NewClient(&redis.Options{Addr: addr})

	redisStore := store.NewSlidingWindowLogWithRedis(redisClient)
	rateLimiterTs = NewRateLimiterTestSuite(redisStore)

	exitCode := testguard.Run(m)
	_ = redisClient.Close()
	if mini != nil {
		mini.Close()
	}
	os.Exit(exitCode)
}
