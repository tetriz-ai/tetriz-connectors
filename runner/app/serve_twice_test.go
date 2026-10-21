// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
)

// Serve can run more than once in one process, as an embedding host or a
// restart in place does: each run registers the metrics on its own registry.
func TestServeRunsTwiceInOneProcess(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("serve", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	logs := &lockedBuffer{}
	logger.SetLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { logger.SetLogger(nil) })

	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	cfg, err := FromEnv()
	require.NoError(t, err)
	a, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })

	for run := 1; run <= 2; run++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- a.Serve(ctx) }()
		deadline := time.Now().Add(20 * time.Second)
		for strings.Count(logs.String(), "serving /_health and /metrics") < run && time.Now().Before(deadline) {
			select {
			case err := <-done:
				cancel()
				t.Fatalf("serve run %d returned before serving: %v", run, err)
			case <-time.After(50 * time.Millisecond):
			}
		}
		require.Equal(t, run, strings.Count(logs.String(), "serving /_health and /metrics"), "serve run %d did not start", run)
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err, "serve run %d", run)
		case <-time.After(40 * time.Second):
			t.Fatalf("serve run %d did not return after cancel", run)
		}
	}
}
