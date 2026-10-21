// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// Two orchestrators with different loggers each log to their own handler: the
// orchestrator's entries and the operation's own, logged with its context.
func TestWithLoggerKeepsEachOrchestratorsLogsApart(t *testing.T) {
	exec := func(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
		logger.Info(ctx, "operation says hello")
		return nil, nil
	}
	var bufA, bufB syncBuffer
	orchA, _ := memOrchestrator(t, setupRegistry("c", "op", exec), sink.NewChannelSink())
	orchB, _ := memOrchestrator(t, setupRegistry("c", "op", exec), sink.NewChannelSink())
	orchA.WithLogger(slog.New(slog.NewTextHandler(&bufA, nil)))
	orchB.WithLogger(slog.New(slog.NewTextHandler(&bufB, nil)))

	require.NoError(t, orchA.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "a"))
	require.NoError(t, orchB.RunOperation(context.Background(), limOrg, "c", "op", sdk.TenantKindOrganization, "b"))

	for name, buf := range map[string]*syncBuffer{"A": &bufA, "B": &bufB} {
		assert.Contains(t, buf.String(), "executing operation", name)
		assert.Contains(t, buf.String(), "operation says hello", name)
	}
	assert.Contains(t, bufA.String(), "trigger=a")
	assert.NotContains(t, bufA.String(), "trigger=b", "B's run must not reach A's handler")
	assert.Contains(t, bufB.String(), "trigger=b")
	assert.NotContains(t, bufB.String(), "trigger=a", "A's run must not reach B's handler")
}
