// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

// A field the caller sets wins over the same key from the context, the trace id
// is read from incoming gRPC metadata, and a nil ctx is tolerated.
func TestEdge_CtxFieldsPrecedenceAndSources(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("trace_id", "trace-md"))
	ctx = reqctx.WithOrgID(ctx, "org-ctx")
	ctx = reqctx.WithConnectorID(ctx, "github")

	fields := GetCtxLogFields(ctx, zap.String(OrgID, "org-caller"))
	byKey := map[string]string{}
	for _, f := range fields {
		byKey[f.Key] = f.String
	}
	assert.Equal(t, "org-caller", byKey[OrgID], "the caller's field must win")
	assert.Equal(t, "trace-md", byKey[TraceID])
	assert.Equal(t, "github", byKey[ConnectorID])
	assert.Len(t, fields, 3, "no duplicated keys")

	var nilCtx context.Context // a nil context is the case under test
	assert.NotPanics(t, func() { _ = GetCtxLogFields(nilCtx) })
	assert.Empty(t, GetCtxLogFields(context.Background()), "no ctx values, no fields, no UNKNOWN_ID trace")
}

// Fields bound with With persist on every later entry, levels map to slog's,
// and the recovered panic value reaches the handler.
func TestEdge_SlogCoreWithFieldsLevelsAndPanicRecovery(t *testing.T) {
	var buf bytes.Buffer
	SetLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer SetLogger(slog.Default())

	l := Log.With(zap.String("bound", "yes"))
	l.Info("dropped below the handler level")
	l.Warn("kept", zap.Int("n", 1))
	func() {
		defer RecoverPanicAndError(reqctx.WithOrgID(context.Background(), "org-p"))
		panic("oh no")
	}()

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.Len(t, lines, 2, "info is filtered by the handler's level: %s", buf.String())
	var warn, panicked map[string]any
	require.NoError(t, json.Unmarshal(lines[0], &warn))
	require.NoError(t, json.Unmarshal(lines[1], &panicked))
	assert.Equal(t, "WARN", warn["level"])
	assert.Equal(t, "yes", warn["bound"])
	assert.Equal(t, "ERROR", panicked["level"])
	assert.Equal(t, "oh no", panicked["recoverMsg"])
	assert.Equal(t, "org-p", panicked[OrgID])
	assert.NotEmpty(t, panicked["stacktrace"])
}
