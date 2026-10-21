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

	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

// Fields logged through the zap-style API reach the host's slog handler with
// their keys and typed values, plus the request fields carried by ctx.
func TestFieldsReachSlog(t *testing.T) {
	var buf bytes.Buffer
	SetLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer SetLogger(slog.Default())

	ctx := reqctx.WithOrgID(context.Background(), "org-1")
	Error(ctx, "boom", zap.String(ConnectorID, "github"), zap.Int("n", 3), zap.Error(assertErr{}))

	var got map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.Equal(t, "ERROR", got["level"])
	assert.Equal(t, "boom", got["msg"])
	assert.Equal(t, "github", got[ConnectorID])
	assert.Equal(t, float64(3), got["n"])
	assert.Equal(t, "bad", got["error"])
	assert.Equal(t, "org-1", got[OrgID])
}

type assertErr struct{}

func (assertErr) Error() string { return "bad" }
