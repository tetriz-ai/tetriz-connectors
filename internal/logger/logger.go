// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package logger is the logging front end the repository's own packages call.
// It keeps zap's typed fields at the call sites and writes every entry to a
// log/slog handler. An entry logged with a context goes to the slog.Logger
// NewContext put on it (runner.Orchestrator.WithLogger and app.WithLogger do),
// and otherwise to the process logger, slog.Default() unless the command set
// one. Context values set through package reqctx (trace, organization, person,
// connector and operation ids) are appended to every entry that carries a
// context.
package logger

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

var (
	// Log is the zap logger every helper in this package writes through. It
	// forwards to slog.Default() until SetLogger or Init replaces it. Tests may
	// swap it for a zap observer and restore it afterwards.
	Log = zap.New(NewSlogCore(nil))

	once sync.Once
)

type ctxKey struct{}

// NewContext returns ctx carrying l: every entry logged with the returned
// context, or one derived from it, goes to l's handler. A nil l leaves ctx
// unchanged.
func NewContext(ctx context.Context, l *slog.Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, zap.New(NewSlogCore(l.Handler())))
}

// from returns the logger NewContext put on ctx, or Log.
func from(ctx context.Context) *zap.Logger {
	if ctx != nil {
		if l, ok := ctx.Value(ctxKey{}).(*zap.Logger); ok {
			return l
		}
	}
	return Log
}

// SetLogger routes every entry without a context logger to l's handler. A nil
// l restores slog.Default(). The command calls it once, at start.
func SetLogger(l *slog.Logger) {
	var h slog.Handler
	if l != nil {
		h = l.Handler()
	}
	Log = zap.New(NewSlogCore(h))
}

// Init installs a text handler on standard error, at debug level for the
// "development" and "test" environments and at info level otherwise. It runs
// once per process; later calls do nothing.
func Init(env string, _ any) {
	once.Do(func() {
		level := slog.LevelInfo
		if env == "development" || env == "test" {
			level = slog.LevelDebug
		}
		SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	})
}

// GetCtxLogFields appends the standard context values as fields. A field the
// caller already set with the same key wins.
func GetCtxLogFields(ctx context.Context, fields ...zap.Field) []zap.Field {
	if ctx == nil {
		ctx = context.Background()
	}
	traceID := reqctx.TraceIDFromContext(ctx)
	if traceID != reqctx.UnknownID && traceID != "" {
		fields = appendFieldIfAbsent(fields, zap.String(TraceID, traceID))
	}
	if orgID := reqctx.OrgIDFromContext(ctx); orgID != "" {
		fields = appendFieldIfAbsent(fields, zap.String(OrgID, orgID))
	}
	if connID := reqctx.ConnectorIDFromContext(ctx); connID != "" {
		fields = appendFieldIfAbsent(fields, zap.String(ConnectorID, connID))
	}
	if opID := reqctx.OperationIDFromContext(ctx); opID != "" {
		fields = appendFieldIfAbsent(fields, zap.String(OperationID, opID))
	}
	if personID := reqctx.PersonIDFromContext(ctx); personID != "" {
		fields = appendFieldIfAbsent(fields, zap.String(PersonID, personID))
	}
	return fields
}

func appendFieldIfAbsent(fields []zap.Field, elem zap.Field) []zap.Field {
	for _, f := range fields {
		if f.Key == elem.Key {
			return fields
		}
	}
	return append(fields, elem)
}

// Debug logs at debug level with the context fields.
func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Debug(msg, GetCtxLogFields(ctx, fields...)...)
}

// Info logs at info level with the context fields.
func Info(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Info(msg, GetCtxLogFields(ctx, fields...)...)
}

// Error logs at error level with the context fields.
func Error(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Error(msg, GetCtxLogFields(ctx, fields...)...)
}

// WarnWithCtx logs at warn level with the context fields.
func WarnWithCtx(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Warn(msg, GetCtxLogFields(ctx, fields...)...)
}

// Warn logs at warn level without context fields.
func Warn(msg string, fields ...zap.Field) {
	Log.Warn(msg, fields...)
}

// DebugNoCtx logs at debug level without context fields.
func DebugNoCtx(msg string, fields ...zap.Field) {
	Log.Debug(msg, fields...)
}

// InfoNoCtx logs at info level without context fields.
func InfoNoCtx(msg string, fields ...zap.Field) {
	Log.Info(msg, fields...)
}

// ErrorNoCtx logs at error level without context fields.
func ErrorNoCtx(msg string, fields ...zap.Field) {
	Log.Error(msg, fields...)
}

// RecoverPanicAndError recovers a panic in the calling goroutine and logs it
// with a stack trace. Call it deferred.
func RecoverPanicAndError(ctx context.Context) {
	if r := recover(); r != nil {
		from(ctx).WithOptions(zap.AddStacktrace(zap.ErrorLevel)).Error(
			"recovered from panic",
			GetCtxLogFields(ctx, zap.Any("recoverMsg", r))...,
		)
	}
}
