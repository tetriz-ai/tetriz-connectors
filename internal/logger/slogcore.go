// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package logger

import (
	"context"
	"log/slog"

	"go.uber.org/zap/zapcore"
)

// slogCore is a zapcore.Core that writes entries to a slog.Handler. A nil
// handler means slog.Default().Handler() at write time, so a host that calls
// slog.SetDefault after this package initialised still receives the entries.
type slogCore struct {
	handler slog.Handler
	attrs   []slog.Attr
}

// NewSlogCore returns a zapcore.Core writing to h (slog.Default() when nil).
func NewSlogCore(h slog.Handler) zapcore.Core {
	return &slogCore{handler: h}
}

func (c *slogCore) h() slog.Handler {
	if c.handler != nil {
		return c.handler
	}
	return slog.Default().Handler()
}

func (c *slogCore) Enabled(l zapcore.Level) bool {
	return c.h().Enabled(context.Background(), toSlogLevel(l))
}

func (c *slogCore) With(fields []zapcore.Field) zapcore.Core {
	return &slogCore{handler: c.handler, attrs: append(append([]slog.Attr(nil), c.attrs...), fieldsToAttrs(fields)...)}
}

func (c *slogCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c *slogCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	r := slog.NewRecord(e.Time, toSlogLevel(e.Level), e.Message, 0)
	r.AddAttrs(c.attrs...)
	r.AddAttrs(fieldsToAttrs(fields)...)
	if e.Stack != "" {
		r.AddAttrs(slog.String("stacktrace", e.Stack))
	}
	return c.h().Handle(context.Background(), r)
}

func (c *slogCore) Sync() error { return nil }

func toSlogLevel(l zapcore.Level) slog.Level {
	switch {
	case l <= zapcore.DebugLevel:
		return slog.LevelDebug
	case l == zapcore.InfoLevel:
		return slog.LevelInfo
	case l == zapcore.WarnLevel:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

// fieldsToAttrs encodes zap fields through zap's own map encoder, so every
// field type (errors, durations, arrays, objects) keeps zap's encoding.
func fieldsToAttrs(fields []zapcore.Field) []slog.Attr {
	if len(fields) == 0 {
		return nil
	}
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	attrs := make([]slog.Attr, 0, len(enc.Fields))
	for _, f := range fields {
		if v, ok := enc.Fields[f.Key]; ok {
			attrs = append(attrs, slog.Any(f.Key, v))
			delete(enc.Fields, f.Key)
		}
	}
	// Keys added by a field under a different name (zap.Error with a
	// Verbose cause adds "<key>Verbose") come last, in no fixed order.
	for k, v := range enc.Fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	return attrs
}
