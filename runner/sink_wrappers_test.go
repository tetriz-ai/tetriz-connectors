// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// fakeSink is a minimal sdk.DataSink that lets each method's behaviour
// be programmed per test. Hand-rolled rather than generated because the sink
// interface is tiny and we want test-call counts inline.
type fakeSink struct {
	emitErr      error
	emitBatchErr error
	flushErr     error
	emitCalls    int
	batchCalls   int
	batchRecords int
}

func (s *fakeSink) Emit(_ context.Context, _ *sdk.RawRecord) error {
	s.emitCalls++
	return s.emitErr
}

func (s *fakeSink) EmitBatch(_ context.Context, records []*sdk.RawRecord) error {
	s.batchCalls++
	if s.emitBatchErr != nil {
		return s.emitBatchErr
	}
	s.batchRecords += len(records)
	return nil
}

func (s *fakeSink) Flush(_ context.Context) error { return s.flushErr }

// TestCountingSinkOnlyCountsSuccess pins the at-least-once semantic: failed
// emits must not inflate the counter (records_emitted is used for
// "operation ran for a while" diagnostics — false positives break that).
func TestCountingSinkOnlyCountsSuccess(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*fakeSink)
		ops       func(*countingSink) error
		wantCount int64
		wantErr   bool
	}{
		{
			name:  "single successful emit increments by 1",
			setup: func(s *fakeSink) {},
			ops: func(c *countingSink) error {
				return c.Emit(context.Background(), &sdk.RawRecord{})
			},
			wantCount: 1,
		},
		{
			name:  "failed emit does not increment",
			setup: func(s *fakeSink) { s.emitErr = errors.New("sqs unavailable") },
			ops: func(c *countingSink) error {
				return c.Emit(context.Background(), &sdk.RawRecord{})
			},
			wantCount: 0,
			wantErr:   true,
		},
		{
			name:  "EmitBatch increments by N on success",
			setup: func(s *fakeSink) {},
			ops: func(c *countingSink) error {
				return c.EmitBatch(context.Background(), []*sdk.RawRecord{{}, {}, {}, {}})
			},
			wantCount: 4,
		},
		{
			name:  "EmitBatch whose first record fails does not increment",
			setup: func(s *fakeSink) { s.emitErr = errors.New("batch fail") },
			ops: func(c *countingSink) error {
				return c.EmitBatch(context.Background(), []*sdk.RawRecord{{}, {}})
			},
			wantCount: 0,
			wantErr:   true,
		},
		{
			name:  "mix of success + failure counts only successes",
			setup: func(s *fakeSink) {},
			ops: func(c *countingSink) error {
				ctx := context.Background()
				_ = c.Emit(ctx, &sdk.RawRecord{})
				_ = c.EmitBatch(ctx, []*sdk.RawRecord{{}, {}})
				// flip to error mode mid-run
				inner := c.inner.(*fakeSink)
				inner.emitErr = errors.New("transient")
				return c.Emit(ctx, &sdk.RawRecord{})
			},
			wantCount: 3,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inner := &fakeSink{}
			tt.setup(inner)
			c := newCountingSink(inner, "")
			err := tt.ops(c)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCount, c.Count())
		})
	}
}

// TestCountingSinkFlushPassesThrough confirms Flush does not touch the
// counter — it's not an emit event.
func TestCountingSinkFlushPassesThrough(t *testing.T) {
	inner := &fakeSink{flushErr: errors.New("flush boom")}
	c := newCountingSink(inner, "")
	require.NoError(t, c.Emit(context.Background(), &sdk.RawRecord{}))
	require.Error(t, c.Flush(context.Background()))
	assert.Equal(t, int64(1), c.Count())
}

// TestClassifiedAsCtxExhausted pins the labeling rule that powers
// ErrKindCtxExhausted. Wrong answer here flips SQS and vendor monitoring into
// false-positive territory.
func TestClassifiedAsCtxExhausted(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		err  error
		want bool
	}{
		{
			name: "parent alive + ctx error → false (inner timeout is genuine)",
			ctx:  func() (context.Context, context.CancelFunc) { return context.Background(), func() {} },
			err:  context.DeadlineExceeded,
			want: false,
		},
		{
			name: "parent expired + DeadlineExceeded → true",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
				time.Sleep(2 * time.Millisecond)
				return ctx, cancel
			},
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "parent expired + Canceled → true",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			err:  context.Canceled,
			want: true,
		},
		{
			name: "parent expired + unrelated error → false",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			err:  errors.New("vendor 500"),
			want: false,
		},
		{
			name: "nil error → false",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			err:  nil,
			want: false,
		},
		{
			name: "wrapped DeadlineExceeded under expired parent → true",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			err:  fmt.Errorf("downstream sink: %w", context.DeadlineExceeded),
			want: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := tt.ctx()
			defer cancel()
			got := classifiedAsCtxExhausted(ctx, tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}
