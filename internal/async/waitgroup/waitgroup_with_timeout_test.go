// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package waitgroup

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSafeWaitCtx(t *testing.T) {
	var (
		badWg                         = &sync.WaitGroup{}
		goodWg                        = &sync.WaitGroup{}
		ctxSmallDeadline, cancelFuncS = context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
		ctxBigDeadline, cancelFuncB   = context.WithDeadline(context.Background(), time.Now().Add(1*time.Hour))
	)
	defer cancelFuncS()
	defer cancelFuncB()
	badWg.Add(1)
	goodWg.Add(1)
	goodWg.Done()
	type args struct {
		ctx context.Context
		wg  *sync.WaitGroup
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "wait group indefinite wait; should timeout",
			args: args{
				ctx: ctxSmallDeadline,
				wg:  badWg,
			},
			want: false,
		},
		{
			name: "wait group success before ctx timeout",
			args: args{
				ctx: ctxBigDeadline,
				wg:  goodWg,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SafeWaitCtx(tt.args.ctx, tt.args.wg); got != tt.want {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}

func TestSafeWait(t *testing.T) {
	var (
		badWg  = &sync.WaitGroup{}
		goodWg = &sync.WaitGroup{}
	)
	badWg.Add(1)
	goodWg.Add(1)
	goodWg.Done()
	type args struct {
		wg      *sync.WaitGroup
		timeout time.Duration
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "wait group indefinite wait; should timeout",
			args: args{
				wg:      badWg,
				timeout: 500 * time.Millisecond,
			},
			want: false,
		},
		{
			name: "wait group success before timeout",
			args: args{
				wg:      goodWg,
				timeout: 500 * time.Millisecond,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SafeWait(tt.args.wg, tt.args.timeout); got != tt.want {
				t.Errorf("SafeWait() = %v, want %v", got, tt.want)
			}
		})
	}
}
