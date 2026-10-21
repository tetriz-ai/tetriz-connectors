// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package reqctx_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

type testCtxKeys string

const (
	testKey1 testCtxKeys = "test-key-1"
	testKey2 testCtxKeys = "test-key-2"
)

func TestCloneCtx(t *testing.T) {
	t.Parallel()
	emptyCtx := context.Background()
	deadlineTime := time.Date(2021, time.September, 27, 0, 0, 0, 0, time.UTC)
	ctxWithDeadline, cancel := context.WithDeadline(emptyCtx, deadlineTime)
	defer cancel()

	ctxWithValue := context.WithValue(emptyCtx, testKey1, "")
	ctxWithValue = context.WithValue(ctxWithValue, testKey2, "test-val-2")

	tests := []struct {
		name          string
		ctx           context.Context
		want          context.Context
		wantKeyValues []struct {
			key testCtxKeys
			val string
		}
	}{
		{
			name: "don't derive context deadline",
			ctx:  ctxWithDeadline,
			want: context.Background(),
		},
		{
			name: "copy all context object",
			ctx:  ctxWithValue,
			want: context.Background(),
			wantKeyValues: []struct {
				key testCtxKeys
				val string
			}{
				{
					key: testKey1,
					val: "",
				},
				{
					key: testKey2,
					val: "test-val-2",
				},
			},
		},
	}
	for _, tt := range tests {
		// tt is mutated on each iteration, so keep a copy of it
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := reqctx.CloneCtx(tt.ctx)

			gotDeadline, _ := got.Deadline()
			wantDeadline, _ := tt.want.Deadline()

			if !gotDeadline.Equal(wantDeadline) {
				t.Errorf("want deadline %s got deadline %s", wantDeadline, gotDeadline)
				return
			}

			for _, wantKv := range tt.wantKeyValues {
				if !reflect.DeepEqual(got.Value(wantKv.key), wantKv.val) {
					t.Errorf("kv mis-match want %s got %s", wantKv.val, got.Value(wantKv.key))
					return
				}
			}
		})
	}
}
