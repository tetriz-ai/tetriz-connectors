// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package goroutine

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

// Run detaches from the caller's cancellation but keeps its values, and bounds
// the work by its own timeout instead.
func TestEdge_RunOutlivesTheParentButKeepsItsValues(t *testing.T) {
	parent, cancel := context.WithCancel(reqctx.WithOrgID(context.Background(), "org-1"))
	cancel() // the request has already finished

	type result struct {
		org      string
		err      error
		deadline time.Time
	}
	got := make(chan result, 1)
	Run(parent, 200*time.Millisecond, func(ctx context.Context) {
		d, _ := ctx.Deadline()
		got <- result{org: reqctx.OrgIDFromContext(ctx), err: ctx.Err(), deadline: d}
	})
	r := <-got
	assert.Equal(t, "org-1", r.org)
	assert.NoError(t, r.err, "a cancelled parent must not cancel the detached goroutine")
	assert.WithinDuration(t, time.Now().Add(200*time.Millisecond), r.deadline, 200*time.Millisecond)
}

// A panic inside Run / RunWithCtx is recovered and does not take the process down.
func TestEdge_RunRecoversPanics(t *testing.T) {
	done := make(chan struct{}, 2)
	Run(context.Background(), time.Second, func(context.Context) {
		defer func() { done <- struct{}{} }()
		panic("run")
	})
	RunWithCtx(context.Background(), func(context.Context) {
		defer func() { done <- struct{}{} }()
		panic("with ctx")
	})
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("goroutine did not run")
		}
	}
}

// RunWithCtxAndPanicErr reports a panic on the channel, and reports nothing for
// a function that returns normally.
func TestEdge_RunWithCtxAndPanicErrReportsOnlyPanics(t *testing.T) {
	errCh := make(chan error, 1)
	RunWithCtxAndPanicErr(context.Background(), func(context.Context) { panic("kaboom") }, errCh)
	select {
	case err := <-errCh:
		require.ErrorContains(t, err, "panic recovered: kaboom")
	case <-time.After(5 * time.Second):
		t.Fatal("no panic reported")
	}

	ok := make(chan struct{})
	RunWithCtxAndPanicErr(context.Background(), func(context.Context) { close(ok) }, errCh)
	<-ok
	select {
	case err := <-errCh:
		t.Fatalf("a normal return reported %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}
