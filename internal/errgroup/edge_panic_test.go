// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package errgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A panic in one goroutine of a WithContext group must surface as Wait's error
// and cancel the shared context, so the siblings that watch it stop instead of
// running to completion against a dead request.
func TestEdge_PanicCancelsTheGroupContext(t *testing.T) {
	g, ctx := WithContext(context.Background())
	stopped := make(chan error, 1)
	g.Go(func() error {
		select {
		case <-ctx.Done():
			stopped <- ctx.Err()
			return nil
		case <-time.After(5 * time.Second):
			stopped <- errors.New("sibling was never cancelled")
			return nil
		}
	})
	g.Go(func() error { panic("boom") })

	err := g.Wait()
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic in errgroup: boom")
	require.ErrorIs(t, <-stopped, context.Canceled)
}

// panic(nil) and a panic with an error value are both recovered into an error
// rather than reported as success.
func TestEdge_NilAndErrorPanicsAreRecovered(t *testing.T) {
	for name, f := range map[string]func() error{
		"nil":   func() error { panic(nil) },
		"error": func() error { panic(errors.New("typed")) },
	} {
		t.Run(name, func(t *testing.T) {
			g := New()
			g.Go(f)
			require.Error(t, g.Wait())
		})
	}
}

// SetLimit is honoured with panicking goroutines: a recovered panic releases
// its slot, so a limit of one does not deadlock the next Go.
func TestEdge_PanicReleasesItsLimitSlot(t *testing.T) {
	g := New()
	g.SetLimit(1)
	g.Go(func() error { panic("first") })
	done := make(chan struct{})
	go func() {
		g.Go(func() error { return nil })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a panicking goroutine kept its limit slot")
	}
	require.Error(t, g.Wait())
}
