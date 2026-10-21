// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package waitgroup

import (
	"context"
	"sync"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// SafeWait waits for the wait group to finish or times out after the specified duration.
// Returns true if the wait group finished before timeout, false otherwise.
func SafeWait(wg *sync.WaitGroup, timeout time.Duration) bool {
	// Create a channel to receive a signal when the waitgroup is done
	done := make(chan struct{})

	// Start a goroutine to wait for the waitgroup to finish
	//nocustomlint:goroutine
	go func() {
		defer logger.RecoverPanicAndError(context.Background())
		defer close(done)
		// This bare Wait IS the safe wait — the timeout is enforced by the select
		// below. Routing it through SafeWait would recurse into this same goroutine.
		//nocustomlint:waitgroup
		wg.Wait()
	}()

	// Select statement to wait for either the waitgroup to finish or the timeout duration
	select {
	case <-done:
		// Waitgroup finished before timeout
		return true
	case <-time.After(timeout):
		// Timeout occurred
		return false
	}
}

// SafeWaitWithDefaultTimeout waits for the wait group to finish or times out after 30 seconds.
// Returns true if the wait group finished before 30s, false otherwise.
func SafeWaitWithDefaultTimeout(wg *sync.WaitGroup) bool {
	return SafeWait(wg, 30*time.Second)
}

// SafeWaitCtx waits for the wait group to finish or times out after the context is cancelled or
// its deadline is exceeded. Returns true if the wait group finished before the context was
// cancelled or its deadline was exceeded, false otherwise.
func SafeWaitCtx(ctx context.Context, wg *sync.WaitGroup) bool {
	done := make(chan struct{})

	//nocustomlint:goroutine
	go func() {
		defer logger.RecoverPanicAndError(ctx)
		defer close(done)
		// This bare Wait IS the safe wait — the bound is the ctx.Done() select
		// below. Routing it through SafeWait would recurse into this same goroutine.
		//nocustomlint:waitgroup
		wg.Wait()
	}()

	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
