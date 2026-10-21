// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package goroutine

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

const DefaultTimeout = 10 * time.Second

// Run executes a function safely in a goroutine. In case of a panic, it recovers and logs the error.
// Uses a new context object with a timeout to avoid getting cancelled when the main thread completes.
func Run(ctx context.Context, timeout time.Duration, fn func(ctx context.Context)) {
	clonedCtx := reqctx.CloneCtx(ctx)

	//nocustomlint:goroutine
	go func() {
		defer logger.RecoverPanicAndError(clonedCtx)

		ctxWithDeadline, cancelCtxWithDeadline := context.WithDeadline(clonedCtx, time.Now().Add(timeout))
		defer cancelCtxWithDeadline()
		fn(ctxWithDeadline)
	}()
}

// RunWithDefaultTimeout executes Run function with a default timeout of DefaultTimeout seconds
func RunWithDefaultTimeout(ctx context.Context, fn func(ctx context.Context)) {
	Run(ctx, DefaultTimeout, fn)
}

// RunWithCtx executes a function safely in a goroutine. In case of a panic, it recovers and logs the error.
// Uses the same context object as passed to the function
func RunWithCtx(ctx context.Context, fn func(ctx context.Context)) {
	//nocustomlint:goroutine
	go func() {
		defer logger.RecoverPanicAndError(ctx)
		fn(ctx)
	}()
}

// RunWithCtxAndPanicErr executes a function safely in a goroutine. In case of a panic, it recovers and sends the error in the given channel.
// Panic due to send on the given closed channel is not recovered
func RunWithCtxAndPanicErr(ctx context.Context, fn func(ctx context.Context), errCh chan error) {
	//nocustomlint:goroutine
	go func() {
		defer recoverError(errCh)
		fn(ctx)
	}()
}

func recoverError(errCh chan error) {
	if r := recover(); r != nil {
		errCh <- fmt.Errorf("panic recovered: %v", r)
	}
}
