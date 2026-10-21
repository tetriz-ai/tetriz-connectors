// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package reqctx

import (
	"context"
	"time"
)

// clonedCtx is a wrapper object which implements context.Context
// It copies all the values from a context without deriving
// deadlines from the parent context
// Ref- http://www.inanzzz.com/index.php/post/o75n/cloning-http-request-context-without-cancel-and-deadline-in-golang
type clonedCtx struct {
	ctx context.Context
}

func (*clonedCtx) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (*clonedCtx) Done() <-chan struct{} {
	return nil
}

func (*clonedCtx) Err() error {
	return nil
}

func (d *clonedCtx) Value(key interface{}) interface{} {
	return d.ctx.Value(key)
}

// CloneCtx deep clones parent context without deriving the context
func CloneCtx(ctx context.Context) context.Context {
	return &clonedCtx{ctx: ctx}
}
