// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package errgroup wraps golang's sync/errgroup to handle panics in the function passed to error group to spawn new goroutine.
// The recovered panic will be converted into an error returned as the result of the function execution.
// A gRPC server's recovery interceptor does not cover a goroutine a handler spawns, so such a
// goroutine has to handle its panics within its own scope.
// Since goroutine doesn't propagate the panic to the one from which it was spawned, including recovery
// block in every newly spawned goroutine is the only way to avoid go process crashes.
package errgroup

import (
	"context"
	"fmt"
	"runtime/debug"

	"golang.org/x/sync/errgroup"
)

// Group provides an implementation of functions supported by golang.org/x/sync/errgroup.Group with safe recovery from panics.
type Group interface {
	// Go is a wrapper function of golang.org/x/sync/errgroup.Group.Go() which will recover from panic on executing func f() and return it as error.
	Go(f func() error)
	// Wait is an equivalent of golang.org/x/sync/errgroup.Group.Wait()
	Wait() error
	// SetLimit is golang.org/x/sync/errgroup.Group.SetLimit.
	SetLimit(n int)
}

type group struct {
	*errgroup.Group
}

// New returns a Group.
func New() Group {
	return &group{
		Group: &errgroup.Group{},
	}
}

// WithContext creates a golang.org/x/sync/errgroup.Group with the given context and wraps in 'group'
// which implements 'Group'
func WithContext(ctx context.Context) (Group, context.Context) {
	eg, ctx := errgroup.WithContext(ctx)
	return &group{eg}, ctx
}

func (g *group) Go(f func() error) {
	g.Group.Go(func() (err error) {
		// recover panic in the execution of input function 'f'
		defer func() {
			if r := recover(); r != nil {
				// set the recovered panic as error value in return
				err = fmt.Errorf("panic in errgroup: %v \n%s", r, string(debug.Stack()))
			}
		}()
		// call the input function 'f' and recover any panics in the above defer block
		return f()
	})
}
