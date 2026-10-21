// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package errgroup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_GO_NewErrGroupHappyFlow(t *testing.T) {
	t.Parallel()
	a := require.New(t)

	g := New()
	c := &counter{}
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	err := g.Wait()
	a.NoError(err)
	a.Equal(int32(5), c.v, "errGroup didn't wait till all functions are executed and received wrong counter value as a result")
}

func Test_GO_NewErrGroupErrFlow(t *testing.T) {
	t.Parallel()
	a := require.New(t)

	g := New()
	c := &counter{}
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(panicHere)
	err := g.Wait()
	a.Error(err, "Panic is not recovered and returned as error")
	a.Equal(int32(5), c.v, "errGroup didn't wait till all functions are executed and received wrong counter value as a result")
}

func Test_GO_WithCtxHappyFlow(t *testing.T) {
	t.Parallel()
	a := require.New(t)

	ctx := context.Background()
	g, _ := WithContext(ctx)
	c := &counter{}
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	err := g.Wait()
	a.NoError(err)
	a.Equal(int32(5), c.v, "errGroup didn't wait till all functions are executed and received wrong counter value as a result")
}

func Test_GO_WithCtxErrFlow(t *testing.T) {
	t.Parallel()
	a := require.New(t)

	ctx := context.Background()
	g, _ := WithContext(ctx)
	c := &counter{}
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(c.Increment)
	g.Go(panicHere)
	err := g.Wait()
	a.Error(err, "Panic is not recovered and returned as error")
	a.Equal(int32(5), c.v, "errGroup didn't wait till all functions are executed and received wrong counter value as a result")
}

type counter struct {
	v int32
}

func (t *counter) Increment() error {
	time.Sleep(time.Millisecond * 100)
	atomic.AddInt32(&t.v, 1)
	return nil
}

func panicHere() error {
	panic("shipwreck")
}
