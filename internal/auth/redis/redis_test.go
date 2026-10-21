// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth"
)

func client(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	mr := miniredis.RunT(t)
	return mr, goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

func TestTokenCache(t *testing.T) {
	ctx := context.Background()
	_, rc := client(t)
	c := NewTokenCache(rc)
	_, err := c.Get(ctx, "k")
	assert.True(t, errors.Is(err, auth.ErrCacheMiss))
	require.NoError(t, c.Set(ctx, "k", "v", time.Minute))
	v, err := c.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", v)
	require.NoError(t, c.Delete(ctx, "k"))
	_, err = c.Get(ctx, "k")
	assert.True(t, errors.Is(err, auth.ErrCacheMiss))
}

// The lock is held by one owner at a time, and an owner whose lease expired
// cannot release the lock a later owner now holds.
func TestLockerIsOwnerSafe(t *testing.T) {
	ctx := context.Background()
	mr, rc := client(t)
	l := NewLocker(rc)

	first, ok, err := l.TryLock(ctx, "lock", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = l.TryLock(ctx, "lock", time.Second)
	require.NoError(t, err)
	assert.False(t, ok, "a held lock is not granted twice")

	mr.FastForward(2 * time.Second) // the first lease expires
	second, ok, err := l.TryLock(ctx, "lock", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, first.Unlock(ctx))
	assert.True(t, mr.Exists("lock"), "a stale owner does not release the new owner's lock")
	require.NoError(t, second.Unlock(ctx))
	assert.False(t, mr.Exists("lock"))
}
