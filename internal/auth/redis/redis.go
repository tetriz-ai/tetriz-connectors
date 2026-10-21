// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package redis implements the auth token cache and lock over Redis.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/tetriz-ai/tetriz-connectors/auth"
)

// TokenCache stores values as plain strings under their keys. A caller that
// must not store a token in the clear encrypts it first.
type TokenCache struct {
	rdb goredis.Cmdable
}

// NewTokenCache returns a TokenCache over rdb.
func NewTokenCache(rdb goredis.Cmdable) *TokenCache { return &TokenCache{rdb: rdb} }

// Get implements auth.TokenCache.
func (c *TokenCache) Get(ctx context.Context, key string) (string, error) {
	v, err := c.rdb.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return "", auth.ErrCacheMiss
	}
	if err != nil {
		return "", fmt.Errorf("token cache get: %w", err)
	}
	return v, nil
}

// Set implements auth.TokenCache.
func (c *TokenCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := c.rdb.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("token cache set: %w", err)
	}
	return nil
}

// Delete implements auth.TokenCache.
func (c *TokenCache) Delete(ctx context.Context, key string) error {
	if err := c.rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("token cache delete: %w", err)
	}
	return nil
}

// Locker takes a lock with SET NX PX under a random owner token and releases
// it with a compare-and-delete script, so a holder whose lock expired cannot
// release a successor's.
type Locker struct {
	rdb goredis.Cmdable
}

// NewLocker returns a Locker over rdb.
func NewLocker(rdb goredis.Cmdable) *Locker { return &Locker{rdb: rdb} }

var unlockScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// TryLock implements auth.Locker.
func (l *Locker) TryLock(ctx context.Context, key string, ttl time.Duration) (auth.Lock, bool, error) {
	token := uuid.NewString()
	ok, err := l.rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("lock %s: %w", key, err)
	}
	if !ok {
		return nil, false, nil
	}
	return &lock{rdb: l.rdb, key: key, token: token}, true, nil
}

type lock struct {
	rdb   goredis.Cmdable
	key   string
	token string
}

func (k *lock) Unlock(ctx context.Context) error {
	if err := unlockScript.Run(ctx, k.rdb, []string{k.key}, k.token).Err(); err != nil {
		return fmt.Errorf("unlock %s: %w", k.key, err)
	}
	return nil
}
