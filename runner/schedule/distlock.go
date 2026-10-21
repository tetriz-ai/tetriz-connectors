// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"github.com/go-redsync/redsync/v4"
	goredispool "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// DistLock abstracts a distributed mutex for testability.
type DistLock interface {
	Lock() error
	Unlock() (bool, error)
	Extend() (bool, error)
}

// DistLockFactory creates distributed locks.
type DistLockFactory interface {
	NewMutex(name string, options ...redsync.Option) DistLock
}

// redsyncLockFactory wraps *redsync.Redsync to implement DistLockFactory.
type redsyncLockFactory struct{ rs *redsync.Redsync }

func (f *redsyncLockFactory) NewMutex(name string, options ...redsync.Option) DistLock {
	return f.rs.NewMutex(name, options...)
}

// NewDistLockFactory creates a DistLockFactory backed by Redis via redsync.
func NewDistLockFactory(redisClient *redis.Client) DistLockFactory {
	pool := goredispool.NewPool(redisClient)
	return &redsyncLockFactory{rs: redsync.New(pool)}
}
