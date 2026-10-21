// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
)

// BudgetStore holds the BudgetLimiter's snapshots: one string hash per key,
// each key with a TTL.
type BudgetStore interface {
	// HashGet returns the named fields of key's hash in order, nil for a
	// field or key that is not there.
	HashGet(ctx context.Context, key string, fields ...string) ([]*string, error)
	// HashSet writes fields into key's hash, keeping its other fields, and
	// sets the key's TTL, both at once.
	HashSet(ctx context.Context, key string, fields map[string]string, ttl time.Duration) error
}

// NewRedisBudgetStore returns a BudgetStore on Redis, shared by every process
// using the same Redis.
func NewRedisBudgetStore(rdb redis.Cmdable) BudgetStore { return redisBudgetStore{rdb: rdb} }

type redisBudgetStore struct{ rdb redis.Cmdable }

func (r redisBudgetStore) HashGet(ctx context.Context, key string, fields ...string) ([]*string, error) {
	vals, err := r.rdb.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, fmt.Errorf("hmget %s: %w", key, err)
	}
	out := make([]*string, len(fields))
	for i := range fields {
		if i < len(vals) {
			if s, ok := vals[i].(string); ok {
				out[i] = &s
			}
		}
	}
	return out, nil
}

func (r redisBudgetStore) HashSet(ctx context.Context, key string, fields map[string]string, ttl time.Duration) error {
	pipe := r.rdb.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("hset %s: %w", key, err)
	}
	return nil
}

// InMemoryBudgetStore is a BudgetStore in process memory, for a process with
// no Redis. Snapshots are then per process. TTLs follow Redis EXPIRE (whole
// seconds; zero or less deletes). Expired keys are dropped when read and by a
// sweep at most once a minute on write. It is safe for concurrent use.
type InMemoryBudgetStore struct {
	mu        sync.Mutex
	clk       clockwork.Clock
	hashes    map[string]*budgetHash
	lastSweep time.Time
}

type budgetHash struct {
	fields  map[string]string
	expires time.Time
}

// NewInMemoryBudgetStore returns an empty InMemoryBudgetStore. clk drives
// TTL expiry; nil means the wall clock.
func NewInMemoryBudgetStore(clk clockwork.Clock) *InMemoryBudgetStore {
	if clk == nil {
		clk = clockwork.NewRealClock()
	}
	return &InMemoryBudgetStore{clk: clk, hashes: map[string]*budgetHash{}, lastSweep: clk.Now()}
}

var _ BudgetStore = (*InMemoryBudgetStore)(nil)

// HashGet implements BudgetStore.
func (m *InMemoryBudgetStore) HashGet(ctx context.Context, key string, fields ...string) ([]*string, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("in-memory budget store: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*string, len(fields))
	h, ok := m.hashes[key]
	if !ok {
		return out, nil
	}
	if !m.clk.Now().Before(h.expires) {
		delete(m.hashes, key)
		return out, nil
	}
	for i, f := range fields {
		if v, ok := h.fields[f]; ok {
			out[i] = &v
		}
	}
	return out, nil
}

// HashSet implements BudgetStore.
func (m *InMemoryBudgetStore) HashSet(ctx context.Context, key string, fields map[string]string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("in-memory budget store: %w", err)
	}
	if len(fields) == 0 {
		return fmt.Errorf("hset %s: no fields", key)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clk.Now()
	if now.Sub(m.lastSweep) >= time.Minute {
		for k, h := range m.hashes {
			if !now.Before(h.expires) {
				delete(m.hashes, k)
			}
		}
		m.lastSweep = now
	}
	secs := ttl / time.Second
	if ttl > 0 && ttl < time.Second {
		secs = 1
	}
	if secs <= 0 {
		delete(m.hashes, key)
		return nil
	}
	h, ok := m.hashes[key]
	if !ok || !now.Before(h.expires) {
		h = &budgetHash{fields: map[string]string{}}
		m.hashes[key] = h
	}
	for f, v := range fields {
		h.fields[f] = v
	}
	h.expires = now.Add(secs * time.Second)
	return nil
}

func (m *InMemoryBudgetStore) keyCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.hashes)
}
