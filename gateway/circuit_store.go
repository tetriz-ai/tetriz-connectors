// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
)

// CircuitBreakerStore is the part of a Redis client the 429 circuit breaker
// uses. Any redis.Cmdable, such as a *redis.Client, satisfies it and shares
// the breaker across processes; NewInMemoryCircuitBreakerStore keeps it in
// this process.
type CircuitBreakerStore interface {
	PTTL(ctx context.Context, key string) *redis.DurationCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

var _ CircuitBreakerStore = redis.Cmdable(nil)

// InMemoryCircuitBreakerStore is a CircuitBreakerStore in process memory, for
// a runner with no Redis: the breaker then trips per process. Each command
// behaves as go-redis and Redis do for string keys, including TTL rounding
// (whole seconds for Expire; milliseconds for Set and SetNX with a sub-second
// or fractional-second expiration). Expired keys are dropped when touched and
// by a sweep at most once a minute. It is safe for concurrent use.
type InMemoryCircuitBreakerStore struct {
	mu        sync.Mutex
	clk       clockwork.Clock
	keys      map[string]cbEntry
	lastSweep time.Time
}

type cbEntry struct {
	value   string
	expires time.Time // zero: no TTL
}

// NewInMemoryCircuitBreakerStore returns an empty in-process store. clk
// drives TTL expiry; nil means the wall clock.
func NewInMemoryCircuitBreakerStore(clk clockwork.Clock) *InMemoryCircuitBreakerStore {
	if clk == nil {
		clk = clockwork.NewRealClock()
	}
	return &InMemoryCircuitBreakerStore{clk: clk, keys: map[string]cbEntry{}, lastSweep: clk.Now()}
}

var _ CircuitBreakerStore = (*InMemoryCircuitBreakerStore)(nil)

var errNotInteger = errors.New("ERR value is not an integer or out of range")

// lock takes the mutex, sweeps when due, and returns the current time.
func (m *InMemoryCircuitBreakerStore) lock() time.Time {
	m.mu.Lock()
	now := m.clk.Now()
	if now.Sub(m.lastSweep) >= time.Minute {
		for k, e := range m.keys {
			if e.expired(now) {
				delete(m.keys, k)
			}
		}
		m.lastSweep = now
	}
	return now
}

func (e cbEntry) expired(now time.Time) bool {
	return !e.expires.IsZero() && !now.Before(e.expires)
}

func (m *InMemoryCircuitBreakerStore) get(key string, now time.Time) (cbEntry, bool) {
	e, ok := m.keys[key]
	if !ok {
		return cbEntry{}, false
	}
	if e.expired(now) {
		delete(m.keys, key)
		return cbEntry{}, false
	}
	return e, true
}

// setTTL is the expiry a SET with this expiration gives, as go-redis sends it:
// PX in milliseconds (at least 1) when the duration is under a second or not
// whole seconds, EX in seconds otherwise.
func setTTL(now time.Time, expiration time.Duration) (time.Time, error) {
	if expiration < 0 {
		return time.Time{}, errors.New("ERR invalid expire time in 'set' command")
	}
	if expiration < time.Millisecond {
		expiration = time.Millisecond
	}
	if expiration < time.Second || expiration%time.Second != 0 {
		return now.Add(expiration.Truncate(time.Millisecond)), nil
	}
	return now.Add(expiration), nil
}

func formatValue(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case bool:
		if x {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprint(x)
	}
}

// PTTL implements CircuitBreakerStore: -2 for a missing key, -1 for no TTL.
func (m *InMemoryCircuitBreakerStore) PTTL(_ context.Context, key string) *redis.DurationCmd {
	now := m.lock()
	defer m.mu.Unlock()
	e, ok := m.get(key, now)
	switch {
	case !ok:
		return redis.NewDurationResult(-2, nil)
	case e.expires.IsZero():
		return redis.NewDurationResult(-1, nil)
	default:
		return redis.NewDurationResult(e.expires.Sub(now).Truncate(time.Millisecond), nil)
	}
}

// Incr implements CircuitBreakerStore. It keeps the key's TTL.
func (m *InMemoryCircuitBreakerStore) Incr(_ context.Context, key string) *redis.IntCmd {
	now := m.lock()
	defer m.mu.Unlock()
	e, ok := m.get(key, now)
	var n int64
	if ok {
		var err error
		if n, err = strconv.ParseInt(e.value, 10, 64); err != nil {
			return redis.NewIntResult(0, errNotInteger)
		}
	}
	n++
	e.value = strconv.FormatInt(n, 10)
	m.keys[key] = e
	return redis.NewIntResult(n, nil)
}

// Expire implements CircuitBreakerStore. A TTL under one second but above
// zero is one second; zero or less deletes the key.
func (m *InMemoryCircuitBreakerStore) Expire(_ context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	now := m.lock()
	defer m.mu.Unlock()
	e, ok := m.get(key, now)
	if !ok {
		return redis.NewBoolResult(false, nil)
	}
	secs := expiration / time.Second
	if expiration > 0 && expiration < time.Second {
		secs = 1
	}
	if secs <= 0 {
		delete(m.keys, key)
		return redis.NewBoolResult(true, nil)
	}
	e.expires = now.Add(secs * time.Second)
	m.keys[key] = e
	return redis.NewBoolResult(true, nil)
}

func (m *InMemoryCircuitBreakerStore) set(key string, value interface{}, expiration time.Duration, nx bool) (bool, error) {
	now := m.lock()
	defer m.mu.Unlock()
	old, exists := m.get(key, now)
	if nx && exists {
		return false, nil
	}
	e := cbEntry{value: formatValue(value)}
	switch expiration {
	case 0:
	case redis.KeepTTL:
		e.expires = old.expires
	default:
		exp, err := setTTL(now, expiration)
		if err != nil {
			return false, err
		}
		e.expires = exp
	}
	m.keys[key] = e
	return true, nil
}

// SetNX implements CircuitBreakerStore.
func (m *InMemoryCircuitBreakerStore) SetNX(_ context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return redis.NewBoolResult(m.set(key, value, expiration, true))
}

// Set implements CircuitBreakerStore.
func (m *InMemoryCircuitBreakerStore) Set(_ context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	if _, err := m.set(key, value, expiration, false); err != nil {
		return redis.NewStatusResult("", err)
	}
	return redis.NewStatusResult("OK", nil)
}

// Exists implements CircuitBreakerStore: how many of keys exist, a key named
// twice counting twice.
func (m *InMemoryCircuitBreakerStore) Exists(_ context.Context, keys ...string) *redis.IntCmd {
	now := m.lock()
	defer m.mu.Unlock()
	var n int64
	for _, k := range keys {
		if _, ok := m.get(k, now); ok {
			n++
		}
	}
	return redis.NewIntResult(n, nil)
}

// Del implements CircuitBreakerStore: how many of keys it deleted.
func (m *InMemoryCircuitBreakerStore) Del(_ context.Context, keys ...string) *redis.IntCmd {
	now := m.lock()
	defer m.mu.Unlock()
	var n int64
	for _, k := range keys {
		if _, ok := m.get(k, now); ok {
			delete(m.keys, k)
			n++
		}
	}
	return redis.NewIntResult(n, nil)
}

func (m *InMemoryCircuitBreakerStore) keyCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.keys)
}
