// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
)

// The Redis keys the runner writes, and how Config.Namespace changes them.
// With an empty namespace every key keeps its unprefixed name; with namespace
// ns each one is "ns:" followed by that name:
//
//   - the leader lock ("scheduler-lock") and the asynq queues
//     ("connector-sync", "individual-connector-sync", "default"), which asynq
//     stores under "asynq:{<queue>}:..." (schedule.Options.Namespace);
//   - the gateways' rate-limit windows (ratelimiter.Store keys);
//   - the vendor budget snapshots of GitHub, GitLab and Bitbucket
//     (ratelimiter.BudgetStore keys);
//   - the cached OAuth access tokens and their refresh locks
//     (auth.TokenCache and auth.Locker keys);
//   - the Bitbucket 429 circuit breaker (gateway.CircuitBreakerStore keys).
//
// asynq's own bookkeeping of servers, workers and schedulers ("asynq:servers",
// "asynq:workers", "asynq:schedulers" and the "asynq:queues" set) stays shared:
// it names each process and queue, and routes no task.

// namespacePattern is what TETRIZ_NAMESPACE accepts: no spaces, colons or
// braces, which asynq uses in its key names.
var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

func validateNamespace(ns string) error {
	if ns == "" || namespacePattern.MatchString(ns) {
		return nil
	}
	return fmt.Errorf("TETRIZ_NAMESPACE %q: use letters, digits, '.', '_' and '-', starting with a letter or digit", ns)
}

// nsKey is key in namespace ns; see schedule.Namespaced.
func nsKey(ns, key string) string { return schedule.Namespaced(ns, key) }

// namespacedRateStore puts every rate-limit key in a namespace.
type namespacedRateStore struct {
	ns    string
	inner ratelimiter.Store
}

// NamespacedRateLimits returns s with every key in namespace ns, or s itself
// for an empty ns.
func NamespacedRateLimits(ns string, s ratelimiter.Store) ratelimiter.Store {
	if ns == "" {
		return s
	}
	return namespacedRateStore{ns: ns, inner: s}
}

func (s namespacedRateStore) RemoveKeysBefore(ctx context.Context, key string, removeBefore time.Time) error {
	return s.inner.RemoveKeysBefore(ctx, nsKey(s.ns, key), removeBefore)
}

func (s namespacedRateStore) IsWindowCountReached(ctx context.Context, key string, fromTime, toTime time.Time, maxRequest int64) (bool, error) {
	return windowCounter(s.inner).IsWindowCountReached(ctx, nsKey(s.ns, key), fromTime, toTime, maxRequest)
}

func (s namespacedRateStore) RefreshKeyTTL(ctx context.Context, key string, period time.Duration) error {
	return s.inner.RefreshKeyTTL(ctx, nsKey(s.ns, key), period)
}

func (s namespacedRateStore) AddMaxN(ctx context.Context, key string, addN, maxRequestAllowed, durationNanos int64) (int64, int64, error) {
	return s.inner.AddMaxN(ctx, nsKey(s.ns, key), addN, maxRequestAllowed, durationNanos)
}

func (s namespacedRateStore) RemoveMaxN(ctx context.Context, key string, addedTimeNanos, removeN int64) (int64, error) {
	return s.inner.RemoveMaxN(ctx, nsKey(s.ns, key), addedTimeNanos, removeN)
}

func (s namespacedRateStore) AddWithPriority(ctx context.Context, key string, fromTime, toTime time.Time, maxRequest map[ratelimiter.Priority]int64, priority ratelimiter.Priority, expiry time.Duration) (bool, error) {
	return s.inner.AddWithPriority(ctx, nsKey(s.ns, key), fromTime, toTime, maxRequest, priority, expiry)
}

func (s namespacedRateStore) IsWindowsCountReached(ctx context.Context, windowsParams []*ratelimiter.WindowParams, enableLogging bool) (bool, error) {
	out := make([]*ratelimiter.WindowParams, len(windowsParams))
	for i, p := range windowsParams {
		if p == nil {
			continue
		}
		c := *p
		c.Key = nsKey(s.ns, c.Key)
		out[i] = &c
	}
	return s.inner.IsWindowsCountReached(ctx, out, enableLogging)
}

func (s namespacedRateStore) CleanAndExpire(ctx context.Context, params []*ratelimiter.CleanAndExpireParams) error {
	out := make([]*ratelimiter.CleanAndExpireParams, len(params))
	for i, p := range params {
		if p == nil {
			continue
		}
		c := *p
		c.Key = nsKey(s.ns, c.Key)
		out[i] = &c
	}
	return s.inner.CleanAndExpire(ctx, out)
}

// windowCounter is the single-window check ratelimiter.Store still carries
// (deprecated there), which the wrapper forwards unchanged.
type windowCounter interface {
	IsWindowCountReached(ctx context.Context, key string, fromTime, toTime time.Time, maxRequest int64) (bool, error)
}

// namespacedBudgetStore puts every budget snapshot key in a namespace.
type namespacedBudgetStore struct {
	ns    string
	inner ratelimiter.BudgetStore
}

// NamespacedBudgets returns s with every key in namespace ns, or s itself for
// an empty ns.
func NamespacedBudgets(ns string, s ratelimiter.BudgetStore) ratelimiter.BudgetStore {
	if ns == "" {
		return s
	}
	return namespacedBudgetStore{ns: ns, inner: s}
}

func (s namespacedBudgetStore) HashGet(ctx context.Context, key string, fields ...string) ([]*string, error) {
	return s.inner.HashGet(ctx, nsKey(s.ns, key), fields...)
}

func (s namespacedBudgetStore) HashSet(ctx context.Context, key string, fields map[string]string, ttl time.Duration) error {
	return s.inner.HashSet(ctx, nsKey(s.ns, key), fields, ttl)
}

// namespacedTokens puts every cached-token and refresh-lock key in a
// namespace.
type namespacedTokens struct {
	ns     string
	cache  auth.TokenCache
	locker auth.Locker
}

// NamespacedTokenCache returns c with every key in namespace ns, or c itself
// for an empty ns.
func NamespacedTokenCache(ns string, c auth.TokenCache) auth.TokenCache {
	if ns == "" {
		return c
	}
	return namespacedTokens{ns: ns, cache: c}
}

// NamespacedLocker returns l with every key in namespace ns, or l itself for
// an empty ns.
func NamespacedLocker(ns string, l auth.Locker) auth.Locker {
	if ns == "" {
		return l
	}
	return namespacedTokens{ns: ns, locker: l}
}

func (t namespacedTokens) Get(ctx context.Context, key string) (string, error) {
	return t.cache.Get(ctx, nsKey(t.ns, key))
}

func (t namespacedTokens) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return t.cache.Set(ctx, nsKey(t.ns, key), value, ttl)
}

func (t namespacedTokens) Delete(ctx context.Context, key string) error {
	return t.cache.Delete(ctx, nsKey(t.ns, key))
}

func (t namespacedTokens) TryLock(ctx context.Context, key string, ttl time.Duration) (auth.Lock, bool, error) {
	return t.locker.TryLock(ctx, nsKey(t.ns, key), ttl)
}

// namespacedCircuit puts every Bitbucket circuit-breaker key in a namespace.
type namespacedCircuit struct {
	ns    string
	inner gateway.CircuitBreakerStore
}

// NamespacedCircuit returns s with every key in namespace ns, or s itself for
// an empty ns.
func NamespacedCircuit(ns string, s gateway.CircuitBreakerStore) gateway.CircuitBreakerStore {
	if ns == "" || s == nil {
		return s
	}
	return namespacedCircuit{ns: ns, inner: s}
}

func (c namespacedCircuit) keys(keys []string) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = nsKey(c.ns, k)
	}
	return out
}

func (c namespacedCircuit) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	return c.inner.PTTL(ctx, nsKey(c.ns, key))
}

func (c namespacedCircuit) Incr(ctx context.Context, key string) *redis.IntCmd {
	return c.inner.Incr(ctx, nsKey(c.ns, key))
}

func (c namespacedCircuit) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.inner.Expire(ctx, nsKey(c.ns, key), expiration)
}

func (c namespacedCircuit) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return c.inner.SetNX(ctx, nsKey(c.ns, key), value, expiration)
}

func (c namespacedCircuit) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return c.inner.Set(ctx, nsKey(c.ns, key), value, expiration)
}

func (c namespacedCircuit) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.inner.Exists(ctx, c.keys(keys)...)
}

func (c namespacedCircuit) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.inner.Del(ctx, c.keys(keys)...)
}
