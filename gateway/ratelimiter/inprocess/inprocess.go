// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package inprocess bundles every store the built-in vendor gateways would
// otherwise keep in Redis, held in process memory instead, for a runner
// embedded in one process with no Redis.
//
// Everything is per process: rate limits, vendor budget snapshots, cached
// access tokens, token-refresh locks and the Bitbucket 429 circuit breaker.
// Two processes calling the same vendor account each get the full budget and
// may refresh the same token concurrently, so run one process, or use Redis.
//
// What each field replaces, in the runner's gateway wiring:
//
//	RateLimits        store.NewSlidingWindowLogWithRedis(rdb)   (ratelimiter.Store)
//	Budgets           ratelimiter.NewBudgetLimiter(rdb, adapter) (via BudgetLimiter)
//	Tokens            authredis.NewTokenCache(rdb) and authredis.NewLocker(rdb)
//	                  (auth.RefreshDeps.Cache and .Locker), and the client given to
//	                  tokencache.Forget (use tokencache.ForgetFromCache)
//	BitbucketCircuit  the redis.Cmdable given to the Bitbucket gateway constructors
//	GitLabCircuit     the redis.Cmdable given to the GitLab gateway constructors
package inprocess

import (
	"github.com/jonboulle/clockwork"

	"github.com/tetriz-ai/tetriz-connectors/auth/memory"
	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Stores is the in-process replacement for every Redis-backed store the
// built-in gateways take. Build it once per process and share it between all
// gateways, as the Redis client is shared. It is safe for concurrent use.
type Stores struct {
	// RateLimits is the ratelimiter.Store every built-in gateway takes.
	RateLimits *store.SlidingWindowLogInMemoryImpl
	// Budgets backs the vendor budget floors; see BudgetLimiter.
	Budgets *ratelimiter.InMemoryBudgetStore
	// Tokens is both auth.RefreshDeps.Cache and auth.RefreshDeps.Locker for
	// the OAuth gateways (GitLab, Bitbucket, Jira, Linear).
	Tokens *memory.Store
	// BitbucketCircuit is the Bitbucket gateways' 429 circuit-breaker store.
	BitbucketCircuit *gateway.InMemoryCircuitBreakerStore
	// GitLabCircuit is the GitLab gateways' 429 circuit-breaker store.
	GitLabCircuit *gateway.InMemoryCircuitBreakerStore
}

// New returns empty Stores on the wall clock.
func New() *Stores { return NewWithClock(clockwork.NewRealClock()) }

// NewWithClock returns empty Stores whose TTLs, and the time AddMaxN books
// against, follow clk. Rate-limit windows follow the clock the rate limiter
// itself is given.
func NewWithClock(clk clockwork.Clock) *Stores {
	return &Stores{
		RateLimits:       store.NewSlidingWindowLogInMemory(store.WithMemoryClock(clk)),
		Budgets:          ratelimiter.NewInMemoryBudgetStore(clk),
		Tokens:           &memory.Store{Now: clk.Now},
		BitbucketCircuit: gateway.NewInMemoryCircuitBreakerStore(clk),
		GitLabCircuit:    gateway.NewInMemoryCircuitBreakerStore(clk),
	}
}

// BudgetLimiter returns a BudgetLimiter for adapter on the shared budget
// store. Limiters for different adapters can share it: keys carry the
// adapter's name.
func (s *Stores) BudgetLimiter(adapter ratelimiter.RateLimitAdapter) *ratelimiter.BudgetLimiter {
	return ratelimiter.NewBudgetLimiterWithStore(s.Budgets, adapter)
}

// BudgetLimiters returns the budget limiters the runner gives the GitHub,
// GitLab and Bitbucket gateways, keyed by connector, on the shared store.
func (s *Stores) BudgetLimiters() map[sdk.ConnectorID]*ratelimiter.BudgetLimiter {
	return map[sdk.ConnectorID]*ratelimiter.BudgetLimiter{
		"github":    s.BudgetLimiter(ratelimiter.GitHubAdapter{}),
		"gitlab":    s.BudgetLimiter(ratelimiter.GitLabAdapter{}),
		"bitbucket": s.BudgetLimiter(ratelimiter.BitbucketAdapter{}),
	}
}
