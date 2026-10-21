// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// budgetBackend is one BudgetStore under test, with a TTL probe (whole
// seconds, -2 for a missing key) and a clock that expires TTLs.
type budgetBackend struct {
	store   BudgetStore
	ttl     func(key string) int64
	advance func(time.Duration)
}

var budgetBackends = []struct {
	name string
	make func(t *testing.T) budgetBackend
}{
	{"redis", func(t *testing.T) budgetBackend {
		mr := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return budgetBackend{
			store: NewRedisBudgetStore(rdb),
			ttl: func(key string) int64 {
				d := rdb.TTL(context.Background(), key).Val()
				if d < 0 {
					return int64(d)
				}
				return int64(d / time.Second)
			},
			advance: mr.FastForward,
		}
	}},
	{"memory", func(*testing.T) budgetBackend {
		clk := clockwork.NewFakeClock()
		s := NewInMemoryBudgetStore(clk)
		return budgetBackend{
			store: s,
			ttl: func(key string) int64 {
				s.mu.Lock()
				defer s.mu.Unlock()
				h, ok := s.hashes[key]
				if !ok || !clk.Now().Before(h.expires) {
					return -2
				}
				return (h.expires.Sub(clk.Now()).Milliseconds() + 500) / 1000
			},
			advance: clk.Advance,
		}
	}},
}

func githubHeaders(remaining, limit int, reset time.Time, resource string) http.Header {
	h := http.Header{
		"X-Ratelimit-Remaining": {strconv.Itoa(remaining)},
		"X-Ratelimit-Limit":     {strconv.Itoa(limit)},
		"X-Ratelimit-Reset":     {strconv.FormatInt(reset.Unix(), 10)},
	}
	if resource != "" {
		h.Set("X-Ratelimit-Resource", resource)
	}
	return h
}

// The BudgetLimiter behaves the same on the Redis and in-memory stores.
func TestBudgetStoreParity(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		run  func(t *testing.T, b budgetBackend, bl *BudgetLimiter)
	}{
		{"floors, buckets and a missing snapshot", func(t *testing.T, _ budgetBackend, bl *BudgetLimiter) {
			ok, wait, err := bl.Allow(ctx, "org", BucketGitHubCore, 3000)
			require.NoError(t, err)
			assert.True(t, ok, "no snapshot allows")
			assert.Zero(t, wait)

			reset := time.Now().Add(10 * time.Minute)
			require.NoError(t, bl.UpdateFromResponse(ctx, "org", githubHeaders(3000, 5000, reset, "core")))
			require.NoError(t, bl.UpdateFromResponse(ctx, "org", githubHeaders(4000, 5000, reset, "graphql")))
			ok, wait, err = bl.Allow(ctx, "org", BucketGitHubCore, 3000)
			require.NoError(t, err)
			assert.False(t, ok, "remaining at the floor denies")
			assert.Greater(t, wait, 9*time.Minute)
			ok, _, err = bl.Allow(ctx, "org", BucketGitHubGraphQL, 3000)
			require.NoError(t, err)
			assert.True(t, ok, "buckets are separate")
			ok, _, err = bl.Allow(ctx, "other-org", BucketGitHubCore, 3000)
			require.NoError(t, err)
			assert.True(t, ok, "orgs are separate")

			lim, observed, found, err := bl.GetLimit(ctx, "org", BucketGitHubCore)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, 5000, lim)
			assert.WithinDuration(t, time.Now(), observed, 5*time.Second)
		}},
		{"a reset in the past allows and gets the minimum TTL", func(t *testing.T, b budgetBackend, bl *BudgetLimiter) {
			require.NoError(t, bl.UpdateFromResponse(ctx, "org", githubHeaders(0, 5000, time.Now().Add(-time.Hour), "")))
			ok, _, err := bl.Allow(ctx, "org", BucketDefault, 10)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, int64(60), b.ttl("rate_budget:org:github"))
		}},
		{"the snapshot expires after reset plus grace", func(t *testing.T, b budgetBackend, bl *BudgetLimiter) {
			require.NoError(t, bl.UpdateFromResponse(ctx, "org", githubHeaders(1, 5000, time.Now().Add(20*time.Minute), "")))
			ttl := b.ttl("rate_budget:org:github")
			assert.InDelta(t, 30*60, ttl, 2)
			b.advance(time.Duration(ttl) * time.Second)
			assert.Equal(t, int64(-2), b.ttl("rate_budget:org:github"))
			_, _, found, err := bl.GetLimit(ctx, "org", BucketDefault)
			require.NoError(t, err)
			assert.False(t, found)
		}},
		{"missing headers write nothing", func(t *testing.T, b budgetBackend, bl *BudgetLimiter) {
			require.NoError(t, bl.UpdateFromResponse(ctx, "org", http.Header{}))
			assert.Equal(t, int64(-2), b.ttl("rate_budget:org:github"))
		}},
		{"partial and malformed snapshots", func(t *testing.T, b budgetBackend, bl *BudgetLimiter) {
			require.NoError(t, b.store.HashSet(ctx, "rate_budget:p:github", map[string]string{"limit": "900"}, time.Hour))
			ok, _, err := bl.Allow(ctx, "p", BucketDefault, 10)
			require.NoError(t, err)
			assert.True(t, ok, "no remaining or reset allows")
			lim, observed, found, err := bl.GetLimit(ctx, "p", BucketDefault)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, 900, lim)
			assert.True(t, observed.IsZero())

			require.NoError(t, b.store.HashSet(ctx, "rate_budget:p:github", map[string]string{"remaining": "x", "reset": "1"}, time.Hour))
			_, _, err = bl.Allow(ctx, "p", BucketDefault, 10)
			assert.Error(t, err, "malformed remaining")
			lim, _, _, err = bl.GetLimit(ctx, "p", BucketDefault)
			require.NoError(t, err)
			assert.Equal(t, 900, lim, "a write keeps the hash's other fields")

			vals, err := b.store.HashGet(ctx, "absent", "a", "b")
			require.NoError(t, err)
			assert.Equal(t, []*string{nil, nil}, vals)
			assert.Error(t, b.store.HashSet(ctx, "k", map[string]string{}, time.Hour), "HSET needs a field")
		}},
		{"concurrent writers and readers", func(t *testing.T, _ budgetBackend, bl *BudgetLimiter) {
			var wg sync.WaitGroup
			reset := time.Now().Add(time.Hour)
			for i := range 32 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					assert.NoError(t, bl.UpdateFromResponse(ctx, "org", githubHeaders(4000+i, 5000, reset, "core")))
					_, _, err := bl.Allow(ctx, "org", BucketGitHubCore, 3000)
					assert.NoError(t, err)
				}()
			}
			wg.Wait()
			ok, _, err := bl.Allow(ctx, "org", BucketGitHubCore, 3000)
			require.NoError(t, err)
			assert.True(t, ok)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, be := range budgetBackends {
				t.Run(be.name, func(t *testing.T) {
					b := be.make(t)
					tc.run(t, b, NewBudgetLimiterWithStore(b.store, GitHubAdapter{}))
				})
			}
		})
	}
}

func TestInMemoryBudgetStoreSweepsExpiredKeys(t *testing.T) {
	ctx := context.Background()
	clk := clockwork.NewFakeClock()
	s := NewInMemoryBudgetStore(clk)
	require.NoError(t, s.HashSet(ctx, "a", map[string]string{"f": "1"}, time.Minute))
	require.NoError(t, s.HashSet(ctx, "b", map[string]string{"f": "1"}, time.Hour))
	clk.Advance(2 * time.Minute)
	require.NoError(t, s.HashSet(ctx, "c", map[string]string{"f": "1"}, time.Hour))
	assert.Equal(t, 2, s.keyCount(), "a expired and was swept without being read")
	require.NoError(t, s.HashSet(ctx, "c", map[string]string{"f": "2"}, 0))
	assert.Equal(t, 1, s.keyCount(), "a zero TTL deletes the key")
}
