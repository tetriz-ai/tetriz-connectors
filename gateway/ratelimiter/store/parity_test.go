// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
)

// The parity suite runs every case against the Redis store (on miniredis) and
// the in-memory store, with the same expectations, so the two cannot drift.

var t0 = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// backend is one Store under test plus the probes and clock the cases need.
type backend struct {
	store ratelimiter.Store
	// window is the store's IsWindowCountReached, called on the concrete
	// type: the interface method is deprecated but still what Acquire uses.
	window func(ctx context.Context, key string, fromTime, toTime time.Time, maxRequest int64) (bool, error)
	// card is ZCARD: how many members key holds (0 for a missing key).
	card func(key string) int64
	// ttl is Redis TTL in whole seconds: -2 for a missing key, -1 for none.
	ttl func(key string) int64
	// now is the backend's current time; advance moves it and expires TTLs.
	now     func() time.Time
	advance func(d time.Duration)
}

func redisBackend(t *testing.T) backend {
	t.Helper()
	mr := miniredis.RunT(t)
	cur := t0
	mr.SetTime(cur)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	var mu sync.Mutex
	rs := NewSlidingWindowLogWithRedis(rc)
	return backend{
		store:  rs,
		window: rs.IsWindowCountReached,
		card:   func(key string) int64 { return rc.ZCard(context.Background(), key).Val() },
		ttl: func(key string) int64 {
			d := rc.TTL(context.Background(), key).Val()
			if d < 0 {
				return int64(d)
			}
			return int64(d / time.Second)
		},
		now: func() time.Time { mu.Lock(); defer mu.Unlock(); return cur },
		advance: func(d time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			cur = cur.Add(d)
			mr.SetTime(cur)
			mr.FastForward(d)
		},
	}
}

func memoryBackend(t *testing.T) backend {
	t.Helper()
	clk := clockwork.NewFakeClockAt(t0)
	s := NewSlidingWindowLogInMemory(WithMemoryClock(clk))
	return backend{
		store:  s,
		window: s.IsWindowCountReached,
		card: func(key string) int64 {
			s.mu.Lock()
			defer s.mu.Unlock()
			if set := s.get(key, clk.Now()); set != nil {
				return int64(len(set.scores))
			}
			return 0
		},
		ttl: func(key string) int64 {
			s.mu.Lock()
			defer s.mu.Unlock()
			set := s.get(key, clk.Now())
			switch {
			case set == nil:
				return -2
			case set.expires.IsZero():
				return -1
			default:
				// Redis rounds the remaining milliseconds to the nearest second.
				return (set.expires.Sub(clk.Now()).Milliseconds() + 500) / 1000
			}
		},
		now:     clk.Now,
		advance: clk.Advance,
	}
}

var backends = []struct {
	name string
	make func(*testing.T) backend
}{
	{"redis", redisBackend},
	{"memory", memoryBackend},
}

// forEachBackend runs fn once per backend, each with a fresh store.
func forEachBackend(t *testing.T, fn func(t *testing.T, b backend)) {
	t.Helper()
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			fn(t, be.make(t))
		})
	}
}

var ctx = context.Background()

func TestParity(t *testing.T) {
	sec := time.Second
	cases := []struct {
		name string
		run  func(t *testing.T, b backend)
	}{
		{"IsWindowCountReached books up to the limit, bounds inclusive, and slides", func(t *testing.T, b backend) {
			for i := range 4 {
				ok, err := b.window(ctx, "k", t0.Add(-sec), t0, 3)
				require.NoError(t, err)
				assert.Equal(t, i < 3, ok, "call %d", i)
			}
			assert.Equal(t, int64(3), b.card("k"))
			assert.Equal(t, int64(-1), b.ttl("k"), "booking sets no TTL")
			// A window starting exactly at the booked score still counts it.
			ok, err := b.window(ctx, "k", t0, t0.Add(sec), 3)
			require.NoError(t, err)
			assert.False(t, ok)
			// A microsecond later the three slots have slid out. (Scores are
			// doubles, so near 2026 a nanosecond rounds away: both stores
			// resolve about 256ns.)
			ok, err = b.window(ctx, "k", t0.Add(time.Microsecond), t0.Add(sec), 3)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, int64(4), b.card("k"))
		}},
		{"IsWindowCountReached with a zero limit books nothing", func(t *testing.T, b backend) {
			ok, err := b.window(ctx, "k", t0.Add(-sec), t0, 0)
			require.NoError(t, err)
			assert.False(t, ok)
			assert.Equal(t, int64(-2), b.ttl("k"))
		}},
		{"AddMaxN books what fits at the store's time and the window slides with the clock", func(t *testing.T, b backend) {
			b.advance(1500 * time.Microsecond)
			added, ts, err := b.store.AddMaxN(ctx, "k", 3, 5, sec.Nanoseconds())
			require.NoError(t, err)
			assert.Equal(t, int64(3), added)
			assert.Equal(t, int64(float64(b.now().UnixMicro())*1000), ts)
			added, ts2, err := b.store.AddMaxN(ctx, "k", 5, 5, sec.Nanoseconds())
			require.NoError(t, err)
			assert.Equal(t, int64(2), added, "only the remainder of the limit")
			assert.Equal(t, ts, ts2)
			added, _, err = b.store.AddMaxN(ctx, "k", 1, 5, sec.Nanoseconds())
			require.NoError(t, err)
			assert.Equal(t, int64(0), added)

			b.advance(sec) // the window start lands exactly on the bookings
			added, _, err = b.store.AddMaxN(ctx, "k", 1, 5, sec.Nanoseconds())
			require.NoError(t, err)
			assert.Equal(t, int64(0), added, "window start is inclusive")

			b.advance(time.Millisecond)
			added, ts3, err := b.store.AddMaxN(ctx, "k", 2, 5, sec.Nanoseconds())
			require.NoError(t, err)
			assert.Equal(t, int64(2), added)
			assert.Greater(t, ts3, ts)
			assert.Equal(t, int64(7), b.card("k"))
		}},
		{"AddMaxN with n of zero books nothing", func(t *testing.T, b backend) {
			added, _, err := b.store.AddMaxN(ctx, "k", 0, 5, sec.Nanoseconds())
			require.NoError(t, err)
			assert.Equal(t, int64(0), added)
			assert.Equal(t, int64(0), b.card("k"))
		}},
		{"RemoveMaxN removes at most what was booked at that time", func(t *testing.T, b backend) {
			_, ts, err := b.store.AddMaxN(ctx, "k", 4, 10, sec.Nanoseconds())
			require.NoError(t, err)
			b.advance(time.Millisecond)
			_, other, err := b.store.AddMaxN(ctx, "k", 1, 10, sec.Nanoseconds())
			require.NoError(t, err)

			removed, err := b.store.RemoveMaxN(ctx, "k", ts, 0)
			require.NoError(t, err)
			assert.Equal(t, int64(0), removed)
			removed, err = b.store.RemoveMaxN(ctx, "k", ts, 3)
			require.NoError(t, err)
			assert.Equal(t, int64(3), removed)
			removed, err = b.store.RemoveMaxN(ctx, "k", ts, 3)
			require.NoError(t, err)
			assert.Equal(t, int64(1), removed)
			removed, err = b.store.RemoveMaxN(ctx, "k", ts+1000, 3)
			require.NoError(t, err)
			assert.Equal(t, int64(0), removed, "no member at that time")
			assert.Equal(t, int64(1), b.card("k"))

			removed, err = b.store.RemoveMaxN(ctx, "k", other, 5)
			require.NoError(t, err)
			assert.Equal(t, int64(1), removed)
			assert.Equal(t, int64(-2), b.ttl("k"), "an emptied set no longer exists")
			removed, err = b.store.RemoveMaxN(ctx, "missing", ts, 1)
			require.NoError(t, err)
			assert.Equal(t, int64(0), removed)
		}},
		{"RemoveMaxN of part of a set all at one time drops its TTL", func(t *testing.T, b backend) {
			_, ts, err := b.store.AddMaxN(ctx, "k", 3, 10, sec.Nanoseconds())
			require.NoError(t, err)
			require.NoError(t, b.store.RefreshKeyTTL(ctx, "k", time.Minute))
			require.Equal(t, int64(60), b.ttl("k"))
			removed, err := b.store.RemoveMaxN(ctx, "k", ts, 1)
			require.NoError(t, err)
			assert.Equal(t, int64(1), removed)
			assert.Equal(t, int64(2), b.card("k"))
			assert.Equal(t, int64(-1), b.ttl("k"))
		}},
		{"RemoveKeysBefore removes up to and including the cutoff", func(t *testing.T, b backend) {
			for i := range 3 {
				ok, err := b.window(ctx, "k", t0, t0.Add(time.Duration(i)*sec), 10)
				require.NoError(t, err)
				require.True(t, ok)
			}
			require.NoError(t, b.store.RemoveKeysBefore(ctx, "k", t0.Add(sec)))
			assert.Equal(t, int64(1), b.card("k"))
			require.NoError(t, b.store.RemoveKeysBefore(ctx, "k", t0.Add(5*sec)))
			assert.Equal(t, int64(-2), b.ttl("k"))
			require.NoError(t, b.store.RemoveKeysBefore(ctx, "missing", t0))
		}},
		{"RefreshKeyTTL follows EXPIRE and the key expires", func(t *testing.T, b backend) {
			require.NoError(t, b.store.RefreshKeyTTL(ctx, "k", time.Minute))
			assert.Equal(t, int64(-2), b.ttl("k"), "EXPIRE on a missing key creates nothing")

			_, err := b.window(ctx, "k", t0.Add(-sec), t0, 10)
			require.NoError(t, err)
			require.NoError(t, b.store.RefreshKeyTTL(ctx, "k", 2500*time.Millisecond))
			assert.Equal(t, int64(2), b.ttl("k"), "truncated to whole seconds")
			require.NoError(t, b.store.RefreshKeyTTL(ctx, "k", 300*time.Millisecond))
			assert.Equal(t, int64(1), b.ttl("k"), "sub-second becomes one second")
			b.advance(999 * time.Millisecond)
			assert.Equal(t, int64(1), b.card("k"))
			b.advance(time.Millisecond)
			assert.Equal(t, int64(-2), b.ttl("k"), "expired")
			assert.Equal(t, int64(0), b.card("k"))

			// A booking after expiry starts a fresh set with no TTL.
			ok, err := b.window(ctx, "k", t0.Add(-sec), t0, 1)
			require.NoError(t, err)
			assert.True(t, ok, "expired members no longer count")
			assert.Equal(t, int64(-1), b.ttl("k"))
			require.NoError(t, b.store.RefreshKeyTTL(ctx, "k", 0))
			assert.Equal(t, int64(-2), b.ttl("k"), "a zero TTL deletes the key")
		}},
		{"AddWithPriority spills down, never up, and sets the booked set's TTL", func(t *testing.T, b backend) {
			limits := map[ratelimiter.Priority]int64{ratelimiter.PriorityHigh: 1, ratelimiter.PriorityMedium: 1, ratelimiter.PriorityLow: 1}
			add := func(key string, p ratelimiter.Priority) bool {
				ok, err := b.store.AddWithPriority(ctx, key, t0.Add(-sec), t0, limits, p, 30*sec)
				require.NoError(t, err)
				return ok
			}
			for i := range 4 {
				assert.Equal(t, i < 3, add("h", ratelimiter.PriorityHigh), "high call %d", i)
			}
			for _, lvl := range []string{"0", "1", "2"} {
				assert.Equal(t, int64(1), b.card("h:"+lvl), "level %s", lvl)
				lvl := lvl
				assert.Eventually(t, func() bool { return b.ttl("h:"+lvl) == 30 }, 2*time.Second, 5*time.Millisecond, "level %s TTL", lvl)
			}

			assert.True(t, add("m", ratelimiter.PriorityMedium))
			assert.True(t, add("m", ratelimiter.PriorityMedium))
			assert.False(t, add("m", ratelimiter.PriorityMedium))
			assert.Equal(t, int64(0), b.card("m:0"), "medium never takes a high slot")
			assert.Equal(t, int64(1), b.card("m:1"))
			assert.Equal(t, int64(1), b.card("m:2"))

			assert.True(t, add("l", ratelimiter.PriorityLow))
			assert.False(t, add("l", ratelimiter.PriorityLow))
			assert.Equal(t, int64(0), b.card("l:0")+b.card("l:1"))

			assert.False(t, add("x", ratelimiter.Priority(3)), "an unknown priority books nothing")
			assert.Equal(t, int64(0), b.card("x:0")+b.card("x:1")+b.card("x:2"))
		}},
		{"AddWithPriority shares are per window", func(t *testing.T, b backend) {
			limits := map[ratelimiter.Priority]int64{ratelimiter.PriorityHigh: 1, ratelimiter.PriorityMedium: 0, ratelimiter.PriorityLow: 0}
			ok, err := b.store.AddWithPriority(ctx, "k", t0.Add(-sec), t0, limits, ratelimiter.PriorityHigh, sec)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = b.store.AddWithPriority(ctx, "k", t0.Add(-sec), t0, limits, ratelimiter.PriorityHigh, sec)
			require.NoError(t, err)
			require.False(t, ok)
			ok, err = b.store.AddWithPriority(ctx, "k", t0.Add(time.Microsecond), t0.Add(sec), limits, ratelimiter.PriorityHigh, sec)
			require.NoError(t, err)
			assert.True(t, ok)
		}},
		{"AddWithPriority without a share for a level it checks is an error", func(t *testing.T, b backend) {
			limits := map[ratelimiter.Priority]int64{ratelimiter.PriorityHigh: 0}
			_, err := b.store.AddWithPriority(ctx, "k", t0.Add(-sec), t0, limits, ratelimiter.PriorityHigh, sec)
			assert.Error(t, err)
		}},
		{"IsWindowsCountReached books every window or none", func(t *testing.T, b backend) {
			ws := func(maxB int64) []*ratelimiter.WindowParams {
				return []*ratelimiter.WindowParams{
					{Key: "a", FromTime: t0.Add(-sec), ToTime: t0, MaxRequest: 5},
					{Key: "b", FromTime: t0.Add(-sec), ToTime: t0, MaxRequest: maxB},
				}
			}
			reached, err := b.store.IsWindowsCountReached(ctx, ws(2), true)
			require.NoError(t, err)
			assert.False(t, reached)
			reached, err = b.store.IsWindowsCountReached(ctx, ws(2), true)
			require.NoError(t, err)
			assert.False(t, reached)
			reached, err = b.store.IsWindowsCountReached(ctx, ws(2), true)
			require.NoError(t, err)
			assert.True(t, reached)
			assert.Equal(t, int64(2), b.card("a"), "a breached window books nothing anywhere")
			assert.Equal(t, int64(2), b.card("b"))

			dup := []*ratelimiter.WindowParams{
				{Key: "d", FromTime: t0.Add(-sec), ToTime: t0, MaxRequest: 5},
				{Key: "d", FromTime: t0.Add(-sec), ToTime: t0.Add(sec), MaxRequest: 5},
			}
			reached, err = b.store.IsWindowsCountReached(ctx, dup, false)
			require.NoError(t, err)
			assert.False(t, reached)
			assert.Equal(t, int64(1), b.card("d"), "a key named twice is one member")
			ok, err := b.window(ctx, "d", t0.Add(time.Microsecond), t0.Add(sec), 1)
			require.NoError(t, err)
			assert.False(t, ok, "the member sits at the later window's end")
		}},
		{"CleanAndExpire trims and sets TTLs, rejecting a non-positive one", func(t *testing.T, b backend) {
			for i := range 3 {
				_, err := b.window(ctx, "k", t0, t0.Add(time.Duration(i)*sec), 10)
				require.NoError(t, err)
			}
			require.NoError(t, b.store.CleanAndExpire(ctx, []*ratelimiter.CleanAndExpireParams{
				{Key: "k", TTL: 10 * sec, CleanBefore: t0.Add(sec)},
				{Key: "missing", TTL: 10 * sec, CleanBefore: t0},
			}))
			assert.Equal(t, int64(1), b.card("k"))
			assert.Equal(t, int64(10), b.ttl("k"))
			assert.Equal(t, int64(-2), b.ttl("missing"))

			require.NoError(t, b.store.CleanAndExpire(ctx, []*ratelimiter.CleanAndExpireParams{{Key: "k", TTL: 0, CleanBefore: t0}}))
			assert.Equal(t, int64(1), b.ttl("k"), "a zero TTL is stored as one second")

			err := b.store.CleanAndExpire(ctx, []*ratelimiter.CleanAndExpireParams{{Key: "k", TTL: -5 * sec, CleanBefore: t0}})
			assert.Error(t, err)
			require.NoError(t, b.store.CleanAndExpire(ctx, nil))
		}},
		{"a rate limiter on an injected clock slides its window", func(t *testing.T, b backend) {
			clk := clockwork.NewFakeClockAt(b.now())
			conf := &ratelimiter.RateLimitConfig{
				Namespace:   "parity",
				ResourceMap: map[string]*ratelimiter.RateLimit{"r": {Rate: 2, Period: sec}},
			}
			rl := ratelimiter.NewRateLimiter(b.store, conf, ratelimiter.WithClock(clk))
			for i := range 3 {
				ok, err := rl.Acquire(ctx, "r", "org")
				require.NoError(t, err)
				assert.Equal(t, i < 2, ok, "acquire %d", i)
			}
			clk.Advance(sec + time.Millisecond)
			b.advance(sec + time.Millisecond)
			ok, err := rl.Acquire(ctx, "r", "org")
			require.NoError(t, err)
			assert.True(t, ok, "the window slid past the first two")

			n, _, err := rl.AcquireUptoN(ctx, "r", "bulk", 5, true)
			require.NoError(t, err)
			assert.Equal(t, int64(2), n)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			forEachBackend(t, tc.run)
		})
	}
}

// Concurrent callers never book past a limit on either store.
func TestParityConcurrency(t *testing.T) {
	const workers = 64
	run := func(t *testing.T, fn func() int64) int64 {
		t.Helper()
		var total atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				total.Add(fn())
			}()
		}
		close(start)
		wg.Wait()
		return total.Load()
	}
	limits := map[ratelimiter.Priority]int64{ratelimiter.PriorityHigh: 5, ratelimiter.PriorityMedium: 5, ratelimiter.PriorityLow: 5}

	forEachBackend(t, func(t *testing.T, b backend) {
		got := run(t, func() int64 {
			ok, err := b.window(ctx, "single", t0.Add(-time.Second), t0, 20)
			assert.NoError(t, err)
			if ok {
				return 1
			}
			return 0
		})
		assert.Equal(t, int64(20), got, "IsWindowCountReached")

		got = run(t, func() int64 {
			n, _, err := b.store.AddMaxN(ctx, "bulk", 3, 25, time.Second.Nanoseconds())
			assert.NoError(t, err)
			return n
		})
		assert.Equal(t, int64(25), got, "AddMaxN")

		got = run(t, func() int64 {
			ok, err := b.store.AddWithPriority(ctx, "prio", t0.Add(-time.Second), t0, limits, ratelimiter.PriorityHigh, time.Minute)
			assert.NoError(t, err)
			if ok {
				return 1
			}
			return 0
		})
		assert.Equal(t, int64(15), got, "AddWithPriority")

		got = run(t, func() int64 {
			reached, err := b.store.IsWindowsCountReached(ctx, []*ratelimiter.WindowParams{
				{Key: "w1", FromTime: t0.Add(-time.Second), ToTime: t0, MaxRequest: 10},
				{Key: "w2", FromTime: t0.Add(-time.Second), ToTime: t0, MaxRequest: 7},
			}, false)
			assert.NoError(t, err)
			if reached {
				return 0
			}
			return 1
		})
		assert.Equal(t, int64(7), got, "IsWindowsCountReached")
	})
}

// Expired keys leave memory even when nothing reads them again.
func TestMemorySweepDropsUntouchedExpiredKeys(t *testing.T) {
	clk := clockwork.NewFakeClockAt(t0)
	s := NewSlidingWindowLogInMemory(WithMemoryClock(clk), WithSweepInterval(time.Minute))
	for _, k := range []string{"a", "b", "c"} {
		_, err := s.IsWindowCountReached(ctx, k, t0.Add(-time.Second), t0, 10)
		require.NoError(t, err)
	}
	require.NoError(t, s.RefreshKeyTTL(ctx, "a", time.Second))
	require.NoError(t, s.RefreshKeyTTL(ctx, "b", time.Second))
	require.Equal(t, 3, s.keyCount())

	clk.Advance(30 * time.Second)
	_, err := s.IsWindowCountReached(ctx, "other", t0, t0, 10)
	require.NoError(t, err)
	assert.Equal(t, 4, s.keyCount(), "no sweep before the interval")

	clk.Advance(30 * time.Second)
	_, err = s.IsWindowCountReached(ctx, "other", t0, t0, 10)
	require.NoError(t, err)
	assert.Equal(t, 2, s.keyCount(), "a and b expired and were swept; c has no TTL")
}

func TestMemoryHonoursCancelledContext(t *testing.T) {
	s := NewSlidingWindowLogInMemory()
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := s.IsWindowCountReached(cctx, "k", t0, t0, 1)
	assert.ErrorIs(t, err, context.Canceled)
}
