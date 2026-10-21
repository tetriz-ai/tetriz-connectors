// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/ratelimiter/metrics"
)

// defaultSweepInterval is how often the in-memory store scans every key for
// expired TTLs. Keys are also expired lazily whenever they are touched.
const defaultSweepInterval = time.Minute

// SlidingWindowLogInMemoryImpl is a ratelimiter.Store held in process memory.
// It mirrors SlidingWindowLogWithRedisImpl operation for operation: each key
// is a sorted multiset of float64 scores (Redis sorted-set scores are doubles,
// so the same rounding applies), a key whose last member is removed ceases to
// exist, and TTLs follow Redis EXPIRE semantics (whole seconds, a sub-second
// positive TTL becomes one second, a non-positive TTL deletes the key).
//
// Limits apply to this process only: two processes sharing a vendor account
// each get the full budget. Use the Redis store when more than one process
// calls the same vendor.
//
// It is safe for concurrent use. Every method runs under one mutex, which
// gives the same atomicity the Redis Lua scripts give.
type SlidingWindowLogInMemoryImpl struct {
	mu         sync.Mutex
	clk        clockwork.Clock
	sets       map[string]*memorySet
	sweepEvery time.Duration
	lastSweep  time.Time
}

type memorySet struct {
	scores  []float64 // ascending
	expires time.Time // zero: no TTL
}

// MemoryOption configures NewSlidingWindowLogInMemory.
type MemoryOption func(*SlidingWindowLogInMemoryImpl)

// WithMemoryClock replaces the wall clock. It drives TTL expiry and the
// timestamp AddMaxN books against (the Redis store reads the Redis server's
// TIME for the latter).
func WithMemoryClock(clk clockwork.Clock) MemoryOption {
	return func(s *SlidingWindowLogInMemoryImpl) { s.clk = clk }
}

// WithSweepInterval sets how often a call also scans every key and drops the
// expired ones, so a key that is never touched again does not stay in memory.
// The default is one minute. A non-positive interval sweeps on every call.
func WithSweepInterval(d time.Duration) MemoryOption {
	return func(s *SlidingWindowLogInMemoryImpl) { s.sweepEvery = d }
}

// NewSlidingWindowLogInMemory returns an empty in-process ratelimiter.Store.
func NewSlidingWindowLogInMemory(opts ...MemoryOption) *SlidingWindowLogInMemoryImpl {
	s := &SlidingWindowLogInMemoryImpl{
		clk:        clockwork.NewRealClock(),
		sets:       map[string]*memorySet{},
		sweepEvery: defaultSweepInterval,
	}
	for _, o := range opts {
		o(s)
	}
	s.lastSweep = s.clk.Now()
	return s
}

var _ ratelimiter.Store = &SlidingWindowLogInMemoryImpl{}

// lock takes the mutex, expires keys when a sweep is due, and returns the
// current time. Callers must Unlock.
func (s *SlidingWindowLogInMemoryImpl) lock(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, fmt.Errorf("in-memory rate limit store: %w", err)
	}
	s.mu.Lock()
	now := s.clk.Now()
	if now.Sub(s.lastSweep) >= s.sweepEvery {
		for k, set := range s.sets {
			if set.expired(now) {
				delete(s.sets, k)
			}
		}
		s.lastSweep = now
	}
	return now, nil
}

func (m *memorySet) expired(now time.Time) bool {
	return !m.expires.IsZero() && !now.Before(m.expires)
}

// get returns the live set for key, dropping it if its TTL has passed.
func (s *SlidingWindowLogInMemoryImpl) get(key string, now time.Time) *memorySet {
	set, ok := s.sets[key]
	if !ok {
		return nil
	}
	if set.expired(now) {
		delete(s.sets, key)
		return nil
	}
	return set
}

// count is ZCOUNT key from to: members with from <= score <= to.
func (s *SlidingWindowLogInMemoryImpl) count(key string, from, to float64, now time.Time) int64 {
	set := s.get(key, now)
	if set == nil || from > to {
		return 0
	}
	lo := sort.SearchFloat64s(set.scores, from)
	hi := sort.Search(len(set.scores), func(i int) bool { return set.scores[i] > to })
	if hi <= lo {
		return 0
	}
	return int64(hi - lo)
}

// add is ZADD of n new members at score. A new key has no TTL; an existing
// key keeps its TTL.
func (s *SlidingWindowLogInMemoryImpl) add(key string, score float64, n int64, now time.Time) {
	if n <= 0 {
		return
	}
	set := s.get(key, now)
	if set == nil {
		set = &memorySet{}
		s.sets[key] = set
	}
	at := sort.Search(len(set.scores), func(i int) bool { return set.scores[i] > score })
	grown := make([]float64, 0, len(set.scores)+int(n))
	grown = append(grown, set.scores[:at]...)
	for range n {
		grown = append(grown, score)
	}
	set.scores = append(grown, set.scores[at:]...)
}

// removeRange is ZREMRANGEBYSCORE key min max. It returns how many members
// it removed; a set left empty is deleted with its TTL, as in Redis.
func (s *SlidingWindowLogInMemoryImpl) removeRange(key string, minScore, maxScore float64, now time.Time) int64 {
	set := s.get(key, now)
	if set == nil || minScore > maxScore {
		return 0
	}
	lo := sort.SearchFloat64s(set.scores, minScore)
	hi := sort.Search(len(set.scores), func(i int) bool { return set.scores[i] > maxScore })
	if hi <= lo {
		return 0
	}
	kept := make([]float64, 0, len(set.scores)-(hi-lo))
	kept = append(kept, set.scores[:lo]...)
	set.scores = append(kept, set.scores[hi:]...)
	if len(set.scores) == 0 {
		delete(s.sets, key)
	}
	return int64(hi - lo)
}

// expire is EXPIRE key seconds: no-op on a missing key, delete on seconds <= 0.
func (s *SlidingWindowLogInMemoryImpl) expire(key string, seconds int64, now time.Time) {
	set := s.get(key, now)
	if set == nil {
		return
	}
	if seconds <= 0 {
		delete(s.sets, key)
		return
	}
	set.expires = now.Add(time.Duration(seconds) * time.Second)
}

// expireSeconds is go-redis's formatSec: whole seconds, and one second for a
// positive TTL under a second.
func expireSeconds(d time.Duration) int64 {
	if d > 0 && d < time.Second {
		return 1
	}
	return int64(d / time.Second)
}

func nanos(t time.Time) float64 { return float64(t.UnixNano()) }

// RemoveKeysBefore removes the members scored from 0 to removeBefore.
func (s *SlidingWindowLogInMemoryImpl) RemoveKeysBefore(ctx context.Context, key string, removeBefore time.Time) error {
	start := time.Now()
	now, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer s.mu.Unlock()
	s.removeRange(key, 0, nanos(removeBefore), now)
	metrics.RecordKeyRemoveTime(float64(time.Since(start).Milliseconds()))
	return nil
}

// IsWindowCountReached books one slot at toTime when fewer than maxRequest
// members fall in [fromTime, toTime], and reports whether it booked one.
func (s *SlidingWindowLogInMemoryImpl) IsWindowCountReached(ctx context.Context, key string, fromTime, toTime time.Time, maxRequest int64) (bool, error) {
	start := time.Now()
	defer func() { metrics.RecordIsWindowBreachedTime(float64(time.Since(start).Milliseconds())) }()
	now, err := s.lock(ctx)
	if err != nil {
		return false, err
	}
	defer s.mu.Unlock()
	used := s.count(key, nanos(fromTime), nanos(toTime), now)
	if used < maxRequest {
		s.add(key, nanos(toTime), 1, now)
	}
	return maxRequest-used > 0, nil
}

// RefreshKeyTTL sets key's TTL to period, as EXPIRE does.
func (s *SlidingWindowLogInMemoryImpl) RefreshKeyTTL(ctx context.Context, key string, period time.Duration) error {
	start := time.Now()
	defer func() { metrics.RecordKeyExpireTime(float64(time.Since(start).Milliseconds())) }()
	now, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer s.mu.Unlock()
	s.expire(key, expireSeconds(period), now)
	return nil
}

// AddMaxN books up to addN slots at the store's current time, microsecond
// precision as Redis TIME gives, and returns how many it booked and that time.
func (s *SlidingWindowLogInMemoryImpl) AddMaxN(ctx context.Context, key string, addN, maxRequestAllowed, durationNanos int64) (int64, int64, error) {
	now, err := s.lock(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer s.mu.Unlock()
	// The Redis script computes (micros * 1000) in Lua doubles; do the same so
	// the returned timestamp matches the stored score exactly.
	cur := float64(now.UnixMicro()) * 1000
	used := s.count(key, cur-float64(durationNanos), cur, now)
	toBook := min(maxRequestAllowed-used, addN)
	if toBook <= 0 {
		return 0, int64(cur), nil
	}
	s.add(key, cur, toBook, now)
	return toBook, int64(cur), nil
}

// RemoveMaxN removes up to removeN members scored exactly addedTimeNanos.
func (s *SlidingWindowLogInMemoryImpl) RemoveMaxN(ctx context.Context, key string, addedTimeNanos int64, removeN int64) (int64, error) {
	now, err := s.lock(ctx)
	if err != nil {
		return 0, err
	}
	defer s.mu.Unlock()
	set := s.get(key, now)
	if set == nil {
		return 0, nil
	}
	score := float64(addedTimeNanos)
	atScore := s.count(key, score, score, now)
	removeN = max(removeN, 0)
	removed := min(atScore, removeN)
	if removed == 0 {
		return 0, nil
	}
	// The Redis script removes every member at the score and re-adds the
	// surplus. When those were all the set's members the key is deleted in
	// between, so the re-added key has no TTL. Mirror that.
	wholeSet := atScore == int64(len(set.scores))
	s.removeRange(key, score, score, now)
	if rest := atScore - removed; rest > 0 {
		s.add(key, score, rest, now)
		if wholeSet {
			s.sets[key].expires = time.Time{}
		}
	}
	return removed, nil
}

// AddWithPriority books one slot at toTime in the set for priority, or, when
// that set's share is full, in the next lower priority's set (high spills to
// medium then low, medium to low, low nowhere). The booked set's TTL becomes
// expiry. A missing share for a level it has to check is an error, as in the
// Redis script.
func (s *SlidingWindowLogInMemoryImpl) AddWithPriority(
	ctx context.Context,
	key string,
	fromTime, toTime time.Time,
	maxRequestForPriority map[ratelimiter.Priority]int64,
	priority ratelimiter.Priority,
	expiry time.Duration,
) (bool, error) {
	start := time.Now()
	defer func() { metrics.RecordAddWithPriorityTime(float64(time.Since(start).Milliseconds())) }()
	now, err := s.lock(ctx)
	if err != nil {
		return false, err
	}
	defer s.mu.Unlock()
	from, to := nanos(fromTime), nanos(toTime)
	for _, level := range []ratelimiter.Priority{ratelimiter.PriorityHigh, ratelimiter.PriorityMedium, ratelimiter.PriorityLow} {
		// The script's checks are priority == 0, priority <= 1, priority <= 2.
		if priority > level || (level == ratelimiter.PriorityHigh && priority != level) {
			continue
		}
		limit, ok := maxRequestForPriority[level]
		if !ok {
			return false, fmt.Errorf("error checking window count and adding key to sorted set for %v: no limit for priority %s", key, level)
		}
		levelKey := getKeyWithPriority(key, level)
		if s.count(levelKey, from, to, now) < limit {
			s.add(levelKey, to, 1, now)
			s.expire(levelKey, expireSeconds(expiry), now)
			return true, nil
		}
	}
	return false, nil
}

// IsWindowsCountReached reports whether any window is full. When none is, it
// books one slot at each window's ToTime. A key named twice gets one slot, at
// the later window's ToTime, as one Redis member would.
func (s *SlidingWindowLogInMemoryImpl) IsWindowsCountReached(ctx context.Context, windowsParams []*ratelimiter.WindowParams, _ bool) (bool, error) {
	now, err := s.lock(ctx)
	if err != nil {
		return false, err
	}
	defer s.mu.Unlock()
	for _, wp := range windowsParams {
		if s.count(wp.Key, nanos(wp.FromTime), nanos(wp.ToTime), now) >= wp.MaxRequest {
			return true, nil
		}
	}
	last := make(map[string]float64, len(windowsParams))
	order := make([]string, 0, len(windowsParams))
	for _, wp := range windowsParams {
		if _, seen := last[wp.Key]; !seen {
			order = append(order, wp.Key)
		}
		last[wp.Key] = nanos(wp.ToTime)
	}
	for _, k := range order {
		s.add(k, last[k], 1, now)
	}
	return false, nil
}

// CleanAndExpire sets each key's TTL and removes its members scored at or
// before CleanBefore. A TTL that rounds to zero seconds or less is an error,
// raised at that key after the earlier keys were already processed, as in
// the Redis script.
func (s *SlidingWindowLogInMemoryImpl) CleanAndExpire(ctx context.Context, params []*ratelimiter.CleanAndExpireParams) error {
	now, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer s.mu.Unlock()
	for _, p := range params {
		ttl := getTTLToStore(p.TTL)
		if ttl <= 0 {
			return fmt.Errorf("could not clean and expire key %v: TTL must be a positive integer", p.Key)
		}
		s.expire(p.Key, ttl, now)
		s.removeRange(p.Key, math.Inf(-1), nanos(p.CleanBefore), now)
	}
	return nil
}

// keyCount is the number of keys held, expired or not.
func (s *SlidingWindowLogInMemoryImpl) keyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sets)
}
