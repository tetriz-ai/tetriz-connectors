// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestBudgetLimiter spins up an in-process Redis via miniredis and wires
// a BudgetLimiter against the GitHub adapter. The miniredis instance is
// cleaned up automatically at test end via miniredis.RunT.
func newTestBudgetLimiter(t *testing.T) (*BudgetLimiter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = rdb.Close()
	})
	return NewBudgetLimiter(rdb, GitHubAdapter{}), mr
}

// seedSnapshot writes a budget snapshot via UpdateFromResponse so tests
// exercise the same write path production code uses.
func seedSnapshot(t *testing.T, bl *BudgetLimiter, orgID string, remaining, limit int, resetAt time.Time) {
	t.Helper()
	h := http.Header{
		"X-Ratelimit-Remaining": []string{strconv.Itoa(remaining)},
		"X-Ratelimit-Limit":     []string{strconv.Itoa(limit)},
		"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt.Unix(), 10)},
	}
	if err := bl.UpdateFromResponse(context.Background(), orgID, h); err != nil {
		t.Fatalf("UpdateFromResponse: %v", err)
	}
}

func TestBudgetLimiter_AllowAboveFloor(t *testing.T) {
	bl, _ := newTestBudgetLimiter(t)
	seedSnapshot(t, bl, "org1", 3500, 5000, time.Now().Add(time.Hour))

	allowed, wait, err := bl.Allow(context.Background(), "org1", BucketDefault, 3000)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !allowed {
		t.Error("expected allowed=true when remaining > floor")
	}
	if wait != 0 {
		t.Errorf("expected wait=0 when allowed, got %v", wait)
	}
}

func TestBudgetLimiter_AllowBelowFloor(t *testing.T) {
	bl, _ := newTestBudgetLimiter(t)
	resetAt := time.Now().Add(10 * time.Minute)
	seedSnapshot(t, bl, "org1", 100, 5000, resetAt)

	allowed, wait, err := bl.Allow(context.Background(), "org1", BucketDefault, 3000)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if allowed {
		t.Error("expected allowed=false when remaining < floor")
	}
	if wait <= 0 {
		t.Errorf("expected positive wait duration, got %v", wait)
	}
	// Wait should be close to time-until-reset (within 1 minute of slack).
	if wait > time.Until(resetAt)+time.Second {
		t.Errorf("wait %v exceeds reset window %v", wait, time.Until(resetAt))
	}
}

func TestBudgetLimiter_AllowAtFloor(t *testing.T) {
	bl, _ := newTestBudgetLimiter(t)
	seedSnapshot(t, bl, "org1", 3000, 5000, time.Now().Add(time.Hour))

	// Allow uses strictly-greater-than semantics: remaining==floor denies.
	allowed, wait, err := bl.Allow(context.Background(), "org1", BucketDefault, 3000)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if allowed {
		t.Error("expected allowed=false when remaining == floor (strictly-greater semantic)")
	}
	if wait <= 0 {
		t.Errorf("expected positive wait, got %v", wait)
	}
}

func TestBudgetLimiter_AllowNoSnapshot(t *testing.T) {
	bl, _ := newTestBudgetLimiter(t)

	allowed, wait, err := bl.Allow(context.Background(), "org-unseeded", BucketDefault, 3000)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !allowed {
		t.Error("expected allowed=true when no snapshot exists (pre-first-request)")
	}
	if wait != 0 {
		t.Errorf("expected wait=0, got %v", wait)
	}
}

func TestBudgetLimiter_AllowAfterReset(t *testing.T) {
	bl, mr := newTestBudgetLimiter(t)
	// Seed with a reset time in the past (simulating a stale snapshot
	// after the budget has regenerated). We must bypass UpdateFromResponse'
	// TTL floor by writing directly via HSET — use miniredis-backed client.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	key := "rate_budget:org1:github"
	pastReset := time.Now().Add(-5 * time.Minute).Unix()
	if err := rdb.HSet(context.Background(), key,
		"remaining", 10,
		"limit", 5000,
		"reset", pastReset,
	).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	allowed, wait, err := bl.Allow(context.Background(), "org1", BucketDefault, 3000)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !allowed {
		t.Error("expected allowed=true when reset time has passed (budget regenerated)")
	}
	if wait != 0 {
		t.Errorf("expected wait=0 after reset, got %v", wait)
	}
}

func TestBudgetLimiter_UpdateFromResponse_RoundTrip(t *testing.T) {
	bl, mr := newTestBudgetLimiter(t)
	resetAt := time.Now().Add(45 * time.Minute).Truncate(time.Second)
	seedSnapshot(t, bl, "org42", 4200, 5000, resetAt)

	// Direct Redis read to confirm fields and TTL.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	key := "rate_budget:org42:github"

	vals, err := rdb.HGetAll(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if vals["remaining"] != "4200" {
		t.Errorf("remaining = %q, want 4200", vals["remaining"])
	}
	if vals["limit"] != "5000" {
		t.Errorf("limit = %q, want 5000", vals["limit"])
	}
	if vals["reset"] != strconv.FormatInt(resetAt.Unix(), 10) {
		t.Errorf("reset = %q, want %d", vals["reset"], resetAt.Unix())
	}

	ttl, err := rdb.TTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	// TTL should exceed time-until-reset (we add a 10-minute grace).
	if ttl < time.Until(resetAt) {
		t.Errorf("TTL %v < time-until-reset %v", ttl, time.Until(resetAt))
	}

	// Allow() round-trip: 4200 > 3000 should allow.
	allowed, _, err := bl.Allow(context.Background(), "org42", "", 3000)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !allowed {
		t.Error("expected allowed=true after round-trip")
	}
}

func TestBudgetLimiter_UpdateFromResponse_MissingHeaders(t *testing.T) {
	bl, mr := newTestBudgetLimiter(t)
	// Missing X-RateLimit-Reset — adapter returns nil snapshot.
	h := http.Header{
		"X-Ratelimit-Remaining": []string{"1000"},
		"X-Ratelimit-Limit":     []string{"5000"},
	}
	if err := bl.UpdateFromResponse(context.Background(), "org-none", h); err != nil {
		t.Fatalf("UpdateFromResponse: %v", err)
	}

	// No key written.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	exists, err := rdb.Exists(context.Background(), "rate_budget:org-none:github").Result()
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists != 0 {
		t.Error("expected no key written when headers incomplete")
	}
}

func TestBudgetLimiter_GetLimit(t *testing.T) {
	tests := []struct {
		name      string
		seed      bool
		remaining int
		limit     int
		resetAt   time.Time
		bucket    Bucket
		wantOK    bool
		wantLimit int
	}{
		{
			name:      "returns observed limit when snapshot exists",
			seed:      true,
			remaining: 4000,
			limit:     5000,
			resetAt:   time.Now().Add(30 * time.Minute),
			bucket:    BucketDefault,
			wantOK:    true,
			wantLimit: 5000,
		},
		{
			name:   "returns ok=false when no snapshot",
			seed:   false,
			bucket: BucketDefault,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bl, _ := newTestBudgetLimiter(t)
			if tt.seed {
				seedSnapshot(t, bl, "org-getlimit", tt.remaining, tt.limit, tt.resetAt)
			}

			limit, observedAt, ok, err := bl.GetLimit(context.Background(), "org-getlimit", tt.bucket)
			if err != nil {
				t.Fatalf("GetLimit: %v", err)
			}
			if ok != tt.wantOK {
				t.Errorf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if limit != tt.wantLimit {
				t.Errorf("limit = %d, want %d", limit, tt.wantLimit)
			}
			if time.Since(observedAt) > 5*time.Second {
				t.Errorf("observedAt %v older than expected", observedAt)
			}
		})
	}
}

func TestBudgetLimiter_UpdateFromResponse_PastResetUsesMinTTL(t *testing.T) {
	bl, mr := newTestBudgetLimiter(t)
	// Reset far enough in the past that time-until-reset + 10m grace
	// would be negative (below the 1-minute floor).
	pastReset := time.Now().Add(-20 * time.Minute)
	seedSnapshot(t, bl, "org-past", 10, 5000, pastReset)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ttl, err := rdb.TTL(context.Background(), "rate_budget:org-past:github").Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	// Expect the 1-minute minimum TTL floor to kick in (allow small slack).
	if ttl <= 0 || ttl > 90*time.Second {
		t.Errorf("expected TTL floored near 1m, got %v", ttl)
	}
}
