// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter

import (
	"fmt"
	"time"
)

// RateLimitPrefix is the key segment between the namespace and the resource
// in every store key the limiter writes.
const RateLimitPrefix = "rate"

// RateLimit is one resource's budget: Rate calls per Period. For 100 calls a
// second, Rate is 100 and Period is one second.
type RateLimit struct {
	Rate   int64
	Period time.Duration
	// PriorityDistribution splits Rate between request priorities. Nil means
	// no split.
	PriorityDistribution *PriorityDistribution
}

// PriorityDistribution allocates percentages of a resource's rate to high,
// medium and low priority requests. It is best effort: a higher priority may
// borrow unused quota from a lower one, never the reverse.
type PriorityDistribution struct {
	Enabled       bool
	HighPercent   int64
	MediumPercent int64
	LowPercent    int64
}

// IsEnabled reports whether the split applies. A nil distribution is disabled.
func (p *PriorityDistribution) IsEnabled() bool { return p != nil && p.Enabled }

// High is the percentage of the rate for high priority requests.
func (p *PriorityDistribution) High() int64 {
	if p == nil {
		return 0
	}
	return p.HighPercent
}

// Medium is the percentage of the rate for medium priority requests.
func (p *PriorityDistribution) Medium() int64 {
	if p == nil {
		return 0
	}
	return p.MediumPercent
}

// Low is the percentage of the rate for low priority requests.
func (p *PriorityDistribution) Low() int64 {
	if p == nil {
		return 0
	}
	return p.LowPercent
}

// RateLimitConfig maps resource names to their budgets. Namespace prefixes
// every store key, so two services sharing one store keep separate windows.
// A config is read concurrently and must not be mutated after it is handed to
// a limiter.
type RateLimitConfig struct {
	ResourceMap map[string]*RateLimit
	Namespace   string
}

// GetPeriod returns the resource's window. It panics for a resource that is
// not configured; callers check IsResourcePresent first.
func (r *RateLimitConfig) GetPeriod(resource string) time.Duration {
	return r.ResourceMap[resource].Period
}

// GetWindowInNanoSeconds returns the resource's window in nanoseconds.
func (r *RateLimitConfig) GetWindowInNanoSeconds(resource string) int64 {
	return r.ResourceMap[resource].Period.Nanoseconds()
}

// GetMaxRequestCount returns the number of calls allowed per window.
func (r *RateLimitConfig) GetMaxRequestCount(resource string) int64 {
	return r.ResourceMap[resource].Rate
}

// GetMaxRequestCountForPriorityDistribution returns the resource's priority
// split, which may be nil.
func (r *RateLimitConfig) GetMaxRequestCountForPriorityDistribution(resource string) *PriorityDistribution {
	return r.ResourceMap[resource].PriorityDistribution
}

// GetKeyWithNamespace builds the store key for one caller of one resource.
func (r *RateLimitConfig) GetKeyWithNamespace(resource, configKey string) string {
	return fmt.Sprintf("%s:%s:%s:%s", r.Namespace, RateLimitPrefix, resource, configKey)
}

// IsResourcePresent reports whether the resource has a budget.
func (r *RateLimitConfig) IsResourcePresent(resource string) bool {
	_, ok := r.ResourceMap[resource]
	return ok
}
