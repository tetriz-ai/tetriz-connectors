// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter

import (
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// RateLimitType classifies the nature of a rate-limit response from a vendor.
// Primary limits are hard budget exhaustion (fixed window). Secondary limits
// are abuse-detection signals requiring short backoff.
type RateLimitType int

const (
	NotRateLimited RateLimitType = iota
	PrimaryRateLimit
	SecondaryRateLimit
)

// BudgetSnapshot captures the remaining/limit/reset triple parsed from vendor
// rate-limit headers. ResetAt is absolute wall-clock time.
type BudgetSnapshot struct {
	Remaining int
	Limit     int
	ResetAt   time.Time
}

// RateLimitAdapter abstracts vendor-specific header and status semantics so
// the BudgetLimiter can be reused across GitHub, GitLab, Bitbucket, etc.
type RateLimitAdapter interface {
	// ParseBudget reads response headers and returns the current budget snapshot.
	// Returns nil when the response carries no budget signal.
	ParseBudget(h http.Header) *BudgetSnapshot

	// ClassifyError inspects an error response to distinguish primary vs
	// secondary rate limits and returns the suggested retry-after duration.
	ClassifyError(resp *http.Response) (RateLimitType, time.Duration)

	// ScopeKey returns the Redis key scope fragment.
	//
	// All current VCS adapters are org-scoped: GitHub uses installation tokens
	// (one principal per org), GitLab and Bitbucket use OAuth install/workspace
	// tokens which likewise present a single principal per connector config.
	// If a future vendor requires per-user scoping, add the user identifier as
	// an explicit arg rather than reviving a generic userID param — the
	// installation-token model should remain the default.
	ScopeKey(orgID string) string

	// Name returns the vendor identifier used in Redis keys and logs
	// (e.g. "github", "gitlab", "bitbucket").
	Name() string

	// BucketFromHeaders returns the rate-limit bucket the response was
	// counted against. Vendors with multiple independent buckets (GitHub's
	// core/graphql/search/code_search) signal this via a response header
	// (GitHub uses X-RateLimit-Resource). Vendors with a single bucket
	// return BucketDefault.
	BucketFromHeaders(h http.Header) Bucket
}

// GitHubAdapter implements RateLimitAdapter for GitHub App installations.
// Budget is scoped per installation (org), not per user. GitHub surfaces
// both primary (X-RateLimit-Remaining=0) and secondary (abuse) rate limits
// through 403/429 responses.
type GitHubAdapter struct{}

// Name returns the GitHub vendor identifier.
func (GitHubAdapter) Name() string { return "github" }

// ParseBudget extracts X-RateLimit-{Remaining,Limit,Reset} headers.
func (GitHubAdapter) ParseBudget(h http.Header) *BudgetSnapshot {
	rem, okR := parseIntHeader(h, "X-RateLimit-Remaining")
	lim, okL := parseIntHeader(h, "X-RateLimit-Limit")
	rst, okT := parseIntHeader(h, "X-RateLimit-Reset")
	if !okR || !okL || !okT {
		return nil
	}
	return &BudgetSnapshot{
		Remaining: rem,
		Limit:     lim,
		ResetAt:   time.Unix(int64(rst), 0),
	}
}

// ClassifyError distinguishes primary from secondary rate limits based on
// the presence of X-RateLimit-Remaining=0 versus Retry-After.
func (GitHubAdapter) ClassifyError(resp *http.Response) (RateLimitType, time.Duration) {
	if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests {
		return NotRateLimited, 0
	}
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		if rst, ok := parseIntHeader(resp.Header, "X-RateLimit-Reset"); ok {
			return PrimaryRateLimit, nonNegative(time.Until(time.Unix(int64(rst), 0)))
		}
		return PrimaryRateLimit, time.Minute
	}
	if ra, ok := parseIntHeader(resp.Header, "Retry-After"); ok {
		return SecondaryRateLimit, nonNegative(time.Duration(ra) * time.Second)
	}
	return SecondaryRateLimit, 30 * time.Second
}

// ScopeKey returns the org identifier — GitHub App installation tokens are
// shared across the org, so per-user scoping is unnecessary.
func (GitHubAdapter) ScopeKey(orgID string) string { return orgID }

// BucketFromHeaders maps GitHub's X-RateLimit-Resource header to a Bucket.
// GitHub responses always include this header for authenticated requests;
// values are "core", "graphql", "search", "code_search", "integration_manifest",
// "source_import", etc. Unknown or missing values map to BucketDefault rather
// than silently colliding with core.
func (GitHubAdapter) BucketFromHeaders(h http.Header) Bucket {
	switch h.Get("X-RateLimit-Resource") {
	case "core":
		return BucketGitHubCore
	case "graphql":
		return BucketGitHubGraphQL
	case "search":
		return BucketGitHubSearch
	default:
		return BucketDefault
	}
}

// GitLabAdapter implements RateLimitAdapter for GitLab's RFC draft-compliant
// rate-limit headers (no X- prefix) and 429 responses.
type GitLabAdapter struct{}

// Name returns the GitLab vendor identifier.
func (GitLabAdapter) Name() string { return "gitlab" }

// ParseBudget extracts RateLimit-{Remaining,Limit,Reset} headers.
func (GitLabAdapter) ParseBudget(h http.Header) *BudgetSnapshot {
	rem, okR := parseIntHeader(h, "RateLimit-Remaining")
	lim, okL := parseIntHeader(h, "RateLimit-Limit")
	rst, okT := parseIntHeader(h, "RateLimit-Reset")
	if !okR || !okL || !okT {
		return nil
	}
	return &BudgetSnapshot{
		Remaining: rem,
		Limit:     lim,
		ResetAt:   time.Unix(int64(rst), 0),
	}
}

// ClassifyError treats 429 as a primary rate limit; Retry-After drives backoff.
func (GitLabAdapter) ClassifyError(resp *http.Response) (RateLimitType, time.Duration) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return NotRateLimited, 0
	}
	if ra, ok := parseIntHeader(resp.Header, "Retry-After"); ok {
		return PrimaryRateLimit, nonNegative(time.Duration(ra) * time.Second)
	}
	return PrimaryRateLimit, time.Minute
}

// ScopeKey returns the org identifier — GitLab OAuth install tokens are
// single-principal per connector config.
func (GitLabAdapter) ScopeKey(orgID string) string { return orgID }

// BucketFromHeaders returns BucketDefault — GitLab has a single
// per-user/token budget, no bucket split.
func (GitLabAdapter) BucketFromHeaders(http.Header) Bucket { return BucketDefault }

// BitbucketAdapter implements RateLimitAdapter for Bitbucket Cloud's
// ~1000 req/hr per-user limit. Per Atlassian's published docs (not yet confirmed
// by a live OAuth header capture), Bitbucket sends no
// X-RateLimit-Remaining/Reset and no Retry-After for any current auth mode — only
// X-RateLimit-Limit/Resource/NearLimit, and those only for Access-Token/Forge auth.
// ParseBudget requires Remaining+Limit+Reset together, so it returns nil for every
// Bitbucket auth mode (OAuth, Access-Token, Forge) and the observed-cap scaling
// never activates; the connector paces in-op instead
// (pacing.ExecuteWaitingForLocalCapacity). Behavior here is unchanged — this note
// only records why the scaling path is inert.
type BitbucketAdapter struct{}

// Name returns the Bitbucket vendor identifier.
func (BitbucketAdapter) Name() string { return "bitbucket" }

// ParseBudget extracts X-RateLimit-{Remaining,Limit,Reset} headers.
func (BitbucketAdapter) ParseBudget(h http.Header) *BudgetSnapshot {
	rem, okR := parseIntHeader(h, "X-RateLimit-Remaining")
	lim, okL := parseIntHeader(h, "X-RateLimit-Limit")
	rst, okT := parseIntHeader(h, "X-RateLimit-Reset")
	if !okR || !okL || !okT {
		return nil
	}
	return &BudgetSnapshot{
		Remaining: rem,
		Limit:     lim,
		ResetAt:   time.Unix(int64(rst), 0),
	}
}

// ClassifyError treats 429 as a primary rate limit; Retry-After drives backoff.
func (BitbucketAdapter) ClassifyError(resp *http.Response) (RateLimitType, time.Duration) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return NotRateLimited, 0
	}
	if ra, ok := parseIntHeader(resp.Header, "Retry-After"); ok {
		return PrimaryRateLimit, nonNegative(time.Duration(ra) * time.Second)
	}
	return PrimaryRateLimit, time.Minute
}

// ScopeKey returns the org identifier — Bitbucket Cloud OAuth refresh-token
// flow presents a single principal per connector config.
func (BitbucketAdapter) ScopeKey(orgID string) string { return orgID }

// BucketFromHeaders returns BucketDefault — Bitbucket Cloud has a single
// per-user budget, no bucket split.
func (BitbucketAdapter) BucketFromHeaders(http.Header) Bucket { return BucketDefault }

// nonNegative clamps a wait to zero: a reset already in the past (clock skew,
// a delayed response) or a negative Retry-After asks for no wait.
func nonNegative(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

// parseIntHeader reads a header value as an integer. Returns (0, false) when
// the header is missing or not parseable.
func parseIntHeader(h http.Header, key string) (int, bool) {
	v := h.Get(key)
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		// Malformed numeric header from the vendor: signal not-parseable so
		// the caller falls back to its default backoff.
		logger.Warn("parsing integer rate-limit header", zap.String("header", key), zap.String("value", v), zap.Error(err))
		return 0, false
	}
	return n, true
}
