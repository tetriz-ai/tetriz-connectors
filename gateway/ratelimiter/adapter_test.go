// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestAdapter_ParseBudget(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).Truncate(time.Second)
	resetStr := strconv.FormatInt(resetAt.Unix(), 10)

	tests := []struct {
		name     string
		adapter  RateLimitAdapter
		headers  http.Header
		wantNil  bool
		wantRem  int
		wantLim  int
		wantRset time.Time
	}{
		{
			name:    "github valid headers",
			adapter: GitHubAdapter{},
			headers: http.Header{
				"X-Ratelimit-Remaining": []string{"4000"},
				"X-Ratelimit-Limit":     []string{"5000"},
				"X-Ratelimit-Reset":     []string{resetStr},
			},
			wantRem:  4000,
			wantLim:  5000,
			wantRset: resetAt,
		},
		{
			name:    "github missing remaining",
			adapter: GitHubAdapter{},
			headers: http.Header{
				"X-Ratelimit-Limit": []string{"5000"},
				"X-Ratelimit-Reset": []string{resetStr},
			},
			wantNil: true,
		},
		{
			name:    "github unparseable reset",
			adapter: GitHubAdapter{},
			headers: http.Header{
				"X-Ratelimit-Remaining": []string{"4000"},
				"X-Ratelimit-Limit":     []string{"5000"},
				"X-Ratelimit-Reset":     []string{"not-a-number"},
			},
			wantNil: true,
		},
		{
			name:    "gitlab valid headers (no X- prefix)",
			adapter: GitLabAdapter{},
			headers: http.Header{
				"Ratelimit-Remaining": []string{"1800"},
				"Ratelimit-Limit":     []string{"2000"},
				"Ratelimit-Reset":     []string{resetStr},
			},
			wantRem:  1800,
			wantLim:  2000,
			wantRset: resetAt,
		},
		{
			name:    "gitlab missing reset",
			adapter: GitLabAdapter{},
			headers: http.Header{
				"Ratelimit-Remaining": []string{"1800"},
				"Ratelimit-Limit":     []string{"2000"},
			},
			wantNil: true,
		},
		{
			name:    "bitbucket valid headers",
			adapter: BitbucketAdapter{},
			headers: http.Header{
				"X-Ratelimit-Remaining": []string{"900"},
				"X-Ratelimit-Limit":     []string{"1000"},
				"X-Ratelimit-Reset":     []string{resetStr},
			},
			wantRem:  900,
			wantLim:  1000,
			wantRset: resetAt,
		},
		{
			name:    "bitbucket no headers",
			adapter: BitbucketAdapter{},
			headers: http.Header{},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.adapter.ParseBudget(tt.headers)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("ParseBudget = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("ParseBudget returned nil, expected snapshot")
			}
			if got.Remaining != tt.wantRem {
				t.Errorf("Remaining = %d, want %d", got.Remaining, tt.wantRem)
			}
			if got.Limit != tt.wantLim {
				t.Errorf("Limit = %d, want %d", got.Limit, tt.wantLim)
			}
			if !got.ResetAt.Equal(tt.wantRset) {
				t.Errorf("ResetAt = %v, want %v", got.ResetAt, tt.wantRset)
			}
		})
	}
}

func TestAdapter_ClassifyError(t *testing.T) {
	resetIn30s := strconv.FormatInt(time.Now().Add(30*time.Second).Unix(), 10)

	tests := []struct {
		name        string
		adapter     RateLimitAdapter
		resp        *http.Response
		wantType    RateLimitType
		wantMinWait time.Duration
		wantMaxWait time.Duration
	}{
		{
			name:    "github 200 not rate-limited",
			adapter: GitHubAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{},
			},
			wantType: NotRateLimited,
		},
		{
			name:    "github 403 primary rate limit (remaining=0)",
			adapter: GitHubAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusForbidden,
				Header: http.Header{
					"X-Ratelimit-Remaining": []string{"0"},
					"X-Ratelimit-Reset":     []string{resetIn30s},
				},
			},
			wantType:    PrimaryRateLimit,
			wantMinWait: 5 * time.Second,
			wantMaxWait: 35 * time.Second,
		},
		{
			name:    "github 403 primary default when reset missing",
			adapter: GitHubAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusForbidden,
				Header: http.Header{
					"X-Ratelimit-Remaining": []string{"0"},
				},
			},
			wantType:    PrimaryRateLimit,
			wantMinWait: time.Minute,
			wantMaxWait: time.Minute,
		},
		{
			name:    "github 403 secondary with Retry-After",
			adapter: GitHubAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusForbidden,
				Header: http.Header{
					"Retry-After": []string{"60"},
				},
			},
			wantType:    SecondaryRateLimit,
			wantMinWait: 60 * time.Second,
			wantMaxWait: 60 * time.Second,
		},
		{
			name:    "github 429 secondary default wait",
			adapter: GitHubAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{},
			},
			wantType:    SecondaryRateLimit,
			wantMinWait: 30 * time.Second,
			wantMaxWait: 30 * time.Second,
		},
		{
			name:    "gitlab 429 primary with Retry-After",
			adapter: GitLabAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header: http.Header{
					"Retry-After": []string{"45"},
				},
			},
			wantType:    PrimaryRateLimit,
			wantMinWait: 45 * time.Second,
			wantMaxWait: 45 * time.Second,
		},
		{
			name:    "gitlab 429 primary default wait",
			adapter: GitLabAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{},
			},
			wantType:    PrimaryRateLimit,
			wantMinWait: time.Minute,
			wantMaxWait: time.Minute,
		},
		{
			name:    "gitlab 200 not rate-limited",
			adapter: GitLabAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{},
			},
			wantType: NotRateLimited,
		},
		{
			name:    "gitlab 403 not rate-limited (auth failure)",
			adapter: GitLabAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     http.Header{},
			},
			wantType: NotRateLimited,
		},
		{
			name:    "bitbucket 429 primary with Retry-After",
			adapter: BitbucketAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header: http.Header{
					"Retry-After": []string{"15"},
				},
			},
			wantType:    PrimaryRateLimit,
			wantMinWait: 15 * time.Second,
			wantMaxWait: 15 * time.Second,
		},
		{
			name:    "bitbucket 429 primary default wait",
			adapter: BitbucketAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{},
			},
			wantType:    PrimaryRateLimit,
			wantMinWait: time.Minute,
			wantMaxWait: time.Minute,
		},
		{
			name:    "bitbucket 500 not rate-limited",
			adapter: BitbucketAdapter{},
			resp: &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     http.Header{},
			},
			wantType: NotRateLimited,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotWait := tt.adapter.ClassifyError(tt.resp)
			if gotType != tt.wantType {
				t.Errorf("type = %v, want %v", gotType, tt.wantType)
			}
			if tt.wantType == NotRateLimited {
				return
			}
			if gotWait < tt.wantMinWait || gotWait > tt.wantMaxWait {
				t.Errorf("wait = %v, want in [%v, %v]", gotWait, tt.wantMinWait, tt.wantMaxWait)
			}
		})
	}
}

func TestAdapter_ScopeKey(t *testing.T) {
	tests := []struct {
		name    string
		adapter RateLimitAdapter
		orgID   string
		want    string
	}{
		{
			name:    "github org-only scope",
			adapter: GitHubAdapter{},
			orgID:   "org-abc",
			want:    "org-abc",
		},
		{
			name:    "gitlab org-only scope (install-token single principal)",
			adapter: GitLabAdapter{},
			orgID:   "org-abc",
			want:    "org-abc",
		},
		{
			name:    "bitbucket org-only scope (OAuth single principal)",
			adapter: BitbucketAdapter{},
			orgID:   "org-abc",
			want:    "org-abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.adapter.ScopeKey(tt.orgID)
			if got != tt.want {
				t.Errorf("ScopeKey = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAdapter_Name(t *testing.T) {
	tests := []struct {
		adapter RateLimitAdapter
		want    string
	}{
		{GitHubAdapter{}, "github"},
		{GitLabAdapter{}, "gitlab"},
		{BitbucketAdapter{}, "bitbucket"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.adapter.Name(); got != tt.want {
				t.Errorf("Name = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAdapter_BucketFromHeaders(t *testing.T) {
	tests := []struct {
		name    string
		adapter RateLimitAdapter
		header  string
		value   string
		want    Bucket
	}{
		{"github core", GitHubAdapter{}, "X-RateLimit-Resource", "core", BucketGitHubCore},
		{"github graphql", GitHubAdapter{}, "X-RateLimit-Resource", "graphql", BucketGitHubGraphQL},
		{"github search", GitHubAdapter{}, "X-RateLimit-Resource", "search", BucketGitHubSearch},
		{"github unknown resource", GitHubAdapter{}, "X-RateLimit-Resource", "code_search", BucketDefault},
		{"github missing header", GitHubAdapter{}, "", "", BucketDefault},
		{"gitlab always default", GitLabAdapter{}, "RateLimit-Resource", "core", BucketDefault},
		{"bitbucket always default", BitbucketAdapter{}, "X-RateLimit-Resource", "core", BucketDefault},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			if tt.header != "" {
				h.Set(tt.header, tt.value)
			}
			if got := tt.adapter.BucketFromHeaders(h); got != tt.want {
				t.Errorf("BucketFromHeaders = %q, want %q", got, tt.want)
			}
		})
	}
}
