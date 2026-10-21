// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// statusGateway answers every raw request with one status, as a vendor that
// rejects the credential (401), throttles (429) or has no such resource (404)
// would.
type statusGateway struct {
	sdk.VendorGateway
	status int
	header http.Header
}

func (g *statusGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: g.status, Body: io.NopCloser(bytes.NewBufferString(`{"message":"no"}`)), Header: g.header}, nil
}
func (g *statusGateway) ForGroup(string) sdk.VendorGateway                      { return g }
func (g *statusGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return g }

var statusKindCases = []struct {
	status int
	header http.Header
	want   sdk.SyncErrorKind
}{
	{http.StatusUnauthorized, http.Header{}, sdk.ErrKindAuth},
	{http.StatusTooManyRequests, http.Header{"Retry-After": []string{"120"}}, sdk.ErrKindRateLimit},
}

// requireKind asserts that err carries a *sdk.SyncError of the wanted kind,
// with the vendor's Retry-After on a 429.
func requireKind(t *testing.T, err error, status int, want sdk.SyncErrorKind) {
	t.Helper()
	var se *sdk.SyncError
	require.True(t, errors.As(err, &se), "status %d: err=%v is not a *sdk.SyncError", status, err)
	assert.Equal(t, want, se.Kind)
	if status == http.StatusTooManyRequests {
		assert.Equal(t, 120, se.RetryAfterSeconds, "the Retry-After is kept")
	}
}

// fetchBranchesStatusGateway is the branch-test gateway with every raw
// request answered by one status.
type fetchBranchesStatusGateway struct {
	branchDoGateway
	status int
	header http.Header
}

func (g *fetchBranchesStatusGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: g.status, Body: io.NopCloser(bytes.NewBufferString(`{"message":"Bad credentials"}`)), Header: g.header}, nil
}
func (g *fetchBranchesStatusGateway) ForGroup(string) sdk.VendorGateway { return g }
func (g *fetchBranchesStatusGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}

// A 401, 404 or 429 on every repository fails fetch_branches with that
// status's kind, so a dead credential pauses the connector and a 429 keeps
// its Retry-After.
func TestFetchBranchesFailsWithTheStatusKind(t *testing.T) {
	cases := append(statusKindCases[:len(statusKindCases):len(statusKindCases)], struct {
		status int
		header http.Header
		want   sdk.SyncErrorKind
	}{http.StatusNotFound, http.Header{}, sdk.ErrKindNotFound})
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			gw := &fetchBranchesStatusGateway{status: tc.status, header: tc.header}
			_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchTestParams(testkit.NewChannelSink(), gw))
			requireKind(t, err, tc.status, tc.want)
		})
	}
}

func TestLineageBlobFetchFailsWithTheStatusKind(t *testing.T) {
	for _, tc := range statusKindCases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			_, err := ghBlobBytes(context.Background(), &statusGateway{status: tc.status, header: tc.header}, "org-1", "acme", "widgets", "abc")
			requireKind(t, err, tc.status, tc.want)
		})
	}
}

func TestDefectScoringDiffFetchFailsWithTheStatusKind(t *testing.T) {
	for _, tc := range statusKindCases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			_, err := fetchGitHubCulpritAddedLines(context.Background(), nil, &statusGateway{status: tc.status, header: tc.header}, "org-1", "acme/widgets", "abc", "a.go")
			requireKind(t, err, tc.status, tc.want)
		})
	}
}
