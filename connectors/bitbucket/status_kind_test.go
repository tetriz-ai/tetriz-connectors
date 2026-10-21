// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

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

func TestBitbucketRawRequestsFailWithTheStatusKind(t *testing.T) {
	for _, tc := range statusKindCases {
		gw := func() *statusGateway { return &statusGateway{status: tc.status, header: tc.header} }
		t.Run("lineage file "+http.StatusText(tc.status), func(t *testing.T) {
			_, _, err := FileAtRevision(context.Background(), gw(), "org-1", "acme/widgets", "abc", "a.go")
			requireKind(t, err, tc.status, tc.want)
		})
		t.Run("defect scoring first parent "+http.StatusText(tc.status), func(t *testing.T) {
			_, err := bitbucketFirstParent(context.Background(), gw(), "org-1", "acme/widgets", "abc")
			requireKind(t, err, tc.status, tc.want)
		})
	}
}
