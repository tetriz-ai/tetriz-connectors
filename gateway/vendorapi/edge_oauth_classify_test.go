// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// RFC 6749 §5.2 error bodies are honoured whatever the status: an
// invalid_grant on a 400 is terminal, and a rate-limit error code on a 400
// is retryable with the vendor's Retry-After honoured.
func TestEdge_OAuthFetchErrorCodesOn400(t *testing.T) {
	cases := []struct {
		body       string
		retryAfter string
		wantKind   sdk.SyncErrorKind
		terminal   bool
		wantWait   int
	}{
		{`{"error":"invalid_grant","error_description":"revoked"}`, "", sdk.ErrKindAuth, true, 0},
		{`{"error":"too_many_requests"}`, "17", sdk.ErrKindRateLimit, false, 17},
		{`<html>bad gateway</html>`, "17", sdk.ErrKindVendorError, false, 0},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if tc.retryAfter != "" {
				w.Header().Set("Retry-After", tc.retryAfter)
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(tc.body))
		}))
		c := NewOAuthRefreshClassifier("jira")
		_, _, err := c.Fetch(context.Background(), "org", func(ctx context.Context) (*http.Request, error) {
			return http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
		})
		srv.Close()
		var se *sdk.SyncError
		require.True(t, errors.As(err, &se), tc.body)
		assert.Equal(t, tc.wantKind, se.Kind, tc.body)
		assert.Equal(t, tc.terminal, se.Terminal, tc.body)
		assert.Equal(t, tc.wantWait, se.RetryAfterSeconds, tc.body)
	}
}
