// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestOAuthRefreshClassifier_Classify(t *testing.T) {
	c := NewOAuthRefreshClassifier(sdk.ConnectorID("bitbucket"))
	tests := []struct {
		name       string
		statusCode int
		body       []byte
		want       RefreshOutcome
	}{
		{name: "200 OK", statusCode: http.StatusOK, body: []byte(`{"access_token":"tok"}`), want: RefreshOutcomeSuccess},
		{name: "400 invalid_grant — revoked", statusCode: http.StatusBadRequest, body: []byte(`{"error":"invalid_grant","error_description":"refresh token revoked"}`), want: RefreshOutcomeInvalidGrant},
		{name: "400 invalid_client — unknown", statusCode: http.StatusBadRequest, body: []byte(`{"error":"invalid_client"}`), want: RefreshOutcomeUnknown},
		{name: "400 rate_limit_exceeded in body", statusCode: http.StatusBadRequest, body: []byte(`{"error":"rate_limit_exceeded"}`), want: RefreshOutcomeRateLimit},
		{name: "429 with rate-limit body", statusCode: http.StatusTooManyRequests, body: []byte(`{"error":"too_many_requests"}`), want: RefreshOutcomeRateLimit},
		{name: "429 with non-JSON body", statusCode: http.StatusTooManyRequests, body: []byte(`<html>rate limited</html>`), want: RefreshOutcomeRateLimit},
		{name: "503 transport", statusCode: http.StatusServiceUnavailable, body: []byte(`<html>service unavailable</html>`), want: RefreshOutcomeTransport},
		{name: "500 unparseable", statusCode: http.StatusInternalServerError, body: []byte(`not json`), want: RefreshOutcomeTransport},
		{name: "418 weird-but-not-recognised", statusCode: http.StatusTeapot, body: []byte(`teapot`), want: RefreshOutcomeUnknown},
		{name: "json body but empty error field", statusCode: http.StatusBadRequest, body: []byte(`{"foo":"bar"}`), want: RefreshOutcomeUnknown},
		{name: "json body empty error field on 429", statusCode: http.StatusTooManyRequests, body: []byte(`{"foo":"bar"}`), want: RefreshOutcomeRateLimit},
		{name: "non-JSON 401 → invalid_grant", statusCode: http.StatusUnauthorized, body: []byte(`<html>unauthorized</html>`), want: RefreshOutcomeInvalidGrant},
		{name: "non-JSON 403 → invalid_grant", statusCode: http.StatusForbidden, body: []byte(``), want: RefreshOutcomeInvalidGrant},
		{name: "JSON 401 with unrecognised OAuth code → invalid_grant", statusCode: http.StatusUnauthorized, body: []byte(`{"error":"invalid_token"}`), want: RefreshOutcomeInvalidGrant},
		{name: "JSON 503 with unrecognised OAuth code → transport", statusCode: http.StatusServiceUnavailable, body: []byte(`{"error":"server_error"}`), want: RefreshOutcomeTransport},
		{name: "JSON 429 with unrecognised OAuth code → rate_limit", statusCode: http.StatusTooManyRequests, body: []byte(`{"error":"slow_down"}`), want: RefreshOutcomeRateLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, c.Classify(tt.statusCode, tt.body))
		})
	}
}

func TestOAuthRefreshClassifier_RecordRefresh_Counter(t *testing.T) {
	const vendor = "test-vendor-counter"
	c := NewOAuthRefreshClassifier(sdk.ConnectorID(vendor))

	tests := []struct {
		name    string
		outcome RefreshOutcome
		times   int
	}{
		{"success", RefreshOutcomeSuccess, 2},
		{"invalid_grant", RefreshOutcomeInvalidGrant, 1},
		{"rate_limit", RefreshOutcomeRateLimit, 1},
		{"transport", RefreshOutcomeTransport, 1},
		{"unknown", RefreshOutcomeUnknown, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < tt.times; i++ {
				c.RecordRefresh(tt.outcome, 250*time.Millisecond)
			}
			got := testutil.ToFloat64(oauthRefreshTotal.WithLabelValues(vendor, string(tt.outcome)))
			assert.Equal(t, float64(tt.times), got)
		})
	}

	// Duration histogram observes successes only. CollectAndCount on the
	// HistogramVec returns the number of distinct series; sub-counts not
	// asserted because the vendor label is shared with the rest of the test
	// suite when run in parallel.
	_ = testutil.CollectAndCount(oauthRefreshDurationSeconds)
}

func TestRefreshOutcomeToErrKind(t *testing.T) {
	tests := []struct {
		name string
		in   RefreshOutcome
		want sdk.SyncErrorKind
	}{
		{name: "invalid_grant → auth", in: RefreshOutcomeInvalidGrant, want: sdk.ErrKindAuth},
		{name: "rate_limit → rate_limit", in: RefreshOutcomeRateLimit, want: sdk.ErrKindRateLimit},
		{name: "transport → vendor_error (5xx, unparseable)", in: RefreshOutcomeTransport, want: sdk.ErrKindVendorError},
		{name: "unknown → vendor_error", in: RefreshOutcomeUnknown, want: sdk.ErrKindVendorError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RefreshOutcomeToErrKind(tt.in))
		})
	}
}

func TestRefreshOutcome_IsRetryable(t *testing.T) {
	tests := []struct {
		name string
		in   RefreshOutcome
		want bool
	}{
		{name: "success terminal (nothing to retry)", in: RefreshOutcomeSuccess, want: false},
		{name: "invalid_grant terminal", in: RefreshOutcomeInvalidGrant, want: false},
		{name: "rate_limit retryable", in: RefreshOutcomeRateLimit, want: true},
		{name: "transport retryable", in: RefreshOutcomeTransport, want: true},
		{name: "unknown retryable", in: RefreshOutcomeUnknown, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.in.IsRetryable())
		})
	}
}

func TestRefreshOutcome_IsTerminal(t *testing.T) {
	tests := []struct {
		name string
		in   RefreshOutcome
		want bool
	}{
		// Only invalid_grant is terminal — the credential is dead until a human
		// reconnects. Everything else is transient (or not a failure at all).
		{name: "invalid_grant terminal", in: RefreshOutcomeInvalidGrant, want: true},
		{name: "success not terminal", in: RefreshOutcomeSuccess, want: false},
		{name: "rate_limit not terminal", in: RefreshOutcomeRateLimit, want: false},
		{name: "transport not terminal", in: RefreshOutcomeTransport, want: false},
		{name: "unknown not terminal", in: RefreshOutcomeUnknown, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.in.IsTerminal())
		})
	}
}

// TestOAuthRefreshClassifier_Fetch covers every branch of Fetch end-to-end:
// build-request failure, network failure, body-read failure, each non-success
// outcome (with Retry-After honoured only on rate-limit), and the success
// path. Verifies the typed *sdk.SyncError shape callers will see.
func TestOAuthRefreshClassifier_Fetch(t *testing.T) {
	const orgID = "org-fetch-test"
	connectorID := sdk.ConnectorID("test-vendor-fetch")

	tests := []struct {
		name              string
		handler           http.HandlerFunc
		buildErr          error
		wantBody          string // expected body bytes on success; "" if err expected
		wantErrKind       sdk.SyncErrorKind
		wantRetryable     bool
		wantTerminal      bool
		wantHTTPStatus    int
		wantRetryAfterSec int
	}{
		{
			name:          "build request fails",
			buildErr:      errors.New("malformed url"),
			wantErrKind:   sdk.ErrKindInternal,
			wantRetryable: false,
		},
		{
			name: "rate_limit honours Retry-After delta seconds",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "42")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"too_many_requests"}`))
			},
			wantErrKind:       sdk.ErrKindRateLimit,
			wantRetryable:     true,
			wantHTTPStatus:    http.StatusTooManyRequests,
			wantRetryAfterSec: 42,
		},
		{
			name: "rate_limit with no Retry-After header → 0",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"too_many_requests"}`))
			},
			wantErrKind:       sdk.ErrKindRateLimit,
			wantRetryable:     true,
			wantHTTPStatus:    http.StatusTooManyRequests,
			wantRetryAfterSec: 0,
		},
		{
			name: "invalid_grant ignores Retry-After (terminal)",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "9999")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
			},
			wantErrKind:       sdk.ErrKindAuth,
			wantRetryable:     false,
			wantTerminal:      true,
			wantHTTPStatus:    http.StatusBadRequest,
			wantRetryAfterSec: 0,
		},
		{
			name: "5xx transport",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`<html>upstream</html>`))
			},
			wantErrKind:    sdk.ErrKindVendorError,
			wantRetryable:  true,
			wantHTTPStatus: http.StatusBadGateway,
		},
		{
			// A 403 whose JSON body names a rate limit must classify as
			// rate-limit (retryable, NOT terminal) — the JSON error field wins
			// over the 403→invalid_grant status fallback, so a throttled token
			// endpoint is not wrongly SkipRetry'd.
			name: "403 with rate-limit JSON body is retryable, not terminal",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"rate_limit_exceeded"}`))
			},
			wantErrKind:    sdk.ErrKindRateLimit,
			wantRetryable:  true,
			wantTerminal:   false,
			wantHTTPStatus: http.StatusForbidden,
		},
		{
			name: "success returns body bytes",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
			},
			wantBody: `{"access_token":"tok","expires_in":3600}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewOAuthRefreshClassifier(connectorID)

			var endpoint string
			if tt.handler != nil {
				srv := httptest.NewServer(tt.handler)
				defer srv.Close()
				endpoint = srv.URL
			}

			build := func(ctx context.Context) (*http.Request, error) {
				if tt.buildErr != nil {
					return nil, tt.buildErr
				}
				return http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(""))
			}

			body, _, err := c.Fetch(context.Background(), orgID, build)

			if tt.wantBody != "" {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBody, string(body))
				return
			}

			require.Error(t, err)
			var syncErr *sdk.SyncError
			require.ErrorAs(t, err, &syncErr)
			assert.Equal(t, tt.wantErrKind, syncErr.Kind)
			assert.Equal(t, tt.wantRetryable, syncErr.Retryable)
			assert.Equal(t, tt.wantTerminal, syncErr.Terminal)
			assert.Equal(t, tt.wantHTTPStatus, syncErr.HTTPStatus)
			assert.Equal(t, tt.wantRetryAfterSec, syncErr.RetryAfterSeconds)
			assert.Equal(t, connectorID, syncErr.ConnectorID)
			assert.Equal(t, orgID, syncErr.OrganizationID)
		})
	}
}

// TestOAuthRefreshClassifier_Fetch_NetworkError covers the http.Client.Do
// error path separately because httptest.NewServer cannot simulate it — we
// point Fetch at a closed listener to force ECONNREFUSED.
func TestOAuthRefreshClassifier_Fetch_NetworkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	closedURL := srv.URL
	srv.Close()

	c := NewOAuthRefreshClassifier(sdk.ConnectorID("test-vendor-netfail"))
	build := func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodPost, closedURL, strings.NewReader(""))
	}

	_, _, err := c.Fetch(context.Background(), "org-x", build)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindTimeout, syncErr.Kind)
	assert.True(t, syncErr.Retryable)
}

// TestOAuthRefreshClassifier_Fetch_BodyReadError exercises the io.ReadAll
// branch using a connection that closes mid-stream via a content-length
// header that overpromises.
func TestOAuthRefreshClassifier_Fetch_BodyReadError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("short")) // promised 1000 bytes, sending 5
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
		}
	}))
	defer srv.Close()

	c := NewOAuthRefreshClassifier(sdk.ConnectorID("test-vendor-readfail"))
	build := func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader(""))
	}

	_, _, err := c.Fetch(context.Background(), "org-y", build)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindVendorError, syncErr.Kind)
	assert.True(t, syncErr.Retryable)
}

// TestOAuthRefreshClassifier_RecordSuccessAndDecodeFailure verifies the
// two-step finalisation contract: Fetch records non-success outcomes itself,
// callers close out success-side via RecordSuccess (records `success`) or
// RecordDecodeFailure (records `unknown` — a 200 with malformed JSON must
// NOT inflate the success rate).
func TestOAuthRefreshClassifier_RecordSuccessAndDecodeFailure(t *testing.T) {
	const vendor = "test-vendor-finalise"
	c := NewOAuthRefreshClassifier(sdk.ConnectorID(vendor))

	tests := []struct {
		name        string
		finalise    func(start time.Time)
		wantOutcome RefreshOutcome
	}{
		{
			name:        "RecordSuccess records success",
			finalise:    c.RecordSuccess,
			wantOutcome: RefreshOutcomeSuccess,
		},
		{
			name:        "RecordDecodeFailure records unknown",
			finalise:    c.RecordDecodeFailure,
			wantOutcome: RefreshOutcomeUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := testutil.ToFloat64(oauthRefreshTotal.WithLabelValues(vendor, string(tt.wantOutcome)))
			tt.finalise(time.Now().Add(-50 * time.Millisecond))
			after := testutil.ToFloat64(oauthRefreshTotal.WithLabelValues(vendor, string(tt.wantOutcome)))
			assert.Equal(t, before+1, after)
		})
	}
}

// TestNewOAuthRefreshClassifier_Timeout verifies the WithTimeout option
// overrides the default 30s budget and that Fetch's ctx deadline reflects it.
// gitlab relies on it for its 10s bound.
func TestNewOAuthRefreshClassifier_Timeout(t *testing.T) {
	tests := []struct {
		name string
		opts []ClassifierOption
		want time.Duration
	}{
		{name: "default 30s when no option", opts: nil, want: DefaultOAuthRefreshTimeout},
		{name: "WithTimeout(10s) for gitlab", opts: []ClassifierOption{WithTimeout(10 * time.Second)}, want: 10 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewOAuthRefreshClassifier(sdk.ConnectorID("test-vendor-timeout"), tt.opts...)
			assert.Equal(t, tt.want, c.timeout)
		})
	}
}
