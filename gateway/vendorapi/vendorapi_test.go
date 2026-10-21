// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestMain(m *testing.M) {
	logger.Log = zap.NewNop()
	os.Exit(testguard.Run(m))
}

// --- Test helpers ---

// stubHTTPDoer implements HTTPDoer for testing.
type stubHTTPDoer struct {
	resp    *http.Response
	err     error
	lastReq *http.Request
}

func (d *stubHTTPDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	d.lastReq = req
	return d.resp, d.err
}

// stubResponse is a VendorResponse that returns a fixed proto message.
type stubResponse struct {
	msg proto.Message
	err error
}

func (r *stubResponse) Unmarshal(body []byte) (proto.Message, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.msg, nil
}

// stubHeaderResponse is a VendorResponse that also implements
// VendorResponseWithHeaders, recording what Handle hands it.
type stubHeaderResponse struct {
	msg            proto.Message
	gotBody        []byte
	gotHeader      http.Header
	headerCalled   bool
	unmarshalCalls int
}

func (r *stubHeaderResponse) Unmarshal(body []byte) (proto.Message, error) {
	r.unmarshalCalls++
	return r.msg, nil
}

func (r *stubHeaderResponse) UnmarshalWithHeaders(body []byte, header http.Header) (proto.Message, error) {
	r.headerCalled = true
	r.gotBody = body
	r.gotHeader = header
	return r.msg, nil
}

// stubRequest is a VendorRequest for testing Handle.
type stubRequest struct {
	method      string
	url         string
	marshalBody []byte
	marshalErr  error
	resp        VendorResponse
}

func (r *stubRequest) HTTPMethod() string          { return r.method }
func (r *stubRequest) URL() string                 { return r.url }
func (r *stubRequest) Marshal() ([]byte, error)    { return r.marshalBody, r.marshalErr }
func (r *stubRequest) GetResponse() VendorResponse { return r.resp }

// stubRequestWithAuth adds SetAuth to a stubRequest.
type stubRequestWithAuth struct {
	stubRequest
	authHeader string
}

func (r *stubRequestWithAuth) SetAuth(req *http.Request) *http.Request {
	req.Header.Set("Authorization", r.authHeader)
	return req
}

func jsonHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
		Header:     make(http.Header),
	}
}

// --- ClassifyHTTPError tests ---

func TestClassifyHTTPError(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		wantKind     sdk.SyncErrorKind
		wantTerminal bool
	}{
		// Dead-credential (401/403) and gone-resource (404) responses are terminal:
		// an identical retry cannot change the outcome, so the scheduler SkipRetries.
		{"401 returns ErrKindAuth", 401, sdk.ErrKindAuth, true},
		{"403 returns ErrKindAuth", 403, sdk.ErrKindAuth, true},
		{"404 returns ErrKindNotFound", 404, sdk.ErrKindNotFound, true},
		// Transient classes stay retryable (not terminal).
		{"429 returns ErrKindRateLimit", 429, sdk.ErrKindRateLimit, false},
		{"500 returns ErrKindVendorError", 500, sdk.ErrKindVendorError, false},
		{"502 returns ErrKindVendorError", 502, sdk.ErrKindVendorError, false},
		{"503 returns ErrKindVendorError", 503, sdk.ErrKindVendorError, false},
		{"400 returns ErrKindVendorError", 400, sdk.ErrKindVendorError, false},
		{"409 returns ErrKindVendorError", 409, sdk.ErrKindVendorError, false},
		{"410 returns ErrKindNotFound", 410, sdk.ErrKindNotFound, true},
		{"422 returns ErrKindVendorError", 422, sdk.ErrKindVendorError, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ClassifyHTTPError(tt.status, []byte("error body"))
			assert.Equal(t, tt.wantKind, err.Kind)
			assert.Equal(t, tt.status, err.HTTPStatus)
			assert.Equal(t, tt.wantTerminal, err.Terminal)
		})
	}
}

func TestClassifyHTTPErrorWithHeaders_RetryAfter(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		header   http.Header
		wantSecs int
		wantKind sdk.SyncErrorKind
	}{
		{
			name:     "429 with Retry-After seconds",
			status:   429,
			header:   http.Header{"Retry-After": []string{"30"}},
			wantSecs: 30,
			wantKind: sdk.ErrKindRateLimit,
		},
		{
			name:     "429 without Retry-After header",
			status:   429,
			header:   http.Header{},
			wantSecs: 0,
			wantKind: sdk.ErrKindRateLimit,
		},
		{
			name:     "429 with nil header map",
			status:   429,
			header:   nil,
			wantSecs: 0,
			wantKind: sdk.ErrKindRateLimit,
		},
		{
			name:     "429 with garbage Retry-After value",
			status:   429,
			header:   http.Header{"Retry-After": []string{"banana"}},
			wantSecs: 0,
			wantKind: sdk.ErrKindRateLimit,
		},
		{
			name:     "503 propagates Retry-After (only 5xx that does per RFC 7231)",
			status:   503,
			header:   http.Header{"Retry-After": []string{"10"}},
			wantSecs: 10,
			wantKind: sdk.ErrKindVendorError,
		},
		{
			name:     "500 does NOT propagate Retry-After (conflates server error with wait)",
			status:   500,
			header:   http.Header{"Retry-After": []string{"42"}},
			wantSecs: 0,
			wantKind: sdk.ErrKindVendorError,
		},
		{
			name:     "502 does NOT propagate Retry-After",
			status:   502,
			header:   http.Header{"Retry-After": []string{"42"}},
			wantSecs: 0,
			wantKind: sdk.ErrKindVendorError,
		},
		{
			name:     "429 with absurd Retry-After clamps to 1h max",
			status:   429,
			header:   http.Header{"Retry-After": []string{"99999999"}},
			wantSecs: 3600,
			wantKind: sdk.ErrKindRateLimit,
		},
		{
			name:     "429 with negative Retry-After treated as zero",
			status:   429,
			header:   http.Header{"Retry-After": []string{"-5"}},
			wantSecs: 0,
			wantKind: sdk.ErrKindRateLimit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ClassifyHTTPErrorWithHeaders(tt.status, []byte(`{"error":"x"}`), tt.header)
			assert.Equal(t, tt.wantKind, err.Kind)
			assert.Equal(t, tt.wantSecs, err.RetryAfterSeconds)
			if tt.wantSecs > 0 {
				assert.Contains(t, err.Error(), fmt.Sprintf("retry after %ds", tt.wantSecs))
			}
		})
	}
}

func TestClassifyHTTPErrorWithHeaders_HTTPDate(t *testing.T) {
	// HTTP-date form of Retry-After. Tolerate 1s drift from time.Until().
	future := time.Now().UTC().Add(45 * time.Second).Format(http.TimeFormat)
	err := ClassifyHTTPErrorWithHeaders(429, nil, http.Header{"Retry-After": []string{future}})
	assert.Equal(t, sdk.ErrKindRateLimit, err.Kind)
	assert.InDelta(t, 45, err.RetryAfterSeconds, 1)
}

// A body that is not JSON contributes only its length, never its text, at
// any size.
func TestClassifyHTTPError_BodyTruncation(t *testing.T) {
	err := ClassifyHTTPError(400, []byte("short error"))
	assert.Equal(t, "HTTP 400: 11-byte unknown content type body", err.Cause.Error())

	longBody := []byte(strings.Repeat("x", 2049))
	err = ClassifyHTTPError(400, longBody)
	assert.NotContains(t, err.Cause.Error(), "xxx")
}

func TestClassifyHTTPError_403RateLimit(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		body         string
		wantKind     sdk.SyncErrorKind
		wantRetry    bool
		wantTerminal bool
	}{
		{
			name:         "plain 403 is auth",
			status:       http.StatusForbidden,
			body:         "",
			wantKind:     sdk.ErrKindAuth,
			wantTerminal: true,
		},
		{
			// A 403 that is really a secondary rate limit must stay retryable and
			// NOT terminal — the looksLikeRateLimit branch must win over the
			// terminal-auth return. Guards against a future branch reorder.
			name:      "403 with rate limit message",
			status:    http.StatusForbidden,
			body:      `{"message":"API rate limit exceeded for installation ID 12345."}`,
			wantKind:  sdk.ErrKindRateLimit,
			wantRetry: true,
		},
		{
			name:      "403 with abuse message",
			status:    http.StatusForbidden,
			body:      `{"message":"You have triggered an abuse detection mechanism."}`,
			wantKind:  sdk.ErrKindRateLimit,
			wantRetry: true,
		},
		{
			name:      "403 with secondary rate limit",
			status:    http.StatusForbidden,
			body:      `{"message":"You have exceeded a secondary rate limit. Please wait."}`,
			wantKind:  sdk.ErrKindRateLimit,
			wantRetry: true,
		},
		{
			name:         "401 is always auth even with rate limit body",
			status:       http.StatusUnauthorized,
			body:         `{"message":"rate limit"}`,
			wantKind:     sdk.ErrKindAuth,
			wantTerminal: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ClassifyHTTPError(tt.status, []byte(tt.body))
			assert.Equal(t, tt.wantKind, err.Kind)
			assert.Equal(t, tt.wantRetry, err.Retryable)
			assert.Equal(t, tt.wantTerminal, err.Terminal)
		})
	}
}

// TestClassifyHTTPError_403WithRetryAfterIsRateLimit guards the throttle-vs-
// dead-credential split for a 403 whose body does NOT match the rate-limit
// phrases: a Retry-After header marks it a throttle (retryable, honour backoff),
// not a Terminal auth failure that would SkipRetry a recoverable run.
func TestClassifyHTTPError_403WithRetryAfterIsRateLimit(t *testing.T) {
	throttle := ClassifyHTTPErrorWithHeaders(http.StatusForbidden,
		[]byte(`{"message":"forbidden"}`), http.Header{"Retry-After": []string{"30"}})
	assert.Equal(t, sdk.ErrKindRateLimit, throttle.Kind)
	assert.True(t, throttle.Retryable)
	assert.False(t, throttle.Terminal, "a Retry-After 403 is a throttle, must not SkipRetry")
	assert.Equal(t, 30, throttle.RetryAfterSeconds)

	// A 403 with neither a rate-limit body nor a Retry-After header stays a
	// Terminal dead-credential auth failure.
	dead := ClassifyHTTPErrorWithHeaders(http.StatusForbidden, []byte(`{"message":"forbidden"}`), nil)
	assert.Equal(t, sdk.ErrKindAuth, dead.Kind)
	assert.True(t, dead.Terminal)
}

// A JSON error body contributes its message field, cut to 200 bytes,
// whatever the body's size.
func TestClassifyHTTPError_BodyTruncation2048(t *testing.T) {
	err := ClassifyHTTPError(500, []byte(`{"message":"`+strings.Repeat("x", 2048)+`"}`))
	assert.Equal(t, `HTTP 500: message="`+strings.Repeat("x", 200)+`…"`, err.Cause.Error())
}

// --- Handle tests ---

func TestHandle_Success(t *testing.T) {
	expectedResp := &connectorpb.AnthropicAnalyticsUsersResponse{
		Data:     [][]byte{[]byte(`{"id":"u1"}`)},
		NextPage: "p2",
	}

	doer := &stubHTTPDoer{
		resp: jsonHTTPResponse(200, `{"users":["u1"]}`),
	}

	req := &stubRequest{
		method: http.MethodGet,
		url:    "https://example.com/users",
		resp:   &stubResponse{msg: expectedResp},
	}

	result, err := Handle(context.Background(), doer, req)
	require.NoError(t, err)
	assert.Equal(t, expectedResp, result)
}

// TestHandle_VendorResponseWithHeaders pins that Handle routes a response
// implementing VendorResponseWithHeaders through UnmarshalWithHeaders (passing
// the response headers) and skips the body-only Unmarshal.
func TestHandle_VendorResponseWithHeaders(t *testing.T) {
	expectedResp := &connectorpb.GitHubGraphQLResponse{Data: []byte(`{"login":"octocat"}`)}

	httpResp := jsonHTTPResponse(200, `{"login":"octocat"}`)
	httpResp.Header.Set("X-Custom-Expiry", "2026-12-31")
	doer := &stubHTTPDoer{resp: httpResp}

	hr := &stubHeaderResponse{msg: expectedResp}
	req := &stubRequest{
		method: http.MethodGet,
		url:    "https://api.github.com/user",
		resp:   hr,
	}

	result, err := Handle(context.Background(), doer, req)
	require.NoError(t, err)
	assert.Equal(t, expectedResp, result)
	assert.True(t, hr.headerCalled, "Handle must call UnmarshalWithHeaders for a header-aware response")
	assert.Equal(t, 0, hr.unmarshalCalls, "Handle must skip the body-only Unmarshal")
	assert.Equal(t, "2026-12-31", hr.gotHeader.Get("X-Custom-Expiry"), "response headers must reach the parser")
	assert.JSONEq(t, `{"login":"octocat"}`, string(hr.gotBody))
}

func TestHandle_201CreatedTreatedAsSuccess(t *testing.T) {
	expectedResp := &connectorpb.GitHubGraphQLResponse{
		Data: []byte(`{"id":"123"}`),
	}

	doer := &stubHTTPDoer{
		resp: jsonHTTPResponse(201, `{"id":"123"}`),
	}

	req := &stubRequest{
		method:      http.MethodPost,
		url:         "https://api.github.com/repos/org/repo/issues",
		marshalBody: []byte(`{"title":"test"}`),
		resp:        &stubResponse{msg: expectedResp},
	}

	result, err := Handle(context.Background(), doer, req)
	require.NoError(t, err)
	assert.Equal(t, expectedResp, result)
}

func TestHandle_SetsAuth(t *testing.T) {
	doer := &stubHTTPDoer{
		resp: jsonHTTPResponse(200, `{}`),
	}

	req := &stubRequestWithAuth{
		stubRequest: stubRequest{
			method: http.MethodGet,
			url:    "https://example.com/users",
			resp:   &stubResponse{msg: &connectorpb.AnthropicAnalyticsUsersResponse{}},
		},
		authHeader: "Bearer test-token",
	}

	_, err := Handle(context.Background(), doer, req)
	require.NoError(t, err)
	assert.Equal(t, "Bearer test-token", doer.lastReq.Header.Get("Authorization"))
}

func TestHandle_HTTPError(t *testing.T) {
	doer := &stubHTTPDoer{
		resp: jsonHTTPResponse(401, `{"error":"unauthorized"}`),
	}

	req := &stubRequest{
		method: http.MethodGet,
		url:    "https://example.com/users",
		resp:   &stubResponse{msg: &connectorpb.AnthropicAnalyticsUsersResponse{}},
	}

	_, err := Handle(context.Background(), doer, req)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
}

func TestHandle_TransportError(t *testing.T) {
	doer := &stubHTTPDoer{
		err: fmt.Errorf("connection refused"),
	}

	req := &stubRequest{
		method: http.MethodGet,
		url:    "https://example.com/users",
		resp:   &stubResponse{msg: &connectorpb.AnthropicAnalyticsUsersResponse{}},
	}

	_, err := Handle(context.Background(), doer, req)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindVendorError, syncErr.Kind)
	assert.True(t, syncErr.Retryable)
}

func TestHandle_MarshalError(t *testing.T) {
	doer := &stubHTTPDoer{}

	req := &stubRequest{
		method:     http.MethodPost,
		url:        "https://example.com/data",
		marshalErr: fmt.Errorf("marshal failed"),
		resp:       &stubResponse{msg: &connectorpb.AnthropicAnalyticsUsersResponse{}},
	}

	_, err := Handle(context.Background(), doer, req)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindInternal, syncErr.Kind)
}

func TestHandle_UnmarshalError(t *testing.T) {
	doer := &stubHTTPDoer{
		resp: jsonHTTPResponse(200, `invalid`),
	}

	req := &stubRequest{
		method: http.MethodGet,
		url:    "https://example.com/users",
		resp:   &stubResponse{err: fmt.Errorf("unmarshal failed")},
	}

	_, err := Handle(context.Background(), doer, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal failed")
}

// --- Metrics-label tests ---
//
// An error ratio read over
// `http_status=~"5..|0"` counts transport failures. Bitbucket's per-org token bucket / circuit breaker
// short-circuits before any HTTP roundtrip and returns
// *sdk.SyncError{Kind: ErrKindRateLimit}. Landing as http_status="0", the
// same bucket as a real transport error, a single rate-limited org would push the bucket-wide error ratio up.
// recordVendorRequestErr must promote rate-limit/auth/not-found SyncErrors
// into their HTTP equivalents so the 5xx series stays clean and rate limits
// count under 429.
func TestRecordVendorRequestErr_PreFlightSyncErrorClassification(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		wantHTTPStatus string
		wantStatus     string
	}{
		{
			name: "rate-limit pre-flight from token bucket maps to 429",
			err: &sdk.SyncError{
				Kind:  sdk.ErrKindRateLimit,
				Cause: fmt.Errorf("rate limit exceeded for group bitbucket-rest"),
			},
			wantHTTPStatus: "429",
			wantStatus:     statusError,
		},
		{
			name: "rate-limit pre-flight from circuit breaker maps to 429",
			err: &sdk.SyncError{
				Kind:              sdk.ErrKindRateLimit,
				Cause:             fmt.Errorf("circuit open for bitbucket-rest"),
				RetryAfterSeconds: 30,
			},
			wantHTTPStatus: "429",
			wantStatus:     statusError,
		},
		{
			name: "SyncError with explicit HTTPStatus preserves the status",
			err: &sdk.SyncError{
				Kind:       sdk.ErrKindVendorError,
				HTTPStatus: 503,
			},
			wantHTTPStatus: "503",
			wantStatus:     statusError,
		},
		{
			name: "auth pre-flight maps to 401",
			err: &sdk.SyncError{
				Kind: sdk.ErrKindAuth,
			},
			wantHTTPStatus: "401",
			wantStatus:     statusError,
		},
		{
			name:           "non-SyncError transport failure stays in 0 bucket",
			err:            fmt.Errorf("connection refused"),
			wantHTTPStatus: "0",
			wantStatus:     statusError,
		},
		{
			name: "wrapped rate-limit SyncError is still unwrapped to 429",
			err: fmt.Errorf("retry wrap: %w", &sdk.SyncError{
				Kind: sdk.ErrKindRateLimit,
			}),
			wantHTTPStatus: "429",
			wantStatus:     statusError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendorRequestsTotal.Reset()
			recordVendorRequestErr("bitbucket", tt.err, 0.01)

			got := testCounterValue(t, "bitbucket", tt.wantStatus, tt.wantHTTPStatus)
			assert.Equal(t, 1.0, got,
				"expected counter at (vendor=bitbucket, status=%s, http_status=%s) to be 1",
				tt.wantStatus, tt.wantHTTPStatus)
		})
	}
}

func TestHandle_PreFlightRateLimitNotInTransportBucket(t *testing.T) {
	// Hand-rolled doer that surfaces the same pre-flight SyncError that
	// rateLimitedDoer.Do emits when the per-org Bitbucket token bucket is
	// exhausted. Handle must record this against http_status="429", not "0",
	// so an error ratio (which counts "0" as a transport failure) does not
	// double-count rate-limit pressure.
	preFlight := &sdk.SyncError{
		Kind:              sdk.ErrKindRateLimit,
		Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest"),
		Retryable:         true,
		RetryAfterSeconds: 30,
	}
	doer := &stubHTTPDoer{err: preFlight}

	req := &stubRequest{
		method: http.MethodGet,
		url:    "https://api.bitbucket.org/2.0/repositories/foo/bar/pullrequests",
		resp:   &stubResponse{msg: &connectorpb.AnthropicAnalyticsUsersResponse{}},
	}

	vendorRequestsTotal.Reset()

	_, err := Handle(context.Background(), doer, req)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindRateLimit, syncErr.Kind,
		"original SyncError.Kind must survive Handle's error path")

	got429 := testCounterValue(t, "bitbucket", statusError, "429")
	assert.Equal(t, 1.0, got429,
		"pre-flight rate-limit must land in http_status=429 series, not the 0 bucket")

	got0 := testCounterValue(t, "bitbucket", statusError, "0")
	assert.Equal(t, 0.0, got0,
		"http_status=0 series must stay empty for pre-flight rate-limit errors")
}

// testCounterValue reads a single (vendor, status, http_status) cell of
// vendorRequestsTotal. Returns 0.0 if the series has not been observed yet.
func testCounterValue(t *testing.T, vendor, status, httpStatus string) float64 {
	t.Helper()
	c, err := vendorRequestsTotal.GetMetricWithLabelValues(vendor, status, httpStatus)
	require.NoError(t, err)
	pb := &dto.Metric{}
	require.NoError(t, c.Write(pb))
	return pb.GetCounter().GetValue()
}

// A vendor's Retry-After is capped at one hour on the way into the SyncError:
// a ten-day hint becomes 3600 seconds.
func TestClassifyHTTPErrorCapsRetryAfterAtOneHour(t *testing.T) {
	err := ClassifyHTTPErrorWithHeaders(http.StatusTooManyRequests, nil, http.Header{"Retry-After": {"864000"}})
	require.NotNil(t, err)
	assert.Equal(t, sdk.ErrKindRateLimit, err.Kind)
	assert.Equal(t, 3600, err.RetryAfterSeconds)

	err = ClassifyHTTPErrorWithHeaders(http.StatusTooManyRequests, nil, http.Header{"Retry-After": {"7200"}})
	require.NotNil(t, err)
	assert.Equal(t, 3600, err.RetryAfterSeconds, "two hours is capped too")
}

// The cap is inclusive: one hour passes through and one second more is capped.
func TestRetryAfterCapBoundary(t *testing.T) {
	for in, want := range map[string]int{"3599": 3599, "3600": 3600, "3601": 3600} {
		err := ClassifyHTTPErrorWithHeaders(http.StatusTooManyRequests, nil, http.Header{"Retry-After": {in}})
		require.NotNil(t, err)
		assert.Equal(t, want, err.RetryAfterSeconds, "Retry-After %s", in)
	}
}

// Linear answers a rate-limited GraphQL request with 400 and a RATELIMITED
// error code; the reset header gives the wait.
func TestClassifyLinearRatelimited400(t *testing.T) {
	h := http.Header{}
	h.Set("X-RateLimit-Requests-Reset", strconv.FormatInt(time.Now().Add(90*time.Second).UnixMilli(), 10))
	se := ClassifyHTTPErrorWithHeaders(400, []byte(`{"errors":[{"message":"Rate limit exceeded","extensions":{"code":"RATELIMITED"}}]}`), h)
	assert.Equal(t, sdk.ErrKindRateLimit, se.Kind)
	assert.True(t, se.Retryable)
	assert.InDelta(t, 90, se.RetryAfterSeconds, 2)
	assert.Equal(t, sdk.ErrKindVendorError, ClassifyHTTPErrorWithHeaders(400, []byte(`{"errors":[{"message":"bad query"}]}`), nil).Kind, "any other 400 is a vendor error")
}
