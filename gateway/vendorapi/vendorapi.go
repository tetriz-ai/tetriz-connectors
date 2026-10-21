// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/internal/httputil"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// MaxResponseBytes caps a vendor response body the shared paths read into
// memory (Handle, DoJSON, the OAuth token refresh, the GitLab gateway). A
// larger body fails its request (ReadResponseBody). Paths with their own,
// larger caps (diffs, file contents, blame, branch pages) keep them.
const MaxResponseBytes = 32 << 20

// ReadResponseBody reads a vendor response body up to MaxResponseBytes. A
// body over the cap fails as a terminal vendor_error naming the cap: the
// same request returns the same body, so a retry cannot help. A read that
// fails for another reason returns that error.
func ReadResponseBody(body io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(body, MaxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > MaxResponseBytes {
		return nil, &sdk.SyncError{
			Kind:     sdk.ErrKindVendorError,
			Terminal: true,
			Cause:    fmt.Errorf("vendor response body exceeds the %d-byte (%d MiB) cap", MaxResponseBytes, MaxResponseBytes>>20),
		}
	}
	return b, nil
}

// HTTPDoer is a minimal interface for executing HTTP requests.
// Used by Handle() to decouple from the full VendorGateway interface.
type HTTPDoer interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

// VendorRequest defines the contract for outbound vendor API requests.
type VendorRequest interface {
	HTTPMethod() string
	URL() string
	Marshal() ([]byte, error)
	GetResponse() VendorResponse
}

// VendorResponse defines the contract for parsing vendor API responses.
type VendorResponse interface {
	Unmarshal(body []byte) (proto.Message, error)
}

// VendorRequestWithAuth is an optional interface for requests that set
// authentication headers (e.g., Bearer tokens).
type VendorRequestWithAuth interface {
	SetAuth(r *http.Request) *http.Request
}

// VendorRequestWithHeaders is an optional interface for requests that need
// to set custom HTTP headers (e.g., Content-Type).
type VendorRequestWithHeaders interface {
	SetHeaders(r *http.Request) *http.Request
}

// VendorResponseWithErrorHandling is an optional interface for responses that
// need custom HTTP error classification.
type VendorResponseWithErrorHandling interface {
	HandleHTTPError(ctx context.Context, statusCode int, body []byte) error
}

// VendorResponseWithHeaders is an optional interface for responses that need
// response headers, not just the body — e.g. an endpoint whose payload lives
// partly in a header (GitHub returns a fine-grained PAT's expiry in the
// github-authentication-token-expiration response header, never the body).
// When a VendorResponse implements this, Handle calls UnmarshalWithHeaders and
// skips Unmarshal; responses that only need the body keep implementing the
// plain VendorResponse and are unaffected.
type VendorResponseWithHeaders interface {
	UnmarshalWithHeaders(body []byte, header http.Header) (proto.Message, error)
}

// SyncRequestFactory creates a VendorRequest from a proto message.
// Keyed by ConnectorID, the factory maps proto request types to concrete
// vendor request implementations.
type SyncRequestFactory func(req proto.Message) (VendorRequest, error)

// BaseVendorGateway provides reusable implementations of Do, DoJSON, and Execute.
// Connector-specific gateways embed this struct and only need to add
// WithRateLimits / ForGroup (which must copy the concrete type).
type BaseVendorGateway struct {
	Doer    HTTPDoer
	Factory SyncRequestFactory
}

// Do executes an HTTP request via the underlying HTTPDoer. Production code
// uses this directly for diff fetches and file content, so it must emit the same telemetry as Handle/DoJSON; otherwise
// those vendor paths would be invisible in the VendorGateway metrics.
func (b *BaseVendorGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	vendor := vendorUnknown
	if req != nil && req.URL != nil {
		vendor = vendorFromURL(req.URL.String())
	}
	start := time.Now()
	resp, err := b.Doer.Do(ctx, req)
	durationSeconds := time.Since(start).Seconds()
	if err != nil {
		recordVendorRequestErr(vendor, err, durationSeconds)
		return nil, fmt.Errorf("executing vendor request: %w", httputil.RedactURLError(err))
	}
	recordVendorRequest(vendor, resp.StatusCode, durationSeconds)
	return resp, nil
}

// DoJSON marshals a request body as JSON, sends the request, and unmarshals
// the response body into respBody.
func (b *BaseVendorGateway) DoJSON(ctx context.Context, method, url string, reqBody, respBody any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		data, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", httputil.RedactURLError(err))
	}
	if reqBody != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	vendor := vendorFromURL(url)
	start := time.Now()
	resp, err := b.Doer.Do(ctx, httpReq)
	durationSeconds := time.Since(start).Seconds()
	if err != nil {
		recordVendorRequestErr(vendor, err, durationSeconds)
		return fmt.Errorf("request failed: %w", httputil.RedactURLError(err))
	}
	recordVendorRequest(vendor, resp.StatusCode, durationSeconds)
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Error(ctx, "failed to close response body", zap.Error(closeErr))
		}
	}()

	body, err := ReadResponseBody(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ClassifyHTTPErrorWithHeaders(resp.StatusCode, body, resp.Header)
	}

	if respBody != nil {
		if err := json.Unmarshal(body, respBody); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}

	return nil
}

// Execute sends a proto request through the vendor API pipeline and returns a proto response.
func (b *BaseVendorGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	reqType := fmt.Sprintf("%T", req)

	vendorReq, err := b.Factory(req)
	if err != nil {
		logger.Error(ctx, "failed to create vendor request from proto", zap.String(logger.RequestType, reqType), zap.Error(err))
		return nil, &sdk.SyncError{
			Kind:  sdk.ErrKindInternal,
			Cause: err,
		}
	}

	resp, err := Handle(ctx, b.Doer, vendorReq)
	if err != nil {
		logger.Error(ctx, "vendor gateway request failed", zap.String(logger.RequestType, reqType), zap.Error(err))
		return nil, fmt.Errorf("handling vendor request %q: %w", reqType, err)
	}

	return resp, nil
}

// looksLikeRateLimit checks whether an HTTP response body contains
// signals that the 403 is actually a rate limit (GitHub secondary rate
// limits return 403, not 429).
func looksLikeRateLimit(body []byte) bool {
	lower := bytes.ToLower(body)
	return bytes.Contains(lower, []byte("rate limit")) ||
		bytes.Contains(lower, []byte("abuse detection mechanism")) ||
		bytes.Contains(lower, []byte("secondary rate limit"))
}

// ClassifyHTTPError maps an HTTP status code to a SyncError.
// For 429 responses where vendor headers or body carry retry guidance,
// prefer ClassifyHTTPErrorWithHeaders so the caller can back off correctly.
func ClassifyHTTPError(status int, body []byte) *sdk.SyncError {
	return ClassifyHTTPErrorWithHeaders(status, body, nil)
}

// ClassifyHTTPErrorWithHeaders is the same as ClassifyHTTPError but also
// reads Retry-After from the supplied header map. On 429 it populates
// SyncError.RetryAfterSeconds so the orchestrator and other callers can
// honour the vendor's backoff window instead of hammering the API.
//
// Dead-credential (401/403) and gone-resource (404) responses are marked
// Terminal so the scheduler fails the run once instead of exhausting MaxRetry:
// re-issuing the identical request cannot change the outcome. A 401/403 only
// reaches here after any in-request token auto-refresh below this layer has
// already run and failed, so the credential is dead until a human reconnects.
//
// A 403 that is actually a throttle stays retryable, not Terminal: GitHub
// secondary rate limits return 403 with a rate-limit body, and some vendors
// signal a 403 throttle with a Retry-After header — both are honoured before
// the dead-credential fallthrough. (401 is never a throttle signal, so it stays
// terminal-auth even with a rate-limit-looking body.)
//
// 404 blast radius: connectors that treat a 404 as per-resource swallow it
// before this layer (bitbucket → DELETED lifecycle; gitlab diff/config
// attempt-and-omit; GitHub per-item), so a 404 that reaches the scheduler
// is a gone/no-permission top-level resource — terminal until the config
// changes. A rare transient 404 self-heals on the next scheduled run.
func ClassifyHTTPErrorWithHeaders(status int, body []byte, h http.Header) *sdk.SyncError {
	msg := errorMessage(status, body, h)

	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		if status == http.StatusForbidden {
			if missing := missingOAuthScopes(h); missing != "" {
				// GitHub names the scopes an endpoint accepts and the ones the
				// token has: none in common is a missing scope, which no retry
				// and no other repository fixes, and which is not a dead token.
				return &sdk.SyncError{
					Kind:       sdk.ErrKindConfigInvalid,
					HTTPStatus: status,
					Cause:      fmt.Errorf("the token lacks a scope this request needs (it accepts %s): %s", missing, msg),
					Terminal:   true,
				}
			}
			if retryAfter := parseRetryAfter(h); looksLikeRateLimit(body) || retryAfter > 0 {
				return &sdk.SyncError{
					Kind:              sdk.ErrKindRateLimit,
					HTTPStatus:        status,
					Cause:             errors.New(msg),
					Retryable:         true,
					RetryAfterSeconds: retryAfter,
				}
			}
		}
		return &sdk.SyncError{
			Kind:       sdk.ErrKindAuth,
			HTTPStatus: status,
			Cause:      errors.New(msg),
			Terminal:   true,
		}
	case status == http.StatusBadRequest && bytes.Contains(body, []byte(`"RATELIMITED"`)):
		// Linear answers a rate-limited GraphQL request with 400 and an error whose
		// extensions.code is RATELIMITED (https://linear.app/developers/rate-limiting).
		return &sdk.SyncError{
			Kind:              sdk.ErrKindRateLimit,
			HTTPStatus:        status,
			Cause:             errors.New(msg),
			Retryable:         true,
			RetryAfterSeconds: rateLimitResetAfter(h),
		}
	case status == http.StatusTooManyRequests:
		return &sdk.SyncError{
			Kind:              sdk.ErrKindRateLimit,
			HTTPStatus:        status,
			Cause:             errors.New(msg),
			Retryable:         true,
			RetryAfterSeconds: parseRetryAfter(h),
		}
	case status == http.StatusNotFound || status == http.StatusGone:
		return &sdk.SyncError{
			Kind:       sdk.ErrKindNotFound,
			HTTPStatus: status,
			Cause:      errors.New(msg),
			Terminal:   true,
		}
	case status == http.StatusServiceUnavailable:
		// 503 is the only other status where RFC 7231 defines Retry-After.
		// Populating the field on 500/502/504 would conflate a genuine server
		// error with a vendor-directed wait and mislead callers into backing
		// off against arbitrary header content.
		return &sdk.SyncError{
			Kind:              sdk.ErrKindVendorError,
			HTTPStatus:        status,
			Cause:             errors.New(msg),
			Retryable:         true,
			RetryAfterSeconds: parseRetryAfter(h),
		}
	case status >= 500:
		return &sdk.SyncError{
			Kind:       sdk.ErrKindVendorError,
			HTTPStatus: status,
			Cause:      errors.New(msg),
			Retryable:  true,
		}
	case status >= 400:
		// Any other 4xx (400, 409, 422, ...) is the vendor refusing this request,
		// not a fault in the connector, so it is vendor_error with its status.
		return &sdk.SyncError{
			Kind:       sdk.ErrKindVendorError,
			HTTPStatus: status,
			Cause:      errors.New(msg),
		}
	default:
		return &sdk.SyncError{
			Kind:       sdk.ErrKindInternal,
			HTTPStatus: status,
			Cause:      errors.New(msg),
		}
	}
}

// maxRetryAfterSeconds caps vendor-supplied Retry-After values. A hostile
// or misconfigured vendor could set an absurd wait (e.g., days) and stall
// any caller that honours the signal. 1 hour is well above typical vendor
// cool-downs (GitHub/Slack are minutes at most) and below anything that
// would indicate we should just pause the sync entirely.
const maxRetryAfterSeconds = 3600

// rateLimitResetAfter reads Retry-After, or else Linear's
// X-RateLimit-Requests-Reset (UTC epoch milliseconds), as seconds to wait,
// clamped like Retry-After. It returns 0 when neither is present.
func rateLimitResetAfter(h http.Header) int {
	if secs := parseRetryAfter(h); secs > 0 {
		return secs
	}
	if h == nil {
		return 0
	}
	ms, err := strconv.ParseInt(h.Get("X-RateLimit-Requests-Reset"), 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	d := time.Until(time.UnixMilli(ms)).Seconds()
	if d <= 0 {
		return 0
	}
	return clampRetryAfter(int(d + 0.5))
}

// parseRetryAfter reads the Retry-After header per RFC 7231 (delta-seconds
// or HTTP-date). Returns 0 when absent or unparseable. The result is
// clamped to [0, maxRetryAfterSeconds] so hostile vendors cannot stall
// schedulers or agent retries by sending oversized values.
func parseRetryAfter(h http.Header) int {
	if h == nil {
		return 0
	}
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	secs, intErr := strconv.Atoi(v)
	if intErr == nil && secs >= 0 {
		return clampRetryAfter(secs)
	}
	// A delta too large for an int is still a vendor asking for a long wait:
	// clamp it to the cap instead of dropping it as "no hint".
	if errors.Is(intErr, strconv.ErrRange) && !strings.HasPrefix(v, "-") {
		return maxRetryAfterSeconds
	}
	t, timeErr := http.ParseTime(v)
	if timeErr == nil {
		d := time.Until(t).Seconds()
		if d <= 0 {
			return 0
		}
		// +0.5 rounds to nearest second; without it, truncation toward zero
		// would under-honour the vendor's wait on fractional deltas.
		return clampRetryAfter(int(d + 0.5))
	}
	// Retry-After was present but parses as neither delta-seconds nor an
	// HTTP-date — we silently drop the vendor's backoff guidance and fall
	// back to scheduler/asynq backoff. Surface it so a malformed-header
	// regression is visible rather than a quiet degradation.
	logger.Warn("vendorapi: unparseable Retry-After header; ignoring vendor backoff guidance",
		zap.String("retry_after", v), zap.Error(intErr), zap.NamedError("http_date_error", timeErr))
	return 0
}

func clampRetryAfter(secs int) int {
	if secs > maxRetryAfterSeconds {
		return maxRetryAfterSeconds
	}
	return secs
}

// Handle executes the shared marshal → HTTP → unmarshal pipeline.
func Handle(ctx context.Context, doer HTTPDoer, req VendorRequest) (proto.Message, error) {
	body, err := req.Marshal()
	if err != nil {
		logger.Error(ctx, "failed to marshal vendor request", zap.String(logger.URL, httputil.RedactURL(req.URL())), zap.Error(err))
		return nil, &sdk.SyncError{
			Kind:  sdk.ErrKindInternal,
			Cause: fmt.Errorf("failed to marshal request: %w", err),
		}
	}

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, req.HTTPMethod(), req.URL(), bodyReader)
	if err != nil {
		err = httputil.RedactURLError(err)
		logger.Error(ctx, "failed to create HTTP request", zap.String(logger.Method, req.HTTPMethod()), zap.String(logger.URL, httputil.RedactURL(req.URL())), zap.Error(err))
		return nil, &sdk.SyncError{
			Kind:  sdk.ErrKindInternal,
			Cause: fmt.Errorf("failed to create request: %w", err),
		}
	}
	if headers, ok := req.(VendorRequestWithHeaders); ok {
		httpReq = headers.SetHeaders(httpReq)
	}

	if auth, ok := req.(VendorRequestWithAuth); ok {
		httpReq = auth.SetAuth(httpReq)
	}

	logger.Debug(ctx, "executing vendor API request", zap.String(logger.Method, req.HTTPMethod()), zap.String(logger.URL, httputil.RedactURL(req.URL())))

	vendor := vendorFromURL(req.URL())
	start := time.Now()
	resp, err := doer.Do(ctx, httpReq)
	durationSeconds := time.Since(start).Seconds()
	if err != nil {
		recordVendorRequestErr(vendor, err, durationSeconds)
		err = httputil.RedactURLError(err)
		logger.Error(ctx, "vendor API request failed", zap.String(logger.Method, req.HTTPMethod()), zap.String(logger.URL, httputil.RedactURL(req.URL())), zap.Error(err))
		// Preserve a SyncError produced upstream (e.g., the rate-limiter or
		// circuit breaker in rateLimitedDoer) — re-wrapping as
		// ErrKindVendorError would lose the original Kind (rate_limit, auth)
		// and any RetryAfterSeconds the doer already populated.
		var existing *sdk.SyncError
		if errors.As(err, &existing) {
			return nil, existing
		}
		return nil, &sdk.SyncError{
			Kind:      sdk.ErrKindVendorError,
			Cause:     fmt.Errorf("API request failed: %w", err),
			Retryable: true,
		}
	}
	recordVendorRequest(vendor, resp.StatusCode, durationSeconds)
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Error(ctx, "failed to close response body", zap.String(logger.URL, httputil.RedactURL(req.URL())), zap.Error(closeErr))
		}
	}()

	respBody, err := ReadResponseBody(resp.Body)
	if err != nil {
		logger.Error(ctx, "failed to read response body", zap.String(logger.URL, httputil.RedactURL(req.URL())), zap.Error(err))
		var capErr *sdk.SyncError
		if errors.As(err, &capErr) {
			return nil, capErr
		}
		return nil, &sdk.SyncError{
			Kind:  sdk.ErrKindInternal,
			Cause: fmt.Errorf("failed to read response body: %w", err),
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Error(ctx, "vendor API returned non-OK status",
			zap.String(logger.Method, req.HTTPMethod()),
			zap.String(logger.URL, httputil.RedactURL(req.URL())),
			zap.Int(logger.Status, resp.StatusCode),
		)
		if errHandler, ok := req.GetResponse().(VendorResponseWithErrorHandling); ok {
			return nil, fmt.Errorf("handling vendor http error (status %d): %w", resp.StatusCode, errHandler.HandleHTTPError(ctx, resp.StatusCode, respBody))
		}
		return nil, ClassifyHTTPErrorWithHeaders(resp.StatusCode, respBody, resp.Header)
	}

	logger.Debug(ctx, "vendor API request succeeded", zap.String(logger.URL, httputil.RedactURL(req.URL())), zap.Int(logger.Status, resp.StatusCode))

	vr := req.GetResponse()
	if hr, ok := vr.(VendorResponseWithHeaders); ok {
		msg, err := hr.UnmarshalWithHeaders(respBody, resp.Header)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling vendor response with headers: %w", err)
		}
		return msg, nil
	}
	msg, err := vr.Unmarshal(respBody)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling vendor response: %w", err)
	}
	return msg, nil
}

// missingOAuthScopes reports the scopes a GitHub endpoint accepts when the
// token has none of them, read from X-Accepted-OAuth-Scopes and X-OAuth-Scopes
// (sent for classic tokens). It returns "" when either header is absent or the
// token has an accepted scope, so any other 403 keeps its classification.
func missingOAuthScopes(h http.Header) string {
	if h == nil {
		return ""
	}
	accepted, granted := h.Values("X-Accepted-OAuth-Scopes"), h.Values("X-OAuth-Scopes")
	if len(accepted) == 0 || len(granted) == 0 {
		return ""
	}
	split := func(vs []string) []string {
		var out []string
		for _, v := range vs {
			for _, s := range strings.Split(v, ",") {
				if s = strings.TrimSpace(s); s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	}
	want, have := split(accepted), map[string]bool{}
	for _, s := range split(granted) {
		have[s] = true
	}
	if len(want) == 0 {
		return ""
	}
	for _, s := range want {
		if have[s] {
			return ""
		}
	}
	return strings.Join(want, ", ")
}

// errorMessage is what an HTTP error's message carries: httputil.ErrorMessage
// (the status, the vendor's request id when a header names one, and only the
// vendor's error code and message fields of a JSON body; only the length and
// content type of any other body).
func errorMessage(status int, body []byte, h http.Header) string {
	return httputil.ErrorMessage(status, body, h)
}
