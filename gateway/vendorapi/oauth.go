// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/httputil"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// RefreshOutcome classifies an OAuth token-refresh attempt. It surfaces the
// distinction between a transient transport failure, a vendor throttling us,
// and a credential that is permanently dead — distinctions a bare
// `fmt.Errorf("token refresh failed with status %d", ...)` would erase.
// The orchestrator branches on the error kind built from it, and pauses
// the connector as auth_revoked on a terminal auth error.
type RefreshOutcome string

const (
	RefreshOutcomeSuccess      RefreshOutcome = "success"
	RefreshOutcomeInvalidGrant RefreshOutcome = "invalid_grant"
	RefreshOutcomeRateLimit    RefreshOutcome = "rate_limit"
	RefreshOutcomeTransport    RefreshOutcome = "transport"
	RefreshOutcomeUnknown      RefreshOutcome = "unknown"
)

// DefaultOAuthRefreshTimeout bounds every Fetch call independent of the
// caller's ctx budget. A token endpoint that accepts the TCP connection and
// then stalls silently (firewall, half-open socket, vendor LB drain) would
// otherwise hang until the asynq ctx expires (~30s). Composes with the caller
// ctx — whichever deadline trips first wins. Vendors can override via
// WithTimeout for a tighter bound (gitlab uses 10s).
const DefaultOAuthRefreshTimeout = 30 * time.Second

var (
	oauthRefreshTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_oauth_refresh_total",
			Help: "Total OAuth token-refresh attempts by vendor and outcome (success / invalid_grant / rate_limit / transport / unknown).",
		},
		[]string{"vendor", "outcome"},
	)

	// Histogram observes successful refreshes only. Failed refreshes have a
	// different latency profile (network timeout vs vendor 4xx) and would
	// inflate the success-latency tail.
	oauthRefreshDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_oauth_refresh_duration_seconds",
			Help:    "OAuth refresh duration in seconds for successful refreshes, by vendor.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10), // 100ms → ~51s (DefaultOAuthRefreshTimeout=30s caps actual durations)
		},
		[]string{"vendor"},
	)
)

// OAuthRefreshClassifier owns the OAuth token-refresh lifecycle for one
// connector: it issues the HTTP exchange, parses the response into a
// RefreshOutcome, emits the matching Prometheus series, and wraps non-success
// outcomes as *sdk.SyncError. The vendor label is derived from the
// connector ID so callers cannot drift the metric label away from the
// SyncError ConnectorID — both live on the same struct.
type OAuthRefreshClassifier struct {
	connectorID sdk.ConnectorID
	timeout     time.Duration
}

// ClassifierOption configures an OAuthRefreshClassifier at construction time.
type ClassifierOption func(*OAuthRefreshClassifier)

// WithTimeout overrides the default 30s Fetch ctx deadline, for a caller that
// needs a tighter bound — e.g. gitlab bounds each refresh at 10s, which the
// 30s default would triple.
func WithTimeout(d time.Duration) ClassifierOption {
	return func(c *OAuthRefreshClassifier) { c.timeout = d }
}

// NewOAuthRefreshClassifier constructs a classifier for a given connector.
// The vendor label on emitted metrics is string(connectorID) — vendor and
// SyncError.ConnectorID can never diverge by construction.
func NewOAuthRefreshClassifier(connectorID sdk.ConnectorID, opts ...ClassifierOption) *OAuthRefreshClassifier {
	c := &OAuthRefreshClassifier{
		connectorID: connectorID,
		timeout:     DefaultOAuthRefreshTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *OAuthRefreshClassifier) vendor() string { return string(c.connectorID) }

// RequestBuilder produces the prepared *http.Request for an OAuth token
// refresh. Per-vendor variations (BasicAuth vs body credentials, custom
// headers) are captured here and nowhere else.
type RequestBuilder func(ctx context.Context) (*http.Request, error)

// Fetch performs an OAuth token-refresh HTTP exchange, classifies the outcome
// and emits the counter on any non-success path. On success it returns the
// response body bytes plus the start timestamp; the caller decodes its
// vendor-specific tokenResponse and MUST call RecordSuccess(start) on success
// or RecordDecodeFailure(start) when decoding fails — this two-step finalisation
// ensures a 200 OK with malformed JSON is not silently recorded as success.
//
// All non-success outcomes return a typed *sdk.SyncError so the
// orchestrator can branch on errors.As(err, **SyncError) and inspect
// Kind / Retryable without parsing the wrapped error string; it pauses the
// connector as auth_revoked on a terminal auth error. RetryAfterSeconds is populated from the
// Retry-After response header on rate-limit outcomes so asynq honours vendor
// backoff guidance instead of hot-retrying into the same throttle.
func (c *OAuthRefreshClassifier) Fetch(
	ctx context.Context,
	orgID string,
	build RequestBuilder,
) (body []byte, start time.Time, err error) {
	start = time.Now()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, buildErr := build(ctx)
	if buildErr != nil {
		// Request construction failure is on us (malformed URL / bad config),
		// not the vendor — record as `unknown` so a config bug is not
		// counted as a transport failure; the
		// SyncError below carries ErrKindInternal so the orchestrator does
		// not retry a structurally broken request.
		c.RecordRefresh(RefreshOutcomeUnknown, time.Since(start))
		return nil, start, &sdk.SyncError{
			ConnectorID:    c.connectorID,
			OrganizationID: orgID,
			Kind:           sdk.ErrKindInternal,
			Cause:          fmt.Errorf("build oauth token request: %w", buildErr),
			Retryable:      false,
		}
	}

	httpClient := &http.Client{Timeout: c.timeout}
	resp, doErr := httpClient.Do(req)
	if doErr != nil {
		c.RecordRefresh(RefreshOutcomeTransport, time.Since(start))
		return nil, start, &sdk.SyncError{
			ConnectorID:    c.connectorID,
			OrganizationID: orgID,
			Kind:           sdk.ErrKindTimeout,
			Cause:          fmt.Errorf("oauth token endpoint network error: %w", httputil.RedactURLError(doErr)),
			Retryable:      true,
		}
	}
	defer resp.Body.Close() //nolint:errcheck // body already consumed; close error is non-actionable

	bodyBytes, readErr := ReadResponseBody(resp.Body)
	var capErr *sdk.SyncError
	if errors.As(readErr, &capErr) {
		// A token response over MaxResponseBytes is not a token: terminal.
		c.RecordRefresh(RefreshOutcomeUnknown, time.Since(start))
		capErr.ConnectorID, capErr.OrganizationID, capErr.HTTPStatus = c.connectorID, orgID, resp.StatusCode
		return nil, start, capErr
	}
	if readErr != nil {
		// Body read failed mid-stream — vendor/network flakiness, retryable.
		// Same outcome bucket as a 5xx + unparseable response: sustained
		// breakage counts as a transport failure, not as `unknown`. Kind stays ErrKindVendorError to match the rest of
		// the transport-class mapping in RefreshOutcomeToErrKind.
		c.RecordRefresh(RefreshOutcomeTransport, time.Since(start))
		return nil, start, &sdk.SyncError{
			ConnectorID:    c.connectorID,
			OrganizationID: orgID,
			Kind:           sdk.ErrKindVendorError,
			Cause:          fmt.Errorf("read token response body: %w", readErr),
			Retryable:      true,
			HTTPStatus:     resp.StatusCode,
		}
	}

	outcome := c.Classify(resp.StatusCode, bodyBytes)
	if outcome != RefreshOutcomeSuccess {
		c.RecordRefresh(outcome, time.Since(start))
		// Honour vendor-supplied Retry-After only for rate-limit outcomes —
		// invalid_grant is terminal (no retry) and transport's backoff is
		// asynq's exponential schedule, not a vendor signal.
		retryAfter := 0
		if outcome == RefreshOutcomeRateLimit {
			retryAfter = parseRetryAfter(resp.Header)
		}
		return nil, start, &sdk.SyncError{
			ConnectorID:       c.connectorID,
			OrganizationID:    orgID,
			Kind:              RefreshOutcomeToErrKind(outcome),
			Cause:             fmt.Errorf("token refresh outcome=%s status=%d", outcome, resp.StatusCode),
			Retryable:         outcome.IsRetryable(),
			Terminal:          outcome.IsTerminal(),
			HTTPStatus:        resp.StatusCode,
			RetryAfterSeconds: retryAfter,
		}
	}

	return bodyBytes, start, nil
}

// RecordSuccess closes the metric pair after a successful Fetch and a
// successful caller-side decode. Always paired with `start` from Fetch.
func (c *OAuthRefreshClassifier) RecordSuccess(start time.Time) {
	c.RecordRefresh(RefreshOutcomeSuccess, time.Since(start))
}

// RecordDecodeFailure closes the metric pair when Fetch returned a 200 OK
// body that the caller could not parse into its vendor-specific tokenResponse
// shape. Recorded as `unknown` (not `success`) so a vendor regression that
// silently breaks the response shape is not masked behind a healthy success
// rate; emitted before the caller returns the decode error to its caller.
func (c *OAuthRefreshClassifier) RecordDecodeFailure(start time.Time) {
	c.RecordRefresh(RefreshOutcomeUnknown, time.Since(start))
}

// Classify maps an OAuth token-endpoint response to a RefreshOutcome. The
// rules follow RFC 6749 §5.2 (`{"error":"invalid_grant", ...}`) plus
// fallbacks based on HTTP status when the response body is non-JSON (vendor
// returned an HTML error page, proxy intercepted, etc.).
//
// Network-layer failures (DNS, connection reset, ctx-cancel) never reach
// this function — Fetch observes those before a status code exists and emits
// RefreshOutcomeTransport directly.
func (c *OAuthRefreshClassifier) Classify(statusCode int, body []byte) RefreshOutcome {
	if statusCode == http.StatusOK {
		return RefreshOutcomeSuccess
	}

	var parsed struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		// Non-JSON error body (HTML error page, empty, garbled). Fall
		// through to the shared status-code classifier. Expected on some
		// vendors; surfaced so a shift to all-HTML error bodies is visible.
		logger.InfoNoCtx("vendorapi: oauth error body is not JSON; classifying by status code",
			zap.Int("status_code", statusCode), zap.Error(err))
		return statusCodeOutcome(statusCode)
	}

	switch parsed.Error {
	case "invalid_grant":
		return RefreshOutcomeInvalidGrant
	case "rate_limit_exceeded", "too_many_requests":
		return RefreshOutcomeRateLimit
	}
	// JSON parsed but the OAuth `error` field is empty or unrecognised
	// (invalid_client, unsupported_grant_type, vendor-specific code, etc.).
	// Defer to the HTTP status so 429s / 5xx / 401-403 are not misclassified
	// as unknown just because the vendor used a non-standard error string.
	return statusCodeOutcome(statusCode)
}

// statusCodeOutcome is the shared HTTP-status fallback used by both the
// non-JSON and unknown-OAuth-error-code branches of Classify. 401/403
// collapse to invalid_grant: when the token endpoint returns those against
// a valid client_id/secret, the refresh token is effectively dead and the
// operator-observable behaviour matches invalid_grant — the
// orchestrator pauses the connector as auth_revoked until an admin
// reconnects either way.
func statusCodeOutcome(statusCode int) RefreshOutcome {
	switch {
	case statusCode == http.StatusUnauthorized, statusCode == http.StatusForbidden:
		return RefreshOutcomeInvalidGrant
	case statusCode == http.StatusTooManyRequests:
		return RefreshOutcomeRateLimit
	case statusCode >= 500:
		return RefreshOutcomeTransport
	default:
		return RefreshOutcomeUnknown
	}
}

// RecordRefresh emits the per-vendor counter and (on success) the duration
// histogram for a completed refresh attempt. Always called exactly once per
// refresh. Outcome values outside the RefreshOutcome enum collapse to
// "unknown" so the label set stays bounded — a guard against future callers
// constructing RefreshOutcome("…") with an ad-hoc string.
func (c *OAuthRefreshClassifier) RecordRefresh(outcome RefreshOutcome, duration time.Duration) {
	label := string(outcome)
	if !isKnownOutcome(outcome) {
		label = string(RefreshOutcomeUnknown)
	}
	oauthRefreshTotal.WithLabelValues(c.vendor(), label).Inc()
	if outcome == RefreshOutcomeSuccess {
		oauthRefreshDurationSeconds.WithLabelValues(c.vendor()).Observe(duration.Seconds())
	}
}

func isKnownOutcome(o RefreshOutcome) bool {
	switch o {
	case RefreshOutcomeSuccess, RefreshOutcomeInvalidGrant,
		RefreshOutcomeRateLimit, RefreshOutcomeTransport, RefreshOutcomeUnknown:
		return true
	}
	return false
}

// RefreshOutcomeToErrKind translates the classifier outcome into the
// sdk.SyncErrorKind callers should use when wrapping the refresh error
// for downstream consumers (orchestrator retry semantics, health-state
// machine, metrics). Success has no SyncError shape — callers must
// only invoke this for non-success outcomes.
func RefreshOutcomeToErrKind(o RefreshOutcome) sdk.SyncErrorKind {
	switch o {
	case RefreshOutcomeInvalidGrant:
		return sdk.ErrKindAuth
	case RefreshOutcomeRateLimit:
		return sdk.ErrKindRateLimit
	case RefreshOutcomeTransport:
		// Vendor-side 5xx and unparseable error bodies — matches the
		// sdk/errors.go docstring for ErrKindVendorError ("5xx from
		// vendor"). True network-level errors (no response, ctx-cancel)
		// are wrapped as ErrKindTimeout by Fetch before Classify runs, so
		// this branch only sees response-with-body failures.
		return sdk.ErrKindVendorError
	default:
		return sdk.ErrKindVendorError
	}
}

// IsRetryable reports whether a given outcome warrants automatic retry by
// asynq / orchestrator backoff. Both terminal outcomes return false: success
// because there is nothing to retry, invalid_grant because the refresh token
// is dead until a human re-authenticates. Transport / rate-limit / unknown
// are transient and retryable.
func (o RefreshOutcome) IsRetryable() bool {
	switch o {
	case RefreshOutcomeRateLimit, RefreshOutcomeTransport, RefreshOutcomeUnknown:
		return true
	default:
		// Success and InvalidGrant are terminal — nothing to retry.
		return false
	}
}

// IsTerminal reports whether a failed refresh is provably pointless to
// re-attempt — the credential is dead until a human reconnects. Only
// invalid_grant qualifies: a rotated/revoked refresh token stays dead, so
// asynq must SkipRetry rather than burn MaxRetry re-hitting the token
// endpoint. Transport / rate-limit / unknown are transient, not terminal.
// (Success is never a failure and so never reaches a Terminal check.)
func (o RefreshOutcome) IsTerminal() bool {
	return o == RefreshOutcomeInvalidGrant
}
