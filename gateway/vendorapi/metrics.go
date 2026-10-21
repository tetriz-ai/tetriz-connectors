// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	vendorUnknown = "unknown"

	statusSuccess = "success"
	statusError   = "error"
)

var (
	vendorRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Total VendorGateway HTTP requests by vendor, HTTP status, and outcome.",
		},
		[]string{"vendor", "status", "http_status"},
	)

	vendorRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "VendorGateway HTTP request duration in seconds, by vendor.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12), // 50ms → ~204s
		},
		[]string{"vendor"},
	)
)

// vendorFromURL classifies a request URL into a stable vendor label. We match
// on host suffix to keep per-tenant subdomains (e.g. *.atlassian.net) collapsed
// into one label value, avoiding cardinality blow-up.
func vendorFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		// An unparseable URL collapses to the "unknown" vendor label — an
		// expected classifier miss, not a fault. Surface the cause so a
		// spike of unknown-labelled requests can be traced to a malformed URL.
		logger.InfoNoCtx("vendorapi: could not classify vendor from URL", zap.Error(err))
		return vendorUnknown
	}
	if u.Host == "" {
		return vendorUnknown
	}
	host := strings.ToLower(u.Hostname())
	switch {
	case strings.HasSuffix(host, "github.com"):
		return "github"
	case strings.HasSuffix(host, "bitbucket.org"):
		return "bitbucket"
	case strings.HasSuffix(host, "gitlab.com"):
		return "gitlab"
	case strings.HasSuffix(host, "atlassian.net") || strings.HasSuffix(host, "atlassian.com"):
		return "jira"
	case strings.HasSuffix(host, "slack.com"):
		return "slack"
	case strings.HasSuffix(host, "cursor.com"):
		return "cursor"
	case strings.HasSuffix(host, "anthropic.com"):
		return "anthropic"
	}
	return vendorUnknown
}

// recordVendorRequest emits the request counter and duration histogram for a
// vendor call. status="error" covers both transport-level failures (httpStatus
// == 0) and non-2xx responses; status="success" covers any 2xx.
func recordVendorRequest(vendor string, httpStatus int, durationSeconds float64) {
	status := statusSuccess
	if httpStatus < 200 || httpStatus >= 300 {
		status = statusError
	}
	httpStatusLabel := "0"
	if httpStatus > 0 {
		httpStatusLabel = strconv.Itoa(httpStatus)
	}
	vendorRequestsTotal.WithLabelValues(vendor, status, httpStatusLabel).Inc()
	vendorRequestDurationSeconds.WithLabelValues(vendor).Observe(durationSeconds)
}

// recordVendorRequestErr emits the counter/histogram when the doer returned an
// error before a usable response. Pre-flight failures (per-org token bucket,
// circuit breaker, BudgetLimiter) surface as *sdk.SyncError with no HTTP
// roundtrip — http_status would otherwise default to "0" and pollute the
// transport-error series. Rate-limit / 429 conditions get their own status
// code, so rate limits count on their own and the 5xx series stays clean.
func recordVendorRequestErr(vendor string, err error, durationSeconds float64) {
	httpStatus := 0
	var syncErr *sdk.SyncError
	if errors.As(err, &syncErr) {
		switch {
		case syncErr.HTTPStatus != 0:
			httpStatus = syncErr.HTTPStatus
		case syncErr.Kind == sdk.ErrKindRateLimit:
			httpStatus = http.StatusTooManyRequests
		case syncErr.Kind == sdk.ErrKindAuth:
			httpStatus = http.StatusUnauthorized
		case syncErr.Kind == sdk.ErrKindNotFound:
			httpStatus = http.StatusNotFound
		}
	}
	recordVendorRequest(vendor, httpStatus, durationSeconds)
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		vendorRequestsTotal,
		vendorRequestDurationSeconds,
		oauthRefreshTotal,
		oauthRefreshDurationSeconds,
	}
}
