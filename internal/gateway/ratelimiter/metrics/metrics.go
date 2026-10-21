// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var recorder = NewRateLimiterMetrics()

type RateLimiterMetrics struct {
	rateLimitDecisionTime     *prometheus.HistogramVec
	redisIsWindowBreachedTime *prometheus.HistogramVec
	redisRemoveTime           *prometheus.HistogramVec
	redisKeyExpireTime        *prometheus.HistogramVec
	rateLimiterErrCounter     *prometheus.CounterVec
	rateLimitBulkAllowTime    *prometheus.HistogramVec
	rateLimitBulkRemoveTime   *prometheus.HistogramVec
	rateLimitBulkErrCounter   *prometheus.CounterVec
	allowRequestTotal         *prometheus.CounterVec
	allowBulkRequestTotal     *prometheus.CounterVec
	removeBulkRequestTotal    *prometheus.CounterVec
	redisAddWithPriorityTime  *prometheus.HistogramVec
	allowTotal                *prometheus.CounterVec
}

func NewRateLimiterMetrics() *RateLimiterMetrics {
	rateLimiterMetrics := &RateLimiterMetrics{
		rateLimitDecisionTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "rate_limiter_decision_time_duration_milliseconds",
			Help:    "total time taken to decide if request is to be allowed or not",
			Buckets: prometheus.ExponentialBuckets(0.01, 3, 10),
		}, []string{}),
		redisIsWindowBreachedTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "redis_is_window_breached_time_milli_seconds",
			Help:    "total time taken to find if window is breached and add new member in set",
			Buckets: prometheus.ExponentialBuckets(0.01, 3, 10),
		}, []string{}),
		redisRemoveTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "redis_remove_key_time_milli_seconds",
			Help:    "total time taken to remove keys from redis which are out of window",
			Buckets: prometheus.ExponentialBuckets(0.01, 3, 10),
		}, []string{}),
		redisKeyExpireTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "redis_key_expire_time_milli_seconds",
			Help:    "total time taken to set expiry for the key",
			Buckets: prometheus.ExponentialBuckets(0.01, 3, 10),
		}, []string{}),
		redisAddWithPriorityTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "redis_add_with_priority_time_milli_seconds",
			Help:    "total time taken to find if window is breached and add new member in set",
			Buckets: prometheus.ExponentialBuckets(0.01, 3, 10),
		}, []string{}),
		rateLimiterErrCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rate_limiter_err_total",
				Help: "Total errors while checking allow request",
			},
			[]string{},
		),
		rateLimitBulkAllowTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "rate_limiter_bulk_allow_time_duration_milliseconds",
			Help:    "total time taken to add bulk requests",
			Buckets: prometheus.ExponentialBuckets(0.01, 3, 10),
		}, []string{}),
		rateLimitBulkRemoveTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "rate_limiter_bulk_remove_time_duration_milliseconds",
			Help:    "total time taken to remove bulk requests",
			Buckets: prometheus.ExponentialBuckets(0.01, 3, 10),
		}, []string{}),
		rateLimitBulkErrCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rate_limiter_bulk_err_total",
				Help: "Total errors while checking bulk allow/remove request",
			},
			[]string{},
		),
		allowRequestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rate_limiter_allow_request_total",
				Help: "Number of allow request calls",
			},
			[]string{},
		),
		allowTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rate_limiter_allow_total",
				Help: "Number of allow calls",
			},
			[]string{},
		),
		allowBulkRequestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rate_limiter_allow_bulk_request_total",
				Help: "Number of allow bulk request calls",
			},
			[]string{},
		),
		removeBulkRequestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rate_limiter_remove_bulk_request_total",
				Help: "Number of remove bulk request calls",
			},
			[]string{},
		),
	}
	initialize(rateLimiterMetrics)
	return rateLimiterMetrics
}

// initialise metrics to 0
func initialize(metrics *RateLimiterMetrics) {
	metrics.rateLimitDecisionTime.WithLabelValues()
	metrics.redisIsWindowBreachedTime.WithLabelValues()
	metrics.redisRemoveTime.WithLabelValues()
	metrics.rateLimiterErrCounter.WithLabelValues()
	metrics.redisKeyExpireTime.WithLabelValues()
	metrics.rateLimitBulkAllowTime.WithLabelValues()
	metrics.rateLimitBulkRemoveTime.WithLabelValues()
	metrics.rateLimitBulkErrCounter.WithLabelValues()
	metrics.allowRequestTotal.WithLabelValues()
	metrics.allowBulkRequestTotal.WithLabelValues()
	metrics.removeBulkRequestTotal.WithLabelValues()
	metrics.allowTotal.WithLabelValues()
	metrics.redisAddWithPriorityTime.WithLabelValues()
}

func RecordDecisionTime(decisionMillis float64) {
	recorder.rateLimitDecisionTime.WithLabelValues().Observe(decisionMillis)
}

func RecordIsWindowBreachedTime(decisionMillis float64) {
	recorder.redisIsWindowBreachedTime.WithLabelValues().Observe(decisionMillis)
}

func RecordKeyRemoveTime(decisionMillis float64) {
	recorder.redisRemoveTime.WithLabelValues().Observe(decisionMillis)
}

func RecordRateLimitError() {
	recorder.rateLimiterErrCounter.WithLabelValues().Inc()
}

func RecordKeyExpireTime(keyExpireMillis float64) {
	recorder.redisKeyExpireTime.WithLabelValues().Observe(keyExpireMillis)
}

func RecordBulkAllowTime(tsMillis float64) {
	recorder.rateLimitBulkAllowTime.WithLabelValues().Observe(tsMillis)
}

func RecordBulkRemoveTime(tsMillis float64) {
	recorder.rateLimitBulkRemoveTime.WithLabelValues().Observe(tsMillis)
}

func RecordRateLimitBulkError() {
	recorder.rateLimitBulkErrCounter.WithLabelValues().Inc()
}

func RecordAllowRequest() {
	recorder.allowRequestTotal.WithLabelValues().Inc()
}

func RecordAllowBulkRequest() {
	recorder.allowBulkRequestTotal.WithLabelValues().Inc()
}

func RecordRemoveBulkRequest() {
	recorder.removeBulkRequestTotal.WithLabelValues().Inc()
}

func RecordAllow() {
	recorder.allowTotal.WithLabelValues().Inc()
}

func RecordAddWithPriorityTime(decisionMillis float64) {
	recorder.redisAddWithPriorityTime.WithLabelValues().Observe(decisionMillis)
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return recorder.collectors()
}

func (m *RateLimiterMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.rateLimitDecisionTime,
		m.redisIsWindowBreachedTime,
		m.redisRemoveTime,
		m.redisKeyExpireTime,
		m.rateLimiterErrCounter,
		m.rateLimitBulkAllowTime,
		m.rateLimitBulkRemoveTime,
		m.rateLimitBulkErrCounter,
		m.allowRequestTotal,
		m.allowBulkRequestTotal,
		m.removeBulkRequestTotal,
		m.allowTotal,
		m.redisAddWithPriorityTime,
	}
}
