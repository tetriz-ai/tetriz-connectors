// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

//go:generate go tool mockgen -source=ratelimiter.go -destination=../../internal/mocks/gateway/ratelimiter/ratelimiter.go -package=mocks

package ratelimiter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/async/goroutine"
	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/ratelimiter/metrics"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

// We delete keys from store only after every 5 minutes from the server restart to reduce the
// number of network calls made to store in case of redis
const (
	// defaultMinCleanupInterval is default min interval after which stale data should be cleared from store
	defaultMinCleanupInterval = 5 * time.Minute
	// defaultCleanupBuffer is the default buffer that decides the old entries from sliding window log which should be deleted
	defaultCleanupBuffer = 1 * time.Minute
)

type WindowParams struct {
	Key              string
	FromTime, ToTime time.Time
	MaxRequest       int64
}

type CleanAndExpireParams struct {
	Key         string
	TTL         time.Duration
	CleanBefore time.Time
}

// A RateLimiter is an interface for various rate limiting access to configured resources.
// It allows acquiring and releasing slots from and to the configured limit pools.
type RateLimiter interface {
	// IsResourceConfigured checks if rate limit is configured for the given resource.
	// Returns true if configured, false otherwise.
	IsResourceConfigured(resource string) bool

	/*
		Acquire tries to acquire a slot for the (resource, requestIdentifier) tuple

		Request parameters
			* ctx : context passed from client
			* resource: resource for which the slot needs to be acquired
			* requestIdentifier : unique request identifier which is trying to access the resource

			A few examples to understand the difference between requestIdentifier and resource key better can be:
			* resource can be sms_status_api, request identifier can be anything passed at run time like client name
			* resource can be an internal API and request identifier can be email of the user like abc@example.com
			  if we are trying to limit the user from accessing that API more than desired number of times
			* In some cases, dynamic request identifier is not a requirement (e.g. in case of simple API rate limiting),
			  it is ok to pass same value in both resource and requestIdentifier. The client may also choose to
			  hard code a unique requestIdentifier per resource.

		Returns
			* bool : true if a slot is granted i.e. rate limit for the resource has not been hit yet, false otherwise
			* err : error encountered during the evaluation

		********* IMPORTANT NOTE **********
		* A unique key is constructed on `namespace:resource:requestIdentifier` hence caller should ensure that it doesn't
		pass same (namespace, resource, requestIdentifier) combination for two different rate limiting use cases.
		* One slot is exhausted in case Acquire returns true
		e.g. If current slots acquired in last 1-second window for resource 'X' is 10, and we have configured the rate limit
		at 12. On calling AllowRequest one slot will be exhausted and we will have headroom for one more request.
	*/
	Acquire(ctx context.Context, resource, requestIdentifier string) (bool, error)

	/*
		AcquireUptoN tries to bulk acquires slots for a (resource, requestIdentifier) tuple. This will check the
		current slots available for the key based on max slots permitted in the duration as per the rate limiter config for
		the resource and add upto numSlots.
		e.g. :
		If current slots acquired in last 1-second window for resource 'X' is 10, and we wanted to book 5 more but rate limit
		is 12 per second, we will only book 2. Further, this method is a thread safe ie multiple concurrent workers running
		on same resource will not end up over booking.

		It can be used for various cases but not limited to:
			* In subscriber layer where client wants to book N slots optimistically for N messages being consumed from
			queue at bulk.

		Request parameters
			* ctx : context passed from client
			* resource : resource for which given request should be rate limited
			* requestIdentifier : unique request identifier which is trying to access the resource
			* n : Maximum Number of slots to book
			* setExpiry : In case callers are using the requestIdentifier as actors or user specific which can lead to a very high
						key set in redis it is recommended to set this flag as true so the auto clean up gets done via TTL
		Returns
			* acquired : number of slots actually acquired, acquired is always <= n
			* ts : Timestamp against which these slots were acquired. This parameter can be used by the caller to remove
			slots using ReleaseUptoN when needed instead of waiting for duration to expire.
			* err : error encountered in executing the operations
	*/
	AcquireUptoN(ctx context.Context, resource, requestIdentifier string, n int64, setExpiry bool) (acquired int64, ts time.Time, err error)

	/*
		ReleaseUptoN bulk releases slots for a (resource, requestIdentifier) tuple for which rate limit is configured. This will check the
		current slots available for the key and release at max N slots. This is a thead safe i.e. multiple concurrent workers
		running on same resource will not end up over removing.
		It can be used for various cases but not limited to:
			* In subscriber layer where client optimistically acquires N slots before hitting an AWS endpoint, if the number of
			packets received actually are less than the acquired slots, the extra slots acquired need to be released back to the pool
		Request parameters
			* ctx : context passed from client
			* resource : resource for which given request should be rate limited
			* requestIdentifier : unique request identifier which is trying to access the resource
			* n : Maximum number of slots to release
			* ts : timestamp returned from rate limiter library when slot was acquired
		Returns
			* removed : number of slots actually released back into the pool, released is always <= n so that the configured rate limit is not breached
			* err : error encountered in executing the operations
	*/
	ReleaseUptoN(ctx context.Context, resource, requestIdentifier string, n int64, ts time.Time) (removed int64, err error)
}

// PriorityRateLimiter is an interface for various rate limiting checks that are Priority aware
// It allows acquiring and releasing slots from and to the configured limit pools with an allowance for higher priority
// requests to acquire slots at the expense of lower priority requests
type PriorityRateLimiter interface {
	// IsResourceConfigured checks if rate limit is configured for the given resource.
	// Returns true if configured, false otherwise
	IsResourceConfigured(resource string) bool

	/*
		AcquireWithPriority tries to acquire a slot for the (resource, requestIdentifier, priority) combination

		Request parameters
			* ctx : context passed from client
			* resource: resource for which given rate limiting evaluation should be done limited
			* requestIdentifier : unique request identifier which is trying to access the resource
			* priority : priority of the current request. Slot booking will take this priority in consideration.
			  So, in case, slots at a priority level are exhausted, a higher priority request will
			  be able to consume slots from a lower priority level but vice-versa won't be possible.

			A few examples to understand the difference between requestIdentifier and resource key better can be:
			* resource can be sms_status_api, request identifier can be anything passed at run time like client name
			* resource can be an internal API and request identifier can be email of the user like abc@example.com
			  if we are trying to limit the user from accessing that API more than desired number of times
			* In some cases, dynamic request identifier is not a requirement (e.g. in case of simple API rate limiting),
			  it is ok to pass same value in both resource and requestIdentifier. The client may also choose to
			  hard code a unique requestIdentifier per resource.

		Response parameters
			* decision : true if case a slot is granted i.e. rate limit for the resource has not been hit yet, false otherwise
			* err : error encountered in executing the operations.

		********* IMPORTANT NOTE **********
		* A unique key is constructed on `namespace:resource:requestIdentifier` hence caller should ensure that it doesn't
		pass same (namespace, resource, requestIdentifier) combination for two different rate limiting use cases.
		* A slot is acquired in case AcquireWithPriority returns true
		e.g. If current slots acquired in last 1-second window for resource 'X' is 10, and we have configured the rate limit
		at 12. On calling AllowRequest one slot will be blocked that means we will have headroom for one more request.
	*/
	AcquireWithPriority(ctx context.Context, resource, requestIdentifier string, priority Priority) (bool, error)
}

// Store interface which serves as the storage layer for rate limiting logic
// It can be implemented by any distributed fast store like redis or also in memory store using any algorithm as per use case.
// NOTE : If in memory store is to be used, rate limit will be applied per instance level and not at the cluster level
type Store interface {
	RemoveKeysBefore(ctx context.Context, key string, removeBefore time.Time) error

	// Deprecated: in favour of AddWithPriority which support priority based slots booking
	IsWindowCountReached(ctx context.Context, key string, fromTime, toTime time.Time, maxRequest int64) (bool, error)

	RefreshKeyTTL(ctx context.Context, key string, period time.Duration) error
	/*
		AddMaxN to book at most n slots for a requestIdentifier in bulk. This will check the current slots available for the key
		based on max slots permitted in the duration as per rate limiter config for the resource and add at most n.

		e.g. :
			If current slots booked in last 1-second window for resource 'X' is 10, and we booked to book 5 more but rate limit is
			12 per second, we will only book 2. Further this method is thread safe, this makes sure that concurrent calls
			to this method for same requestIdentifier do not end up overbooking.

		Request parameters
			* ctx : context passed from client
			* key : redis key identifier
			* n : Maximum Number of slots to book/add
			* maxRequestAllowed : Defined rate limit as per client in rate limiter config
			* durationNanos : duration over which maxRequestAllowed is defined example 1 second, 1 minute, etc
		Response parameters
			* added : number of slots actually added , added <= addN
			* addedTimeNanos : Timestamp against which these slots were added. This parameter is returned so that clients
								can remove the slots as well when needed instead of waiting for duration to expire
			* err : error encountered in executing the operations
	*/
	AddMaxN(ctx context.Context, key string, addN, maxRequestAllowed, durationNanos int64) (added, addedTimeNanos int64, err error)
	/*
		RemoveMaxN to un book at most N slots for a key in bulk. Client has to pass the timestamp against which
		it expects to remove the slots. This timestamp should be the one which was returned when the booking was made.
		This method will only try to remove at most N slots IF AVAILABLE for the key for that timestamp.
		Request parameters
			* ctx : context passed from client
			* key : redis key identifier
			* addedTimeNanos : timestamp returned when booking was made
			* removeN : maximum number of slots to remove/un-book
		Response parameters
			* removed : number of slots actually removed , removed <= removeN
			* err : error encountered in executing the operations
	*/
	RemoveMaxN(ctx context.Context, key string, addedTimeNanos int64, removeN int64) (removed int64, err error)

	/*
		AddWithPriority adds a request with a specific priority to a rate limiting system within a given time range.
		Request parameters
		  * ctx: The context.Context used for cancellation or deadline propagation.
		  * key: The key identifying the rate limiting system.
		  * fromTime: The start time of the time range within which the request should be considered.
		  * toTime: The end time of the time range within which the request should be considered.
		  * maxRequest: A map representing the maximum number of requests allowed for each priority level.
		    The key is the Priority and the value is the maximum number of requests.
		  * priority: The priority level of the request.
		  * expiry: The duration after which the rate limiting data should expire.
		Response parameters
		  * bool: A boolean value indicating whether the request was successfully added or not.
		  * error: An error if any occurred during the execution of the function.
	*/
	AddWithPriority(
		ctx context.Context,
		key string,
		fromTime, toTime time.Time,
		maxRequest map[Priority]int64,
		priority Priority,
		expiry time.Duration,
	) (bool, error)

	// IsWindowsCountReached is the same as IsWindowCountReached but for multiple keys at once.
	// It returns true in case window count was not reached for any of the windows.
	IsWindowsCountReached(ctx context.Context, windowsParams []*WindowParams, enableLogging bool) (bool, error)

	// CleanAndExpire sets time to live for a key and removes all the elements in the respective
	// set that are older than cleanBefore time. Multiple keys are passed and the script is run atomically.
	CleanAndExpire(ctx context.Context, params []*CleanAndExpireParams) error
}

type RateLimiterImpl struct {
	store         Store
	rateLimitConf *RateLimitConfig
	// Internal, used for maintaining last time stale data was cleared from store
	lastDeleteTime time.Time
	// Internal cleanup token that clears data from the store
	cleanUpToken chan struct{}
	// Internal - the interval at which stale data should be cleared from store
	minCleanupInterval time.Duration
	// Internal - buffer duration older than the start of the current sliding window for which stale data should be cleared from store.
	cleanupBuffer time.Duration
	clk           clockwork.Clock
}

type Option func(impl *RateLimiterImpl)

func WithCleanupInterval(interval time.Duration) Option {
	return func(impl *RateLimiterImpl) {
		impl.minCleanupInterval = interval
	}
}

func WithCleanupBuffer(buffer time.Duration) Option {
	return func(impl *RateLimiterImpl) {
		impl.cleanupBuffer = buffer
	}
}

func WithClock(clock clockwork.Clock) Option {
	return func(impl *RateLimiterImpl) {
		impl.clk = clock
	}
}

func NewRateLimiter(store Store, rateLimitConf *RateLimitConfig, opts ...Option) *RateLimiterImpl {
	token := make(chan struct{}, 1)
	token <- struct{}{}
	r := &RateLimiterImpl{
		store:              store,
		rateLimitConf:      rateLimitConf,
		cleanUpToken:       token,
		clk:                clockwork.NewRealClock(),
		minCleanupInterval: defaultMinCleanupInterval,
		cleanupBuffer:      defaultCleanupBuffer,
	}
	for _, o := range opts {
		o(r)
	}
	r.lastDeleteTime = r.clk.Now()
	return r
}

func (r *RateLimiterImpl) IsResourceConfigured(resource string) bool {
	// if resource is present in rate limit config it implies rate limit is configured otherwise it's not.
	return r.rateLimitConf.IsResourcePresent(resource)
}

func (r *RateLimiterImpl) Acquire(ctx context.Context, resource, requestIdentifier string) (bool, error) {
	metrics.RecordAllowRequest()
	if ok := r.rateLimitConf.IsResourcePresent(resource); !ok {
		return false, fmt.Errorf("resource pass does not have valid rate limiter config : %s", resource)
	}
	requestID := r.rateLimitConf.GetKeyWithNamespace(resource, requestIdentifier)
	currentTime := r.clk.Now()
	defer func() { metrics.RecordDecisionTime(float64(time.Since(currentTime).Milliseconds())) }()
	lastWindowTime := currentTime.Add(-r.rateLimitConf.GetPeriod(resource))
	reached, err := r.store.IsWindowCountReached(ctx, requestID, lastWindowTime, currentTime, r.rateLimitConf.GetMaxRequestCount(resource))
	if err != nil {
		metrics.RecordRateLimitError()
		return false, fmt.Errorf("checking window count: %w", err)
	}
	// clean up stale values in go routine
	r.cleanupStaleEntries(reqctx.CloneCtx(ctx), requestID, lastWindowTime.Add(-r.cleanupBuffer), currentTime)

	// This can be done in separate go routine as it does not affect functionality of rate limiter
	goroutine.RunWithDefaultTimeout(reqctx.CloneCtx(ctx), func(ctx context.Context) {
		if refreshErr := r.store.RefreshKeyTTL(ctx, requestID, r.rateLimitConf.GetPeriod(resource)); refreshErr != nil {
			logger.Error(ctx, "error refreshing key expiry in store", zap.String("requestID", requestID), zap.Error(refreshErr))
		}
	})

	return reached, nil
}

func (r *RateLimiterImpl) cleanupStaleEntries(ctx context.Context, requestID string, cleanRangeTill, currentTime time.Time) {
	// channel of size 1 is taken so that concurrent requests do not end up spinning more go routines than desired
	select {
	case <-r.cleanUpToken:
		// Since this step is only to remove keys from redis to reduce memory footprint it is run as an async process
		goroutine.RunWithCtx(ctx, func(ctx context.Context) {
			defer func() { r.cleanUpToken <- struct{}{} }()
			if !r.lastDeleteTime.IsZero() && currentTime.Sub(r.lastDeleteTime) < r.minCleanupInterval {
				return
			}

			remErr := r.store.RemoveKeysBefore(ctx, requestID, cleanRangeTill)
			if remErr != nil {
				metrics.RecordRateLimitError()
				logger.Error(ctx, "error removing keys from store", zap.String("requestID", requestID), zap.Error(remErr))
			}
			r.lastDeleteTime = currentTime
		})
	default:
		// skip cleanup
	}
}

// Cleanup runs at most once per minCleanupInterval per limiter; moving it into the Lua script would clean every key.
func (r *RateLimiterImpl) cleanupStaleEntriesWithPriority(ctx context.Context, requestID string, cleanRangeTill, currentTime time.Time) {
	// channel of size 1 is taken so that concurrent requests do not end up spinning more go routines than desired
	select {
	case <-r.cleanUpToken:
		// Since this step is only to remove keys from redis to reduce memory footprint it is run as an async process
		goroutine.RunWithCtx(ctx, func(ctx context.Context) {
			defer func() { r.cleanUpToken <- struct{}{} }()
			if !r.lastDeleteTime.IsZero() && currentTime.Sub(r.lastDeleteTime) < r.minCleanupInterval {
				return
			}
			// The keys are built as the store's Lua script builds them; the two
			// must stay in step.
			for _, priority := range priorities {
				remErr := r.store.RemoveKeysBefore(ctx, getKeyWithPriority(requestID, priority), cleanRangeTill)
				if remErr != nil {
					metrics.RecordRateLimitError()
					logger.Error(ctx, "error removing keys from store", zap.String("requestID", requestID), zap.Error(remErr))
				}
			}
			r.lastDeleteTime = currentTime
		})
	default:
		// skip cleanup
	}
}

func getKeyWithPriority(key string, priority Priority) string {
	return key + ":" + strconv.Itoa(int(priority))
}

func (r *RateLimiterImpl) AcquireUptoN(ctx context.Context, resource, requestIdentifier string, n int64, setExpiry bool) (int64, time.Time, error) {
	metrics.RecordAllowBulkRequest()
	currentTime := r.clk.Now()
	defer func() { metrics.RecordBulkAllowTime(float64(time.Since(currentTime).Milliseconds())) }()
	if ok := r.rateLimitConf.IsResourcePresent(resource); !ok {
		return 0, time.Time{}, fmt.Errorf("resource passed does not have valid rate limiter config : %s", resource)
	}
	requestID := r.rateLimitConf.GetKeyWithNamespace(resource, requestIdentifier)
	dur := r.rateLimitConf.GetPeriod(resource)
	addedCount, addedTimeNanos, err := r.store.AddMaxN(ctx, requestID, n, r.rateLimitConf.GetMaxRequestCount(resource), dur.Nanoseconds())
	addedTime := time.Unix(0, addedTimeNanos)
	if err != nil {
		metrics.RecordRateLimitBulkError()
		return 0, addedTime, fmt.Errorf("adding max n: %w", err)
	}
	// clean up stale values in go routine
	r.cleanupStaleEntries(reqctx.CloneCtx(ctx), requestID, addedTime.Add(-r.rateLimitConf.GetPeriod(resource)).Add(-r.cleanupBuffer), addedTime)
	if setExpiry {
		// This can be done in separate go routine as it does not affect functionality of rate limiter
		goroutine.RunWithDefaultTimeout(reqctx.CloneCtx(ctx), func(ctx context.Context) {
			if refreshErr := r.store.RefreshKeyTTL(ctx, requestID, r.rateLimitConf.GetPeriod(resource)); refreshErr != nil {
				logger.Error(ctx, "error refreshing key expiry in store", zap.String("requestID", requestID), zap.Error(refreshErr))
			}
		})
	}

	return addedCount, addedTime, nil
}

func (r *RateLimiterImpl) ReleaseUptoN(ctx context.Context, resource, requestIdentifier string, n int64, ts time.Time) (int64, error) {
	metrics.RecordRemoveBulkRequest()
	currentTime := r.clk.Now()
	configKey := r.rateLimitConf.GetKeyWithNamespace(resource, requestIdentifier)
	metrics.RecordBulkRemoveTime(float64(time.Since(currentTime).Milliseconds()))
	rem, remErr := r.store.RemoveMaxN(ctx, configKey, ts.UnixNano(), n)
	if remErr != nil {
		metrics.RecordRateLimitBulkError()
		return 0, fmt.Errorf("removing max n: %w", remErr)
	}
	metrics.RecordBulkRemoveTime(float64(time.Since(currentTime).Milliseconds()))
	return rem, nil
}

func (r *RateLimiterImpl) AcquireWithPriority(ctx context.Context, resource, requestIdentifier string, priority Priority) (bool, error) {
	metrics.RecordAllow()

	if ok := r.rateLimitConf.IsResourcePresent(resource); !ok {
		return false, fmt.Errorf("resource passed does not have valid rate limiter config : %s", resource)
	}

	requestID := r.rateLimitConf.GetKeyWithNamespace(resource, requestIdentifier)
	rangeEndTime := r.clk.Now()
	rangeStartTime := rangeEndTime.Add(-r.rateLimitConf.GetPeriod(resource))
	priorityDistributionMap := getPriorityBasedRates(r.rateLimitConf.GetMaxRequestCountForPriorityDistribution(resource), r.rateLimitConf.GetMaxRequestCount(resource))
	expiry := r.rateLimitConf.GetPeriod(resource)
	booked, err := r.store.AddWithPriority(ctx, requestID, rangeStartTime, rangeEndTime, priorityDistributionMap, priority, expiry)
	if err != nil {
		metrics.RecordRateLimitError()
		return false, fmt.Errorf("adding with priority: %w", err)
	}

	// clean up stale values in go routine
	r.cleanupStaleEntriesWithPriority(reqctx.CloneCtx(ctx), requestID, rangeStartTime.Add(-r.cleanupBuffer), rangeEndTime)
	return booked, nil
}

func getPriorityBasedRates(priorityDistribution *PriorityDistribution, totalRate int64) map[Priority]int64 {
	if !priorityDistribution.IsEnabled() {
		return map[Priority]int64{
			PriorityHigh:   totalRate,
			PriorityMedium: totalRate,
			PriorityLow:    totalRate,
		}
	}

	return map[Priority]int64{
		PriorityHigh:   (priorityDistribution.High() * totalRate) / 100,
		PriorityMedium: (priorityDistribution.Medium() * totalRate) / 100,
		PriorityLow:    (priorityDistribution.Low() * totalRate) / 100,
	}
}
