// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropicgateway

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestMain(m *testing.M) {
	os.Exit(testguard.Run(m))
}

// anthropicCapPerMinute is the cap Anthropic publishes for the Claude Enterprise
// Analytics family, stated per organisation rather than per endpoint. The
// requests-per-second value in gateway/defaults.yaml is derived from it, so a
// test re-deriving the limiter's budget compares against this rather than against
// the derived value it is checking.
const anthropicCapPerMinute = 60

// testWindowStart is a fixed window start for tests that only need a valid one.
func testWindowStart() *timestamppb.Timestamp {
	return timestamppb.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
}

// allFiveRequests returns one request of each type the factory dispatches on. Every
// test that must cover the whole family builds from this, so adding a sixth endpoint
// without extending the family's coverage is not possible by omission.
func allFiveRequests() []proto.Message {
	return []proto.Message{
		&connectorpb.AnthropicAnalyticsUsersRequest{Date: "2026-08-01"},
		&connectorpb.AnthropicAnalyticsSkillsRequest{Date: "2026-08-01"},
		&connectorpb.AnthropicAnalyticsUserCostReportRequest{StartingAt: testWindowStart()},
		&connectorpb.AnthropicAnalyticsUserUsageReportRequest{StartingAt: testWindowStart()},
		&connectorpb.AnthropicOrganizationMembersRequest{},
	}
}

// TestTheWholeFamilyIsOneRateLimitGroup is the property this gateway exists for.
// Anthropic meters 60 requests per minute per ORGANISATION across the whole family,
// so the five endpoints must draw on one budget. A group is the rate-limit pool, so
// the assertion is that the config declares exactly one group and every endpoint
// resolves against it — five groups would each be granted the organisation's whole
// budget and collectively spend five times it.
func TestTheWholeFamilyIsOneRateLimitGroup(t *testing.T) {
	t.Parallel()

	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)
	endpoints := cfg.AnthropicEndpoints()

	require.Len(t, endpoints, 1,
		"one group per organisation-level cap: a second group would be a second budget the vendor never granted")
	ep, ok := endpoints[constants.GroupAnthropicAnalytics]
	require.Truef(t, ok, "the single group must be %q", constants.GroupAnthropicAnalytics)

	// The Len assertion above is the group property. This loop is a weaker but
	// distinct check: that no request builder hardcodes a host of its own.
	factory := NewAnthropicVendorRequest("key", map[string]string{
		constants.GroupAnthropicAnalytics: ep.BaseURL,
	})
	for _, req := range allFiveRequests() {
		vendorReq, err := factory(req)
		require.NoErrorf(t, err, "factory rejected %T", req)
		assert.Containsf(t, vendorReq.URL(), ep.BaseURL,
			"%T must resolve against the single group's base URL", req)
	}
}

// countingLimiter records every acquisition so a test can prove which bucket a
// request was charged to.
type countingLimiter struct {
	mu   sync.Mutex
	keys []string
}

func (c *countingLimiter) IsResourceConfigured(string) bool { return true }

func (c *countingLimiter) Acquire(_ context.Context, _, key string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = append(c.keys, key)
	return true, nil
}

func (c *countingLimiter) AcquireUptoN(context.Context, string, string, int64, bool) (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

func (c *countingLimiter) ReleaseUptoN(context.Context, string, string, int64, time.Time) (int64, error) {
	return 0, nil
}

// recordingDoer stands in for the transport.
type recordingDoer struct {
	reqs []*http.Request
}

func (d *recordingDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	d.reqs = append(d.reqs, req)
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

// TestBucketKeyIsScopedPerOrganisation pins the bucket key's derivation, which is
// what decides whose budget a request spends. Two organisations must land on
// different keys — otherwise one organisation's sync starves another's — while every
// endpoint within one organisation lands on the same key, so the vendor's
// per-organisation metering and ours agree rather than approximate each other.
//
// Note what this does NOT prove: the doer is built with one group, so the same-key
// half cannot catch a caller that asks ForGroup for a second group. The single group
// is pinned statically instead, by TestTheWholeFamilyIsOneRateLimitGroup.
func TestBucketKeyIsScopedPerOrganisation(t *testing.T) {
	t.Parallel()

	factory := NewAnthropicVendorRequest("key", map[string]string{
		constants.GroupAnthropicAnalytics: "https://api.anthropic.com",
	})

	// One request is enough: key derivation reads the group and the context, never
	// the request, so looping over all five would assert nothing extra.
	charge := func(t *testing.T, ctx context.Context) string {
		t.Helper()
		limiter := &countingLimiter{}
		doer := &rateLimitedDoer{
			inner:       &recordingDoer{},
			rateLimiter: limiter,
			group:       constants.GroupAnthropicAnalytics,
			window:      time.Minute,
		}
		vendorReq, err := factory(&connectorpb.AnthropicOrganizationMembersRequest{})
		require.NoError(t, err)
		httpReq, err := http.NewRequestWithContext(ctx, vendorReq.HTTPMethod(), vendorReq.URL(), nil)
		require.NoError(t, err)
		_, err = doer.Do(ctx, httpReq)
		require.NoError(t, err)
		require.Len(t, limiter.keys, 1, "the request must pass the limiter")
		return limiter.keys[0]
	}

	first := charge(t, reqctx.WithOrgID(context.Background(), "org-1"))
	second := charge(t, reqctx.WithOrgID(context.Background(), "org-2"))

	assert.Equal(t, constants.GroupAnthropicAnalytics+":org-1", first,
		"a request must be charged to its own organisation's bucket")
	assert.NotEqual(t, first, second,
		"two organisations sharing a bucket would let one starve the other")

	// An orgless context must not silently borrow another organisation's bucket; it
	// falls back to the bare group, which is the shared-and-obvious case.
	assert.Equal(t, constants.GroupAnthropicAnalytics, charge(t, context.Background()))
}

// TestConfiguredRateStaysUnderAnthropicsPublishedCap re-derives the enforced budget
// from the configured rate and compares it against the published cap, so raising the
// yml value past what the vendor allows fails here rather than in real use.
func TestConfiguredRateStaysUnderAnthropicsPublishedCap(t *testing.T) {
	t.Parallel()

	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)
	ep, ok := cfg.AnthropicEndpoints()[constants.GroupAnthropicAnalytics]
	require.True(t, ok)

	rate, period := scaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
	require.LessOrEqual(t, period, time.Minute,
		"a window wider than a minute cannot bound a per-minute cap")

	burstInAMinute := float64(rate) * (float64(time.Minute) / float64(period))
	assert.LessOrEqualf(t, burstInAMinute, float64(anthropicCapPerMinute),
		"%v rps permits %.0f requests in a minute against a published cap of %d",
		ep.RequestsPerSecond, burstInAMinute, anthropicCapPerMinute)
}

// TestConfiguredWindowMatchesTheCapsUnit is the lower bound the cap test cannot
// give. That test only checks the budget is not too generous, so a window NARROWER
// than the minute Anthropic states its cap in would satisfy it while making the
// operations unusable: the limiter rejects rather than waits, and these operations
// page back-to-back, so a second request inside one second would fail.
func TestConfiguredWindowMatchesTheCapsUnit(t *testing.T) {
	t.Parallel()

	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)
	ep, ok := cfg.AnthropicEndpoints()[constants.GroupAnthropicAnalytics]
	require.True(t, ok)

	_, period := scaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
	assert.GreaterOrEqualf(t, period, time.Minute,
		"%v rps lands on a %v window, narrower than the minute Anthropic states its cap in",
		ep.RequestsPerSecond, period)
}

// TestScaleRateToIntegerPerPeriod covers both invariants the function owes, which
// fail differently: rounding up breaks the average rate, while widening the window
// breaks the burst — the limiter bounds a count over a trailing window, so an hourly
// bucket for a per-minute cap lets the whole hour's budget through in the first
// minute while every average-rate assertion still passes.
func TestScaleRateToIntegerPerPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reqsPerSec float64
		wantRate   int64
		wantPeriod time.Duration
	}{
		{
			name:       "the configured rate lands on a per-minute window under the cap",
			reqsPerSec: 0.9,
			wantRate:   54,
			wantPeriod: time.Minute,
		},
		{
			name:       "a whole rate is still expressed per minute, never per second",
			reqsPerSec: 1,
			wantRate:   60,
			wantPeriod: time.Minute,
		},
		{
			name:       "a rate too small for a minute widens to an hour rather than rounding to zero",
			reqsPerSec: 0.001,
			wantRate:   3,
			wantPeriod: time.Hour,
		},
		{
			// Below one an hour the floor would grant one anyway, which is the
			// round-up the first invariant forbids: 0.0001 rps asks for 0.36 an hour.
			name:       "a rate below one an hour widens the window rather than rounding the count up",
			reqsPerSec: 0.0001,
			wantRate:   1,
			wantPeriod: 10000 * time.Second,
		},
		{
			name:       "exactly one an hour still lands on the hour",
			reqsPerSec: 1.0 / 3600,
			wantRate:   1,
			wantPeriod: time.Hour,
		},
		{
			// A time.Duration is int64 nanoseconds, so an absurdly small rate would
			// overflow the widened window and wrap NEGATIVE — no bound at all, which
			// is worse than the round-up the widening exists to avoid.
			name:       "a rate too small even to express as a window clamps rather than overflowing",
			reqsPerSec: 1e-300,
			wantRate:   1,
			wantPeriod: time.Duration(maxWindowSeconds) * time.Second,
		},
		{
			name:       "a non-positive rate cannot be expressed and falls back rather than permitting everything",
			reqsPerSec: 0,
			wantRate:   1,
			wantPeriod: time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rate, period := scaleRateToIntegerPerPeriod(tt.reqsPerSec)
			assert.Equal(t, tt.wantRate, rate)
			assert.Equal(t, tt.wantPeriod, period)
		})
	}
}

// TestBuildRateLimitConfigFromFramework_RejectsUnusableRate pins the refusal. A
// non-positive override would fall through the scaling floor to 1/s — 60/min, the
// published cap with none of the margin the configured value keeps — so it must be
// rejected rather than loosening the limiter. TestUnusableRatesAreRefused covers the
// values that survive a bare non-positive test.
func TestBuildRateLimitConfigFromFramework_RejectsUnusableRate(t *testing.T) {
	t.Parallel()

	_, err := buildRateLimitConfigFromFramework([]sdk.RateLimitConfig{
		{Group: constants.GroupAnthropicAnalytics, RequestsPerSecond: 0},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unusable rate")
}

// TestLocalRejectionWaitsOutItsOwnWindow pins the hint the scheduler cannot derive:
// its default backoff is seconds wide while the window that rejected is a minute, so
// without this every retry is spent inside the same exhausted window.
func TestLocalRejectionWaitsOutItsOwnWindow(t *testing.T) {
	t.Parallel()

	doer := &rateLimitedDoer{
		inner:       &recordingDoer{},
		rateLimiter: denyingLimiter{},
		group:       constants.GroupAnthropicAnalytics,
		window:      time.Minute,
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.anthropic.com/v1/organizations/users", nil)
	require.NoError(t, err)

	_, err = doer.Do(context.Background(), req)
	require.Error(t, err)

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindRateLimit, syncErr.Kind)
	assert.True(t, syncErr.Retryable)
	assert.GreaterOrEqual(t, syncErr.RetryAfterSeconds, 60,
		"a retry sooner than the window that rejected is spent against the same exhausted window")
	assert.ErrorIs(t, err, sdk.ErrLocalRateLimit,
		"the marker is what lets a caller tell our own bucket from a vendor 429 without matching prose")
}

// denyingLimiter rejects every acquisition, the only state that reaches the branch
// under test.
type denyingLimiter struct{}

func (denyingLimiter) IsResourceConfigured(string) bool { return true }

func (denyingLimiter) Acquire(context.Context, string, string) (bool, error) { return false, nil }

func (denyingLimiter) AcquireUptoN(context.Context, string, string, int64, bool) (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

func (denyingLimiter) ReleaseUptoN(context.Context, string, string, int64, time.Time) (int64, error) {
	return 0, nil
}

// The tests below are the same as the cursor gateway's, whose rate-limiter plumbing
// this package copies byte-for-byte. Copying the implementation without them left
// five mutations alive — the window fallback, the ForGroup guard, the WithRateLimits
// keep-on-error path, and both constructor refusals could each be broken with the
// suite still green. The implementation being a copy is exactly why the coverage has
// to be one too.

// TestWindowForGroupIsTheCapsUnit pins the fallback. Dropping it to a second would
// hand a rejected caller a wait shorter than the bucket it must outlast, which is the
// defect the retry-after hint exists to prevent.
func TestWindowForGroupIsTheCapsUnit(t *testing.T) {
	t.Parallel()

	g := &AnthropicVendorGateway{
		rateLimits: []sdk.RateLimitConfig{
			{Group: constants.GroupAnthropicAnalytics, RequestsPerSecond: 0.9},
		},
	}
	assert.Equal(t, time.Minute, g.windowForGroup(constants.GroupAnthropicAnalytics),
		"a configured group reports the window its rate scales to")
	assert.Equal(t, time.Minute, g.windowForGroup("no-such-group"),
		"an unconfigured group falls back to the minute the cap is stated in, not a second")
}

// TestForGroupStampsTheWindowOntoTheDoer pins the wiring between the two: the doer
// carries the window, and a doer built without it silently reverts to the scheduler's
// default backoff.
func TestForGroupStampsTheWindowOntoTheDoer(t *testing.T) {
	t.Parallel()

	g := &AnthropicVendorGateway{
		rateLimiter: denyingLimiter{},
		baseDoer:    &recordingDoer{},
		rateLimits: []sdk.RateLimitConfig{
			{Group: constants.GroupAnthropicAnalytics, RequestsPerSecond: 0.9},
		},
	}
	scoped, ok := g.ForGroup(constants.GroupAnthropicAnalytics).(*AnthropicVendorGateway)
	require.True(t, ok)
	doer, ok := scoped.Doer.(*rateLimitedDoer)
	require.True(t, ok, "a configured group must have its doer wrapped")
	assert.Equal(t, time.Minute, doer.window)
	assert.Equal(t, constants.GroupAnthropicAnalytics, doer.group)
}

// unconfiguredLimiter reports every group as unconfigured.
type unconfiguredLimiter struct{ denyingLimiter }

func (unconfiguredLimiter) IsResourceConfigured(string) bool { return false }

// TestForGroup_WrapsTheDoerOnlyForAConfiguredGroup pins the guard. Without it a
// typo'd group would wrap an unconfigured limiter; with it removed entirely, an
// unconfigured group would run unlimited. Both are silent.
func TestForGroup_WrapsTheDoerOnlyForAConfiguredGroup(t *testing.T) {
	t.Parallel()

	base := &recordingDoer{}
	// Mirror real construction: NewAnthropicVendorGateway sets both the embedded
	// Doer and baseDoer to the injected transport.
	configured := &AnthropicVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{Doer: base},
		rateLimiter:       denyingLimiter{},
		baseDoer:          base,
	}
	wrapped, ok := configured.ForGroup(constants.GroupAnthropicAnalytics).(*AnthropicVendorGateway)
	require.True(t, ok)
	assert.IsType(t, &rateLimitedDoer{}, wrapped.Doer)

	unconfigured := &AnthropicVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{Doer: base},
		rateLimiter:       unconfiguredLimiter{},
		baseDoer:          base,
	}
	unwrapped, ok := unconfigured.ForGroup(constants.GroupAnthropicAnalytics).(*AnthropicVendorGateway)
	require.True(t, ok)
	assert.Equal(t, vendorapi.HTTPDoer(base), unwrapped.Doer,
		"an unconfigured group must leave the doer alone rather than wrap a limiter that would not enforce")
}

// TestWithRateLimits_KeepsTheExistingLimiterOnABadOverride pins the direction of the
// failure. A rejected override must leave the previous limiter enforcing; nilling it
// would turn a bad config value into no rate limiting at all.
func TestWithRateLimits_KeepsTheExistingLimiterOnABadOverride(t *testing.T) {
	t.Parallel()

	existing := denyingLimiter{}
	g := &AnthropicVendorGateway{rateLimiter: existing, baseDoer: &recordingDoer{}}

	got, ok := g.WithRateLimits([]sdk.RateLimitConfig{
		{Group: constants.GroupAnthropicAnalytics, RequestsPerSecond: 0},
	}).(*AnthropicVendorGateway)
	require.True(t, ok)
	assert.Equal(t, existing, got.rateLimiter,
		"a rejected override must not replace or clear the limiter that was enforcing")
}

// TestWithRateLimits_AnEmptyOverrideDoesNotDisableLimiting pins the gap the
// non-positive guard leaves: an empty list passes every value check, yet would build
// a limiter that configures no group and so enforces nothing.
func TestWithRateLimits_AnEmptyOverrideDoesNotDisableLimiting(t *testing.T) {
	t.Parallel()

	existing := denyingLimiter{}
	g := &AnthropicVendorGateway{rateLimiter: existing, baseDoer: &recordingDoer{}}

	got, ok := g.WithRateLimits(nil).(*AnthropicVendorGateway)
	require.True(t, ok)
	assert.Equal(t, existing, got.rateLimiter,
		"an empty override must keep the limiter that was enforcing, not silently remove limiting")
}

// TestNewAnthropicVendorGateway_RefusesBadConfig pins both constructor refusals. Each
// substitutes something worse than the failure: an absent group builds a request
// against a bare path and dies inside the HTTP client as a transport error, and a
// non-positive rate floors to the published cap with no margin.
func TestNewAnthropicVendorGateway_RefusesBadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		endpoints map[string]gateway.APIEndpointConfig
		wantErr   string
	}{
		{
			name:      "no group configured",
			endpoints: map[string]gateway.APIEndpointConfig{},
			wantErr:   "no base URL configured",
		},
		{
			name: "group present but base URL empty",
			endpoints: map[string]gateway.APIEndpointConfig{
				constants.GroupAnthropicAnalytics: {Group: constants.GroupAnthropicAnalytics, RequestsPerSecond: 0.9},
			},
			wantErr: "no base URL configured",
		},
		{
			name: "unusable rate",
			endpoints: map[string]gateway.APIEndpointConfig{
				constants.GroupAnthropicAnalytics: {
					Group: constants.GroupAnthropicAnalytics, BaseURL: "https://api.anthropic.com",
				},
			},
			wantErr: "unusable rate",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateAnthropicEndpoints(tt.endpoints)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}

	require.NoError(t, validateAnthropicEndpoints(map[string]gateway.APIEndpointConfig{
		constants.GroupAnthropicAnalytics: {
			Group: constants.GroupAnthropicAnalytics, BaseURL: "https://api.anthropic.com", RequestsPerSecond: 0.9,
		},
	}), "a well-formed config must be accepted")
}

// TestRateLimitedDoer_AcquireErrorIsRetryable pins the kind and the retryability of a
// limiter failure. Marking it terminal would fail a whole sync run on a Redis blip.
func TestRateLimitedDoer_AcquireErrorIsRetryable(t *testing.T) {
	t.Parallel()

	doer := &rateLimitedDoer{
		inner:       &recordingDoer{},
		rateLimiter: erroringLimiter{},
		group:       constants.GroupAnthropicAnalytics,
		window:      time.Minute,
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		"https://api.anthropic.com/v1/organizations/users", nil)
	require.NoError(t, err)

	_, err = doer.Do(context.Background(), req)
	require.Error(t, err)

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindRateLimit, syncErr.Kind)
	assert.True(t, syncErr.Retryable, "a limiter outage must not fail the run terminally")
	assert.NotErrorIs(t, err, sdk.ErrLocalRateLimit,
		"an acquire failure is not a local rejection; the marker must distinguish them")
}

// erroringLimiter fails every acquisition, standing in for a limiter-store outage.
type erroringLimiter struct{ denyingLimiter }

func (erroringLimiter) Acquire(context.Context, string, string) (bool, error) {
	return false, assert.AnError
}

// TestUnusableRatesAreRefused pins the guard that a bare non-positive test misses.
// NaN and +Inf both survive `<= 0`, yet NaN scales to a zero-width window and +Inf to
// a count of MaxInt64 — unlimited. Both can arrive through a per-organisation override
// carrying an arbitrary float64, which is the same door a bad positive value uses.
func TestUnusableRatesAreRefused(t *testing.T) {
	t.Parallel()

	for _, rate := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		t.Run(fmt.Sprintf("%v", rate), func(t *testing.T) {
			t.Parallel()

			err := validateAnthropicEndpoints(map[string]gateway.APIEndpointConfig{
				constants.GroupAnthropicAnalytics: {
					Group:             constants.GroupAnthropicAnalytics,
					BaseURL:           "https://api.anthropic.com",
					RequestsPerSecond: rate,
				},
			})
			require.Errorf(t, err, "rate %v must be refused", rate)
			assert.Contains(t, err.Error(), "unusable rate")

			_, err = buildRateLimitConfigFromFramework([]sdk.RateLimitConfig{
				{Group: constants.GroupAnthropicAnalytics, RequestsPerSecond: rate},
			})
			require.Errorf(t, err, "an override of %v must be refused", rate)
		})
	}
}
