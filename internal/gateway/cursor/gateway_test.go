// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursorgateway

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	mocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestMain(m *testing.M) {
	os.Exit(testguard.Run(m))
}

// cursorAdminCapPerMinute and cursorUsageCapPerMinute are the caps Cursor
// publishes for the two endpoint groups. The requests-per-second values in
// gateway/defaults.yaml are derived from them, so a test that re-derives the
// limiter's budget has to compare against these rather than against the derived
// value it is checking.
const (
	cursorAdminCapPerMinute = 20
	cursorUsageCapPerMinute = 60
)

// TestScaleRateToIntegerPerPeriod covers both invariants the function owes, and
// they fail differently. Rounding up breaks the average rate: Ceil(0.33)
// would become 1/s and let real 429s through. Widening the window breaks
// the burst instead: the limiter bounds a count over a trailing window, so an
// hourly bucket for a per-minute cap lets the whole hour's budget through in the
// first minute while every average-rate assertion still passes.
func TestScaleRateToIntegerPerPeriod(t *testing.T) {
	tests := []struct {
		name       string
		reqsPerSec float64
		wantRate   int64
		wantPeriod time.Duration
	}{
		{
			name:       "the admin group's fractional rate lands on a per-minute window",
			reqsPerSec: 0.33,
			wantRate:   19,
			wantPeriod: time.Minute,
		},
		{
			name:       "a whole rate is still expressed in the cap's own minute",
			reqsPerSec: 1,
			wantRate:   60,
			wantPeriod: time.Minute,
		},
		{
			name:       "zero is a misconfiguration and floors to a usable 1/s",
			reqsPerSec: 0,
			wantRate:   1,
			wantPeriod: time.Second,
		},
		{
			name:       "negative is a misconfiguration and floors to a usable 1/s",
			reqsPerSec: -5,
			wantRate:   1,
			wantPeriod: time.Second,
		},
		{
			// A rate below 1/min cannot be expressed on a minute window without
			// flooring to zero, which would disable the limiter, so it is the
			// one case that legitimately widens to an hour.
			name:       "a rate below one per minute widens to an hourly window",
			reqsPerSec: 0.01,
			wantRate:   36,
			wantPeriod: time.Hour,
		},
		{
			// Below 1/3600 even the hourly floor rounds to zero, so the function
			// bumps it to 1-per-hour. This is the sole case that exceeds the
			// configured rate, and it is exempt from the checks below.
			name:       "a rate below one per hour still floors to a usable limiter",
			reqsPerSec: 0.0001,
			wantRate:   1,
			wantPeriod: time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rate, period := scaleRateToIntegerPerPeriod(tt.reqsPerSec)
			require.Equal(t, tt.wantRate, rate)
			require.Equal(t, tt.wantPeriod, period)

			if tt.reqsPerSec < 1.0/3600 {
				return
			}

			effectivePerSec := float64(rate) / period.Seconds()
			assert.LessOrEqualf(t, effectivePerSec, tt.reqsPerSec+1e-9,
				"average rate %.6f/s must not exceed the configured %.6f/s", effectivePerSec, tt.reqsPerSec)

			// The burst check: the whole bucket drained the instant the window
			// opens must still respect the configured rate read as a per-minute
			// budget. An hourly window for a per-minute cap fails here and
			// passes the average check above, which is why both exist.
			if period <= time.Minute {
				assert.LessOrEqualf(t, float64(rate), tt.reqsPerSec*60+1e-9,
					"a full bucket of %d over %s exceeds the configured %.6f/min", rate, period, tt.reqsPerSec*60)
			}
		})
	}
}

// TestConfiguredRatesStayUnderCursorsPublishedCaps re-derives the enforced budget
// from the rate the yml actually carries and compares it against Cursor's own
// published number. It reads the config rather than restating it, because a
// second copy of the rate here would leave the guard green when someone raises
// the yml value — which is the only mistake it exists to catch.
func TestConfiguredRatesStayUnderCursorsPublishedCaps(t *testing.T) {
	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)
	endpoints := cfg.CursorEndpoints()

	tests := []struct {
		group        string
		capPerMinute int64
	}{
		{group: constants.GroupCursorAdmin, capPerMinute: cursorAdminCapPerMinute},
		{group: constants.GroupCursorUsage, capPerMinute: cursorUsageCapPerMinute},
	}

	for _, tt := range tests {
		t.Run(tt.group, func(t *testing.T) {
			t.Parallel()
			ep, ok := endpoints[tt.group]
			require.Truef(t, ok, "no endpoint configured for group %q", tt.group)

			rate, period := scaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
			require.LessOrEqual(t, period, time.Minute,
				"a window wider than a minute cannot bound a per-minute cap")

			burstInAMinute := float64(rate) * (float64(time.Minute) / float64(period))
			assert.LessOrEqualf(t, burstInAMinute, float64(tt.capPerMinute),
				"%s at %v rps permits %.0f requests in a minute against a published cap of %d",
				tt.group, ep.RequestsPerSecond, burstInAMinute, tt.capPerMinute)
		})
	}
}

// TestConfiguredWindowsMatchTheCapsUnit is the lower bound the cap test cannot
// give. The cap test only checks the enforced budget is not too GENEROUS, so a
// window NARROWER than the minute Cursor states its caps in satisfies it while
// making the usage walk unusable: the limiter rejects rather than waits, and that
// operation pages and chunks back-to-back, so its second request inside one second
// fails and the run dies having covered a single chunk.
//
// Asserting the window rather than a request count is what makes this independent
// of the cap numbers. A count floor binds only by arithmetic accident of today's
// caps — raise the published cap and a whole rate could satisfy both guards with a
// one-second window fully armed.
func TestConfiguredWindowsMatchTheCapsUnit(t *testing.T) {
	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)
	endpoints := cfg.CursorEndpoints()

	for _, group := range []string{constants.GroupCursorAdmin, constants.GroupCursorUsage} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			ep, ok := endpoints[group]
			require.Truef(t, ok, "no endpoint configured for group %q", group)

			_, period := scaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
			// The cap test pins the other side (no wider than a minute), so together
			// the two fix the window at exactly the unit Cursor publishes.
			assert.GreaterOrEqualf(t, period, time.Minute,
				"%s at %v rps lands on a %v window, narrower than the minute Cursor states "+
					"its cap in — a caller inside the cap that issues its requests together "+
					"has the second one rejected",
				group, ep.RequestsPerSecond, period)
		})
	}
}

// denyingLimiter rejects every acquisition, which is the only state that reaches
// the branch under test.
type denyingLimiter struct{}

func (denyingLimiter) IsResourceConfigured(string) bool { return true }

func (denyingLimiter) Acquire(context.Context, string, string) (bool, error) { return false, nil }

func (denyingLimiter) AcquireUptoN(context.Context, string, string, int64, bool) (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

func (denyingLimiter) ReleaseUptoN(context.Context, string, string, int64, time.Time) (int64, error) {
	return 0, nil
}

// TestLocalRejectionWaitsOutItsOwnWindow pins the hint the scheduler cannot derive.
// The default backoff is seconds wide; the window that rejected is a minute, so
// without this every retry is spent inside the same exhausted window and the
// attempt budget is gone before the vendor is ever reached again.
func TestLocalRejectionWaitsOutItsOwnWindow(t *testing.T) {
	t.Parallel()

	doer := &rateLimitedDoer{
		rateLimiter: denyingLimiter{},
		group:       constants.GroupCursorUsage,
		window:      time.Minute,
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.cursor.com/x", nil)
	require.NoError(t, err)

	_, doErr := doer.Do(context.Background(), req)

	var syncErr *sdk.SyncError
	require.ErrorAs(t, doErr, &syncErr)
	assert.Equal(t, sdk.ErrKindRateLimit, syncErr.Kind)
	assert.True(t, syncErr.Retryable)
	assert.GreaterOrEqualf(t, syncErr.RetryAfterSeconds, 60,
		"a rejection from a %v window asked for a %ds retry — shorter than the window, so the retry "+
			"lands inside the same exhausted budget", doer.window, syncErr.RetryAfterSeconds)
}

// TestWindowForGroupIsTheCapsUnit binds the wait above to the configured rate
// rather than a constant, so a rate change moves the retry hint with it.
func TestWindowForGroupIsTheCapsUnit(t *testing.T) {
	t.Parallel()

	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)
	gw := &CursorVendorGateway{}
	for _, ep := range cfg.CursorEndpoints() {
		gw.rateLimits = append(gw.rateLimits, ep.ToRateLimitConfig())
	}

	for _, group := range []string{constants.GroupCursorAdmin, constants.GroupCursorUsage} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			// Without this the assertion below cannot tell a correctly-scaled window
			// from the unconfigured fallback, which returns the same minute.
			configured := false
			for _, rl := range gw.rateLimits {
				if rl.Group == group && rl.RequestsPerSecond > 0 {
					configured = true
				}
			}
			require.Truef(t, configured, "no positive rate configured for group %q", group)
			assert.Equal(t, time.Minute, gw.windowForGroup(group))
		})
	}
	assert.Equal(t, time.Minute, gw.windowForGroup("not-a-configured-group"),
		"an unconfigured group must still yield a usable wait, not zero")
}

// TestForGroupStampsTheWindowOntoTheDoer covers the seam the two tests above
// each miss half of. Both exercise the window either side of ForGroup, so
// dropping the assignment inside it leaves every test in the package green while
// a rejection reverts to asking for a wait shorter than the bucket it must
// outlast — the original defect, silently restored.
func TestForGroupStampsTheWindowOntoTheDoer(t *testing.T) {
	t.Parallel()

	cfg, err := gateway.LoadConfig()
	require.NoError(t, err)
	gw := &CursorVendorGateway{rateLimiter: denyingLimiter{}}
	for _, ep := range cfg.CursorEndpoints() {
		gw.rateLimits = append(gw.rateLimits, ep.ToRateLimitConfig())
	}

	scoped, ok := gw.ForGroup(constants.GroupCursorUsage).(*CursorVendorGateway)
	require.True(t, ok)
	doer, ok := scoped.Doer.(*rateLimitedDoer)
	require.True(t, ok, "ForGroup must install the rate-limited doer")
	assert.Equal(t, time.Minute, doer.window,
		"ForGroup left the window zero-valued, so a rejection would ask for a retry shorter than its window")
}

// TestBuildRateLimitConfigFromFramework_RejectsNonPositiveRate covers the
// per-org override path. A non-positive rate would otherwise reach the scaling
// floor and come back as 1/s — above Cursor's cap — so the build has to fail and
// let WithRateLimits keep the limiter it already had.
func TestBuildRateLimitConfigFromFramework_RejectsNonPositiveRate(t *testing.T) {
	tests := []struct {
		name       string
		reqsPerSec float64
		wantErr    bool
	}{
		{name: "a positive override builds", reqsPerSec: 0.2},
		{name: "zero is rejected", reqsPerSec: 0, wantErr: true},
		{name: "negative is rejected", reqsPerSec: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildRateLimitConfigFromFramework([]sdk.RateLimitConfig{
				{Group: constants.GroupCursorAdmin, RequestsPerSecond: tt.reqsPerSec},
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "non-positive rate")
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestWithRateLimits_KeepsTheExistingLimiterOnABadOverride pins the consequence
// of the rejection above: WithRateLimits has no error return, so the only way a
// bad override can fail safe is by leaving the previous limiter in place.
// These two tests are a contract on the method, not evidence that anything calls it: the
// scheduler decides which connectors get stored rate-limit observations applied, and
// applyObservedRateLimit in runner/app/gateways.go is where that is recorded.
//
// What they hold honest is the fail-safe. WithRateLimits is part of
// sdk.VendorGateway, so it has to behave whoever calls it, and a zero or negative
// override must never disarm a limiter that was already enforcing.
func TestWithRateLimits_KeepsTheExistingLimiterOnABadOverride(t *testing.T) {
	ctrl := gomock.NewController(t)
	existing := mocks.NewMockRateLimiter(ctrl)

	base := &recordingDoer{}
	gw := &CursorVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{Doer: base},
		rateLimiter:       existing,
		baseDoer:          base,
	}

	got, ok := gw.WithRateLimits([]sdk.RateLimitConfig{
		{Group: constants.GroupCursorAdmin, RequestsPerSecond: 0},
	}).(*CursorVendorGateway)
	require.True(t, ok)
	assert.Same(t, existing, got.rateLimiter,
		"a rejected override must not replace the limiter that was already enforcing")
}

type recordingDoer struct {
	called bool
}

func (d *recordingDoer) Do(context.Context, *http.Request) (*http.Response, error) {
	d.called = true
	return &http.Response{StatusCode: http.StatusOK}, nil
}

// TestForGroup_WrapsTheDoerOnlyForAConfiguredGroup pins the branch that decides
// whether a request is rate limited at all. An unconfigured group silently
// returns the bare doer, so a typo in a group constant disables rate limiting
// for that endpoint rather than failing.
func TestForGroup_WrapsTheDoerOnlyForAConfiguredGroup(t *testing.T) {
	tests := []struct {
		name            string
		configured      bool
		wantRateLimited bool
	}{
		{name: "configured group is wrapped", configured: true, wantRateLimited: true},
		{name: "unknown group falls through unwrapped", configured: false, wantRateLimited: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			rl := mocks.NewMockRateLimiter(ctrl)
			rl.EXPECT().IsResourceConfigured(constants.GroupCursorAdmin).Return(tt.configured)

			base := &recordingDoer{}
			gw := &CursorVendorGateway{
				BaseVendorGateway: vendorapi.BaseVendorGateway{Doer: base},
				rateLimiter:       rl,
				baseDoer:          base,
			}

			scoped, ok := gw.ForGroup(constants.GroupCursorAdmin).(*CursorVendorGateway)
			require.True(t, ok)
			_, isRateLimited := scoped.Doer.(*rateLimitedDoer)
			assert.Equal(t, tt.wantRateLimited, isRateLimited)
		})
	}
}

// TestRateLimitedDoer_RefusalIsRetryable matters because the caller branches on
// it: a rate-limit refusal has to reach the orchestrator as a retryable
// SyncError, not as an opaque failure that burns the sync run.
func TestRateLimitedDoer_RefusalIsRetryable(t *testing.T) {
	tests := []struct {
		name       string
		allowed    bool
		acquireErr error
	}{
		{name: "budget exhausted", allowed: false},
		{name: "limiter itself failed", acquireErr: errors.New("redis unreachable")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			rl := mocks.NewMockRateLimiter(ctrl)
			rl.EXPECT().
				Acquire(gomock.Any(), constants.GroupCursorAdmin, constants.GroupCursorAdmin).
				Return(tt.allowed, tt.acquireErr)

			base := &recordingDoer{}
			d := &rateLimitedDoer{inner: base, rateLimiter: rl, group: constants.GroupCursorAdmin}

			req, err := http.NewRequest(http.MethodGet, "https://api.cursor.com/teams/members", nil)
			require.NoError(t, err)

			_, err = d.Do(context.Background(), req)
			require.Error(t, err)

			var syncErr *sdk.SyncError
			require.ErrorAs(t, err, &syncErr)
			assert.Equal(t, sdk.ErrKindRateLimit, syncErr.Kind)
			assert.True(t, syncErr.Retryable, "a rate-limit refusal must be retryable")
			assert.False(t, base.called, "no request may reach the vendor once the limiter refuses")
		})
	}
}
