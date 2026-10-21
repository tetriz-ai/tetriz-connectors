// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package pacing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
)

// scriptedGateway is a minimal sdk.VendorGateway whose Execute replays a scripted
// sequence of (resp, err) results, one per call, repeating the final entry once
// the script is exhausted. A case that should succeed "eventually" lists its
// rejections then one success. calls records how many times Execute ran, which
// is what separates a retry from an abort.
type scriptedGateway struct {
	results []gwResult
	calls   int
}

type gwResult struct {
	resp proto.Message
	err  error
}

func (g *scriptedGateway) Execute(_ context.Context, _ proto.Message) (proto.Message, error) {
	idx := g.calls
	g.calls++
	if idx >= len(g.results) {
		idx = len(g.results) - 1
	}
	return g.results[idx].resp, g.results[idx].err
}

// Only Execute is exercised by the pacing helper; the rest of the interface is
// unused and must never be reached.
func (g *scriptedGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	panic("scriptedGateway.Do: unused")
}

func (g *scriptedGateway) DoJSON(context.Context, string, string, any, any) error {
	panic("scriptedGateway.DoJSON: unused")
}

func (g *scriptedGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	panic("scriptedGateway.WithRateLimits: unused")
}

func (g *scriptedGateway) ForGroup(string) sdk.VendorGateway {
	panic("scriptedGateway.ForGroup: unused")
}

// localLimitErr mirrors the shape our own limiter's rejection travels in: a
// *sdk.SyncError of kind rate_limit whose cause wraps the sdk.ErrLocalRateLimit
// sentinel, carrying the limiter's sized wait as RetryAfterSeconds.
func localLimitErr(retryAfterSeconds int) error {
	return &sdk.SyncError{
		Kind:              sdk.ErrKindRateLimit,
		Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
		Retryable:         true,
		RetryAfterSeconds: retryAfterSeconds,
	}
}

func TestExecuteWaitingForLocalCapacity(t *testing.T) {
	okResp := wrapperspb.String("ok")
	// A vendor 429: kind rate_limit, but no local sentinel in the cause chain —
	// the vendor is pacing us, so it must abort, not wait.
	vendor429 := &sdk.SyncError{
		Kind:              sdk.ErrKindRateLimit,
		Cause:             errors.New("bitbucket: 429 Too Many Requests"),
		HTTPStatus:        429,
		RetryAfterSeconds: 60,
	}
	genericErr := errors.New("boom")

	tests := []struct {
		name string
		// results is the scripted Execute sequence.
		results []gwResult
		// timeout is the ctx deadline from now; 0 means no deadline (Background).
		timeout time.Duration
		// wantCalls is the exact Execute call count — the retry-vs-abort proof.
		wantCalls int
		wantErr   bool
		// errTarget, when set, is asserted via errors.Is on the returned error.
		errTarget error
		// assertNotLocal asserts the returned error is NOT the local sentinel
		// (i.e. a vendor/other error was returned unchanged, not waited on).
		assertNotLocal bool
		// wantResp, when set, must be the exact response returned.
		wantResp proto.Message
		// maxElapsed (>0) proves no sleep happened; minElapsed (>0) proves a real wait did.
		maxElapsed time.Duration
		minElapsed time.Duration
	}{
		{
			name:       "succeeds first try, no wait",
			results:    []gwResult{{resp: okResp}},
			wantCalls:  1,
			wantResp:   okResp,
			maxElapsed: 800 * time.Millisecond,
		},
		{
			name: "waits out two local-limit rejections then succeeds",
			results: []gwResult{
				{err: localLimitErr(1)},
				{err: localLimitErr(1)},
				{resp: okResp},
			},
			timeout:    3 * time.Minute, // ample for two 1s waits (each needs > 1s + 90s margin)
			wantCalls:  3,
			wantResp:   okResp,
			minElapsed: time.Second, // at least one real wait elapsed
		},
		{
			name:           "vendor 429 aborts immediately without waiting",
			results:        []gwResult{{err: vendor429}},
			timeout:        3 * time.Minute, // a deadline IS present; the abort is due to the error kind, not the clock
			wantCalls:      1,
			wantErr:        true,
			errTarget:      vendor429,
			assertNotLocal: true,
			maxElapsed:     800 * time.Millisecond,
		},
		{
			name:           "generic error aborts immediately without waiting",
			results:        []gwResult{{err: genericErr}},
			timeout:        3 * time.Minute,
			wantCalls:      1,
			wantErr:        true,
			errTarget:      genericErr,
			assertNotLocal: true,
			maxElapsed:     800 * time.Millisecond,
		},
		{
			// A single hint larger than the whole per-run cap (maxPacingWaitPerRun,
			// 5m) is unaffordable on the FIRST rejection — canAfford refuses on the
			// budget cap before ever consulting the clock — so pacing is inert and
			// returns immediately. Locks the "inert when hint exceeds budget" cliff.
			name:       "hint larger than the per-run cap gives up on first rejection without sleeping",
			results:    []gwResult{{err: localLimitErr(301)}}, // 301s > 5m cap
			timeout:    time.Hour,                             // ample deadline: the cap, not the clock, refuses
			wantCalls:  1,
			wantErr:    true,
			errTarget:  sdk.ErrLocalRateLimit,
			maxElapsed: 800 * time.Millisecond,
		},
		{
			name:       "no deadline fails closed on local limit without sleeping",
			results:    []gwResult{{err: localLimitErr(1)}},
			timeout:    0, // Background: nothing would bound the sleep, so never wait
			wantCalls:  1,
			wantErr:    true,
			errTarget:  sdk.ErrLocalRateLimit,
			maxElapsed: 800 * time.Millisecond,
		},
		{
			name:       "deadline too near to fit wait plus margin fails closed",
			results:    []gwResult{{err: localLimitErr(1)}},
			timeout:    10 * time.Second, // < 1s wait + 90s safety margin
			wantCalls:  1,
			wantErr:    true,
			errTarget:  sdk.ErrLocalRateLimit,
			maxElapsed: 800 * time.Millisecond,
		},
	}

	// t.Parallel() is deliberately omitted on these subtests: each asserts on
	// elapsed wall-clock time (maxElapsed/minElapsed), which parallel scheduling
	// jitter would make flaky.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}
			gw := &scriptedGateway{results: tt.results}

			start := time.Now()
			resp, err := ExecuteWaitingForLocalCapacity(ctx, gw, wrapperspb.String("req"), &PacingBudget{}, "bitbucket", "fetch_prs", "org-test")
			elapsed := time.Since(start)

			assert.Equal(t, tt.wantCalls, gw.calls, "Execute call count")
			if tt.wantErr {
				require.Error(t, err)
				if tt.errTarget != nil {
					require.ErrorIs(t, err, tt.errTarget)
				}
				if tt.assertNotLocal {
					require.NotErrorIs(t, err, sdk.ErrLocalRateLimit, "vendor/other error must not be reported as the local sentinel")
				}
			} else {
				require.NoError(t, err)
			}
			if tt.wantResp != nil {
				assert.Same(t, tt.wantResp, resp, "the successful attempt's response must reach the caller unchanged")
			}
			if tt.maxElapsed > 0 {
				assert.Less(t, elapsed, tt.maxElapsed, "must not sleep on this path")
			}
			if tt.minElapsed > 0 {
				assert.GreaterOrEqual(t, elapsed, tt.minElapsed, "must have waited out the local-limit rejection")
			}
		})
	}
}

// TestExecuteWaitingForLocalCapacity_GiveupMetric proves the give-up counter
// increments with the right reason label when pacing refuses to wait.
func TestExecuteWaitingForLocalCapacity_GiveupMetric(t *testing.T) {
	// t.Parallel() omitted: these read a package-global counter, so concurrent
	// subtests touching the same series would race the before/after delta.
	const (
		conn = sdk.ConnectorID("bitbucket")
		op   = sdk.OperationID("fetch_prs")
		org  = "org-giveup-test"
	)
	metricOrg := tenantlabel.OrgID(org)

	tests := []struct {
		name    string
		timeout time.Duration
		reason  string
	}{
		{
			name:    "no deadline gives up with no_deadline reason",
			timeout: 0, // Background: nothing would bound the sleep
			reason:  giveupReasonNoDeadline,
		},
		{
			name:    "deadline too near gives up with deadline_too_near reason",
			timeout: 10 * time.Second, // < 1s wait + 90s safety margin
			reason:  giveupReasonDeadlineTooNear,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}
			gw := &scriptedGateway{results: []gwResult{{err: localLimitErr(1)}}}

			child := LocalRateLimitGiveupTotal.WithLabelValues(string(conn), string(op), metricOrg, tt.reason)
			before := testutil.ToFloat64(child)

			_, err := ExecuteWaitingForLocalCapacity(ctx, gw, wrapperspb.String("req"), &PacingBudget{}, conn, op, org)
			require.Error(t, err)
			require.ErrorIs(t, err, sdk.ErrLocalRateLimit)

			assert.Equal(t, before+1, testutil.ToFloat64(child),
				"give-up counter must increment for reason %q", tt.reason)
		})
	}
}

// TestExecuteWaitingForLocalCapacity_CollapsesIndividualOrgLabel proves the emit
// site wraps the org label through tenantlabel.OrgID, so a live individual
// tenant records under the collapsed constant label and not its raw org id (the
// ~10k-series fan-out this collapse exists to prevent).
func TestExecuteWaitingForLocalCapacity_CollapsesIndividualOrgLabel(t *testing.T) {
	// t.Parallel() omitted: mutates the package-global live-individual set and reads
	// a package-global counter.
	const (
		conn          = sdk.ConnectorID("bitbucket")
		op            = sdk.OperationID("fetch_reviews")
		individualOrg = "indiv-org-collapse-test"
	)
	restore := tenantlabel.SetLiveIndividualSetForTest(map[string]struct{}{individualOrg: {}})
	defer restore()

	// Precondition: this org genuinely collapses, so the assertion on the collapsed
	// label is meaningful rather than trivially equal to the raw id.
	collapsed := tenantlabel.OrgID(individualOrg)
	require.NotEqual(t, individualOrg, collapsed, "individual org must collapse to a constant label")

	rawChild := LocalRateLimitGiveupTotal.WithLabelValues(string(conn), string(op), individualOrg, giveupReasonNoDeadline)
	collapsedChild := LocalRateLimitGiveupTotal.WithLabelValues(string(conn), string(op), collapsed, giveupReasonNoDeadline)
	rawBefore := testutil.ToFloat64(rawChild)
	collapsedBefore := testutil.ToFloat64(collapsedChild)

	// A no-deadline give-up is deterministic and never sleeps.
	gw := &scriptedGateway{results: []gwResult{{err: localLimitErr(1)}}}
	_, err := ExecuteWaitingForLocalCapacity(context.Background(), gw, wrapperspb.String("req"), &PacingBudget{}, conn, op, individualOrg)
	require.Error(t, err)

	assert.Equal(t, collapsedBefore+1, testutil.ToFloat64(collapsedChild),
		"metric must record under the collapsed individual label")
	assert.Equal(t, rawBefore, testutil.ToFloat64(rawChild),
		"metric must NOT record under the raw individual org id")
}
