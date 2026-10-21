// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// reportRunFor drives one real report run for a distinct organisation, so counter assertions
// do not collide across parallel tests.
func reportRunFor(
	t *testing.T, orgID string, cur *sdk.Cursor, opts operationOptions,
	settings Settings, watermark time.Time,
) *sdk.Cursor {
	t.Helper()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			resp := &vgpb.AnthropicAnalyticsUserCostReportResponse{HasMore: false}
			if !watermark.IsZero() {
				resp.DataRefreshedAt = timestamppb.New(watermark)
			}
			return resp, nil
		}).AnyTimes()

	params := testParams(t, gw, &collectingSink{}, cur, settings)
	params.Config.OrganizationID = orgID
	if opts != (operationOptions{}) {
		raw, err := json.Marshal(opts)
		require.NoError(t, err)
		params.Config.OperationOverrides = map[sdk.OperationID]json.RawMessage{
			opFetchAnalyticsUserCost: raw,
		}
	}

	out, err := (&FetchAnalyticsUserCostOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	return out
}

func covered(t *testing.T, orgID string) float64 {
	t.Helper()
	return testutil.ToFloat64(runCovered.WithLabelValues(tenantlabel.OrgID(orgID), string(opFetchAnalyticsUserCost)))
}

func yielded(t *testing.T, orgID, reason string) float64 {
	t.Helper()
	return testutil.ToFloat64(runYielded.WithLabelValues(tenantlabel.OrgID(orgID), string(opFetchAnalyticsUserCost), reason))
}

// TestRunCoveredMeansReachedThePresent pins the predicate. A plan of exactly the per-run cap
// that happens to reach the clock read its whole window — counting it as a yield would make
// every healthy run of a narrow-width organisation look like the stall the yield/covered pair
// exists to detect, because no run would ever be counted as covered.
func TestRunCoveredMeansReachedThePresent(t *testing.T) {
	t.Parallel()

	const org = "org-metrics-exactly-at-cap"
	today := time.Now().UTC().Truncate(24 * time.Hour)
	// Exactly maxForwardChunksPerRun one-day chunks, ending at the clock.
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -maxForwardChunksPerRun).Format(dateLayout),
		DataRefreshedAt: today.Format(time.RFC3339),
	})
	require.NoError(t, err)

	before, beforeYield := covered(t, org), yielded(t, org, yieldForwardCap)
	reportRunFor(t, org, cur, operationOptions{ChunkDays: 1}, Settings{}, today)

	assert.Equal(t, before+1, covered(t, org),
		"a plan that reached the clock covered its window, whatever its chunk count")
	assert.Equal(t, beforeYield, yielded(t, org, yieldForwardCap),
		"and it must not also be counted as a yield")
}

// TestRunYieldedIsCountedOnlyForAPlanThatFellShort is the other side: a gap wider than one run
// can read is a yield, and it must not be counted as covered.
func TestRunYieldedIsCountedOnlyForAPlanThatFellShort(t *testing.T) {
	t.Parallel()

	const org = "org-metrics-short-plan"
	today := time.Now().UTC().Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -60).Format(dateLayout),
		DataRefreshedAt: today.Format(time.RFC3339),
	})
	require.NoError(t, err)

	before, beforeYield := covered(t, org), yielded(t, org, yieldForwardCap)
	reportRunFor(t, org, cur, operationOptions{ChunkDays: 1}, Settings{}, today)

	assert.Equal(t, beforeYield+1, yielded(t, org, yieldForwardCap))
	assert.Equal(t, before, covered(t, org), "a plan that fell short has not covered its window")
}

// TestFirstSyncDoesNotReportAnUnreadableSpan separates the two clamps. The year clamp only ever
// bites a resumed position, which is real loss; the vendor-floor clamp only ever bites a first
// sync, where nothing was dropped — no position covered those days and the vendor holds none.
// Reporting that as loss fires a never-self-healing alarm on every deep first sync.
func TestFirstSyncDoesNotReportAnUnreadableSpan(t *testing.T) {
	t.Parallel()

	const org = "org-metrics-first-sync"
	before := yielded(t, org, yieldUnreadable)
	// No cursor, and a lookback deep enough to reach past the vendor floor.
	reportRunFor(t, org, nil, operationOptions{}, Settings{LookbackDays: 365}, time.Now().UTC())

	assert.Equal(t, before, yielded(t, org, yieldUnreadable),
		"a first sync drops nothing: the floor clamp is not data loss")
}

// TestStoredWatermarkNeverRegresses — a vendor that reports an older watermark than the one
// stored must not have it written back. Written back, it then reads as "unchanged" on the next
// run and silently retires the trailing re-read at the lower value.
func TestStoredWatermarkNeverRegresses(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	newest := today.Add(-2 * time.Hour)
	older := today.AddDate(0, 0, -5)

	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -1).Format(dateLayout),
		DataRefreshedAt: newest.Format(time.RFC3339),
	})
	require.NoError(t, err)

	out := reportRunFor(t, "org-metrics-regress", cur, operationOptions{}, Settings{}, older)
	stored, err := parseReportPosition(out)
	require.NoError(t, err)
	assert.Equal(t, newest.Format(time.RFC3339), stored.Stored.DataRefreshedAt,
		"the stored watermark must not be replaced by an older one the vendor reported")
}

// TestEmitsCollapseAnIndividualTenantsOrgID pins the label wrapping. Four of the five
// operations run for individual tenants, and there are on the order of ten thousand of them —
// so a raw org_id label fans each counter to a series per tenant, which is the cardinality
// blow-up metrictenant exists to prevent.
//
// Not parallel, and that is what makes it safe: Go starts a t.Parallel() body only after every
// sequential top-level test has returned, so the deferred restore runs before any parallel test
// reads a label. Adding a SECOND non-parallel test that asserts a counter would break that.
func TestEmitsCollapseAnIndividualTenantsOrgID(t *testing.T) {
	const individual = "org-an-individual-tenant"
	restore := tenantlabel.SetLiveIndividualSetForTest(map[string]struct{}{individual: {}})
	defer restore()

	require.Equal(t, tenantlabel.MetricLabelIndividual, tenantlabel.OrgID(individual),
		"fixture check: the set must actually collapse this org")

	today := time.Now().UTC().Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -1).Format(dateLayout),
		DataRefreshedAt: today.Format(time.RFC3339),
	})
	require.NoError(t, err)

	rawBefore := testutil.ToFloat64(
		runCovered.WithLabelValues(individual, string(opFetchAnalyticsUserCost)))
	collapsedBefore := testutil.ToFloat64(
		runCovered.WithLabelValues(tenantlabel.MetricLabelIndividual, string(opFetchAnalyticsUserCost)))

	reportRunFor(t, individual, cur, operationOptions{}, Settings{}, today)

	assert.Equal(t, collapsedBefore+1, testutil.ToFloat64(
		runCovered.WithLabelValues(tenantlabel.MetricLabelIndividual, string(opFetchAnalyticsUserCost))),
		"the emit must land on the collapsed label")
	assert.Equal(t, rawBefore, testutil.ToFloat64(
		runCovered.WithLabelValues(individual, string(opFetchAnalyticsUserCost))),
		"and must not create a series for the tenant's own id")
}

// TestAShallowWindowFailsWithItsOwnReason is the first-sync case where every chunk sits after
// the vendor's currency, so nothing in the window is final.
//
// It repeats identically on every run, because the window and the finality ceiling BOTH
// re-anchor to the clock — so it cannot self-heal, and the reason has to name the two numbers
// whose relation causes it. Left as "covered nothing", the message points at a window that was
// in fact read and says nothing about the lookback or the vendor's lag.
func TestAShallowWindowFailsWithItsOwnReason(t *testing.T) {
	t.Parallel()

	// The vendor is 10 days behind; the configured window is 7.
	lagging := time.Now().UTC().AddDate(0, 0, -10).Truncate(24 * time.Hour)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(lagging.Add(9 * time.Hour)),
			}, nil
		}).AnyTimes()

	params := testParams(t, gw, &collectingSink{}, nil, Settings{LookbackDays: 7})
	_, err := (&FetchAnalyticsUserCostOp{}).Execute(context.Background(), params)

	require.Error(t, err, "a window with nothing final in it cannot report success")
	assert.Contains(t, err.Error(), "lookback_days",
		"the failure must name the lever, not just say it covered nothing")
	assert.Contains(t, err.Error(), "days behind",
		"and it must name the vendor's lag, which is the other half of the cause")
}

// TestACeilingHeldRunIsCoveredBecauseNothingMoreWasClaimable is the calibration the counter
// pair needs, and getting it wrong inverts the pair for the most ordinary configuration there
// is.
//
// Coverage is capped at the vendor's currency by design, so a vendor one day behind means a
// healthy run's claim is one day behind the clock — every run, forever. Measured against the
// clock alone, that reports "yields with no covers", which is exactly the signature the pair
// alarms on. A run held short by the vendor's own currency has done everything available to it.
//
// What must still hold is the correctness property: the position does not advance past the
// ceiling.
func TestACeilingHeldRunIsCoveredBecauseNothingMoreWasClaimable(t *testing.T) {
	t.Parallel()

	const org = "org-metrics-ceiling-held"
	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.AddDate(0, 0, -1)
	// The vendor is one day behind — the ordinary steady state.
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -3).Format(dateLayout),
		DataRefreshedAt: yesterday.Add(9 * time.Hour).Format(time.RFC3339),
	})
	require.NoError(t, err)

	before, beforeYield := covered(t, org), yielded(t, org, yieldForwardCap)
	out := reportRunFor(t, org, cur, operationOptions{}, Settings{}, yesterday.Add(9*time.Hour))

	stored, err := parseReportPosition(out)
	require.NoError(t, err)
	assert.Equal(t, yesterday.Format(dateLayout), stored.Stored.CoveredThrough,
		"coverage must stop at the vendor's currency, not at the day requested")

	assert.Equal(t, before+1, covered(t, org),
		"a run that reached the vendor's currency did all it could; counting it a yield would "+
			"make every healthy sync report the stall signature")
	assert.Equal(t, beforeYield, yielded(t, org, yieldForwardCap))
}

// TestACaughtUpRunIsCoveredNotYielded — an empty plan means already caught up. Counting it as a
// yield makes every run of a healthy organisation on a sub-daily schedule report the exact
// signature the pair alarms on: yields with no covers.
func TestACaughtUpRunIsCoveredNotYielded(t *testing.T) {
	t.Parallel()

	const org = "org-metrics-caught-up"
	today := time.Now().UTC().Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.Format(dateLayout),
		DataRefreshedAt: today.Format(time.RFC3339),
	})
	require.NoError(t, err)

	before, beforeYield := covered(t, org), yielded(t, org, yieldForwardCap)
	reportRunFor(t, org, cur, operationOptions{}, Settings{}, today)

	assert.Equal(t, before+1, covered(t, org), "caught up is covered")
	assert.Equal(t, beforeYield, yielded(t, org, yieldForwardCap), "and not a yield")
}

// TestUnboundedCoverageIsCountedOncePerRunAndIsNotAYield — the vendor's currency is a property
// of the run, not of each chunk, so counting it per chunk made any ratio meaningless. And it is
// not a yield: such a run covers everything, it just could not confirm the span final.
func TestUnboundedCoverageIsCountedOncePerRunAndIsNotAYield(t *testing.T) {
	t.Parallel()

	const org = "org-metrics-stale-per-run"
	frozen := time.Now().UTC().AddDate(0, 0, -60).Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  frozen.Format(dateLayout),
		DataRefreshedAt: frozen.Format(time.RFC3339),
	})
	require.NoError(t, err)

	before := testutil.ToFloat64(coverageUnbounded.WithLabelValues(tenantlabel.OrgID(org), string(opFetchAnalyticsUserCost)))
	// chunk_days=7 gives four chunks, each of which would have counted separately.
	reportRunFor(t, org, cur, operationOptions{ChunkDays: 7}, Settings{}, frozen)

	assert.Equal(t, before+1, testutil.ToFloat64(
		coverageUnbounded.WithLabelValues(tenantlabel.OrgID(org), string(opFetchAnalyticsUserCost))),
		"once per run, however many chunks the run walked")
}

// TestStoredWatermarkNeverRegressesWithinADay is the half of the no-regression guard that a
// multi-day fixture cannot reach. The guard compared a full timestamp against a day-truncated
// stored value, so any same-day value passed — and same-day is the only ordering that needs it,
// since two watermarks on one calendar day are ordered by their times alone.
func TestStoredWatermarkNeverRegressesWithinADay(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	stored := today.Add(18 * time.Hour)
	older := today.Add(6 * time.Hour) // same day, earlier

	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -1).Format(dateLayout),
		DataRefreshedAt: stored.Format(time.RFC3339),
	})
	require.NoError(t, err)

	out := reportRunFor(t, "org-metrics-regress-sameday", cur, operationOptions{}, Settings{}, older)
	got, err := parseReportPosition(out)
	require.NoError(t, err)
	assert.Equal(t, stored.Format(time.RFC3339), got.Stored.DataRefreshedAt,
		"an earlier watermark on the same day must not replace the stored one")
}

// TestAFailedRunCountsNoYield — every counter is read after the walks for the same reason the
// covered counter is: a run that failed partway has neither covered its window nor made the
// judgement a yield records. Counted at planning time, a permanently failing organisation
// would emit an unreadable-span yield on every run while never actually skipping anything,
// because the walk that would have skipped it never completed.
func TestAFailedRunCountsNoYield(t *testing.T) {
	t.Parallel()

	const org = "org-metrics-failed-run"
	today := time.Now().UTC().Truncate(24 * time.Hour)
	// Resumed position well past the vendor's year limit, so a span really is unreachable.
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -400).Format(dateLayout),
		DataRefreshedAt: today.Format(time.RFC3339),
	})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(nil, vendorErr(http.StatusInternalServerError, `{"error":"boom"}`)).AnyTimes()

	params := testParams(t, gw, &collectingSink{}, cur, Settings{})
	params.Config.OrganizationID = org

	before := yielded(t, org, yieldUnreadable)
	_, err = (&FetchAnalyticsUserCostOp{}).Execute(context.Background(), params)
	require.Error(t, err, "the fixture must actually fail for this to mean anything")

	assert.Equal(t, before, yielded(t, org, yieldUnreadable),
		"a run that failed made no judgement about a skipped span; only a completed one did")
}

// TestTheStoredWatermarkStillBoundsCoverageWhenTheVendorOmitsIt — the last finality point known
// must keep bounding coverage on a run whose every response omits `data_refreshed_at`. Falling
// back to nothing would let that run claim right up to the clock, over days the vendor last
// told us were not final.
func TestTheStoredWatermarkStillBoundsCoverageWhenTheVendorOmitsIt(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	knownFinal := today.AddDate(0, 0, -2)

	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -5).Format(dateLayout),
		DataRefreshedAt: knownFinal.Add(9 * time.Hour).Format(time.RFC3339),
	})
	require.NoError(t, err)

	// A vendor that reports no watermark at all this run (zero time == field omitted).
	out := reportRunFor(t, "org-metrics-omitted-wm", cur, operationOptions{}, Settings{}, time.Time{})

	stored, err := parseReportPosition(out)
	require.NoError(t, err)
	assert.Equal(t, knownFinal.Format(dateLayout), stored.Stored.CoveredThrough,
		"with no watermark this run, the last one known must still bound what is claimed")
}

// TestUnboundedCoverageIsNotAYield keeps the two halves of the counter pair exclusive.
//
// A run whose watermark is too stale to bound anything does not stop short — it covers
// everything, it just could not confirm the span final. Filed as a yield it made one run
// increment both halves, and any yield-versus-covered ratio then reads as a stall.
//
// The fixture sits near the present deliberately, so the per-run chunk cap is not also in play.
func TestUnboundedCoverageIsNotAYield(t *testing.T) {
	t.Parallel()

	const org = "org-metrics-unbounded-not-yield"
	today := time.Now().UTC().Truncate(24 * time.Hour)
	frozen := today.AddDate(0, 0, -60) // past the staleness bound, so the ceiling releases

	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -2).Format(dateLayout),
		DataRefreshedAt: frozen.Format(time.RFC3339),
	})
	require.NoError(t, err)

	beforeCovered := covered(t, org)
	beforeYield := yielded(t, org, yieldForwardCap)
	beforeUnbounded := testutil.ToFloat64(
		coverageUnbounded.WithLabelValues(tenantlabel.OrgID(org), string(opFetchAnalyticsUserCost)))

	reportRunFor(t, org, cur, operationOptions{}, Settings{}, frozen)

	assert.Equal(t, beforeUnbounded+1, testutil.ToFloat64(
		coverageUnbounded.WithLabelValues(tenantlabel.OrgID(org), string(opFetchAnalyticsUserCost))),
		"the completeness risk must be recorded")
	assert.Equal(t, beforeCovered+1, covered(t, org), "the run did reach the present")
	assert.Equal(t, beforeYield, yielded(t, org, yieldForwardCap),
		"and must not also count as having stopped short")
}

func badRequests(t *testing.T, orgID, kind string) float64 {
	t.Helper()
	return testutil.ToFloat64(vendorBadRequest.WithLabelValues(
		tenantlabel.OrgID(orgID), string(opFetchAnalyticsUsers), kind))
}

// TestTheTwoBadRequestKindsAreCountedApart is this slice's demoable proof, and the reason it is
// a named requirement rather than a nicety: a 400 saying a day is not yet aggregated is expected
// and holds the position, and a 400 saying a parameter changed mid-sequence is a defect in our
// own query. They are the same status code. Counted together, the defect hides inside the normal
// condition and nothing tells a vendor running late from a connector asking wrongly.
func TestTheTwoBadRequestKindsAreCountedApart(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	twoDaysAgo := yesterday.AddDate(0, 0, -1)

	// The vendor's not-ready answer names a day EARLIER than the one asked for, on a fresh
	// request. A 400 while paginating names the same shape and means the opposite.
	for _, tt := range []struct {
		name      string
		body      string
		paginated bool
		wantKind  string
	}{
		{
			name:     "a day not yet aggregated is the normal condition",
			body:     `{"error":{"message":"data available through ` + twoDaysAgo.Format(dateLayout) + `"}}`,
			wantKind: badRequestNotAggregated,
		},
		{
			name:      "the same message mid-pagination is a defect in our query",
			body:      `{"error":{"message":"data available through ` + twoDaysAgo.Format(dateLayout) + `"}}`,
			paginated: true,
			wantKind:  badRequestQueryDefect,
		},
		{
			name:     "a 400 naming no day at all is a defect, not a licence to stop",
			body:     `{"error":{"message":"invalid parameter: group_by"}}`,
			wantKind: badRequestQueryDefect,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			org := "org-400-" + tt.wantKind + "-" + tt.name[:8]
			// Covered well back, so the walk reads several days and only the LAST one 400s —
			// which is what a not-ready answer looks like: the vendor has finished the earlier
			// days and names the newest it has.
			cur, err := encodeCursor(activityPosition{
				CoveredThrough: yesterday.AddDate(0, 0, -5).Format(dateLayout),
			})
			require.NoError(t, err)

			ctrl := gomock.NewController(t)
			gw := fwmocks.NewMockVendorGateway(ctrl)
			gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
			gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
					r := req.(*vgpb.AnthropicAnalyticsUsersRequest)
					if r.GetDate() != yesterday.Format(dateLayout) {
						// An earlier day the vendor has finished.
						return &vgpb.AnthropicAnalyticsUsersResponse{
							Data: [][]byte{[]byte(`{"user":{"id":"user_A"}}`)},
						}, nil
					}
					// For the paginated case, hand out a page cursor first so the 400 arrives
					// mid-sequence — which is what makes the identical message a defect.
					if tt.paginated && r.GetPage() == "" {
						return &vgpb.AnthropicAnalyticsUsersResponse{
							Data:     [][]byte{[]byte(`{"user":{"id":"user_A"}}`)},
							NextPage: "p2",
						}, nil
					}
					return nil, vendorErr(http.StatusBadRequest, tt.body)
				}).AnyTimes()

			params := testParams(t, gw, &collectingSink{}, cur, Settings{})
			params.Config.OrganizationID = org

			beforeWant := badRequests(t, org, tt.wantKind)
			other := badRequestQueryDefect
			if tt.wantKind == badRequestQueryDefect {
				other = badRequestNotAggregated
			}
			beforeOther := badRequests(t, org, other)

			// The outcome differs by kind — not-ready holds the position and succeeds, a
			// defect fails — and either way the counter must record which it was.
			_, _ = (&FetchAnalyticsUsersOp{}).Execute(context.Background(), params)

			assert.Greaterf(t, badRequests(t, org, tt.wantKind), beforeWant,
				"the %s counter must record this 400", tt.wantKind)
			assert.Equalf(t, beforeOther, badRequests(t, org, other),
				"and the %s counter must not, or the two meanings are indistinguishable", other)
		})
	}
}

// TestTheVolumeCountersRecordAFixtureRun is the rest of the slice's proof: the counters the
// design names are visible after a run, per operation.
func TestTheVolumeCountersRecordAFixtureRun(t *testing.T) {
	t.Parallel()

	const org = "org-volume-counters"
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(activityPosition{CoveredThrough: yesterday.AddDate(0, 0, -1).Format(dateLayout)})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	page := 0
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUsersRequest)
			if r.GetDate() != yesterday.Format(dateLayout) {
				return &vgpb.AnthropicAnalyticsUsersResponse{}, nil
			}
			page++
			if page == 1 {
				return &vgpb.AnthropicAnalyticsUsersResponse{
					Data:     [][]byte{[]byte(`{"user":{"id":"user_A"}}`)},
					NextPage: "p2",
				}, nil
			}
			return &vgpb.AnthropicAnalyticsUsersResponse{
				Data: [][]byte{[]byte(`{"user":{"id":"user_B"}}`)},
			}, nil
		}).AnyTimes()

	params := testParams(t, gw, &collectingSink{}, cur, Settings{})
	params.Config.OrganizationID = org

	lbl := []string{tenantlabel.OrgID(org), string(opFetchAnalyticsUsers)}
	beforeRecords := testutil.ToFloat64(recordsEmitted.WithLabelValues(lbl...))
	beforePages := testutil.ToFloat64(pagesRead.WithLabelValues(lbl...))
	beforePeriods := testutil.ToFloat64(periodsCovered.WithLabelValues(lbl...))

	_, err = (&FetchAnalyticsUsersOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, beforeRecords+2, testutil.ToFloat64(recordsEmitted.WithLabelValues(lbl...)),
		"two actors on the read day, so two records")
	// Exact, not "greater than": the window is fully determined here — coverage through
	// yesterday-1 and an overlap of two re-enter yesterday-2, so the walk reads three days,
	// one page each except the two on the read day. A `Greater` would pass just as happily if
	// the overlap silently widened or a day were read twice.
	assert.Equal(t, beforePages+4, testutil.ToFloat64(pagesRead.WithLabelValues(lbl...)),
		"three days, two pages on the read day and one on each re-entered day")
	// One, not three: a period is counted on the ADVANCE, and only the newest of the three
	// days walked is past the stored coverage. The other two are the overlap being re-read,
	// which is not progress — counting them would report progress on a run that advanced
	// nothing, and the exact number is what holds that distinction in place.
	assert.Equal(t, beforePeriods+1, testutil.ToFloat64(periodsCovered.WithLabelValues(lbl...)),
		"only the day that moved the position forward is a covered period")

	// A healthy position must NOT be counted as far behind, or the signal is noise.
	assert.Zero(t, testutil.ToFloat64(positionFarBehind.WithLabelValues(lbl...)),
		"a position a day behind is a healthy sync, not a stalled one")
}

// TestAStuckIndividualTenantIsNotMaskedByAHealthyOne guards the property the metric type was
// chosen for. See positionFarBehind for why a gauge cannot hold it.
func TestAStuckIndividualTenantIsNotMaskedByAHealthyOne(t *testing.T) {
	restore := tenantlabel.SetLiveIndividualSetForTest(map[string]struct{}{
		"ind-stuck": {}, "ind-healthy": {},
	})
	defer restore()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	collapsed := []string{tenantlabel.MetricLabelIndividual, string(opFetchAnalyticsUserCost)}
	before := testutil.ToFloat64(positionFarBehind.WithLabelValues(collapsed...))

	run := func(org string, behindDays int) {
		t.Helper()
		at := today.AddDate(0, 0, -behindDays)
		cur, err := encodeCursor(reportPosition{
			CoveredThrough: at.Format(dateLayout), DataRefreshedAt: at.Format(time.RFC3339),
		})
		require.NoError(t, err)
		// The vendor is as stale as the position, so the walk claims nothing new and the
		// stored position stays where it is — which is the state being measured.
		reportRunFor(t, org, cur, operationOptions{}, Settings{}, at)
	}

	// The stuck one first, then a healthy one — the order a gauge would lose the signal in.
	// Measured when each run LOADS its position, so what the run then manages to do does not
	// change the count.
	run("ind-stuck", 200)
	run("ind-healthy", 1)

	assert.Equal(t, before+1, testutil.ToFloat64(positionFarBehind.WithLabelValues(collapsed...)),
		"the stuck tenant must still be counted after a healthy one syncs; a gauge would have "+
			"overwritten it and lost the stall for every collapsed tenant")
}

// rejectingSink stands in for a queue outage: every Emit fails.
type rejectingSink struct{}

func (rejectingSink) Emit(context.Context, *sdk.RawRecord) error {
	return errors.New("queue unavailable")
}

func (s rejectingSink) EmitBatch(ctx context.Context, rs []*sdk.RawRecord) error {
	for _, r := range rs {
		if err := s.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}
func (rejectingSink) Flush(context.Context) error { return nil }

// TestARejectedRecordIsNotCountedAsEmitted separates "constructed" from "reached the queue".
//
// Counted before the sink, the series would report healthy volume straight through a queue
// outage — and because the unit retries, the same unsent record would be counted again on
// every attempt, so the number would climb while nothing was ingested at all.
func TestARejectedRecordIsNotCountedAsEmitted(t *testing.T) {
	t.Parallel()

	const org = "org-sink-rejects"
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(activityPosition{CoveredThrough: yesterday.AddDate(0, 0, -1).Format(dateLayout)})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUsersRequest)
			if r.GetDate() != yesterday.Format(dateLayout) {
				return &vgpb.AnthropicAnalyticsUsersResponse{}, nil
			}
			return &vgpb.AnthropicAnalyticsUsersResponse{
				Data: [][]byte{[]byte(`{"user":{"id":"user_A"}}`)},
			}, nil
		}).AnyTimes()

	params := testParams(t, gw, rejectingSink{}, cur, Settings{})
	params.Config.OrganizationID = org

	lbl := []string{tenantlabel.OrgID(org), string(opFetchAnalyticsUsers)}
	before := testutil.ToFloat64(recordsEmitted.WithLabelValues(lbl...))

	_, err = (&FetchAnalyticsUsersOp{}).Execute(context.Background(), params)
	require.Error(t, err, "a sink failure must fail the run, not be absorbed")

	assert.Equal(t, before, testutil.ToFloat64(recordsEmitted.WithLabelValues(lbl...)),
		"nothing reached the queue, so nothing is counted as emitted")
}

// TestAFirstSyncStoppedByAggregationLagIsAYield is the other half of
// TestActivityWalk_ANotYetAggregatedDayIsACoverWhenTheWalkReachedIt. Without it the branch
// passes just as happily counting a cover unconditionally.
//
// The consequence is not cosmetic. A first sync that reaches nothing also fails the run, and the
// coverage series is read as pages against progress — so a cover counted here would make that
// series look healthy for precisely the organisation whose sync never started.
func TestAFirstSyncStoppedByAggregationLagIsAYield(t *testing.T) {
	t.Parallel()

	const org = "org-first-sync-lagged"
	// No stored position, so nothing is covered when the refusal arrives — the run reached
	// nothing and genuinely fell short, whatever the vendor's currency is.
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			day, err := time.Parse(dateLayout, req.(*vgpb.AnthropicAnalyticsUsersRequest).GetDate())
			require.NoError(t, err)
			return nil, vendorErr(http.StatusBadRequest,
				`{"error":{"message":"engagement data is available through `+day.AddDate(0, 0, -1).Format(dateLayout)+`"}}`)
		}).AnyTimes()

	params := testParams(t, gw, &collectingSink{}, nil, Settings{})
	params.Config.OrganizationID = org

	lbl := []string{tenantlabel.OrgID(org), string(opFetchAnalyticsUsers)}
	beforeCovered := testutil.ToFloat64(runCovered.WithLabelValues(lbl...))
	beforeYielded := testutil.ToFloat64(runYielded.WithLabelValues(append(append([]string{}, lbl...), yieldNotAggregated)...))

	// The run also fails: covering nothing with no stored position would report success on a
	// stall, which storeCoveredPosition refuses. The counters are what an operator sees either
	// way, so they are asserted on the failing run rather than skipped with it.
	_, err := (&FetchAnalyticsUsersOp{}).Execute(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "covered nothing and has no stored position")

	assert.Equal(t, beforeYielded+1,
		testutil.ToFloat64(runYielded.WithLabelValues(append(append([]string{}, lbl...), yieldNotAggregated)...)),
		"a run that covered nothing fell short, whatever the vendor's currency")
	assert.Equal(t, beforeCovered, testutil.ToFloat64(runCovered.WithLabelValues(lbl...)),
		"and must not be counted as having reached its target")
}

// TestEveryUnkeyableReportRowShapeIsCounted walks the three ways a report row fails to acquire
// an identity. All three fail the run; the counter is what tells an operator which organisation
// and operation, and a counter wired to only the decode failure looks identical from outside.
func TestEveryUnkeyableReportRowShapeIsCounted(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name, row, wantErr string
	}{
		{"not json at all", `{"actor":`, "unreadable"},
		{"no actor and no bucket", `{"actor":{},"starting_at":""}`, "no actor or bucket"},
		{"an unparseable bucket start", `{"actor":{"user_id":"u1"},"starting_at":"not-a-timestamp"}`, "unparseable bucket start"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			org := "org-unkeyable-" + strconv.Itoa(len(tt.name))
			lbl := []string{tenantlabel.OrgID(org), string(opFetchAnalyticsUserCost)}
			before := testutil.ToFloat64(rowsUnreadable.WithLabelValues(lbl...))

			acc := newRecordAccumulator(opFetchAnalyticsUserCost, org)
			err := collectReportRows(acc, opFetchAnalyticsUserCost, org, [][]byte{[]byte(tt.row)})

			require.Error(t, err, "an unkeyable row must fail the run rather than be skipped")
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Equal(t, before+1, testutil.ToFloat64(rowsUnreadable.WithLabelValues(lbl...)),
				"every shape that cannot be keyed is counted, not just the decode failure")
		})
	}
}

// TestTheRosterCountsItsOwnCompletion — the roster is the operation most easily left out. It has
// no date, no watermark and no incremental position, so it is the one whose completion counters
// are easiest to forget; and because the coverage series is read as pages against covers, an
// operation that reads pages and never covers would look stalled forever.
func TestTheRosterCountsItsOwnCompletion(t *testing.T) {
	t.Parallel()

	const org = "org-roster-completion"
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(&vgpb.AnthropicOrganizationMembersResponse{
			Data:    [][]byte{[]byte(`{"id":"user_A","email":"a@example.com","role":"admin"}`)},
			LastId:  "user_A",
			HasMore: false,
		}, nil).Times(1)

	params := testParams(t, gw, &collectingSink{}, nil, Settings{})
	params.Config.OrganizationID = org

	lbl := []string{tenantlabel.OrgID(org), string(opFetchOrganizationMembers)}
	beforePeriods := testutil.ToFloat64(periodsCovered.WithLabelValues(lbl...))
	beforeCovered := testutil.ToFloat64(runCovered.WithLabelValues(lbl...))

	_, err := (&FetchOrganizationMembersOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, beforePeriods+1, testutil.ToFloat64(periodsCovered.WithLabelValues(lbl...)),
		"a completed snapshot is a covered period")
	assert.Equal(t, beforeCovered+1, testutil.ToFloat64(runCovered.WithLabelValues(lbl...)),
		"and the run reached its target, which is the diagnostic that splits a stall's two causes")
}

// TestTheRosterCountsAVendorBadRequestAsOurDefect — the roster request carries no date, so the
// vendor has no not-yet-aggregated answer to give and every 400 there is unambiguously ours.
// Counting it is what keeps the two-kind counter covering all five operations.
func TestTheRosterCountsAVendorBadRequestAsOurDefect(t *testing.T) {
	t.Parallel()

	const org = "org-roster-bad-request"
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(nil, vendorErr(http.StatusBadRequest, "HTTP 400: unknown parameter")).Times(1)

	params := testParams(t, gw, &collectingSink{}, nil, Settings{})
	params.Config.OrganizationID = org

	defect := []string{tenantlabel.OrgID(org), string(opFetchOrganizationMembers), badRequestQueryDefect}
	lagged := []string{tenantlabel.OrgID(org), string(opFetchOrganizationMembers), badRequestNotAggregated}
	beforeDefect := testutil.ToFloat64(vendorBadRequest.WithLabelValues(defect...))
	beforeLagged := testutil.ToFloat64(vendorBadRequest.WithLabelValues(lagged...))

	_, err := (&FetchOrganizationMembersOp{}).Execute(context.Background(), params)
	require.Error(t, err)

	assert.Equal(t, beforeDefect+1, testutil.ToFloat64(vendorBadRequest.WithLabelValues(defect...)),
		"a 400 on a dateless request is our own query defect")
	assert.Equal(t, beforeLagged, testutil.ToFloat64(vendorBadRequest.WithLabelValues(lagged...)),
		"and never the vendor's aggregation lag, which the roster cannot have")
}

// TestTheFarBehindThresholdBracketsAConvergingFirstSync pins the threshold from BOTH sides.
//
// Only the upper side was pinned, so shrinking the constant left the package green — and a
// threshold below the 90-day first-sync lookback pages every new organisation for its own
// backfill, which is the failure that makes the signal worthless rather than merely noisy.
func TestTheFarBehindThresholdBracketsAConvergingFirstSync(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		daysBehind  int
		wantCounted bool
	}{
		{"a first sync at its lookback floor", 90, false},
		{"a first sync most of the way closed", 30, false},
		{"just inside the threshold", 119, false},
		{"past the threshold", 121, true},
		{"a position that has plainly stopped", 300, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			org := "org-threshold-" + strconv.Itoa(tt.daysBehind)
			lbl := []string{tenantlabel.OrgID(org), string(opFetchAnalyticsUsers)}
			before := testutil.ToFloat64(positionFarBehind.WithLabelValues(lbl...))

			notePositionFarBehind(opFetchAnalyticsUsers, org,
				time.Now().UTC().AddDate(0, 0, -tt.daysBehind).Truncate(24*time.Hour))

			want := before
			if tt.wantCounted {
				want = before + 1
			}
			assert.Equal(t, want, testutil.ToFloat64(positionFarBehind.WithLabelValues(lbl...)))
		})
	}
}

// TestAnAbsentPositionIsNotCountedAsFarBehind — the zero time is the guard that matters. Reaching
// time.Since with it would read as two millennia behind and count every run that has no position
// at all, which is every first sync.
func TestAnAbsentPositionIsNotCountedAsFarBehind(t *testing.T) {
	t.Parallel()

	const org = "org-no-position"
	lbl := []string{tenantlabel.OrgID(org), string(opFetchAnalyticsSkills)}
	before := testutil.ToFloat64(positionFarBehind.WithLabelValues(lbl...))

	notePositionFarBehind(opFetchAnalyticsSkills, org, time.Time{})

	assert.Equal(t, before, testutil.ToFloat64(positionFarBehind.WithLabelValues(lbl...)),
		"no stored position is not a stalled one")
}

// TestTheRosterDoesNotCountARejectedRecordEither. The roster is the one operation that emits
// without the accumulator, so it needs its own guard for the same property — and it is the
// reason this matters: the accumulator's fix was tested on the accumulator path only, which is
// exactly how the roster was left counting before its sink for a whole round.
func TestTheRosterDoesNotCountARejectedRecordEither(t *testing.T) {
	t.Parallel()

	const org = "org-roster-sink-rejects"
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(&vgpb.AnthropicOrganizationMembersResponse{
			Data:    [][]byte{[]byte(`{"id":"user_A","email":"a@example.com","role":"admin"}`)},
			LastId:  "user_A",
			HasMore: false,
		}, nil).Times(1)

	params := testParams(t, gw, rejectingSink{}, nil, Settings{})
	params.Config.OrganizationID = org

	lbl := []string{tenantlabel.OrgID(org), string(opFetchOrganizationMembers)}
	before := testutil.ToFloat64(recordsEmitted.WithLabelValues(lbl...))

	_, err := (&FetchOrganizationMembersOp{}).Execute(context.Background(), params)
	require.Error(t, err)

	assert.Equal(t, before, testutil.ToFloat64(recordsEmitted.WithLabelValues(lbl...)),
		"nothing reached the queue, so nothing is counted as emitted")
}
