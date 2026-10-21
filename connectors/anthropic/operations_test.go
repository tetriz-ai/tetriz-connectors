// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// collectingSink records what an operation emitted, so a test can assert on identities and
// payloads rather than on call counts.
type collectingSink struct{ records []*sdk.RawRecord }

func (s *collectingSink) Emit(_ context.Context, r *sdk.RawRecord) error {
	s.records = append(s.records, r)
	return nil
}

func (s *collectingSink) EmitBatch(ctx context.Context, rs []*sdk.RawRecord) error {
	for _, r := range rs {
		if err := s.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}
func (s *collectingSink) Flush(context.Context) error { return nil }

func (s *collectingSink) externalIDs() []string {
	out := make([]string, 0, len(s.records))
	for _, r := range s.records {
		out = append(out, r.ExternalID)
	}
	return out
}

// checkpointRecorder captures what a walk persists mid-run. Every test wires one, because
// the mid-run checkpoint is the walk's real progress record — the orchestrator discards the
// returned cursor on any error — so leaving Checkpoint nil would exercise none of it.
type checkpointRecorder struct {
	mu        sync.Mutex
	positions []string
}

func (c *checkpointRecorder) fn(t *testing.T) func(context.Context, *sdk.Cursor) error {
	t.Helper()
	return func(_ context.Context, cur *sdk.Cursor) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.positions = append(c.positions, string(cur.Data))
		return nil
	}
}

func (c *checkpointRecorder) covered(t *testing.T) []string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.positions))
	for _, raw := range c.positions {
		var pos struct {
			CoveredThrough string `json:"covered_through"`
		}
		require.NoError(t, json.Unmarshal([]byte(raw), &pos))
		out = append(out, pos.CoveredThrough)
	}
	return out
}

func testParams(t *testing.T, gw sdk.VendorGateway, sink sdk.DataSink, cursor *sdk.Cursor, settings Settings) *sdk.ExecutionParams {
	t.Helper()
	return testParamsWithCheckpoint(t, gw, sink, cursor, settings, &checkpointRecorder{})
}

func testParamsWithCheckpoint(
	t *testing.T, gw sdk.VendorGateway, sink sdk.DataSink,
	cursor *sdk.Cursor, settings Settings, cp *checkpointRecorder,
) *sdk.ExecutionParams {
	t.Helper()
	raw, err := json.Marshal(settings)
	require.NoError(t, err)
	creds, err := json.Marshal(Credentials{APIKey: "sk-ant-api01-x"})
	require.NoError(t, err)
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    connectorID,
			OrganizationID: "org-1",
			Credentials:    creds,
			Settings:       raw,
		},
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gw,
		Checkpoint:    cp.fn(t),
	}
}

// vendorErr builds the error production builds. A non-2xx reaches an operation only after
// the vendor gateway has classified it through vendorapi, which decides Kind/Terminal and
// formats the Cause as "HTTP <status>: <body>" — and that Cause string is exactly what the
// 400 aggregation-lag parser reads. A hand-rolled SyncError here would let these tests pass
// against a shape the vendor path never produces.
func vendorErr(status int, body string) error {
	return vendorapi.ClassifyHTTPErrorWithHeaders(status, []byte(body), nil)
}

// --- shape 1: the activity walk ---

// TestActivityWalk_EmitsPerActorPerDayAndCheckpoints covers the first shape: whole calendar
// days, oldest first, paginating on next_page with no has_more.
//
// The fixture computes its dates from the clock the same way the walk does, rather than
// hardcoding them — a date-pinned fixture here would pass only on the day it was written.
func TestActivityWalk_EmitsPerActorPerDayAndCheckpoints(t *testing.T) {
	t.Parallel()

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	dayOne := latest.AddDate(0, 0, -1)
	covered := dayOne.AddDate(0, 0, -1)
	// The walk re-enters the overlap, so it also asks for the already-covered days. Those
	// return nothing new; the assertion is about the two unread days.
	overlapStart := covered.AddDate(0, 0, -(defaultActivityOverlapDays - 1))

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	// Two pages on the first unread day, one on the second. An absent next_page is the
	// only end condition these endpoints offer — there is no has_more to read.
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.AnthropicAnalyticsUsersRequest{})).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUsersRequest)
			switch {
			case r.GetDate() == dayOne.Format(dateLayout) && r.GetPage() == "":
				return &vgpb.AnthropicAnalyticsUsersResponse{
					Data:     [][]byte{[]byte(`{"user":{"id":"user_A"},"claude_code_metrics":{"core_metrics":{"commit_count":3}}}`)},
					NextPage: "p2",
				}, nil
			case r.GetDate() == dayOne.Format(dateLayout) && r.GetPage() == "p2":
				return &vgpb.AnthropicAnalyticsUsersResponse{
					Data: [][]byte{[]byte(`{"user":{"id":"user_B"}}`)},
				}, nil
			case r.GetDate() == latest.Format(dateLayout) && r.GetPage() == "":
				return &vgpb.AnthropicAnalyticsUsersResponse{
					Data: [][]byte{[]byte(`{"user":{"id":"user_A"}}`)},
				}, nil
			}
			// The overlap days: already covered, nothing new arrived.
			for d := overlapStart; !d.After(covered); d = d.AddDate(0, 0, 1) {
				if r.GetDate() == d.Format(dateLayout) {
					return &vgpb.AnthropicAnalyticsUsersResponse{}, nil
				}
			}
			return nil, fmt.Errorf("unexpected request: date=%s page=%s", r.GetDate(), r.GetPage())
		}).Times(3 + defaultActivityOverlapDays)

	sink := &collectingSink{}
	cur, err := encodeCursor(activityPosition{CoveredThrough: covered.Format(dateLayout)})
	require.NoError(t, err)

	op := &FetchAnalyticsUsersOp{}
	out, err := op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.NoError(t, err)

	// One record per actor per day: two actors on the first day, one on the second. The
	// second page's actor must be present — dropping it is what a walk that stops at the
	// first page would do, and no other assertion here would notice.
	assert.ElementsMatch(t, []string{
		"user_A|" + dayOne.Format(dateLayout),
		"user_B|" + dayOne.Format(dateLayout),
		"user_A|" + latest.Format(dateLayout),
	}, sink.externalIDs())

	// The payload is the vendor's row untouched, and DataDate carries the period.
	for _, r := range sink.records {
		assert.NotEmpty(t, r.Payload)
		assert.False(t, r.DataDate.IsZero(), "the period must be set, not left to FetchedAt")
	}
	assert.Contains(t, string(sink.records[0].Payload), `"commit_count":3`)

	// The position advances to the last day fully covered.
	got, _, present, err := parseActivityPosition(out)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, latest.Format(dateLayout), got.CoveredThrough)
}

// TestActivityWalk_StopsOnANotYetAggregatedDayWithoutAdvancing is a vendor condition
// that is normal rather than a failure: a 400 naming the most recent available day.
//
// The position must NOT move past the requested day, or that day is skipped forever — the
// next run would resume after it and no retry ever comes back.
func TestActivityWalk_StopsOnANotYetAggregatedDayWithoutAdvancing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	covered := latest.AddDate(0, 0, -2)
	// The walk re-enters the overlap, so the first day it asks for precedes `covered`. The
	// vendor names a day earlier still, which is what makes this a not-ready answer rather
	// than a query defect.
	firstRequested := covered.AddDate(0, 0, -(defaultActivityOverlapDays - 1))
	named := firstRequested.AddDate(0, 0, -1)

	calls := 0
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			calls++
			return nil, vendorErr(http.StatusBadRequest,
				`{"error":{"message":"engagement data is available through `+named.Format(dateLayout)+`"}}`)
		}).Times(1)

	sink := &collectingSink{}
	cur, err := encodeCursor(activityPosition{CoveredThrough: covered.Format(dateLayout)})
	require.NoError(t, err)

	op := &FetchAnalyticsUsersOp{}
	out, err := op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.NoError(t, err, "a not-yet-aggregated day is a normal condition, not a run failure")
	require.NotNil(t, out)

	// Exactly one request: the stop must happen on the refusal, not after grinding through
	// every remaining day. A passing test with zero requests would be the no-work outcome
	// wearing this test's name.
	assert.Equal(t, 1, calls)

	got, _, present, err := parseActivityPosition(out)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, covered.Format(dateLayout), got.CoveredThrough,
		"the position must not advance past a day the vendor has not aggregated")
	assert.Empty(t, sink.records, "nothing was read, so nothing should be emitted")
}

// lastPosition decodes the last checkpoint a walk wrote.
func (c *checkpointRecorder) last(t *testing.T, into any) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotEmpty(t, c.positions, "the walk checkpointed nothing")
	require.NoError(t, json.Unmarshal([]byte(c.positions[len(c.positions)-1]), into))
}

// TestActivityWalk_WalksPastAFailedDayAndRetriesItNextRun. A day that fails on its own does
// not hold back the days after it: the walk lists it in the position, reads the later days,
// and fails the run, retryably. The next run reads the listed day again, and once it
// succeeds the list is empty.
func TestActivityWalk_WalksPastAFailedDayAndRetriesItNextRun(t *testing.T) {
	t.Parallel()

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	bad := latest.AddDate(0, 0, -1)
	covered := latest.AddDate(0, 0, -2)
	failing := true

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUsersRequest)
			switch r.GetDate() {
			case bad.Format(dateLayout):
				if failing {
					return nil, vendorErr(http.StatusInternalServerError, "HTTP 500: upstream")
				}
				return &vgpb.AnthropicAnalyticsUsersResponse{Data: [][]byte{[]byte(`{"user":{"id":"user_B"}}`)}}, nil
			case latest.Format(dateLayout):
				return &vgpb.AnthropicAnalyticsUsersResponse{Data: [][]byte{[]byte(`{"user":{"id":"user_A"}}`)}}, nil
			}
			return &vgpb.AnthropicAnalyticsUsersResponse{}, nil
		}).AnyTimes()

	cur, err := encodeCursor(activityPosition{CoveredThrough: covered.Format(dateLayout)})
	require.NoError(t, err)
	sink := &collectingSink{}
	cp := &checkpointRecorder{}
	_, err = (&FetchAnalyticsUsersOp{}).Execute(context.Background(), testParamsWithCheckpoint(t, gw, sink, cur, Settings{}, cp))
	require.Error(t, err, "a run that walked past a failed day must not report success")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.False(t, se.Terminal)
	assert.Contains(t, err.Error(), bad.Format(dateLayout))
	assert.Equal(t, []string{"user_A|" + latest.Format(dateLayout)}, sink.externalIDs(), "the day after the failed one is read")
	var pos activityPosition
	cp.last(t, &pos)
	assert.Equal(t, latest.Format(dateLayout), pos.CoveredThrough)
	assert.Equal(t, []string{bad.Format(dateLayout)}, pos.FailedDays)

	// The next run: the day now succeeds and leaves the list.
	failing = false
	next, err := encodeCursor(pos)
	require.NoError(t, err)
	sink = &collectingSink{}
	out, err := (&FetchAnalyticsUsersOp{}).Execute(context.Background(), testParams(t, gw, sink, next, Settings{}))
	require.NoError(t, err)
	assert.Contains(t, sink.externalIDs(), "user_B|"+bad.Format(dateLayout))
	got, _, _, err := parseActivityPosition(out)
	require.NoError(t, err)
	assert.Empty(t, got.FailedDays)
	assert.Equal(t, latest.Format(dateLayout), got.CoveredThrough)
}

// TestActivityWalk_StopsAdvancingWhenTheFailedListIsFull. With the list full, a further
// failed day stops the run there, as before failed days were tracked: the position stays on
// the day before it and no listed day is dropped.
func TestActivityWalk_StopsAdvancingWhenTheFailedListIsFull(t *testing.T) {
	t.Parallel()

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	bad := latest.AddDate(0, 0, -1)
	var listed []string
	for i := 0; i < maxFailedUnits; i++ {
		listed = append(listed, latest.AddDate(0, 0, -85+i).Format(dateLayout))
	}
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUsersRequest)
			if r.GetDate() == latest.Format(dateLayout) {
				return &vgpb.AnthropicAnalyticsUsersResponse{Data: [][]byte{[]byte(`{"user":{"id":"user_A"}}`)}}, nil
			}
			if r.GetDate() < latest.AddDate(0, 0, -50).Format(dateLayout) || r.GetDate() == bad.Format(dateLayout) {
				return nil, vendorErr(http.StatusInternalServerError, "HTTP 500: upstream")
			}
			return &vgpb.AnthropicAnalyticsUsersResponse{}, nil
		}).AnyTimes()

	covered := latest.AddDate(0, 0, -2)
	cur, err := encodeCursor(activityPosition{CoveredThrough: covered.Format(dateLayout), FailedDays: listed})
	require.NoError(t, err)
	sink := &collectingSink{}
	cp := &checkpointRecorder{}
	_, err = (&FetchAnalyticsUsersOp{}).Execute(context.Background(), testParamsWithCheckpoint(t, gw, sink, cur, Settings{}, cp))
	require.Error(t, err)
	assert.Contains(t, err.Error(), bad.Format(dateLayout))
	assert.Empty(t, sink.records, "the day after the unlisted failure waits")
	var pos activityPosition
	cp.last(t, &pos)
	assert.Equal(t, covered.Format(dateLayout), pos.CoveredThrough, "the position does not pass the unlisted failure")
	assert.Equal(t, listed, pos.FailedDays, "no listed day is dropped")
}

// TestReportWalk_WalksPastAFailedChunkAndRetriesItNextRun. A chunk that fails on its own is
// listed and walked past; the next run reads it again and, once it succeeds, unlists it.
func TestReportWalk_WalksPastAFailedChunkAndRetriesItNextRun(t *testing.T) {
	t.Parallel()

	refreshedAt, refreshedStr := steadyWatermark()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	covered := today.AddDate(0, 0, -14)
	failing := true
	var badStart time.Time

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUserCostReportRequest)
			start := r.GetStartingAt().AsTime().UTC()
			if badStart.IsZero() {
				badStart = start // the first chunk of the forward pass
			}
			if start.Equal(badStart) && failing {
				return nil, vendorErr(http.StatusInternalServerError, "HTTP 500: upstream")
			}
			bucket := start.Format(time.RFC3339)
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				Data:            [][]byte{[]byte(`{"actor":{"user_id":"user_A"},"starting_at":"` + bucket + `","product":"claude_code","amount":"1"}`)},
				DataRefreshedAt: timestamppb.New(refreshedAt),
				OrganizationId:  "org_abc",
			}, nil
		}).AnyTimes()

	cur, err := encodeCursor(reportPosition{CoveredThrough: covered.Format(dateLayout), DataRefreshedAt: refreshedStr})
	require.NoError(t, err)
	sink := &collectingSink{}
	cp := &checkpointRecorder{}
	_, err = (&FetchAnalyticsUserCostOp{}).Execute(context.Background(), testParamsWithCheckpoint(t, gw, sink, cur, Settings{}, cp))
	require.Error(t, err, "a run that walked past a failed chunk must not report success")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.False(t, se.Terminal)
	assert.NotEmpty(t, sink.records, "the chunks after the failed one are read")
	var pos reportPosition
	cp.last(t, &pos)
	require.Len(t, pos.FailedChunks, 1)
	assert.True(t, strings.HasPrefix(pos.FailedChunks[0], badStart.Format(dateLayout)+".."), pos.FailedChunks[0])
	assert.Greater(t, pos.CoveredThrough, covered.Format(dateLayout), "the position moves past the listed chunk")

	failing = false
	next, err := encodeCursor(pos)
	require.NoError(t, err)
	sink = &collectingSink{}
	out, err := (&FetchAnalyticsUserCostOp{}).Execute(context.Background(), testParams(t, gw, sink, next, Settings{}))
	require.NoError(t, err)
	assert.Contains(t, sink.externalIDs(), "user_A|"+badStart.Format(dateLayout)+"|claude_code")
	got, err := parseReportPosition(out)
	require.NoError(t, err)
	assert.Empty(t, got.Stored.FailedChunks)
}

// TestActivityWalk_AnUnreadableRowFailsRatherThanSkipping pins the cursor rule's direction. The day
// checkpoints once the emit loop returns, so a skipped row is permanently lost.
func TestActivityWalk_AnUnreadableRowFailsRatherThanSkipping(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(&vgpb.AnthropicAnalyticsUsersResponse{
			Data: [][]byte{[]byte(`{"user":{}}`)}, // no actor id
		}, nil).AnyTimes()

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(activityPosition{CoveredThrough: latest.AddDate(0, 0, -1).Format(dateLayout)})
	require.NoError(t, err)

	op := &FetchAnalyticsUsersOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
	require.Error(t, err, "a row with no identity must stop the run rather than be dropped")
	assert.Contains(t, err.Error(), "no actor id")
}

// --- shape 2: the report walk ---

// steadyWatermark returns a vendor watermark for a test to seed on BOTH sides — the stored
// position and the observed response — so no trailing re-read fires and the test isolates the
// behaviour it names. A test that wants the re-read seeds a differing pair instead.
func steadyWatermark() (time.Time, string) {
	at := time.Now().UTC().Truncate(24 * time.Hour)
	return at, at.Format(time.RFC3339)
}

// TestReportWalk_RollsPerModelRowsIntoOneRecordPerProduct shows the identity built without the model.
// The model is excluded deliberately: a model in a uniqueness key means the day the vendor
// returns a display name instead of a slug, one actor-bucket-product becomes two rows and
// its cost double-counts.
func TestReportWalk_RollsPerModelRowsIntoOneRecordPerProduct(t *testing.T) {
	t.Parallel()

	refreshedAt, refreshedStr := steadyWatermark()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	bucket := "2026-05-20T00:00:00Z"
	rows := [][]byte{
		[]byte(`{"actor":{"user_id":"user_A"},"starting_at":"` + bucket + `","product":"claude_code","model":"claude-opus-4","amount":"41280.000000"}`),
		[]byte(`{"actor":{"user_id":"user_A"},"starting_at":"` + bucket + `","product":"claude_code","model":"Claude Sonnet 4","amount":"120.500000"}`),
		[]byte(`{"actor":{"user_id":"user_A"},"starting_at":"` + bucket + `","product":"chat","model":"claude-opus-4","amount":"7.000000"}`),
		[]byte(`{"actor":{"user_id":"user_B"},"starting_at":"` + bucket + `","product":"claude_code","model":"claude-opus-4","amount":"3.000000"}`),
	}
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(&vgpb.AnthropicAnalyticsUserCostReportResponse{
			Data:            rows,
			HasMore:         false,
			DataRefreshedAt: timestamppb.New(refreshedAt),
			OrganizationId:  "org_abc",
		}, nil).AnyTimes()

	sink := &collectingSink{}
	// Caught up to yesterday so exactly one chunk is planned.
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: refreshedStr,
	})
	require.NoError(t, err)

	op := &FetchAnalyticsUserCostOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.NoError(t, err)

	// Three records, not four: the two claude_code models for user_A roll into one.
	assert.ElementsMatch(t, []string{
		"user_A|2026-05-20|claude_code",
		"user_A|2026-05-20|chat",
		"user_B|2026-05-20|claude_code",
	}, sink.externalIDs())

	// And the rolled-up payload holds both models verbatim, including the exact decimal.
	for _, r := range sink.records {
		if r.ExternalID != "user_A|2026-05-20|claude_code" {
			continue
		}
		var arr []json.RawMessage
		require.NoError(t, json.Unmarshal(r.Payload, &arr))
		require.Len(t, arr, 2, "both models for the product must be in the array")
		assert.Contains(t, string(r.Payload), `"41280.000000"`,
			"the decimal amount must survive verbatim — parsing it would lose precision")
		assert.Contains(t, string(r.Payload), `"Claude Sonnet 4"`,
			"a display-name model must not have been normalised or dropped")
	}
}

// TestReportWalk_AnUnreadableRowFailsRatherThanSkipping — the cursor rule's direction is the same as for activity.
func TestReportWalk_AnUnreadableRowFailsRatherThanSkipping(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(&vgpb.AnthropicAnalyticsUserCostReportResponse{
			Data: [][]byte{[]byte(`{"actor":{},"starting_at":""}`)},
		}, nil).AnyTimes()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: "s"})
	require.NoError(t, err)

	op := &FetchAnalyticsUserCostOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no actor or bucket")
}

// --- shape 3: the roster walk ---

// TestRosterWalk_FollowsAfterIDAndSnapshots covers the third shape and the snapshot
// semantics: the whole roster is re-read each run, keyed by the snapshot date, so a member
// who left simply leaves the next snapshot rather than needing a tombstone.
func TestRosterWalk_FollowsAfterIDAndSnapshots(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.AnthropicOrganizationMembersRequest{})).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicOrganizationMembersRequest)
			if r.GetAfterId() == "" {
				return &vgpb.AnthropicOrganizationMembersResponse{
					Data:    [][]byte{[]byte(`{"id":"user_A","email":"a@example.com","role":"admin"}`)},
					LastId:  "user_A",
					HasMore: true,
				}, nil
			}
			return &vgpb.AnthropicOrganizationMembersResponse{
				Data:    [][]byte{[]byte(`{"id":"user_B","email":"b@example.com","role":"user"}`)},
				LastId:  "user_B",
				HasMore: false,
			}, nil
		}).Times(2)

	sink := &collectingSink{}
	op := &FetchOrganizationMembersOp{}
	out, err := op.Execute(context.Background(), testParams(t, gw, sink, nil, Settings{}))
	require.NoError(t, err)

	snapshot := time.Now().UTC().Truncate(24 * time.Hour).Format(dateLayout)
	assert.ElementsMatch(t, []string{"user_A|" + snapshot, "user_B|" + snapshot}, sink.externalIDs(),
		"the walk must follow after_id across pages, keyed by member and snapshot date")

	// The role rides in the payload: a role change is a fact about the snapshot, not a
	// different member.
	assert.Contains(t, string(sink.records[0].Payload), `"role":"admin"`)

	var pos rosterPosition
	require.NoError(t, json.Unmarshal(out.Data, &pos))
	assert.Equal(t, 2, pos.MemberCount)
}

// TestRosterWalk_FailsWhenTheVendorClaimsMoreWithoutACursor — the vendor asserts there is
// data we did not read, and this roster is the DENOMINATOR an adoption rate divides by. A
// short count does not read as missing data; it reads as a smaller organisation, and every
// rate computed from it is wrong in a plausible-looking direction.
func TestRosterWalk_FailsWhenTheVendorClaimsMoreWithoutACursor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(&vgpb.AnthropicOrganizationMembersResponse{
			Data:    [][]byte{[]byte(`{"id":"user_A"}`)},
			HasMore: true, // claims more...
			LastId:  "",   // ...but hands back nothing to resume from
		}, nil).Times(1)

	sink := &collectingSink{}
	op := &FetchOrganizationMembersOp{}
	_, err := op.Execute(context.Background(), testParams(t, gw, sink, nil, Settings{}))
	require.Error(t, err, "a partial denominator must not be stored as a complete snapshot")
	assert.Contains(t, err.Error(), "no cursor")
}

// TestReportWalk_HasMoreTerminatesTheWalkNotThePageLength pins which signal ends a report
// chunk, and the distinction is the vendor's own: cost_type and token_type fan-out rows do
// not count toward the requested limit, so a page can be SHORT and still not be the last.
//
// A walk that terminated on page length would stop here after the first page and lose
// every row after it — silently, because the chunk would checkpoint as covered.
func TestReportWalk_HasMoreTerminatesTheWalkNotThePageLength(t *testing.T) {
	t.Parallel()

	refreshedAt, refreshedStr := steadyWatermark()

	bucket := "2026-05-20T00:00:00Z"
	row := func(user, product string) []byte {
		return []byte(`{"actor":{"user_id":"` + user + `"},"starting_at":"` + bucket +
			`","product":"` + product + `","model":"m","amount":"1.0"}`)
	}

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	calls := 0
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.AnthropicAnalyticsUserCostReportRequest{})).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			calls++
			r := req.(*vgpb.AnthropicAnalyticsUserCostReportRequest)
			if r.GetPage() == "" {
				// One row — far short of the limit — but the vendor says there is more.
				return &vgpb.AnthropicAnalyticsUserCostReportResponse{
					Data:            [][]byte{row("user_A", "claude_code")},
					HasMore:         true,
					NextPage:        "p2",
					DataRefreshedAt: timestamppb.New(refreshedAt),
				}, nil
			}
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				Data:            [][]byte{row("user_B", "claude_code")},
				HasMore:         false,
				DataRefreshedAt: timestamppb.New(refreshedAt),
			}, nil
		}).Times(2)

	sink := &collectingSink{}
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: refreshedStr,
	})
	require.NoError(t, err)

	op := &FetchAnalyticsUserCostOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.NoError(t, err)

	assert.Equal(t, 2, calls, "the walk must follow next_page while has_more is set, however short the page")
	assert.ElementsMatch(t, []string{
		"user_A|2026-05-20|claude_code",
		"user_B|2026-05-20|claude_code",
	}, sink.externalIDs(), "the second page's rows must not be lost")
}

// TestReportWalk_AMovedWatermarkTriggersTheTrailingReread is the test a unit-level one cannot
// be. A unit test supplies both watermarks by hand, so it proves the planner reacts to a
// difference — not that production ever presents one. Only driving the real walk shows that,
// which is why this asserts the requests actually issued.
func TestReportWalk_AMovedWatermarkTriggersTheTrailingReread(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	// Stored watermark is stale; the vendor will report a newer one.
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  yesterday.Format(dateLayout),
		DataRefreshedAt: "2020-01-01T00:00:00Z",
	})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	var requested []time.Time
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.AnthropicAnalyticsUserCostReportRequest{})).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUserCostReportRequest)
			requested = append(requested, r.GetStartingAt().AsTime().UTC())
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore:         false,
				DataRefreshedAt: timestamppb.New(time.Now().UTC()),
			}, nil
		}).AnyTimes()

	op := &FetchAnalyticsUserCostOp{}
	out, err := op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
	require.NoError(t, err)
	require.NotEmpty(t, requested)

	// The earliest request must reach back into the re-read window, not just cover the one
	// unread day. Without the second planning pass this is the unread day alone.
	earliest := requested[0]
	for _, r := range requested {
		if r.Before(earliest) {
			earliest = r
		}
	}
	assert.Truef(t, earliest.Before(yesterday.AddDate(0, 0, -1)),
		"a moved watermark must pull the trailing window back in; earliest request was %v", earliest)

	// And the newly observed watermark is stored, so the NEXT run can find it unchanged.
	resolvedPos, err := parseReportPosition(out)
	require.NoError(t, err)
	require.True(t, resolvedPos.Present)
	stored := resolvedPos.Stored
	assert.NotEqual(t, "2020-01-01T00:00:00Z", stored.DataRefreshedAt,
		"the observed watermark must be stored or the comparison can never settle")

	// The trailing pass walks history, so it must not drag the position back with it. The
	// forward pass reached today and the vendor reports itself current through today, so
	// that is where coverage must stand — a position rewound to the re-read's own end would
	// re-read the same window every run and never reach the present.
	today := time.Now().UTC().Truncate(24 * time.Hour)
	assert.Equal(t, today.Format(dateLayout), stored.CoveredThrough,
		"the re-read must not rewind the coverage the forward pass earned")
}

// TestReportWalk_AnUnchangedWatermarkSkipsTheReread is the other half: on a quiet day the
// operation costs one new day of rows, not sixteen.
func TestReportWalk_AnUnchangedWatermarkSkipsTheReread(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	refreshedAt, steady := steadyWatermark()
	cur, err := encodeCursor(reportPosition{
		CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: steady,
	})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	var earliest time.Time
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUserCostReportRequest)
			at := r.GetStartingAt().AsTime().UTC()
			if earliest.IsZero() || at.Before(earliest) {
				earliest = at
			}
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(refreshedAt),
			}, nil
		}).AnyTimes()

	op := &FetchAnalyticsUserCostOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
	require.NoError(t, err)

	if !earliest.IsZero() {
		assert.Falsef(t, earliest.Before(yesterday.AddDate(0, 0, -1)),
			"an unchanged watermark must not pull the trailing window in; earliest was %v", earliest)
	}
}

// TestEachOperationCallsItsOwnEndpoint is the guard against a mis-wired operation. Pointing
// the skills op at the users request builder, or the usage op at the cost builder, is a
// well-typed program that ingests the wrong endpoint's rows under the wrong operation_id —
// and every other test in this file stays green.
func TestEachOperationCallsItsOwnEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      sdk.APIFetchOperation
		wantReq proto.Message
	}{
		{"users", &FetchAnalyticsUsersOp{}, &vgpb.AnthropicAnalyticsUsersRequest{}},
		{"skills", &FetchAnalyticsSkillsOp{}, &vgpb.AnthropicAnalyticsSkillsRequest{}},
		{"cost", &FetchAnalyticsUserCostOp{}, &vgpb.AnthropicAnalyticsUserCostReportRequest{}},
		{"usage", &FetchAnalyticsUserUsageOp{}, &vgpb.AnthropicAnalyticsUserUsageReportRequest{}},
		{"roster", &FetchOrganizationMembersOp{}, &vgpb.AnthropicOrganizationMembersRequest{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			gw := fwmocks.NewMockVendorGateway(ctrl)
			gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

			var seen []proto.Message
			gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
					seen = append(seen, req)
					switch req.(type) {
					case *vgpb.AnthropicAnalyticsUsersRequest:
						return &vgpb.AnthropicAnalyticsUsersResponse{}, nil
					case *vgpb.AnthropicAnalyticsSkillsRequest:
						return &vgpb.AnthropicAnalyticsSkillsResponse{}, nil
					case *vgpb.AnthropicAnalyticsUserCostReportRequest:
						return &vgpb.AnthropicAnalyticsUserCostReportResponse{}, nil
					case *vgpb.AnthropicAnalyticsUserUsageReportRequest:
						return &vgpb.AnthropicAnalyticsUserUsageReportResponse{}, nil
					case *vgpb.AnthropicOrganizationMembersRequest:
						return &vgpb.AnthropicOrganizationMembersResponse{}, nil
					}
					return nil, fmt.Errorf("unexpected request type %T", req)
				}).AnyTimes()

			yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
			cur, err := encodeCursor(reportPosition{
				CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: "s",
			})
			require.NoError(t, err)

			_, _ = tt.op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))

			require.NotEmpty(t, seen, "%s made no vendor call at all", tt.name)
			for _, req := range seen {
				assert.IsTypef(t, tt.wantReq, req,
					"%s must call its own endpoint; a mis-wired builder is well-typed and silent", tt.name)
			}
		})
	}
}

// TestWalksRefuseToAdvancePastThePageCap is the silent-loss guard. A vendor that never stops
// handing back a cursor would otherwise exhaust the page loop, fall through to the
// "unit complete" assignment, and mark a truncated day or chunk as covered — dropping every
// page past the cap permanently, with no error and a healthy-looking run.
func TestWalksRefuseToAdvancePastThePageCap(t *testing.T) {
	t.Parallel()

	t.Run("activity", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(&vgpb.AnthropicAnalyticsUsersResponse{
				Data:     [][]byte{[]byte(`{"user":{"id":"user_A"}}`)},
				NextPage: "same-forever", // never terminates
			}, nil).AnyTimes()

		yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
		cur, err := encodeCursor(activityPosition{CoveredThrough: yesterday.AddDate(0, 0, -1).Format(dateLayout)})
		require.NoError(t, err)

		op := &FetchAnalyticsUsersOp{}
		_, err = op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
		require.Error(t, err, "a truncated day must fail rather than be declared covered")
		assert.Contains(t, err.Error(), "refusing to advance past unread pages")
	})

	t.Run("report", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(&vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: true, NextPage: "same-forever",
			}, nil).AnyTimes()

		yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
		cur, err := encodeCursor(reportPosition{
			CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: "s",
		})
		require.NoError(t, err)

		op := &FetchAnalyticsUserCostOp{}
		_, err = op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
		require.Error(t, err, "a truncated chunk must fail rather than be declared covered")
		assert.Contains(t, err.Error(), "refusing to advance past unread pages")
	})
}

// TestAccumulatorGroupsAcrossPageBoundaries pins the invariant recordAccumulator exists for:
// one identity, one emit, however its rows are paged.
func TestAccumulatorGroupsAcrossPageBoundaries(t *testing.T) {
	t.Parallel()

	refreshedAt, refreshedStr := steadyWatermark()
	bucket := "2026-05-20T00:00:00Z"
	row := func(model, amount string) []byte {
		return []byte(`{"actor":{"user_id":"user_A"},"starting_at":"` + bucket +
			`","product":"claude_code","model":"` + model + `","amount":"` + amount + `"}`)
	}

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	page := 0
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			page++
			if page == 1 {
				return &vgpb.AnthropicAnalyticsUserCostReportResponse{
					Data:    [][]byte{row("claude-opus-4", "100.000000")},
					HasMore: true, NextPage: "p2",
					DataRefreshedAt: timestamppb.New(refreshedAt),
				}, nil
			}
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				Data:            [][]byte{row("claude-sonnet-4", "200.000000")},
				HasMore:         false,
				DataRefreshedAt: timestamppb.New(refreshedAt),
			}, nil
		}).AnyTimes()

	sink := &collectingSink{}
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: refreshedStr,
	})
	require.NoError(t, err)

	op := &FetchAnalyticsUserCostOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.NoError(t, err)

	// ONE record for the identity, holding BOTH models. Two records here would mean the
	// second silently replaced the first.
	var forIdentity []*sdk.RawRecord
	for _, r := range sink.records {
		if r.ExternalID == "user_A|2026-05-20|claude_code" {
			forIdentity = append(forIdentity, r)
		}
	}
	require.Len(t, forIdentity, 1,
		"an identity split across pages must be emitted once; twice means the later emit replaced the earlier")

	var arr []json.RawMessage
	require.NoError(t, json.Unmarshal(forIdentity[0].Payload, &arr))
	assert.Len(t, arr, 2, "both pages' rows must be in the one payload")
	assert.Contains(t, string(forIdentity[0].Payload), "100.000000")
	assert.Contains(t, string(forIdentity[0].Payload), "200.000000")
}

// TestSkillsRollUpPerActorDay — the skills endpoint returns one row per actor per SKILL, so
// without a roll-up every row lands on the same actor-day identity and only the last survives.
func TestSkillsRollUpPerActorDay(t *testing.T) {
	t.Parallel()

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.AnthropicAnalyticsSkillsRequest{})).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsSkillsRequest)
			if r.GetDate() != latest.Format(dateLayout) {
				return &vgpb.AnthropicAnalyticsSkillsResponse{}, nil
			}
			return &vgpb.AnthropicAnalyticsSkillsResponse{
				Data: [][]byte{
					[]byte(`{"user":{"id":"user_A"},"skill_name":"code-review","usage":3}`),
					[]byte(`{"user":{"id":"user_A"},"skill_name":"refactor","usage":5}`),
				},
			}, nil
		}).AnyTimes()

	sink := &collectingSink{}
	cur, err := encodeCursor(activityPosition{CoveredThrough: latest.AddDate(0, 0, -1).Format(dateLayout)})
	require.NoError(t, err)

	op := &FetchAnalyticsSkillsOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.NoError(t, err)

	var forDay []*sdk.RawRecord
	want := "user_A|" + latest.Format(dateLayout)
	for _, r := range sink.records {
		if r.ExternalID == want {
			forDay = append(forDay, r)
		}
	}
	require.Len(t, forDay, 1, "both skills share one actor-day identity, so there must be one record")

	var arr []json.RawMessage
	require.NoError(t, json.Unmarshal(forDay[0].Payload, &arr))
	assert.Len(t, arr, 2, "the actor's skill rows must be an array, not the last one to arrive")
	assert.Contains(t, string(forDay[0].Payload), "code-review")
	assert.Contains(t, string(forDay[0].Payload), "refactor")
}

// TestReportWalk_AStaleWatermarkDoesNotStallProgress is the bound that keeps the finality
// ceiling from becoming its own stall.
//
// Coverage normally stops at data_refreshed_at, so a day still being revised is asked for
// again. But a watermark weeks behind the day just read is not a lag, it is a vendor that has
// stopped reporting finality — and honouring it would park the position there and re-read the
// same span on every run, forever. Past the bound the read stands.
func TestReportWalk_AStaleWatermarkDoesNotStallProgress(t *testing.T) {
	t.Parallel()

	// The vendor's watermark is three months behind its own data.
	stale := time.Now().UTC().AddDate(0, 0, -95).Truncate(24 * time.Hour)
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  yesterday.Format(dateLayout),
		DataRefreshedAt: stale.Format(time.RFC3339),
	})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(stale),
			}, nil
		}).AnyTimes()

	op := &FetchAnalyticsUserCostOp{}
	out, err := op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
	require.NoError(t, err)

	resolvedPos, err := parseReportPosition(out)
	require.NoError(t, err)
	require.True(t, resolvedPos.Present)
	stored := resolvedPos.Stored
	assert.Equal(t, time.Now().UTC().Truncate(24*time.Hour).Format(dateLayout), stored.CoveredThrough,
		"coverage must advance to what was read, whatever the vendor says about its own currency")
}

// TestActivityWalk_TwoRowsForOneActorDayFailsRatherThanPickingOne pins the accumulator's
// vendor-shape guard. The activity endpoints contract one row per actor per day, so the
// identity carries a single payload; if the vendor ever returns two, storing either silently
// drops the other and the record still looks well-formed. That has to stop the run.
func TestActivityWalk_TwoRowsForOneActorDayFailsRatherThanPickingOne(t *testing.T) {
	t.Parallel()

	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			return &vgpb.AnthropicAnalyticsUsersResponse{
				Data: [][]byte{
					[]byte(`{"user":{"id":"user_A"},"claude_code_metrics":{"core_metrics":{"commit_count":1}}}`),
					[]byte(`{"user":{"id":"user_A"},"claude_code_metrics":{"core_metrics":{"commit_count":9}}}`),
				},
			}, nil
		}).AnyTimes()

	sink := &collectingSink{}
	cur, err := encodeCursor(activityPosition{CoveredThrough: day.AddDate(0, 0, -1).Format(dateLayout)})
	require.NoError(t, err)

	op := &FetchAnalyticsUsersOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.Error(t, err, "two rows for one actor-day is a vendor-shape change, not a pick-one")
	assert.Empty(t, sink.records, "nothing may be stored once the identity is ambiguous")
}

// TestActivityWalk_CheckpointsNeverMoveThePositionBackwards is the guard on the interaction
// between the overlap and the mid-run checkpoint.
//
// The window deliberately re-enters already-covered days to catch late arrivals, so the
// first days a run completes are EARLIER than the stored point. Writing them unguarded
// persists a position older than the one the run loaded — and a run that then dies leaves the
// cursor behind where it started, ratcheting back a day per attempt with the window widening
// each time. A cursor is monotonic by contract.
func TestActivityWalk_CheckpointsNeverMoveThePositionBackwards(t *testing.T) {
	t.Parallel()

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	// Already covered through the newest day available, so every day the overlap re-enters
	// is strictly older than the stored position.
	cur, err := encodeCursor(activityPosition{CoveredThrough: latest.Format(dateLayout)})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUsersRequest)
			return &vgpb.AnthropicAnalyticsUsersResponse{
				Data: [][]byte{[]byte(`{"user":{"id":"user_A"},"date":"` + r.GetDate() + `"}`)},
			}, nil
		}).AnyTimes()

	cp := &checkpointRecorder{}
	op := &FetchAnalyticsUsersOp{}
	out, err := op.Execute(context.Background(),
		testParamsWithCheckpoint(t, gw, &collectingSink{}, cur, Settings{}, cp))
	require.NoError(t, err)

	// Every persisted position, in order, must be at or after the one the run loaded.
	persisted := cp.covered(t)
	require.NotEmpty(t, persisted, "the walk must checkpoint, or a killed run repeats everything")
	for i, got := range persisted {
		assert.GreaterOrEqualf(t, got, latest.Format(dateLayout),
			"checkpoint %d persisted %s, behind the loaded position %s", i, got, latest.Format(dateLayout))
	}

	stored, _, _, err := parseActivityPosition(out)
	require.NoError(t, err)
	assert.Equal(t, latest.Format(dateLayout), stored.CoveredThrough)
}

// TestAccumulatorRefusesToHoldMoreThanAUnitMayHold bounds memory explicitly. The page caps
// bound requests, not rows, and the request budget that actually stops a walk today lives in
// another package's config — so an override there silently unlocks a resident accumulator
// holding hundreds of thousands of vendor rows, twice over for the two report operations.
//
// It fails rather than emitting what it has: the write is an update in place, so emitting a
// partial unit would overwrite the complete record a previous run stored.
func TestAccumulatorRefusesToHoldMoreThanAUnitMayHold(t *testing.T) {
	t.Parallel()

	acc := newRecordAccumulator(opFetchAnalyticsUserCost, "org-1")
	dataDate := time.Now().UTC().Truncate(24 * time.Hour)

	var err error
	for i := 0; i <= maxRowsPerUnit; i++ {
		// One identity per row, which is the worst case for the map and the order slice.
		if err = acc.add("actor-"+strconv.Itoa(i), dataDate, []byte(`{}`)); err != nil {
			break
		}
	}
	require.Error(t, err, "the cap must stop the unit rather than growing without bound")
	assert.Contains(t, err.Error(), "chunk_days",
		"a report unit is a date range, so the failure must name the knob that narrows it")

	// The activity shape has no such knob — its unit is one calendar day — so it must not
	// send an operator to a lever that cannot move.
	activityAcc := newRecordAccumulator(opFetchAnalyticsUsers, "org-1")
	for i := 0; i <= maxRowsPerUnit; i++ {
		if err = activityAcc.add("actor-"+strconv.Itoa(i), dataDate, []byte(`{}`)); err != nil {
			break
		}
	}
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "chunk_days",
		"an activity unit is one calendar day; chunk_days does nothing for it")

	sink := &collectingSink{}
	require.Error(t, acc.emitAll(context.Background(), sink, false),
		"an over-cap unit must not be emitted; a partial unit overwrites a complete record")
}

// TestReportWalk_TheRereadRunsWhenTheVendorReportsNoWatermark is the direction that fails
// silently. A vendor that omits data_refreshed_at tells us nothing about its own currency, and
// assuming nothing changed would make the entire restatement-collection mechanism — the whole
// reason the two-pass design exists — dead for that vendor, with no error and no log.
func TestReportWalk_TheRereadRunsWhenTheVendorReportsNoWatermark(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{CoveredThrough: yesterday.Format(dateLayout)})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	var earliest time.Time
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			at := req.(*vgpb.AnthropicAnalyticsUserCostReportRequest).GetStartingAt().AsTime().UTC()
			if earliest.IsZero() || at.Before(earliest) {
				earliest = at
			}
			// No DataRefreshedAt at all.
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{HasMore: false}, nil
		}).AnyTimes()

	op := &FetchAnalyticsUserCostOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
	require.NoError(t, err)

	require.False(t, earliest.IsZero())
	assert.Truef(t, earliest.Before(yesterday.AddDate(0, 0, -1)),
		"with no watermark the trailing window must still be read; earliest request was %v", earliest)
}

// TestReportWalk_NoCheckpointCarriesTheNewWatermark pins when the watermark may be persisted.
//
// The watermark is what authorises the trailing re-read. Storing it during pass one means a
// run killed between the passes has already recorded the new value, so the next run sees an
// unchanged watermark, skips the re-read, and never collects the restatements that watermark
// announced — the exact failure the two-pass design exists to prevent.
func TestReportWalk_NoCheckpointCarriesTheNewWatermark(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	moved := time.Now().UTC()
	cur, err := encodeCursor(reportPosition{
		CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: "2020-01-01T00:00:00Z",
	})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(moved),
			}, nil
		}).AnyTimes()

	cp := &checkpointRecorder{}
	op := &FetchAnalyticsUserCostOp{}
	out, err := op.Execute(context.Background(),
		testParamsWithCheckpoint(t, gw, &collectingSink{}, cur, Settings{}, cp))
	require.NoError(t, err)

	cp.mu.Lock()
	persisted := append([]string(nil), cp.positions...)
	cp.mu.Unlock()
	require.NotEmpty(t, persisted, "the walk must checkpoint per chunk")
	for i, raw := range persisted {
		assert.NotContainsf(t, raw, moved.UTC().Format(time.RFC3339),
			"checkpoint %d already carries the new watermark; a kill here loses the re-read", i)
	}

	// It does reach the returned position, once both passes are done.
	resolvedPos, err := parseReportPosition(out)
	require.NoError(t, err)
	stored := resolvedPos.Stored
	assert.Equal(t, moved.UTC().Format(time.RFC3339), stored.DataRefreshedAt)
}

// TestRosterWalk_ARepeatedCursorFailsRatherThanSpinning covers the vendor handing back the
// same after_id. Left to the page cap alone, the walk spends more wall-clock than this
// operation's timeout allows, so the run dies on a deadline with the whole roster re-emitted
// repeatedly — and the deadline names nothing about the cursor that caused it.
func TestRosterWalk_ARepeatedCursorFailsRatherThanSpinning(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	calls := 0
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			calls++
			return &vgpb.AnthropicOrganizationMembersResponse{
				Data:    [][]byte{[]byte(`{"id":"user_A","email":"a@example.com"}`)},
				HasMore: true,
				LastId:  "stuck", // never advances
			}, nil
		}).AnyTimes()

	sink := &collectingSink{}
	op := &FetchOrganizationMembersOp{}
	_, err := op.Execute(context.Background(), testParams(t, gw, sink, nil, Settings{}))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not advance")
	assert.Less(t, calls, maxRosterPages,
		"the walk must stop on the repeated cursor, not spin to the page cap")
}

// TestActivityWalk_RepeatedCursorExpiryFailsWithItsOwnReason bounds the 410 restart separately
// from the page budget. Sharing them lets repeated restarts exhaust the pages and then report
// "did not finish within N pages" — a message that names the wrong cause, and points an
// operator at a cap that was never the problem.
func TestActivityWalk_RepeatedCursorExpiryFailsWithItsOwnReason(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	// Page one always succeeds and hands out a cursor; that cursor is always expired.
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			if req.(*vgpb.AnthropicAnalyticsUsersRequest).GetPage() == "" {
				return &vgpb.AnthropicAnalyticsUsersResponse{
					Data:     [][]byte{[]byte(`{"user":{"id":"user_A"}}`)},
					NextPage: "p2",
				}, nil
			}
			return nil, vendorErr(http.StatusGone, `{"error":"cursor expired"}`)
		}).AnyTimes()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(activityPosition{CoveredThrough: yesterday.AddDate(0, 0, -1).Format(dateLayout)})
	require.NoError(t, err)

	op := &FetchAnalyticsUsersOp{}
	_, err = op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "expired", "the failure must name the cursor, not the page cap")
	assert.NotContains(t, err.Error(), "refusing to advance past unread pages",
		"the page cap was never the cause, so its message must not be the one an operator reads")
}

// TestReportWalk_CoverageStopsAtAFreshWatermark is the ceiling doing its job. A day the vendor
// is still revising must not be claimed as covered, or the next run starts after it and never
// asks again — so a restatement to that day is lost, behind a cursor that looks healthy.
func TestReportWalk_CoverageStopsAtAFreshWatermark(t *testing.T) {
	t.Parallel()

	// The vendor is one day behind the clock: today's rows exist but are not final.
	lagging := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	twoDaysAgo := lagging.AddDate(0, 0, -1)

	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  twoDaysAgo.Format(dateLayout),
		DataRefreshedAt: lagging.Format(time.RFC3339),
	})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	var latestRequested time.Time
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			at := req.(*vgpb.AnthropicAnalyticsUserCostReportRequest).GetEndingAt().AsTime().UTC()
			if at.After(latestRequested) {
				latestRequested = at
			}
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(lagging),
			}, nil
		}).AnyTimes()

	op := &FetchAnalyticsUserCostOp{}
	out, err := op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
	require.NoError(t, err)

	// The request reaches the clock — that is the only way a newer watermark is ever learned,
	// since it arrives in a response and nowhere else.
	assert.Equal(t, time.Now().UTC().Truncate(24*time.Hour), latestRequested,
		"the request must reach the clock, or a caught-up walk can never learn a newer watermark")

	// The position stops at the watermark, so the unfinished tail is asked for again.
	resolvedPos, err := parseReportPosition(out)
	require.NoError(t, err)
	require.True(t, resolvedPos.Present)
	assert.Equal(t, lagging.Format(dateLayout), resolvedPos.Stored.CoveredThrough,
		"coverage must stop at the vendor's currency, not at the day we requested")
}

// TestFinalityCeiling covers the bound directly, both sides of it.
func TestFinalityCeiling(t *testing.T) {
	t.Parallel()

	readThrough := time.Date(2026, time.June, 10, 0, 0, 0, 0, time.UTC)

	got, ok := finalityCeiling(time.Date(2026, time.June, 9, 0, 0, 0, 0, time.UTC), readThrough)
	require.True(t, ok, "a watermark a day behind is a normal lag and must bound coverage")
	assert.Equal(t, time.Date(2026, time.June, 9, 0, 0, 0, 0, time.UTC), got)

	_, ok = finalityCeiling(readThrough.Add(-maxUnfinalizedLag), readThrough)
	assert.True(t, ok, "exactly at the bound still counts as a lag")

	_, ok = finalityCeiling(readThrough.Add(-maxUnfinalizedLag-24*time.Hour), readThrough)
	assert.False(t, ok, "past the bound the vendor has stopped reporting finality; the read stands")

	_, ok = finalityCeiling(time.Time{}, readThrough)
	assert.False(t, ok, "no watermark means no ceiling, not a zero one")
}

// TestGatewayErrorClassificationSurvivesEveryWalk is the invariant a plausible-looking wrapper
// destroys. SyncError.Unwrap returns Cause, so wrapping a gateway error in a NEW SyncError
// makes errors.As find the outer one — replacing the vendor's kind, HTTP status, terminal bit
// and Retry-After with generic defaults.
//
// The consequence is not cosmetic: the orchestrator keys dead-credential handling on Terminal,
// so a revoked key would never reach auth_revoked and every run would burn its whole retry
// budget against a 401 that cannot succeed.
func TestGatewayErrorClassificationSurvivesEveryWalk(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	twoDaysAgo := yesterday.AddDate(0, 0, -1)

	activityCur, err := encodeCursor(activityPosition{CoveredThrough: twoDaysAgo.Format(dateLayout)})
	require.NoError(t, err)
	reportCur, err := encodeCursor(reportPosition{
		CoveredThrough: twoDaysAgo.Format(dateLayout), DataRefreshedAt: yesterday.Format(time.RFC3339),
	})
	require.NoError(t, err)

	for _, tt := range []struct {
		name   string
		op     sdk.APIFetchOperation
		cursor *sdk.Cursor
	}{
		{"activity", &FetchAnalyticsUsersOp{}, activityCur},
		{"report", &FetchAnalyticsUserCostOp{}, reportCur},
		{"roster", &FetchOrganizationMembersOp{}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			gw := fwmocks.NewMockVendorGateway(ctrl)
			gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
			// A revoked key: the classifier marks a 401 terminal.
			gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
				Return(nil, vendorErr(http.StatusUnauthorized, `{"error":"invalid x-api-key"}`)).AnyTimes()

			_, execErr := tt.op.Execute(context.Background(),
				testParams(t, gw, &collectingSink{}, tt.cursor, Settings{}))
			require.Error(t, execErr)

			var syncErr *sdk.SyncError
			require.ErrorAs(t, execErr, &syncErr)
			assert.Equal(t, http.StatusUnauthorized, syncErr.HTTPStatus,
				"the vendor's status must survive; a new SyncError wrapper would erase it")
			assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind,
				"the vendor's kind must survive, not become a generic vendor_error")
			assert.True(t, syncErr.Terminal,
				"the terminal bit must survive, or a revoked key burns the whole retry budget every run")
		})
	}
}

// TestReportWalk_TheHighestWatermarkWins pins the comparison's precision. Pages are read
// across both passes and the trailing pass reads older ranges, so the LAST watermark seen is
// not the newest — and two watermarks on one calendar day are ordered only by their times.
// Letting an older one win stores it, and the next run then sees a difference that is not one
// and re-reads the trailing window for nothing, on every run thereafter.
func TestReportWalk_TheHighestWatermarkWins(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	newest := yesterday.Add(10 * time.Hour)
	older := yesterday.Add(9 * time.Hour) // same calendar day, earlier time

	cur, err := encodeCursor(reportPosition{
		CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: "2020-01-01T00:00:00Z",
	})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	page := 0
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			page++
			at := newest
			if page > 1 {
				at = older // every later page reports the older same-day value
			}
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(at),
			}, nil
		}).AnyTimes()

	op := &FetchAnalyticsUserCostOp{}
	out, err := op.Execute(context.Background(), testParams(t, gw, &collectingSink{}, cur, Settings{}))
	require.NoError(t, err)
	require.Greater(t, page, 1, "the fixture needs more than one page for this to mean anything")

	stored, err := parseReportPosition(out)
	require.NoError(t, err)
	assert.Equal(t, newest.Format(time.RFC3339), stored.Stored.DataRefreshedAt,
		"the highest watermark must be stored, not the last one seen")
}

// TestReportWalk_ADivergingWalkSaysSo covers the invariant the per-run chunk cap creates: a
// run advances at most maxForwardChunksPerRun × chunk_days days, so that product must exceed
// the interval between runs or the position falls behind the clock by the difference on EVERY
// run — and once it falls past the vendor's year limit the excess is dropped for good.
//
// The two knobs an operator reaches for under rate-limit pressure push in the same direction,
// so this has to be visible from the first occurrence rather than years later.
func TestReportWalk_ADivergingWalkSaysSo(t *testing.T) {
	t.Parallel()

	// chunk_days=1 gives a reach of 4 days; the position is 60 days behind.
	behind := time.Now().UTC().AddDate(0, 0, -60).Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  behind.Format(dateLayout),
		DataRefreshedAt: time.Now().UTC().Format(time.RFC3339),
	})
	require.NoError(t, err)

	overrides, err := json.Marshal(operationOptions{ChunkDays: 1})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	requests := 0
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			requests++
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(time.Now().UTC()),
			}, nil
		}).AnyTimes()

	params := testParams(t, gw, &collectingSink{}, cur, Settings{})
	params.Config.OperationOverrides = map[sdk.OperationID]json.RawMessage{
		opFetchAnalyticsUserCost: overrides,
	}

	op := &FetchAnalyticsUserCostOp{}
	out, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	// The run reads only what it planned — the cap holds — and the position advances by that
	// much rather than jumping to the present, so nothing in between is skipped.
	stored, err := parseReportPosition(out)
	require.NoError(t, err)
	require.True(t, stored.Present)
	assert.Equal(t, behind.AddDate(0, 0, maxForwardChunksPerRun).Format(dateLayout), stored.Stored.CoveredThrough,
		"a capped run must advance by exactly its reach: further would skip days, less would stall")

	// And it must make forward progress at all, or a narrow width is a stall rather than a pace.
	assert.Greater(t, stored.Stored.CoveredThrough, behind.Format(dateLayout))
}

// TestReportWalk_TheRereadReachesTheWholeWindow drives the real walk to pin which pass the
// per-run chunk cap is passed to.
//
// The forward pass is capped so a wide gap closes over runs. Passing that cap to the trailing
// re-read truncates it from the NEWEST end, so with a narrow chunk_days the days most likely
// restated are the ones dropped — and because the window re-anchors to the clock each run they
// are never re-read at all. A unit test cannot catch that: it supplies the cap itself.
func TestReportWalk_TheRereadReachesTheWholeWindow(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: "2020-01-01T00:00:00Z",
	})
	require.NoError(t, err)

	// One day per chunk: the re-read window needs far more chunks than the forward cap allows.
	overrides, err := json.Marshal(operationOptions{ChunkDays: 1})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	requested := map[string]bool{}
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			at := req.(*vgpb.AnthropicAnalyticsUserCostReportRequest).GetStartingAt().AsTime().UTC()
			requested[at.Format(dateLayout)] = true
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(time.Now().UTC()),
			}, nil
		}).AnyTimes()

	params := testParams(t, gw, &collectingSink{}, cur, Settings{})
	params.Config.OperationOverrides = map[sdk.OperationID]json.RawMessage{
		opFetchAnalyticsUserCost: overrides,
	}

	op := &FetchAnalyticsUserCostOp{}
	_, err = op.Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotEmpty(t, requested)

	// Every day of the re-read window must have been asked for. Asserting only the OLDEST day
	// would miss the defect entirely: the cap counts chunks from the oldest end, so it drops
	// the newest days — the ones most likely to have been restated — while the oldest request
	// looks exactly right.
	today := time.Now().UTC().Truncate(24 * time.Hour)
	var missing []string
	for d := today.AddDate(0, 0, -defaultReportRereadDays); d.Before(today); d = d.AddDate(0, 0, 1) {
		if !requested[d.Format(dateLayout)] {
			missing = append(missing, d.Format(dateLayout))
		}
	}
	assert.Emptyf(t, missing, "these days of the re-read window were never requested: %v", missing)
}

// TestReportWalk_ANarrowWidthStillEscapesAFrozenWatermark is the interaction guard, and it
// needs two consecutive runs to see anything — one run alone looks perfectly healthy.
//
// The finality ceiling holds coverage back to the vendor's watermark, and a staleness bound
// releases it once the watermark is too far behind to be a lag. Judging that bound against the
// chunk just read instead of against the clock makes the release unreachable whenever one
// run's reach is narrower than the bound: no planned chunk can get far enough past a frozen
// watermark, coverage collapses back to it on every chunk of every run, and the walk re-reads
// the identical range forever while reporting success — until those days age past the vendor's
// year limit and are unrecoverable.
//
// chunk_days=7 gives a reach of 28 days, inside the 31-day bound, and it is the exact lever the
// connector's own failure messages tell an operator to pull.
func TestReportWalk_ANarrowWidthStillEscapesAFrozenWatermark(t *testing.T) {
	t.Parallel()

	frozen := time.Now().UTC().AddDate(0, 0, -60).Truncate(24 * time.Hour)
	overrides, err := json.Marshal(operationOptions{ChunkDays: 7})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			// The watermark never moves.
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				HasMore: false, DataRefreshedAt: timestamppb.New(frozen.Add(9 * time.Hour)),
			}, nil
		}).AnyTimes()

	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  frozen.Format(dateLayout),
		DataRefreshedAt: frozen.Add(9 * time.Hour).Format(time.RFC3339),
	})
	require.NoError(t, err)

	op := &FetchAnalyticsUserCostOp{}
	positions := make([]string, 0, 2)
	for range 2 {
		params := testParams(t, gw, &collectingSink{}, cur, Settings{})
		params.Config.OperationOverrides = map[sdk.OperationID]json.RawMessage{
			opFetchAnalyticsUserCost: overrides,
		}
		out, execErr := op.Execute(context.Background(), params)
		require.NoError(t, execErr)

		stored, parseErr := parseReportPosition(out)
		require.NoError(t, parseErr)
		positions = append(positions, stored.Stored.CoveredThrough)
		cur = out
	}

	assert.Greater(t, positions[0], frozen.Format(dateLayout),
		"the first run must advance past a watermark too stale to be a lag")
	assert.Greater(t, positions[1], positions[0],
		"and the second must advance again; equal positions are a stall that reports success forever")
}

// TestActivityWalk_ANotYetAggregatedDayIsACoverWhenTheWalkReachedIt separates the two things a
// stop can mean. The vendor lagging a day or two is the ordinary configuration, and a run that
// read everything available reached its target — counting those as short-stops would leave the
// covered/yielded pair reporting a stall for a connector that is working.
func TestActivityWalk_ANotYetAggregatedDayIsACoverWhenTheWalkReachedIt(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	const org = "org-activity-caught-up"
	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	coveredThrough := latest.AddDate(0, 0, -2)
	firstRequested := coveredThrough.AddDate(0, 0, -(defaultActivityOverlapDays - 1))
	named := firstRequested.AddDate(0, 0, -1)

	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			return nil, vendorErr(http.StatusBadRequest,
				`{"error":{"message":"engagement data is available through `+named.Format(dateLayout)+`"}}`)
		}).Times(1)

	cur, err := encodeCursor(activityPosition{CoveredThrough: coveredThrough.Format(dateLayout)})
	require.NoError(t, err)

	lbl := []string{tenantlabel.OrgID(org), string(opFetchAnalyticsUsers)}
	beforeCovered := testutil.ToFloat64(runCovered.WithLabelValues(lbl...))
	beforeYielded := testutil.ToFloat64(runYielded.WithLabelValues(append(append([]string{}, lbl...), yieldNotAggregated)...))

	params := testParams(t, gw, &collectingSink{}, cur, Settings{})
	params.Config.OrganizationID = org
	op := &FetchAnalyticsUsersOp{}
	_, err = op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, beforeCovered+1, testutil.ToFloat64(runCovered.WithLabelValues(lbl...)),
		"the walk read every day the vendor has, so the run covered its target")
	assert.Equal(t, beforeYielded,
		testutil.ToFloat64(runYielded.WithLabelValues(append(append([]string{}, lbl...), yieldNotAggregated)...)),
		"and it is not a short-stop")
}

// TestRosterStoresNothingUntilTheSnapshotIsWhole. The unit here is the whole roster, and a record
// already on the queue cannot be recalled — so emitting per page meant a later failure left a
// partial roster stored under identities that look complete. The next successful attempt keys on
// its own UTC date, so it never overwrites them: the partial day survives as a permanently
// valid-looking snapshot of an organisation that was never that size. And this roster is the
// denominator an adoption rate divides by, so the damage reads as "smaller organisation".
func TestRosterStoresNothingUntilTheSnapshotIsWhole(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()

	// A first page that reads cleanly, then a vendor failure on the second.
	call := 0
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
			call++
			if call == 1 {
				return &vgpb.AnthropicOrganizationMembersResponse{
					Data:    [][]byte{[]byte(`{"id":"user_A","email":"a@example.com","role":"admin"}`)},
					LastId:  "user_A",
					HasMore: true,
				}, nil
			}
			return nil, vendorErr(http.StatusInternalServerError, "HTTP 500: upstream unavailable")
		}).AnyTimes()

	sink := &collectingSink{}
	_, err := (&FetchOrganizationMembersOp{}).Execute(context.Background(),
		testParams(t, gw, sink, nil, Settings{}))

	require.Error(t, err, "a roster that could not be completed must fail the run")
	assert.Empty(t, sink.records,
		"the first page's member must NOT be on the queue: a partial roster stored under a complete-looking "+
			"identity is never overwritten, because the retry lands on its own date")
	assert.Greater(t, call, 1, "the test must actually reach the second page, or it proves nothing")
}

// TestCollectReportRows_DataDateIsTheExternalIDsUTCDay pins the two halves of a report
// record's key to one day. A bucket start sent with an offset, or not at midnight, still
// files under its UTC date, and the DataDate is midnight UTC of that same date. A row with
// no product files under the explicit "unknown" product rather than an empty segment.
func TestCollectReportRows_DataDateIsTheExternalIDsUTCDay(t *testing.T) {
	t.Parallel()

	acc := newRecordAccumulator(opFetchAnalyticsUserCost, "org-1")
	require.NoError(t, collectReportRows(acc, opFetchAnalyticsUserCost, "org-1", [][]byte{
		[]byte(`{"actor":{"user_id":"user_A"},"starting_at":"2026-05-20T00:00:00-07:00","product":"claude_code"}`),
		[]byte(`{"actor":{"user_id":"user_B"},"starting_at":"2026-05-21T01:00:00+02:00","product":"chat"}`),
		[]byte(`{"actor":{"user_id":"user_C"},"starting_at":"2026-05-20T00:00:00Z"}`),
		[]byte(`{"actor":{"user_id":"user_C"},"starting_at":"2026-05-20T00:00:00Z","product":""}`),
	}))
	sink := &collectingSink{}
	require.NoError(t, acc.emitAll(context.Background(), sink, false))

	got := map[string]time.Time{}
	for _, r := range sink.records {
		got[r.ExternalID] = r.DataDate
	}
	may20 := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, map[string]time.Time{
		"user_A|2026-05-20|claude_code": may20,
		"user_B|2026-05-20|chat":        may20,
		"user_C|2026-05-20|unknown":     may20,
	}, got)
}

// TestActivityWalk_ARefusedListedDayDoesNotBlockTheWindow. A listed day the vendor refuses
// on retry stays listed and the walk goes on with the window; a listed day older than the
// lookback is dropped without a request.
func TestActivityWalk_ARefusedListedDayDoesNotBlockTheWindow(t *testing.T) {
	t.Parallel()

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	refused := latest.AddDate(0, 0, -20).Format(dateLayout)
	tooOld := latest.AddDate(0, 0, -(DefaultLookbackDays + 5)).Format(dateLayout)
	var asked []string
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUsersRequest)
			asked = append(asked, r.GetDate())
			if r.GetDate() == refused {
				return nil, vendorErr(http.StatusForbidden, "HTTP 403: forbidden")
			}
			return &vgpb.AnthropicAnalyticsUsersResponse{Data: [][]byte{[]byte(`{"user":{"id":"user_A"}}`)}}, nil
		}).AnyTimes()

	cur, err := encodeCursor(activityPosition{CoveredThrough: latest.AddDate(0, 0, -1).Format(dateLayout), FailedDays: []string{tooOld, refused}})
	require.NoError(t, err)
	sink := &collectingSink{}
	cp := &checkpointRecorder{}
	_, err = (&FetchAnalyticsUsersOp{}).Execute(context.Background(), testParamsWithCheckpoint(t, gw, sink, cur, Settings{}, cp))
	require.Error(t, err, "a listed day that keeps failing keeps the run failing, retryably")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.False(t, se.Terminal)
	assert.Contains(t, asked, latest.Format(dateLayout), "the window is read past the refused listed day")
	assert.NotContains(t, asked, tooOld, "a day past the lookback is not asked for")
	var pos activityPosition
	cp.last(t, &pos)
	assert.Equal(t, []string{refused}, pos.FailedDays, "the refused day stays listed, the old one is dropped")
	assert.Equal(t, latest.Format(dateLayout), pos.CoveredThrough)
}

// TestReportWalk_ARefusedListedChunkDoesNotBlockTheWindow. A listed chunk the vendor refuses
// on retry stays listed and the forward pass still runs.
func TestReportWalk_ARefusedListedChunkDoesNotBlockTheWindow(t *testing.T) {
	t.Parallel()

	refreshedAt, refreshedStr := steadyWatermark()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	covered := today.AddDate(0, 0, -14)
	refusedStart := today.AddDate(0, 0, -40)
	refused := chunkUnit(reportChunk{StartingAt: refusedStart, EndingAt: refusedStart.AddDate(0, 0, 1)})
	var forwardRead bool
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r := req.(*vgpb.AnthropicAnalyticsUserCostReportRequest)
			start := r.GetStartingAt().AsTime().UTC()
			if start.Equal(refusedStart) {
				return nil, vendorErr(http.StatusForbidden, "HTTP 403: forbidden")
			}
			if !start.Before(covered) {
				forwardRead = true
			}
			bucket := start.Format(time.RFC3339)
			return &vgpb.AnthropicAnalyticsUserCostReportResponse{
				Data:            [][]byte{[]byte(`{"actor":{"user_id":"user_A"},"starting_at":"` + bucket + `","product":"claude_code","amount":"1"}`)},
				DataRefreshedAt: timestamppb.New(refreshedAt),
				OrganizationId:  "org_abc",
			}, nil
		}).AnyTimes()

	cur, err := encodeCursor(reportPosition{CoveredThrough: covered.Format(dateLayout), DataRefreshedAt: refreshedStr, FailedChunks: []string{refused}})
	require.NoError(t, err)
	sink := &collectingSink{}
	cp := &checkpointRecorder{}
	_, err = (&FetchAnalyticsUserCostOp{}).Execute(context.Background(), testParamsWithCheckpoint(t, gw, sink, cur, Settings{}, cp))
	require.Error(t, err)
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.False(t, se.Terminal)
	assert.True(t, forwardRead, "the forward pass runs past the refused listed chunk")
	var pos reportPosition
	cp.last(t, &pos)
	assert.Equal(t, []string{refused}, pos.FailedChunks, "the refused chunk stays listed")
	assert.Greater(t, pos.CoveredThrough, covered.Format(dateLayout))
}
