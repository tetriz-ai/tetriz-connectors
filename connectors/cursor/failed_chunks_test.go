// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	vgcursorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// dayGateway answers by the requested chunk's first day: an error for a failing day, else
// one event on that day.
type dayGateway struct {
	failing map[string]bool
	starts  []string
}

func (g *dayGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	r, ok := req.(*vgcursorpb.CursorFilteredUsageEventsRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected request %T", req)
	}
	start := time.UnixMilli(r.GetStartDate()).UTC()
	day := start.Format(dateLayout)
	g.starts = append(g.starts, day)
	if g.failing[day] {
		return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: http.StatusServiceUnavailable, Cause: errors.New("HTTP 503")}
	}
	return &vgcursorpb.CursorFilteredUsageEventsResponse{UsageEvents: [][]byte{
		event("conv-"+day, "dev@example.com", start.Add(time.Hour), ""),
	}}, nil
}

func (g *dayGateway) ForGroup(string) sdk.VendorGateway                      { return g }
func (g *dayGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return g }
func (g *dayGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, errors.New("the operation must not make raw HTTP calls")
}
func (g *dayGateway) DoJSON(context.Context, string, string, any, any) error {
	return errors.New("the operation must not make raw HTTP calls")
}

func lastPosition(t *testing.T, params *sdk.ExecutionParams) func() usagePosition {
	t.Helper()
	var last *sdk.Cursor
	params.Checkpoint = func(_ context.Context, cur *sdk.Cursor) error {
		last = cur
		return nil
	}
	return func() usagePosition {
		require.NotNil(t, last, "the walk checkpointed nothing")
		var p usagePosition
		require.NoError(t, json.Unmarshal(last.Data, &p))
		return p
	}
}

// TestExecute_WalksPastAFailedChunkAndRetriesItNextRun. A chunk that fails on its own does
// not hold back the chunks after it: the walk lists its days, moves the floor past it, reads
// the later chunks, and fails the run, retryably. The next run reads the listed days again,
// and once they succeed the list is empty.
func TestExecute_WalksPastAFailedChunkAndRetriesItNextRun(t *testing.T) {
	t.Parallel()

	today := startOfUTCDay(time.Now())
	// A first run over a nine-day lookback: three chunks, the first of which fails.
	first := today.AddDate(0, 0, -9)
	gw := &dayGateway{failing: map[string]bool{first.Format(dateLayout): true}}
	sink := testkit.NewChannelSink()
	params := newParams(t, gw, sink, nil)
	params.Config.Settings = json.RawMessage(`{"lookback_days":9}`)
	stored := lastPosition(t, params)

	next, err := (&FetchUsageEventsOp{}).Execute(context.Background(), params)
	require.Error(t, err, "a run that walked past a failed chunk must not report success")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.False(t, se.Terminal)
	assert.Nil(t, next)
	assert.Len(t, sink.Records(), 2, "the chunks after the failed one are read")
	pos := stored()
	assert.Equal(t, today.Format(dateLayout), pos.FloorDate, "the floor moves past the listed chunk")
	assert.Equal(t, []string{
		first.Format(dateLayout), first.AddDate(0, 0, 1).Format(dateLayout), first.AddDate(0, 0, 2).Format(dateLayout),
	}, pos.FailedDays)

	// The next run: the days now succeed and leave the list.
	gw.failing = nil
	gw.starts = nil
	cur, err := newUsagePositionCursor(pos)
	require.NoError(t, err)
	sink = testkit.NewChannelSink()
	params = newParams(t, gw, sink, cur)
	next, err = (&FetchUsageEventsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Contains(t, gw.starts, first.Format(dateLayout), "the listed days are read again")
	var after usagePosition
	require.NoError(t, json.Unmarshal(next.Data, &after))
	assert.Empty(t, after.FailedDays)
	assert.Equal(t, today.Format(dateLayout), after.FloorDate)
}

// TestExecute_StopsAdvancingWhenTheFailedListIsFull. With the list unable to take a failed
// chunk's days, the run stops there, as before failed days were tracked: the floor stays on
// the chunk's first day and no listed day is dropped.
func TestExecute_StopsAdvancingWhenTheFailedListIsFull(t *testing.T) {
	t.Parallel()

	today := startOfUTCDay(time.Now())
	var listed []string
	for i := 0; i < maxFailedDays; i++ {
		listed = append(listed, today.AddDate(0, 0, -80+i).Format(dateLayout))
	}
	first := today.AddDate(0, 0, -9)
	gw := &dayGateway{failing: map[string]bool{first.Format(dateLayout): true}}
	for _, d := range listed {
		gw.failing[d] = true
	}
	start, err := newUsagePositionCursor(usagePosition{
		FloorDate: first.Format(dateLayout), BackfillUntil: today.AddDate(0, 0, -1).Format(dateLayout), FailedDays: listed,
	})
	require.NoError(t, err)
	sink := testkit.NewChannelSink()
	params := newParams(t, gw, sink, start)
	stored := lastPosition(t, params)

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), first.Format(dateLayout))
	assert.Empty(t, sink.Records(), "the chunks after the unlisted failure wait")
	pos := stored()
	assert.Equal(t, first.Format(dateLayout), pos.FloorDate, "the floor does not pass the unlisted failure")
	assert.Equal(t, listed, pos.FailedDays, "no listed day is dropped")
}

// TestRetryChunksGroupsListedDaysBeforeTheWindow. Listed days the window reaches are read by
// the window; the rest are grouped into consecutive runs of at most chunkDays.
func TestRetryChunksGroupsListedDaysBeforeTheWindow(t *testing.T) {
	t.Parallel()

	day := func(s string) time.Time {
		d, err := time.ParseInLocation(dateLayout, s, time.UTC)
		require.NoError(t, err)
		return d
	}
	window := usageWindow{Start: day("2026-08-20"), End: day("2026-08-23").Add(-time.Nanosecond)}
	got := retryChunks([]string{"2026-08-01", "2026-08-02", "2026-08-03", "2026-08-04", "2026-08-10", "2026-08-21"}, window)
	var spans []string
	for _, c := range got {
		spans = append(spans, chunkDayList(c)[0]+".."+startOfUTCDay(c.End).Format(dateLayout))
	}
	assert.Equal(t, []string{"2026-08-01..2026-08-03", "2026-08-04..2026-08-04", "2026-08-10..2026-08-10"}, spans)
}

// TestExecute_ARefusedListedDayDoesNotBlockTheWindow. Listed days the vendor refuses on
// retry (a 400) stay listed and the walk goes on with the window; a listed day older than
// the lookback is dropped without a request.
func TestExecute_ARefusedListedDayDoesNotBlockTheWindow(t *testing.T) {
	t.Parallel()

	today := startOfUTCDay(time.Now())
	refused := today.AddDate(0, 0, -30).Format(dateLayout)
	tooOld := today.AddDate(0, 0, -(DefaultLookbackDays + 5)).Format(dateLayout)
	gw := &refusingDayGateway{dayGateway: dayGateway{failing: map[string]bool{}}, refused: refused}
	start, err := newUsagePositionCursor(usagePosition{
		FloorDate: today.AddDate(0, 0, -1).Format(dateLayout), FailedDays: []string{tooOld, refused},
	})
	require.NoError(t, err)
	sink := testkit.NewChannelSink()
	params := newParams(t, gw, sink, start)
	stored := lastPosition(t, params)

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), params)
	require.Error(t, err, "a listed day that keeps failing keeps the run failing, retryably")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.False(t, se.Terminal)
	assert.NotContains(t, gw.starts, tooOld, "a day past the lookback is not asked for")
	assert.NotEmpty(t, sink.Records(), "the window is read past the refused listed day")
	pos := stored()
	assert.Equal(t, []string{refused}, pos.FailedDays, "the refused day stays listed, the old one is dropped")
}

// refusingDayGateway refuses one day with a 400 and answers every other as dayGateway does.
type refusingDayGateway struct {
	dayGateway
	refused string
}

func (g *refusingDayGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if r, ok := req.(*vgcursorpb.CursorFilteredUsageEventsRequest); ok {
		if day := time.UnixMilli(r.GetStartDate()).UTC().Format(dateLayout); day == g.refused {
			g.starts = append(g.starts, day)
			return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: http.StatusBadRequest, Cause: errors.New("HTTP 400")}
		}
	}
	return g.dayGateway.Execute(ctx, req)
}

func (g *refusingDayGateway) ForGroup(string) sdk.VendorGateway { return g }
