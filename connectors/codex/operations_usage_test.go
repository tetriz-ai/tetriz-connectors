// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const testWorkspace = "00000000-0000-4000-8000-00000000c0de"

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

// scriptedGateway answers a request from a script keyed by the DAY it asks for, and returns
// an empty page for any day the script does not name.
//
// Keyed by day rather than by call order because the walk deliberately re-reads a trailing
// overlap, so the number of requests is a property of the window resolution and not
// something a fixture should have to count. A day-keyed script also makes the overlap
// visible: a test that expects records only for two days still sees the walk ask for five.
type scriptedGateway struct {
	byDay map[string][]*vgpb.CodexUsageResponse
	errs  map[string]error
	seen  []*vgpb.CodexUsageRequest
	calls int
	// pageOf tracks how far into a day's script the walk has read.
	pageOf map[string]int
}

func newScriptedGateway() *scriptedGateway {
	return &scriptedGateway{
		byDay:  map[string][]*vgpb.CodexUsageResponse{},
		errs:   map[string]error{},
		pageOf: map[string]int{},
	}
}

func (g *scriptedGateway) on(day time.Time, pages ...*vgpb.CodexUsageResponse) *scriptedGateway {
	g.byDay[day.Format(dateLayout)] = pages
	return g
}

func (g *scriptedGateway) failOn(day time.Time, err error) *scriptedGateway {
	g.errs[day.Format(dateLayout)] = err
	return g
}

func (g *scriptedGateway) ForGroup(string) sdk.VendorGateway { return g }

// The rest of the interface. Only ForGroup and Execute are on this walk's path; the HTTP
// methods panic rather than returning a zero value, so a change that started reaching for
// one is a loud failure rather than a silently empty response.
func (g *scriptedGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	panic("scriptedGateway: the usage walk must go through Execute, not raw HTTP")
}

func (g *scriptedGateway) DoJSON(context.Context, string, string, any, any) error {
	panic("scriptedGateway: the usage walk must go through Execute, not DoJSON")
}

func (g *scriptedGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}

func (g *scriptedGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	usage, ok := req.(*vgpb.CodexUsageRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected request type %T", req)
	}
	g.seen = append(g.seen, usage)
	g.calls++

	day := time.Unix(usage.GetStartTime(), 0).UTC().Format(dateLayout)
	if err, bad := g.errs[day]; bad {
		return nil, err
	}
	pages := g.byDay[day]
	g.pageOf[day]++
	// Paged by the CURSOR the request carries, not by an internal counter. A gateway that
	// counted its own calls would answer page two even for a walk that never sent a cursor —
	// so dropping `Page` from the request would still pass, and in real use the walk would
	// read page one five hundred times and then blame the page cap.
	i := 0
	if c := usage.GetPage(); c != "" {
		i = -1
		for n, pg := range pages {
			if pg.GetNextPage() == c {
				i = n + 1
				break
			}
		}
		if i < 0 {
			return nil, fmt.Errorf("scriptedGateway: %s was sent an unknown page cursor %q", day, c)
		}
	}
	if i < len(pages) {
		return pages[i], nil
	}
	// A day the script does not cover: the vendor had nothing for it. Realistic, and it
	// keeps a fixture from having to enumerate the trailing overlap's quiet days.
	return &vgpb.CodexUsageResponse{}, nil
}

func page(t *testing.T, hasMore bool, next string, rows ...string) *vgpb.CodexUsageResponse {
	t.Helper()
	resp := &vgpb.CodexUsageResponse{HasMore: hasMore, NextPage: next}
	for _, r := range rows {
		require.True(t, json.Valid([]byte(r)), "fixture row is not valid JSON")
		resp.UsageRows = append(resp.UsageRows, []byte(r))
	}
	return resp
}

func testParams(t *testing.T, gw sdk.VendorGateway, sink sdk.DataSink, cur *sdk.Cursor, s Settings) *sdk.ExecutionParams {
	t.Helper()
	settings, err := json.Marshal(s)
	require.NoError(t, err)
	creds, err := json.Marshal(Credentials{APIKey: "sk-codex-x"})
	require.NoError(t, err)
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    connectorID,
			OrganizationID: "org-1",
			Credentials:    creds,
			Settings:       settings,
		},
		Cursor:        cur,
		Sink:          sink,
		VendorGateway: gw,
	}
}

// cursorAt builds a stored position for testWorkspace, so a test can start the walk from a
// known covered day.
func cursorAt(t *testing.T, day string) *sdk.Cursor {
	t.Helper()
	cur, err := storePosition(position{CoveredThrough: day, WorkspaceID: testWorkspace})
	require.NoError(t, err)
	return cur
}

// yesterday is the newest day the walk will ever request. It never asks for today, because
// OpenAI publishes no aggregation-lag statement and a partially-aggregated day read as
// complete is a wrong number no later run revisits.
func yesterday() time.Time {
	return time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
}

// TestUsageWalk_EmitsOneRecordPerDeveloperPerDay walks two days and checks the whole
// contract: the record identity, the day appearing in both halves of it, the payload
// surviving untouched, and the position advancing to the last day read.
func TestUsageWalk_EmitsOneRecordPerDeveloperPerDay(t *testing.T) {
	t.Parallel()

	d2 := yesterday()
	d1 := d2.AddDate(0, 0, -1)
	// An unknown field and a null counter, both of which must survive into the payload
	// verbatim: a null separates "not applicable" from zero, and flattening it would
	// understate every later aggregate.
	rowA := `{"user_id":"u_1","actor":{"email":"a@example.com"},"credits":12,"tokens":null,"a_field_we_have_never_seen":{"x":1}}`
	rowB := `{"user_id":"u_2","actor":{"email":null},"credits":3}`

	gw := newScriptedGateway().on(d1, page(t, false, "", rowA, rowB)).on(d2, page(t, false, "", rowA))
	sink := &collectingSink{}
	cur, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, d1.AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)

	require.Len(t, sink.records, 3)
	assert.Equal(t, []string{
		testWorkspace + "|u_1|" + d1.Format(dateLayout),
		testWorkspace + "|u_2|" + d1.Format(dateLayout),
		testWorkspace + "|u_1|" + d2.Format(dateLayout),
	}, sink.externalIDs())

	// The day appears twice in the uniqueness tuple — inside the key and in DataDate — and
	// both must come from one value. Derived separately they could disagree, splitting a
	// single developer-day into two rows: the exact failure the identity exists to prevent.
	for _, r := range sink.records {
		assert.True(t, strings.HasSuffix(r.ExternalID, r.DataDate.Format(dateLayout)),
			"the key's trailing day and DataDate must be the same value, not two derivations")
	}

	// Stored verbatim: nothing parsed out beyond the identity.
	assert.JSONEq(t, rowA, string(sink.records[0].Payload))
	var payload map[string]any
	require.NoError(t, json.Unmarshal(sink.records[0].Payload, &payload))
	assert.Contains(t, payload, "a_field_we_have_never_seen", "an unknown field must survive untouched")
	require.Contains(t, payload, "tokens")
	assert.Nil(t, payload["tokens"], "a null counter must stay null, not become zero")

	report := DecodePosition(cur)
	assert.Equal(t, d2, report.Covered)
}

// TestUsageWalk_WalksEveryPageOfADay is the multi-page success path, and it exists because
// every other paging test in this file ends in a failure — so none of them proves the cursor
// is sent at all. The scripted gateway pages by the cursor it receives, so a walk that
// stopped sending one gets an unknown-cursor error rather than a quiet second page.
func TestUsageWalk_WalksEveryPageOfADay(t *testing.T) {
	t.Parallel()

	day := yesterday()
	gw := newScriptedGateway().on(day,
		page(t, true, "cur-1", `{"user_id":"u_1"}`),
		page(t, true, "cur-2", `{"user_id":"u_2"}`),
		page(t, false, "", `{"user_id":"u_3"}`),
	)
	sink := &collectingSink{}
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)

	assert.Len(t, sink.records, 3, "every page's rows must be emitted, not just the first")
	assert.Equal(t, 3, gw.pageOf[day.Format(dateLayout)])

	// The cursors actually sent, in order. Asserting the sequence is what catches a walk that
	// pages by something other than the vendor's own cursor.
	var sent []string
	for _, req := range gw.seen {
		if time.Unix(req.GetStartTime(), 0).UTC().Format(dateLayout) == day.Format(dateLayout) {
			sent = append(sent, req.GetPage())
		}
	}
	assert.Equal(t, []string{"", "cur-1", "cur-2"}, sent)
}

// TestUsageWalk_CheckpointsAfterEachDay is the property that makes a first sync possible.
//
// The per-organisation request budget is small enough that ninety days spans several runs. A
// walk that only returned its cursor on completion would have every one of those runs die
// mid-window, discard the days it had finished, and start again at day one — reading the same
// early days forever and never reaching the later ones. So the checkpoint after each day is
// not an optimisation, and a test that only checked the returned cursor would not see it.
func TestUsageWalk_CheckpointsAfterEachDay(t *testing.T) {
	t.Parallel()

	last := yesterday()
	var checkpoints []string
	params := testParams(t, newScriptedGateway(), &collectingSink{},
		cursorAt(t, last.AddDate(0, 0, -4).Format(dateLayout)), Settings{WorkspaceID: testWorkspace})
	params.Checkpoint = func(_ context.Context, cur *sdk.Cursor) error {
		var pos struct {
			CoveredThrough string `json:"covered_through"`
		}
		require.NoError(t, json.Unmarshal(cur.Data, &pos))
		checkpoints = append(checkpoints, pos.CoveredThrough)
		return nil
	}
	_, err := (&FetchUsageOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	require.NotEmpty(t, checkpoints, "a walk that checkpoints nothing cannot resume, so a first sync never converges")
	assert.Equal(t, last.Format(dateLayout), checkpoints[len(checkpoints)-1])
	// Non-decreasing, not strictly increasing: the walk re-enters the trailing overlap, so the
	// days before the stored point re-report it unchanged. What must never happen is a
	// checkpoint moving BACKWARDS — that would ratchet the position down a day per run and
	// widen the window each time.
	for i := 1; i < len(checkpoints); i++ {
		assert.GreaterOrEqual(t, checkpoints[i], checkpoints[i-1], "a checkpoint must never retreat")
	}
	assert.Equal(t, checkpoints[0], last.AddDate(0, 0, -4).Format(dateLayout),
		"the overlap's re-read days must re-report the stored point, not move it back")
}

// TestUsageWalk_RefusesARowBucketedOnAnotherDay. A vendor that honours start_time but rounds
// or ignores end_time would have tomorrow's row stored under today — and tomorrow's own
// request would store it again under its real day. One developer-day becomes two records, one
// holding the other's counters, and neither the count invariant nor the duplicate guard can
// see it because both days look plausible.
func TestUsageWalk_RefusesARowBucketedOnAnotherDay(t *testing.T) {
	t.Parallel()

	day := yesterday()
	other := day.AddDate(0, 0, 1).Unix()
	gw := newScriptedGateway().on(day,
		page(t, false, "", fmt.Sprintf(`{"user_id":"u_1","bucket_start_time":%d}`, other)))
	sink := &collectingSink{}
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a day it did not happen on")
	assert.Empty(t, sink.records)
}

// TestUsageWalk_StallsLoudlyOnAPositionAheadOfTheWindow. A stored day past yesterday — a
// restored backup, a clock-skewed writer, a hand edit that typed the wrong year — plans no
// days. Returning the cursor with no error would report a healthy sync that ingests nothing,
// every night, indefinitely.
func TestUsageWalk_StallsLoudlyOnAPositionAheadOfTheWindow(t *testing.T) {
	t.Parallel()

	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, newScriptedGateway(), &collectingSink{},
			cursorAt(t, yesterday().AddDate(0, 0, 10).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.Error(t, err, "a stalled position must fail loudly; reported as success it alerts nobody")
	assert.Contains(t, err.Error(), "needs repair")
}

// TestUsageWalk_CarriesTheFourMetadataValues. The upsert replaces the payload on a re-read, so
// there is no history: a value not lifted into metadata here cannot be recovered later.
func TestUsageWalk_CarriesTheFourMetadataValues(t *testing.T) {
	t.Parallel()

	day := yesterday()
	gw := newScriptedGateway().on(day, page(t, false, "", `{"user_id":"u_1","actor":{"email":"a@example.com"}}`))
	sink := &collectingSink{}
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)
	require.Len(t, sink.records, 1)
	assert.Equal(t, map[string]string{
		"workspace_id":    testWorkspace,
		"developer_id":    "u_1",
		"developer_email": "a@example.com",
		"data_date":       day.Format(dateLayout),
	}, sink.records[0].Metadata)
}

// TestUsageWalk_StopsOnACursorCycle. An alternating cursor advances on every comparison
// against its predecessor, so a check against only the last page would burn the whole budget
// and then blame the page cap — naming the wrong cause.
func TestUsageWalk_StopsOnACursorCycle(t *testing.T) {
	t.Parallel()

	day := yesterday()
	gw := newScriptedGateway().on(day,
		page(t, true, "a", `{"user_id":"u_1"}`),
		page(t, true, "b", `{"user_id":"u_2"}`),
		page(t, true, "a", `{"user_id":"u_3"}`),
	)
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, &collectingSink{}, cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a cursor it had already given")
	assert.Less(t, gw.pageOf[day.Format(dateLayout)], 10, "it must stop at the repeat, not burn the page budget")
}

// TestOperationOptions covers the two values the design assigns to an operator, and the clamps
// that keep a tuning mistake from becoming a dead operation.
func TestOperationOptions(t *testing.T) {
	t.Parallel()

	t.Run("absent overrides take the defaults", func(t *testing.T) {
		t.Parallel()
		opts, err := parseOperationOptions(&sdk.ConnectorConfig{}, OpFetchUsage)
		require.NoError(t, err)
		assert.Equal(t, defaultOverlapDays, opts.overlapDays())
		assert.EqualValues(t, defaultPageSize, opts.pageSize())
	})
	t.Run("an unreadable override is an error, not a silent default", func(t *testing.T) {
		t.Parallel()
		_, err := parseOperationOptions(&sdk.ConnectorConfig{
			OperationOverrides: map[sdk.OperationID]json.RawMessage{OpFetchUsage: []byte(`{`)},
		}, OpFetchUsage)
		require.Error(t, err, "an operator whose deliberate setting is being ignored needs to know")
	})
	t.Run("values are honoured and clamped", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 7, operationOptions{OverlapDays: 7}.overlapDays())
		assert.Equal(t, maxOverlapDays, operationOptions{OverlapDays: 9999}.overlapDays())
		assert.EqualValues(t, 50, operationOptions{PageSize: 50}.pageSize())
		assert.EqualValues(t, maxPageSize, operationOptions{PageSize: 99999}.pageSize())
	})
}

// TestUsageWalk_ReReadIsIdempotent. The trailing overlap re-enters days already covered, so
// a second run over the same days must produce the SAME identities — the write is an update
// in place, so identical keys replace rather than duplicate. Different keys would silently
// double every metric built on them.
func TestUsageWalk_ReReadIsIdempotent(t *testing.T) {
	t.Parallel()

	day := yesterday()
	row := `{"user_id":"u_1","credits":1}`
	run := func(cur *sdk.Cursor) ([]string, *sdk.Cursor) {
		gw := newScriptedGateway().on(day, page(t, false, "", row))
		sink := &collectingSink{}
		out, err := (&FetchUsageOp{}).Execute(context.Background(),
			testParams(t, gw, sink, cur, Settings{WorkspaceID: testWorkspace}))
		require.NoError(t, err)
		return sink.externalIDs(), out
	}

	first, cur := run(cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)))
	second, _ := run(cur)
	require.NotEmpty(t, first)
	assert.Equal(t, first, second, "a re-read must produce identical identities, or every metric doubles")
}

// TestUsageWalk_NeverRequestsToday. A partially-aggregated day read as complete is a wrong
// number no later run revisits: the day is marked covered and never asked for again.
func TestUsageWalk_NeverRequestsToday(t *testing.T) {
	t.Parallel()

	gw := newScriptedGateway()
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, &collectingSink{}, cursorAt(t, yesterday().AddDate(0, 0, -2).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)

	todayStart := time.Now().UTC().Truncate(24 * time.Hour).Unix()
	require.NotEmpty(t, gw.seen)
	for _, req := range gw.seen {
		assert.Less(t, req.GetStartTime(), todayStart, "the walk must never request today")
		// Unix seconds on both bounds, and a day's end is its last second rather than the
		// next day's midnight — so two consecutive days cannot both claim one boundary second.
		assert.Equal(t, int64(24*60*60-1), req.GetEndTime()-req.GetStartTime())
		assert.Equal(t, testWorkspace, req.GetWorkspaceId())
	}
}

// TestUsageWalk_RefusesRowsItCannotKey covers the two ways a row has no usable identity.
// Both are hard failures rather than skips: this day checkpoints once it completes, so a
// skipped row is permanently lost — the position moves past it and no retry comes back.
func TestUsageWalk_RefusesRowsItCannotKey(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"a row with no developer id":                `{"credits":1}`,
		"a developer id carrying the key delimiter": `{"user_id":"u` + RecordKeyDelimiter + `1"}`,
		"a row that is not an object":               `"just a string"`,
	}
	for name, row := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gw := newScriptedGateway().on(yesterday(), page(t, false, "", row))
			sink := &collectingSink{}
			_, err := (&FetchUsageOp{}).Execute(context.Background(),
				testParams(t, gw, sink, cursorAt(t, yesterday().AddDate(0, 0, -1).Format(dateLayout)),
					Settings{WorkspaceID: testWorkspace}))
			require.Error(t, err)
			assert.Empty(t, sink.records, "nothing may be emitted for a day that failed, or the caller advances past it")
		})
	}
}

// TestUsageWalk_RefusesTwoRowsForOneDeveloperDay. The endpoint's grain is one row per
// developer per day. A second row under one identity means the grain changed, and emitting
// both would silently overwrite the first with the second, since the write is an update in
// place. So the run stops rather than picking a winner.
func TestUsageWalk_RefusesTwoRowsForOneDeveloperDay(t *testing.T) {
	t.Parallel()

	gw := newScriptedGateway().on(yesterday(),
		page(t, false, "", `{"user_id":"u_1","credits":1}`, `{"user_id":"u_1","credits":2}`))
	sink := &collectingSink{}
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, yesterday().AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "one row per developer per day")
	assert.Empty(t, sink.records)
}

// TestUsageWalk_StopsOnACursorThatDoesNotAdvance. A has_more of true beside an absent or
// unchanged cursor is an unbounded loop. The page budget alone would end it only after five
// hundred wasted requests, and the failure would name the wrong cause — so this is caught
// where it happens and says what the vendor did.
func TestUsageWalk_StopsOnACursorThatDoesNotAdvance(t *testing.T) {
	t.Parallel()

	for name, second := range map[string]*vgpb.CodexUsageResponse{
		"an absent next cursor":    page(t, true, "", `{"user_id":"u_1"}`),
		"an unchanged next cursor": page(t, true, "p1", `{"user_id":"u_2"}`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gw := newScriptedGateway().on(yesterday(), page(t, true, "p1", `{"user_id":"u_0"}`), second)
			sink := &collectingSink{}
			_, err := (&FetchUsageOp{}).Execute(context.Background(),
				testParams(t, gw, sink, cursorAt(t, yesterday().AddDate(0, 0, -1).Format(dateLayout)),
					Settings{WorkspaceID: testWorkspace}))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "a cursor it had already given")
			assert.Empty(t, sink.records)
			assert.Equal(t, 2, gw.pageOf[yesterday().Format(dateLayout)],
				"it must stop at the non-advancing page, not burn the day's page budget")
		})
	}
}

// TestUsageWalk_StopsAtThePageCap. Emitting what was read and advancing would declare the day
// covered and drop the rest permanently. So nothing is emitted, and the run FAILS rather than
// being reported as a healthy sync — a day that ended silently would never be re-read.
func TestUsageWalk_StopsAtThePageCap(t *testing.T) {
	t.Parallel()

	pages := make([]*vgpb.CodexUsageResponse, 0, maxPagesPerDay+1)
	for i := 0; i <= maxPagesPerDay; i++ {
		pages = append(pages, page(t, true, fmt.Sprintf("p%d", i+1), fmt.Sprintf(`{"user_id":"u_%d"}`, i)))
	}
	gw := newScriptedGateway().on(yesterday(), pages...)
	sink := &collectingSink{}
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, yesterday().AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to advance past unread pages")
	assert.Empty(t, sink.records)
	assert.Equal(t, maxPagesPerDay, gw.pageOf[yesterday().Format(dateLayout)],
		"the cap bounds ONE day's walk, so it is the day's own page count that must stop at it")
}

// TestUsageWalk_APartialDayLeavesThePositionWhereItWas. The position must never advance past
// a day the walk did not finish, or the gap becomes permanent.
func TestUsageWalk_APartialDayLeavesThePositionWhereItWas(t *testing.T) {
	t.Parallel()

	covered := yesterday().AddDate(0, 0, -2)
	gw := newScriptedGateway().
		on(yesterday().AddDate(0, 0, -1), page(t, false, "", `{"user_id":"u_1"}`)).
		failOn(yesterday(), errors.New("vendor exploded"))
	sink := &collectingSink{}
	cur, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, covered.Format(dateLayout)), Settings{WorkspaceID: testWorkspace}))
	require.Error(t, err)
	assert.Nil(t, cur, "a failed run returns no cursor, so the stored position is left untouched")
}

// storedPosition is the stored JSON shape, read the way a store would hand it back.
type storedPosition struct {
	CoveredThrough string   `json:"covered_through"`
	FailedDays     []string `json:"failed_days"`
}

func lastCheckpoint(t *testing.T, params *sdk.ExecutionParams) func() storedPosition {
	t.Helper()
	var last *sdk.Cursor
	params.Checkpoint = func(_ context.Context, cur *sdk.Cursor) error {
		last = cur
		return nil
	}
	return func() storedPosition {
		require.NotNil(t, last, "the walk checkpointed nothing")
		var p storedPosition
		require.NoError(t, json.Unmarshal(last.Data, &p))
		return p
	}
}

func dayFailure(day time.Time) error {
	return &sdk.SyncError{ConnectorID: connectorID, OperationID: OpFetchUsage, Kind: sdk.ErrKindVendorError,
		Cause: fmt.Errorf("HTTP 500 for %s", day.Format(dateLayout))}
}

// TestUsageWalk_WalksPastAFailedDayAndRetriesItNextRun. A day that fails on its own does not
// hold back the days after it: the walk lists it in the position, reads the later days, and
// fails the run, retryably. The next run reads the listed day again, and once it succeeds the
// list is empty. The position never moves past the failed day without listing it.
func TestUsageWalk_WalksPastAFailedDayAndRetriesItNextRun(t *testing.T) {
	t.Parallel()

	last := yesterday()
	bad := last.AddDate(0, 0, -2)
	gw := newScriptedGateway().
		failOn(bad, dayFailure(bad)).
		on(last.AddDate(0, 0, -1), page(t, false, "", `{"user_id":"u_1"}`)).
		on(last, page(t, false, "", `{"user_id":"u_2"}`))
	sink := &collectingSink{}
	params := testParams(t, gw, sink, cursorAt(t, last.AddDate(0, 0, -4).Format(dateLayout)), Settings{WorkspaceID: testWorkspace})
	stored := lastCheckpoint(t, params)
	cur, err := (&FetchUsageOp{}).Execute(context.Background(), params)
	require.Error(t, err, "a run that walked past a failed day must not report success")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.False(t, se.Terminal, "the failed day is read again, so the run is retryable")
	assert.Contains(t, err.Error(), bad.Format(dateLayout))
	assert.Nil(t, cur)
	assert.Len(t, sink.records, 2, "the days after the failed one are read")
	pos := stored()
	assert.Equal(t, last.Format(dateLayout), pos.CoveredThrough)
	assert.Equal(t, []string{bad.Format(dateLayout)}, pos.FailedDays)

	// The next run: the day now succeeds and leaves the list.
	delete(gw.errs, bad.Format(dateLayout))
	gw.on(bad, page(t, false, "", `{"user_id":"u_3"}`))
	next, err := storePosition(position{CoveredThrough: pos.CoveredThrough, WorkspaceID: testWorkspace, FailedDays: pos.FailedDays})
	require.NoError(t, err)
	sink2 := &collectingSink{}
	cur, err = (&FetchUsageOp{}).Execute(context.Background(), testParams(t, gw, sink2, next, Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)
	var after storedPosition
	require.NoError(t, json.Unmarshal(cur.Data, &after))
	assert.Empty(t, after.FailedDays)
	assert.Contains(t, sink2.externalIDs(), testWorkspace+RecordKeyDelimiter+"u_3"+RecordKeyDelimiter+bad.Format(dateLayout))
}

// TestUsageWalk_RetriesAFailedDayOlderThanTheWindow. A listed day the trailing overlap no
// longer reaches is still read.
func TestUsageWalk_RetriesAFailedDayOlderThanTheWindow(t *testing.T) {
	t.Parallel()

	old := yesterday().AddDate(0, 0, -40)
	gw := newScriptedGateway().on(old, page(t, false, "", `{"user_id":"u_1"}`))
	cur, err := storePosition(position{CoveredThrough: yesterday().AddDate(0, 0, -1).Format(dateLayout), WorkspaceID: testWorkspace,
		FailedDays: []string{old.Format(dateLayout)}})
	require.NoError(t, err)
	sink := &collectingSink{}
	out, err := (&FetchUsageOp{}).Execute(context.Background(), testParams(t, gw, sink, cur, Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)
	assert.Contains(t, sink.externalIDs(), testWorkspace+RecordKeyDelimiter+"u_1"+RecordKeyDelimiter+old.Format(dateLayout))
	var after storedPosition
	require.NoError(t, json.Unmarshal(out.Data, &after))
	assert.Empty(t, after.FailedDays)
}

// TestUsageWalk_StopsAdvancingWhenTheFailedListIsFull. The list is bounded. With it full, a
// further failed day stops the run there, as before failed days were tracked: the position
// stays on the day before it, the days after it wait, and no listed day is dropped.
func TestUsageWalk_StopsAdvancingWhenTheFailedListIsFull(t *testing.T) {
	t.Parallel()

	gw := newScriptedGateway()
	var listed []string
	for i := 0; i < maxFailedDays; i++ {
		d := yesterday().AddDate(0, 0, -80+i)
		gw.failOn(d, dayFailure(d))
		listed = append(listed, d.Format(dateLayout))
	}
	last := yesterday()
	bad := last.AddDate(0, 0, -1)
	gw.failOn(bad, dayFailure(bad)).on(last, page(t, false, "", `{"user_id":"u_1"}`))
	start, err := storePosition(position{CoveredThrough: last.AddDate(0, 0, -2).Format(dateLayout), WorkspaceID: testWorkspace, FailedDays: listed})
	require.NoError(t, err)
	sink := &collectingSink{}
	params := testParams(t, gw, sink, start, Settings{WorkspaceID: testWorkspace})
	stored := lastCheckpoint(t, params)
	_, err = (&FetchUsageOp{}).Execute(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), bad.Format(dateLayout))
	assert.Empty(t, sink.records, "the day after the unlisted failure waits")
	pos := stored()
	assert.Equal(t, last.AddDate(0, 0, -2).Format(dateLayout), pos.CoveredThrough, "the position does not pass the unlisted failure")
	assert.Equal(t, listed, pos.FailedDays, "no listed day is dropped")
}

// TestUsageWalk_RefusesAConfigWithNoWorkspace. The workspace is a path parameter, so a
// configuration without one cannot form a request. Failing at config-parse time keeps that
// from reaching the vendor, where a malformed path returns 404 and reads as a wrong
// workspace rather than a missing one.
func TestUsageWalk_RefusesAConfigWithNoWorkspace(t *testing.T) {
	t.Parallel()

	gw := newScriptedGateway()
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, &collectingSink{}, nil, Settings{}))
	require.Error(t, err)
	assert.Zero(t, gw.calls, "a configuration that cannot form a request must not reach the vendor")
}

// TestResolveWindow covers the two shapes and the overlap between them.
func TestResolveWindow(t *testing.T) {
	t.Parallel()

	latest := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	t.Run("a first run reads the lookback, ending at latest", func(t *testing.T) {
		t.Parallel()
		days := resolveWindow(time.Time{}, false, latest, 90, defaultOverlapDays)
		require.Len(t, days, 90)
		assert.Equal(t, latest.AddDate(0, 0, -89), days[0])
		assert.Equal(t, latest, days[len(days)-1])
	})
	t.Run("a later run re-enters the trailing overlap", func(t *testing.T) {
		t.Parallel()
		covered := latest.AddDate(0, 0, -1)
		days := resolveWindow(covered, true, latest, 90, defaultOverlapDays)
		// Covered through the 28th, overlap 3 → resume at the 26th, through the 29th.
		assert.Equal(t, covered.AddDate(0, 0, -2), days[0])
		assert.Equal(t, latest, days[len(days)-1])
	})
	t.Run("a position already at latest still re-reads the overlap", func(t *testing.T) {
		t.Parallel()
		days := resolveWindow(latest, true, latest, 90, defaultOverlapDays)
		require.NotEmpty(t, days, "the overlap is what picks up a row that arrived after its day closed")
		assert.Equal(t, latest, days[len(days)-1])
	})
	t.Run("a resumed run reads the WHOLE gap, however wide", func(t *testing.T) {
		t.Parallel()
		// The lookback bounds a first sync only. Applying it to a resumed run would skip
		// every day between the position and the lookback's start while the position still
		// advanced past them — a permanent, silent gap. A connector paused longer than a
		// shortened lookback is all it takes.
		covered := latest.AddDate(0, 0, -30)
		days := resolveWindow(covered, true, latest, 7, defaultOverlapDays)
		assert.Equal(t, covered.AddDate(0, 0, 1-defaultOverlapDays), days[0],
			"a resumed run must start at its covered point, not at the first-run lookback")
		assert.Equal(t, latest, days[len(days)-1])
		assert.Len(t, days, 33, "every day between the position and yesterday must be read")
	})
	t.Run("a position ahead of latest plans nothing, which the walk must not call success", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, resolveWindow(latest.AddDate(0, 0, 5), true, latest, 90, defaultOverlapDays))
	})
}

// TestUsageWalk_ARePointedWorkspaceBackfillsFromScratch is the reconnect case neither sibling
// has. Their credential implies the tenant, so a reconnect cannot re-point them; a Codex
// administrator types a different workspace and the sync-state row survives it.
//
// Without the workspace in the position, the next run reads a covered day belonging to the OLD
// workspace, treats the new one as already backfilled, and reads only the trailing overlap —
// permanently omitting the rest of its lookback, with no error anywhere.
func TestUsageWalk_ARePointedWorkspaceBackfillsFromScratch(t *testing.T) {
	t.Parallel()

	last := yesterday()
	// A position covering right up to yesterday, written against a different workspace.
	cur, err := storePosition(position{CoveredThrough: last.Format(dateLayout), WorkspaceID: "5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a81"})
	require.NoError(t, err)

	gw := newScriptedGateway()
	_, err = (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, &collectingSink{}, cur, Settings{WorkspaceID: "6f7a8b9c-0d1e-4f2a-9b3c-4d5e6f7a8b92", LookbackDays: 10}))
	require.NoError(t, err)

	assert.Len(t, gw.seen, 10,
		"a re-pointed workspace must read its whole lookback, not the previous workspace's overlap")
}

// TestUsageWalk_DiscardsAPositionWithNoWorkspace. Every run stamps the workspace, so a
// position without one belongs to no workspace and is discarded like one for another
// workspace: the walk reads its whole lookback.
func TestUsageWalk_DiscardsAPositionWithNoWorkspace(t *testing.T) {
	t.Parallel()

	last := yesterday()
	cur, err := storePosition(position{CoveredThrough: last.AddDate(0, 0, -1).Format(dateLayout)})
	require.NoError(t, err)

	gw := newScriptedGateway()
	out, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, &collectingSink{}, cur, Settings{WorkspaceID: testWorkspace, LookbackDays: 10}))
	require.NoError(t, err)
	assert.Len(t, gw.seen, 10, "an unstamped position must be discarded, so the walk reads its lookback")

	var stored position
	require.NoError(t, json.Unmarshal(out.Data, &stored))
	assert.Equal(t, testWorkspace, stored.WorkspaceID)
}

// TestPositionRoundTrip pins the stored shape's JSON in BOTH directions. A tag rename would
// otherwise leave every organisation reading as never-synced while the connector kept
// working — silent, and only visible as a re-read of the whole lookback.
func TestPositionRoundTrip(t *testing.T) {
	t.Parallel()

	cur, err := storePosition(position{CoveredThrough: "2026-08-29"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"covered_through":"2026-08-29"}`, string(cur.Data),
		"the serialised field name is a stored contract, not an implementation detail")

	report := DecodePosition(cur)
	assert.True(t, report.Readable)
	assert.True(t, report.Present)
	assert.Equal(t, time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC), report.Covered)

	// The suspect input an operator actually reaches for. Failing here would refuse the one
	// case the report exists to explain.
	unreadable := DecodePosition(&sdk.Cursor{Data: []byte(`{"covered_through":"not-a-date"}`)})
	assert.False(t, unreadable.Readable, "a position that does not decode must say so, not error")
	assert.False(t, unreadable.Present)
	assert.Contains(t, unreadable.Unreadable, "covered_through",
		"the report must say WHY it could not read the position, or an operator rewrites a row blind")

	for name, cur := range map[string]*sdk.Cursor{
		"an absent cursor":             nil,
		"an empty cursor":              {},
		"a cursor with no covered day": {Data: []byte(`{}`)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			report := DecodePosition(cur)
			assert.True(t, report.Readable, "an absent position is readable; it simply holds nothing")
			assert.False(t, report.Present, "an absent position must read as never-synced, not as an error")
		})
	}
}

// TestUsageWalk_ARefusedListedDayDoesNotBlockTheWindow. A listed day the vendor refuses on
// retry (a 400) stays listed and the walk goes on with the window; a listed day older than
// the lookback is dropped without a request.
func TestUsageWalk_ARefusedListedDayDoesNotBlockTheWindow(t *testing.T) {
	t.Parallel()

	last := yesterday()
	refused := last.AddDate(0, 0, -20)
	tooOld := last.AddDate(0, 0, -(DefaultLookbackDays + 5))
	gw := newScriptedGateway().
		failOn(refused, &sdk.SyncError{ConnectorID: connectorID, OperationID: OpFetchUsage, Kind: sdk.ErrKindVendorError,
			HTTPStatus: 400, Cause: fmt.Errorf("HTTP 400 for %s", refused.Format(dateLayout))}).
		on(last, page(t, false, "", `{"user_id":"u_1"}`)).
		on(tooOld, page(t, false, "", `{"user_id":"u_old"}`))
	start, err := storePosition(position{CoveredThrough: last.AddDate(0, 0, -1).Format(dateLayout), WorkspaceID: testWorkspace,
		FailedDays: []string{tooOld.Format(dateLayout), refused.Format(dateLayout)}})
	require.NoError(t, err)
	sink := &collectingSink{}
	params := testParams(t, gw, sink, start, Settings{WorkspaceID: testWorkspace})
	stored := lastCheckpoint(t, params)
	_, err = (&FetchUsageOp{}).Execute(context.Background(), params)
	require.Error(t, err, "a listed day that keeps failing keeps the run failing, retryably")
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.False(t, se.Terminal)
	assert.Contains(t, sink.externalIDs(), testWorkspace+RecordKeyDelimiter+"u_1"+RecordKeyDelimiter+last.Format(dateLayout), "the window is read")
	assert.NotContains(t, sink.externalIDs(), testWorkspace+RecordKeyDelimiter+"u_old"+RecordKeyDelimiter+tooOld.Format(dateLayout), "a day past the lookback is not asked for")
	pos := stored()
	assert.Equal(t, []string{refused.Format(dateLayout)}, pos.FailedDays, "the refused day stays listed, the old one is dropped")
	assert.Equal(t, last.Format(dateLayout), pos.CoveredThrough)
}

// TestUsageWalk_RetriesListedDaysBeforeTheWindow. Listed days are read before the window, so
// a window that runs out of budget does not keep them waiting: here the window's first day is
// rate limited, and the listed day has already been read and left the list.
func TestUsageWalk_RetriesListedDaysBeforeTheWindow(t *testing.T) {
	t.Parallel()

	last := yesterday()
	listed := last.AddDate(0, 0, -30)
	gw := newScriptedGateway().on(listed, page(t, false, "", `{"user_id":"u_1"}`))
	for d := last.AddDate(0, 0, -10); !d.After(last); d = d.AddDate(0, 0, 1) {
		gw.failOn(d, &sdk.SyncError{ConnectorID: connectorID, OperationID: OpFetchUsage, Kind: sdk.ErrKindRateLimit,
			Retryable: true, Cause: fmt.Errorf("HTTP 429 for %s", d.Format(dateLayout))})
	}
	start, err := storePosition(position{CoveredThrough: last.AddDate(0, 0, -1).Format(dateLayout), WorkspaceID: testWorkspace,
		FailedDays: []string{listed.Format(dateLayout)}})
	require.NoError(t, err)
	sink := &collectingSink{}
	params := testParams(t, gw, sink, start, Settings{WorkspaceID: testWorkspace})
	stored := lastCheckpoint(t, params)
	_, err = (&FetchUsageOp{}).Execute(context.Background(), params)
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, sdk.ErrKindRateLimit, se.Kind, "the window stops on the rate limit")
	assert.Contains(t, sink.externalIDs(), testWorkspace+RecordKeyDelimiter+"u_1"+RecordKeyDelimiter+listed.Format(dateLayout), "the listed day was read first")
	assert.Empty(t, stored().FailedDays, "and left the list")
}
