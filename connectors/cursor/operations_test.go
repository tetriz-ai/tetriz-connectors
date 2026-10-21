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

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	vgcursorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// fakeGateway answers a scripted sequence of pages and records what was asked for, so
// a test can assert on the window a run requested as well as on what it stored.
type fakeGateway struct {
	pages    [][]json.RawMessage
	calls    []*vgcursorpb.CursorFilteredUsageEventsRequest
	err      error
	callsFor int
}

func (f *fakeGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	r, ok := req.(*vgcursorpb.CursorFilteredUsageEventsRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected request %T", req)
	}
	f.calls = append(f.calls, r)
	if f.err != nil {
		return nil, f.err
	}
	idx := f.callsFor
	f.callsFor++
	if idx >= len(f.pages) {
		return &vgcursorpb.CursorFilteredUsageEventsResponse{}, nil
	}
	page := make([][]byte, 0, len(f.pages[idx]))
	for _, e := range f.pages[idx] {
		page = append(page, e)
	}
	return &vgcursorpb.CursorFilteredUsageEventsResponse{UsageEvents: page}, nil
}

func (f *fakeGateway) ForGroup(string) sdk.VendorGateway { return f }
func (f *fakeGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return f
}

// Do and DoJSON complete the interface. The operation speaks proto through Execute and
// never reaches for raw HTTP, which is the boundary these two returning an error pins.
func (f *fakeGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, errors.New("the operation must not make raw HTTP calls")
}

func (f *fakeGateway) DoJSON(context.Context, string, string, any, any) error {
	return errors.New("the operation must not make raw HTTP calls")
}

// event builds one vendor event in the shape captured live: the timestamp is epoch
// milliseconds encoded as a string, which the vendor's own documentation gets wrong.
func event(conversationID, email string, at time.Time, extra string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"timestamp":"%d","conversationId":%q,"userEmail":%q,"chargedCents":1.5%s}`,
		at.UnixMilli(), conversationID, email, extra))
}

func newParams(t *testing.T, gw sdk.VendorGateway, sink sdk.DataSink, cur *sdk.Cursor) *sdk.ExecutionParams {
	t.Helper()
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    connectorID,
			OrganizationID: "org-1",
			Credentials:    json.RawMessage(`{"api_key":"key_test"}`),
			Settings:       json.RawMessage(`{}`),
		},
		Cursor:        cur,
		Sink:          sink,
		VendorGateway: gw,
	}
}

// TestExecute_StoresOneRecordPerEvent covers the landing contract: the payload is
// verbatim, the identity is the conversation plus the call's timestamp, the business
// date is the billed day in UTC, and the two routing fields reach metadata.
func TestExecute_StoresOneRecordPerEvent(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 9, 15, 0, 0, time.UTC)
	gw := &fakeGateway{pages: [][]json.RawMessage{{
		event("conv-a", "dev@example.com", at, ""),
	}}}
	sink := testkit.NewChannelSink()

	// A present position: the steady-state window.
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	next, err := (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
	require.NoError(t, err)
	require.NotNil(t, next)

	records := sink.Records()
	require.Len(t, records, 1)
	rec := records[0]

	assert.Equal(t, "cursor", rec.ConnectorID)
	assert.Equal(t, "fetch_usage_events", rec.OperationID)
	assert.Equal(t, "conv-a#"+fmt.Sprint(at.UnixMilli()), rec.ExternalID,
		"the vendor issues no per-event identifier, so one is composed from the conversation and the call's timestamp")

	// The business date must be the billed day, explicitly in UTC. Left unset it falls
	// back to the fetch day, which for a ninety-day first sync would file every
	// historical event under today.
	assert.Equal(t, "2026-08-18", rec.DataDate.Format("2006-01-02"))
	assert.Equal(t, time.UTC, rec.DataDate.Location())

	assert.Equal(t, "conv-a", rec.Metadata["conversation_id"])
	assert.Equal(t, "dev@example.com", rec.Metadata["user_email"])

	// Verbatim: a field the vendor adds later has to survive ingestion, and several
	// existing fields punish eager parsing.
	assert.JSONEq(t, string(event("conv-a", "dev@example.com", at, "")), string(rec.Payload))
}

// TestExecute_QuirkyFieldsStoreWithoutLoss pins the shapes the research actually found,
// each of which breaks a parser that assumes the documentation.
func TestExecute_QuirkyFieldsStoreWithoutLoss(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	// serviceAccountId is the literal text "null", not JSON null; kind is a
	// human-facing phrase rather than a stable code; the token breakdown is absent.
	raw := json.RawMessage(fmt.Sprintf(
		`{"timestamp":"%d","conversationId":"conv-q","userEmail":"svc@example.com",`+
			`"serviceAccountId":"null","kind":"Example plan","chargedCents":2.5}`, at.UnixMilli()))

	gw := &fakeGateway{pages: [][]json.RawMessage{{raw}}}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
	require.NoError(t, err, "none of these shapes is an ingestion failure")

	records := sink.Records()
	require.Len(t, records, 1)
	assert.JSONEq(t, string(raw), string(records[0].Payload))
}

// TestExecute_DuplicateIdentityFailsTheRun is the check storage cannot do. The write is
// an update in place, so the second of a colliding pair would silently replace the
// first and the upsert would report success — nothing below the operation can notice.
func TestExecute_DuplicateIdentityFailsTheRun(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 11, 0, 0, 0, time.UTC)
	dup := event("conv-same", "dev@example.com", at, "")
	gw := &fakeGateway{pages: [][]json.RawMessage{{dup, dup}}}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
	require.Error(t, err, "a collision must fail the run rather than lose an event quietly")
	assert.Contains(t, err.Error(), "share the identity")
}

// TestExecute_UnroutableEventIsSkippedAndTheRestSurvives pins the cost of an event
// this connector cannot read: that event, and nothing else.
//
// The pairing is what matters. Refusing to EMIT is forced — the consumer treats an
// empty identity as a permanent contract violation — but refusing to CONTINUE is a
// choice, and the expensive one: it costs every later event in the page, every later
// page, and every later day, on every run, for as long as the vendor keeps returning
// the event. Every other connector's fetch loop skips and carries on; this one now
// does too.
func TestExecute_UnroutableEventIsSkippedAndTheRestSurvives(t *testing.T) {
	t.Parallel()

	// Readable, and sits AFTER the unreadable one in the same page — so a walk that
	// stopped at the first bad event would lose it.
	good := json.RawMessage(`{"timestamp":"1787184000000","conversationId":"conv-good","userEmail":"dev@example.com"}`)

	tests := []struct {
		name string
		bad  json.RawMessage
	}{
		{
			name: "no conversation identifier",
			bad:  json.RawMessage(`{"timestamp":"1787184000000","userEmail":"dev@example.com"}`),
		},
		{
			name: "timestamp is not a number",
			bad:  json.RawMessage(`{"timestamp":"not-a-number","conversationId":"conv-x"}`),
		},
		{
			name: "event is not an object at all",
			bad:  json.RawMessage(`"a bare string"`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := &fakeGateway{pages: [][]json.RawMessage{{tt.bad, good}}}
			sink := testkit.NewChannelSink()
			cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
			require.NoError(t, err)

			next, err := (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
			require.NoError(t, err)
			require.NotNil(t, next)

			records := sink.Records()
			require.Len(t, records, 1, "the readable event beside it must still be stored")
			assert.Equal(t, "conv-good#1787184000000", records[0].ExternalID,
				"and it is the good one that survived, not the bad one under some other key")

			pos, present, err := parseUsagePosition(next)
			require.NoError(t, err)
			require.True(t, present)
			// One unreadable event must not freeze the floor — that costs every later day, not one event.
			assert.Equal(t, startOfUTCDay(time.Now()).Format(dateLayout), pos.Floor.Format(dateLayout),
				"the run covered its whole window and the position moved past it")
		})
	}
}

// TestExecute_FirstRunChunksAndCoversTheWholeWindow asserts the request pattern rather
// than only the outcome: a first sync is sliced, and the slices together cover the
// lookback without ever reaching into the unsettled current day.
func TestExecute_FirstRunChunksAndCoversTheWholeWindow(t *testing.T) {
	t.Parallel()

	gw := &fakeGateway{}
	sink := testkit.NewChannelSink()

	// No cursor: a first run.
	next, err := (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, nil))
	require.NoError(t, err)
	require.NotNil(t, next)

	require.Greater(t, len(gw.calls), 1, "a ninety-day first sync must not be one request")

	// Every request is a whole-day span inside the lookback, and none reaches today.
	startOfToday := startOfUTCDay(time.Now())
	for _, call := range gw.calls {
		assert.Less(t, call.GetStartDate(), call.GetEndDate())
		assert.Less(t, call.GetEndDate(), startOfToday.UnixMilli(),
			"no request may reach into the current day, which the vendor's endpoints disagree about")
		assert.Equal(t, int32(usagePageSize), call.GetPageSize(),
			"the vendor maximum, because this endpoint carries a tighter budget than the rest")
	}

	pos, present, err := parseUsagePosition(next)
	require.NoError(t, err)
	require.True(t, present)
	assert.False(t, pos.Floor.IsZero(), "a clean first run stores a floor")
}

// TestExecute_PaginatesUntilAShortPage covers the walk. The response's pagination
// fields are named differently than the vendor documents, so page length is the signal
// that does not depend on reading them correctly.
func TestExecute_PaginatesUntilAShortPage(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	full := make([]json.RawMessage, usagePageSize)
	for i := range full {
		full[i] = event(fmt.Sprintf("conv-%d", i), "dev@example.com", at, "")
	}
	tail := []json.RawMessage{event("conv-tail", "dev@example.com", at, "")}

	gw := &fakeGateway{pages: [][]json.RawMessage{full, tail}}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
	require.NoError(t, err)

	assert.Len(t, sink.Records(), usagePageSize+1, "every page's events must land")
	require.Len(t, gw.calls, 2, "a short page ends the walk")
	assert.Equal(t, int32(1), gw.calls[0].GetPage(), "the vendor's pages are 1-based")
	assert.Equal(t, int32(2), gw.calls[1].GetPage())
}

// TestExecute_VendorFailureLeavesThePositionAlone is the mechanism the trailing overlap
// relies on: a partial run must not record progress it did not make.
func TestExecute_VendorFailureLeavesThePositionAlone(t *testing.T) {
	t.Parallel()

	gw := &fakeGateway{err: errors.New("503 service unavailable")}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	next, err := (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
	require.Error(t, err)
	assert.Nil(t, next, "returning a position here would record coverage the run never achieved")
}

// TestExecute_UnreadablePositionRestartsFromTheLookback prefers a safe re-read over a
// stall. The write is an update in place, so re-reading costs nothing but rows already
// held — where refusing to run leaves the organisation stuck until a person intervenes.
func TestExecute_UnreadablePositionRestartsFromTheLookback(t *testing.T) {
	t.Parallel()

	gw := &fakeGateway{}
	sink := testkit.NewChannelSink()
	broken := &sdk.Cursor{Data: json.RawMessage(`{not json`)}

	next, err := (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, broken))
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Greater(t, len(gw.calls), 1, "an unreadable position falls back to the chunked first-run window")
}

// TestExecute_DuplicateAcrossPagesFailsTheRun is why the identity set is scoped to the
// RUN and never to a page or a chunk. A first sync flushes progressively, so a
// per-page set would let a colliding pair through at a page boundary — and the second
// would silently replace the first, with storage reporting success.
func TestExecute_DuplicateAcrossPagesFailsTheRun(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 13, 0, 0, 0, time.UTC)
	dup := event("conv-split", "dev@example.com", at, "")

	// A full first page forces the walk to ask for a second, where the twin waits.
	full := make([]json.RawMessage, usagePageSize)
	for i := range full {
		full[i] = event(fmt.Sprintf("conv-p1-%d", i), "dev@example.com", at, "")
	}
	full[0] = dup

	gw := &fakeGateway{pages: [][]json.RawMessage{full, {dup}}}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
	require.Error(t, err, "a collision spanning two pages must still fail the run")
	assert.Contains(t, err.Error(), "share the identity")
}

// TestExecute_SameConversationDifferentCallsBothLand is the other side of the identity.
// A conversation spans many billed calls — the research found one covering seventy-nine
// hours — so an identity built from the conversation alone would collapse every call in
// it into one record and report success while losing all but the last.
func TestExecute_SameConversationDifferentCallsBothLand(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, 8, 18, 14, 0, 0, 0, time.UTC)
	second := first.Add(90 * time.Second)
	gw := &fakeGateway{pages: [][]json.RawMessage{{
		event("conv-long", "dev@example.com", first, ""),
		event("conv-long", "dev@example.com", second, ""),
	}}}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
	require.NoError(t, err, "two calls in one conversation are two events, not a collision")

	records := sink.Records()
	require.Len(t, records, 2)
	assert.NotEqual(t, records[0].ExternalID, records[1].ExternalID,
		"the call's own timestamp is what separates them")
}

// TestExecute_SkipCounterCollapsesIndividualTenants guards the counter's LABEL by driving
// the real skip, not the helper that produces the label.
//
// An individual tenant is one organisation each, so a raw ID here mints a series per
// tenant and a series already ingested cannot be recovered by a recording rule. The
// sibling counter in the schedule source was covered and this one was not, which is the
// more useful lesson: instrumenting two emit sites and testing one leaves the untested
// one free to drift.
func TestExecute_SkipCounterCollapsesIndividualTenants(t *testing.T) {
	const individualOrg = "org-individual-cursor"
	restore := tenantlabel.SetLiveIndividualSetForTest(map[string]struct{}{individualOrg: {}})
	defer restore()

	collapsed := usageEventsSkipped.WithLabelValues(tenantlabel.MetricLabelIndividual)
	raw := usageEventsSkipped.WithLabelValues(individualOrg)
	collapsedBefore, rawBefore := promtestutil.ToFloat64(collapsed), promtestutil.ToFloat64(raw)

	// An event with no conversation identifier: unroutable, so the run skips it.
	// A readable event beside it, so the chunk is a blemished page rather than the
	// vendor's whole shape moving — the latter fails the run by design.
	gw := &fakeGateway{pages: [][]json.RawMessage{{
		json.RawMessage(`{"timestamp":"1787184000000","userEmail":"dev@example.com"}`),
		json.RawMessage(`{"timestamp":"1787184000000","conversationId":"conv-ok","userEmail":"dev@example.com"}`),
	}}}
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	params := newParams(t, gw, testkit.NewChannelSink(), cur)
	params.Config.OrganizationID = individualOrg

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, collapsedBefore+1, promtestutil.ToFloat64(collapsed),
		"an individual tenant's skip must land on the collapsed label")
	assert.Equal(t, rawBefore, promtestutil.ToFloat64(raw),
		"and never on a series keyed by its own organisation ID")
}
