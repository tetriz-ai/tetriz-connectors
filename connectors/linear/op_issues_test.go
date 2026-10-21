// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

const (
	teamsOnePage = `{"data":{"teams":{"nodes":[{"id":"team-1"}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`
	teamsTwoTeam = `{"data":{"teams":{"nodes":[{"id":"team-1"},{"id":"team-2"}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`
)

// respondIssues builds a respond func for FetchIssues paging over team-1.
// Page 1 returns two issues with hasNextPage=true (endCursor "c1"); page 2
// returns one issue with hasNextPage=false.
func respondIssuesTwoPages() func(string, map[string]any, any) error {
	return func(query string, vars map[string]any, dest any) error {
		switch {
		case strings.Contains(query, "DiscoverTeams"):
			return unmarshalInto(dest, teamsOnePage)
		case strings.Contains(query, "FetchIssues"):
			if after, ok := vars["after"].(string); ok && after == "c1" {
				return unmarshalInto(dest, `{"data":{"issues":{"nodes":[
					{"id":"u3","identifier":"ENG-3","updatedAt":"2026-06-10T03:00:00.000Z"}
				],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
			}
			return unmarshalInto(dest, `{"data":{"issues":{"nodes":[
				{"id":"u1","identifier":"ENG-1","updatedAt":"2026-06-10T01:00:00.000Z"},
				{"id":"u2","identifier":"ENG-2","updatedAt":"2026-06-10T02:00:00.000Z"}
			],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}}}`)
		default:
			return unmarshalInto(dest, `{"data":{}}`)
		}
	}
}

func TestFetchIssuesOp_HappyPath_TwoPages(t *testing.T) {
	gw := &fakeGateway{respond: respondIssuesTwoPages()}
	sink := testkit.NewChannelSink()

	op := &FetchIssuesOp{}
	cursor, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config:        testConfig("org-1"),
		VendorGateway: gw,
		Sink:          sink,
	})
	require.NoError(t, err)

	// One Emit per issue across both pages.
	records := sink.Records()
	require.Len(t, records, 3)
	gotKeys := []string{records[0].ExternalID, records[1].ExternalID, records[2].ExternalID}
	assert.ElementsMatch(t, []string{"u1", "u2", "u3"}, gotKeys)

	// Page 2 was requested with the page-1 endCursor.
	issueCalls := gw.callsFor("FetchIssues")
	require.Len(t, issueCalls, 2)
	assert.Equal(t, "c1", issueCalls[1].Vars["after"])

	// Cursor advanced to the high-water updatedAt for team-1.
	states := loadTeamCursors(context.Background(), cursor)
	require.Contains(t, states, "team-1")
	assert.Equal(t, time.Date(2026, 6, 10, 3, 0, 0, 0, time.UTC), states["team-1"].Since.UTC())
}

// An issue that moves team gets a new identifier (ENG-42 becomes OPS-7) but keeps its
// id, so both runs emit it under one ExternalID instead of forking it into two records.
func TestFetchIssuesOp_MovedIssueKeepsItsExternalID(t *testing.T) {
	run := func(identifier string) *sdk.RawRecord {
		t.Helper()
		gw := &fakeGateway{respond: func(query string, _ map[string]any, dest any) error {
			if strings.Contains(query, "DiscoverTeams") {
				return unmarshalInto(dest, teamsOnePage)
			}
			return unmarshalInto(dest, `{"data":{"issues":{"nodes":[
				{"id":"3f0c9a52-issue","identifier":"`+identifier+`","updatedAt":"2026-06-10T01:00:00.000Z"}
			],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
		}}
		sink := testkit.NewChannelSink()
		_, err := (&FetchIssuesOp{}).Execute(context.Background(), &sdk.ExecutionParams{
			Config: testConfig("org-1"), VendorGateway: gw, Sink: sink,
		})
		require.NoError(t, err)
		require.Len(t, sink.Records(), 1)
		return sink.Records()[0]
	}

	before, after := run("ENG-42"), run("OPS-7")
	assert.Equal(t, "3f0c9a52-issue", before.ExternalID)
	assert.Equal(t, before.ExternalID, after.ExternalID)
	assert.Contains(t, string(after.Payload), `"identifier":"OPS-7"`, "the payload still carries the identifier")
}

func TestFetchIssuesOp_Backfill_UsesSentinelSince(t *testing.T) {
	gw := &fakeGateway{respond: respondIssuesTwoPages()}
	op := &FetchIssuesOp{}
	_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config:        testConfig("org-1"),
		VendorGateway: gw,
		Sink:          testkit.NewChannelSink(),
	})
	require.NoError(t, err)

	issueCalls := gw.callsFor("FetchIssues")
	require.NotEmpty(t, issueCalls)
	// First run (nil cursor) backfills from the far-past sentinel.
	assert.Equal(t, linearBackfillSentinel, issueCalls[0].Vars["since"])
}

func TestFetchIssuesOp_Incremental_UsesHighWaterMinusOverlap(t *testing.T) {
	gw := &fakeGateway{respond: respondIssuesTwoPages()}
	op := &FetchIssuesOp{}

	highWater := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	priorCursor := buildTeamCursor(map[string]teamCursorState{"team-1": {Since: highWater}})

	_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config:        testConfig("org-1"),
		VendorGateway: gw,
		Sink:          testkit.NewChannelSink(),
		Cursor:        priorCursor,
	})
	require.NoError(t, err)

	issueCalls := gw.callsFor("FetchIssues")
	require.NotEmpty(t, issueCalls)
	want := highWater.Add(-linearCursorOverlap).Format(linearTimeFormat)
	assert.Equal(t, want, issueCalls[0].Vars["since"])
	assert.NotEqual(t, linearBackfillSentinel, issueCalls[0].Vars["since"])
}

func TestFetchIssuesOp_EmptyTeam_NoEmitsCursorPresent(t *testing.T) {
	gw := &fakeGateway{respond: func(query string, _ map[string]any, dest any) error {
		if strings.Contains(query, "DiscoverTeams") {
			return unmarshalInto(dest, teamsOnePage)
		}
		return unmarshalInto(dest, `{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
	}}
	sink := testkit.NewChannelSink()

	op := &FetchIssuesOp{}
	cursor, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config:        testConfig("org-1"),
		VendorGateway: gw,
		Sink:          sink,
	})
	require.NoError(t, err)
	assert.Empty(t, sink.Records())

	states := loadTeamCursors(context.Background(), cursor)
	require.Contains(t, states, "team-1")
	assert.True(t, states["team-1"].Since.IsZero())
}

// A page that claims more data (hasNextPage) but gives no endCursor must fail
// the team loudly, not be treated as a complete walk — otherwise the cursor
// advances past unfetched issues and silently drops them.
func TestFetchIssuesOp_DegradedPage_ErrorsNotSilentComplete(t *testing.T) {
	gw := &fakeGateway{respond: func(query string, _ map[string]any, dest any) error {
		if strings.Contains(query, "DiscoverTeams") {
			return unmarshalInto(dest, teamsOnePage)
		}
		return unmarshalInto(dest, `{"data":{"issues":{"nodes":[
			{"id":"u1","identifier":"ENG-1","updatedAt":"2026-06-10T01:00:00.000Z"}
		],"pageInfo":{"hasNextPage":true,"endCursor":""}}}}`)
	}}

	op := &FetchIssuesOp{}
	_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config:        testConfig("org-1"),
		VendorGateway: gw,
		Sink:          testkit.NewChannelSink(),
	})
	require.Error(t, err)
}

func TestFetchIssuesOp_PerTeamCursorIsolation(t *testing.T) {
	gw := &fakeGateway{respond: func(query string, vars map[string]any, dest any) error {
		switch {
		case strings.Contains(query, "DiscoverTeams"):
			return unmarshalInto(dest, teamsTwoTeam)
		case strings.Contains(query, "FetchIssues"):
			switch vars["teamId"] {
			case "team-1":
				return unmarshalInto(dest, `{"data":{"issues":{"nodes":[
					{"id":"u1","identifier":"ENG-1","updatedAt":"2026-06-10T01:00:00.000Z"}
				],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
			case "team-2":
				return unmarshalInto(dest, `{"data":{"issues":{"nodes":[
					{"id":"u9","identifier":"OPS-9","updatedAt":"2026-06-11T05:00:00.000Z"}
				],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
			}
		}
		return unmarshalInto(dest, `{"data":{}}`)
	}}

	op := &FetchIssuesOp{}
	cursor, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config:        testConfig("org-1"),
		VendorGateway: gw,
		Sink:          testkit.NewChannelSink(),
	})
	require.NoError(t, err)

	states := loadTeamCursors(context.Background(), cursor)
	require.Contains(t, states, "team-1")
	require.Contains(t, states, "team-2")
	assert.Equal(t, time.Date(2026, 6, 10, 1, 0, 0, 0, time.UTC), states["team-1"].Since.UTC())
	assert.Equal(t, time.Date(2026, 6, 11, 5, 0, 0, 0, time.UTC), states["team-2"].Since.UTC())
}
