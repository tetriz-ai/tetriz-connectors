// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func TestFetchCyclesOp_EmitsPerCycle_SprintKeyIsCycleID(t *testing.T) {
	gw := &fakeGateway{respond: func(query string, vars map[string]any, dest any) error {
		switch {
		case strings.Contains(query, "DiscoverTeams"):
			return unmarshalInto(dest, teamsTwoTeam)
		case strings.Contains(query, "FetchCycles"):
			switch vars["teamId"] {
			case "team-1":
				return unmarshalInto(dest, `{"data":{"cycles":{"nodes":[
					{"id":"cyc-1","number":12,"updatedAt":"2026-06-10T01:00:00.000Z","team":{"id":"team-1","key":"ENG"}}
				],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
			case "team-2":
				return unmarshalInto(dest, `{"data":{"cycles":{"nodes":[
					{"id":"cyc-2","number":3,"updatedAt":"2026-06-11T02:00:00.000Z","team":{"id":"team-2","key":"OPS"}}
				],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
			}
		}
		return unmarshalInto(dest, `{"data":{}}`)
	}}
	sink := testkit.NewChannelSink()

	op := &FetchCyclesOp{}
	_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config:        testConfig("org-1"),
		VendorGateway: gw,
		Sink:          sink,
	})
	require.NoError(t, err)

	// Operation registers as the sprints op, the id sprint records use.
	assert.Equal(t, sdk.OperationID("fetch_sprints"), op.OperationID())

	records := sink.Records()
	require.Len(t, records, 2)
	for _, r := range records {
		assert.Equal(t, "fetch_sprints", r.OperationID)
	}
	// ExternalID (sprint_key) is the cycle UUID, not the per-team number.
	assert.ElementsMatch(t, []string{"cyc-1", "cyc-2"}, []string{records[0].ExternalID, records[1].ExternalID})
}

func TestFetchCyclesOp_TeamDiscovery_OneQueryPerTeam(t *testing.T) {
	gw := &fakeGateway{respond: func(query string, _ map[string]any, dest any) error {
		switch {
		case strings.Contains(query, "DiscoverTeams"):
			return unmarshalInto(dest, teamsTwoTeam)
		case strings.Contains(query, "FetchCycles"):
			return unmarshalInto(dest, `{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
		}
		return unmarshalInto(dest, `{"data":{}}`)
	}}

	op := &FetchCyclesOp{}
	_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config:        testConfig("org-1"),
		VendorGateway: gw,
		Sink:          testkit.NewChannelSink(),
	})
	require.NoError(t, err)

	// Two teams discovered → cycles query issued once per team.
	cycleCalls := gw.callsFor("FetchCycles")
	require.Len(t, cycleCalls, 2)
	assert.ElementsMatch(t, []string{"team-1", "team-2"},
		[]string{cycleCalls[0].Vars["teamId"].(string), cycleCalls[1].Vars["teamId"].(string)})
}
