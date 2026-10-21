// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func runLinearIssues(t *testing.T, forbidden func(team string) bool) (*sdk.Cursor, error) {
	t.Helper()
	gw := &fakeGateway{respond: func(query string, vars map[string]any, dest any) error {
		switch {
		case strings.Contains(query, "DiscoverTeams"):
			return unmarshalInto(dest, teamsTwoTeam)
		case strings.Contains(query, "FetchIssues"):
			if team, _ := vars["teamId"].(string); forbidden(team) {
				return &sdk.SyncError{ConnectorID: "linear", Kind: sdk.ErrKindAuth, HTTPStatus: 403, Terminal: true}
			}
			return unmarshalInto(dest, `{"data":{"issues":{"nodes":[{"id":"u1","identifier":"ENG-1","updatedAt":"2026-06-10T01:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
		default:
			return unmarshalInto(dest, `{"data":{}}`)
		}
	}}
	return (&FetchIssuesOp{}).Execute(context.Background(), &sdk.ExecutionParams{Config: testConfig("org-1"), VendorGateway: gw, Sink: testkit.NewChannelSink()})
}

// One team that answers 403 while the other syncs is that team's failure: it
// is skipped and the run completes. Every team answering 403 pauses.
func TestLinearOneForbiddenTeamIsSkipped(t *testing.T) {
	cursor, err := runLinearIssues(t, func(team string) bool { return team == "team-2" })
	require.NoError(t, err, "one forbidden team does not fail or pause the run")
	states := loadTeamCursors(context.Background(), cursor)
	assert.False(t, states["team-1"].Since.IsZero(), "team-1's cursor advances")
	assert.True(t, states["team-2"].Since.IsZero(), "the forbidden team keeps its (empty) cursor")

	_, err = runLinearIssues(t, func(string) bool { return true })
	var se *sdk.SyncError
	require.True(t, errors.As(err, &se), "err=%v", err)
	assert.Equal(t, sdk.ErrKindAuth, se.Kind)
	assert.True(t, se.Terminal, "every team forbidden pauses the connector")
}
