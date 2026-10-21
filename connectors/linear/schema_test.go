// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// Vendor-shaped GraphQL pages. Each holds one fully populated node, one node
// with every nullable field null, and fields the connector does not select,
// so the drift guard sees both ends of what Linear returns.
const (
	schemaTeamsPage = `{"data":{"teams":{"nodes":[
		{"id":"team-1","name":"Engineering","key":"ENG","description":"Core","timezone":"Europe/London","createdAt":"2024-01-02T03:04:05.678Z","updatedAt":"2026-06-10T01:00:00.000Z","archivedAt":"2026-06-11T00:00:00.000Z","color":"#fff"},
		{"id":"team-2","name":"Design","key":"DES","description":null,"timezone":"UTC","createdAt":"2024-01-02T03:04:05.000Z","updatedAt":"2026-06-10T02:00:00.000Z","archivedAt":null}
	],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`

	schemaUsersPage = `{"data":{"users":{"nodes":[
		{"id":"user-1","name":"Ada Lovelace","displayName":"ada","email":"ada@example.com","active":true,"admin":true,"createdAt":"2024-01-02T03:04:05.000Z","updatedAt":"2026-06-10T01:00:00.000Z","archivedAt":null},
		{"id":"user-2","name":"Gone User","displayName":"gone","email":"gone@example.com","active":false,"admin":false,"createdAt":"2024-01-02T03:04:05.000Z","updatedAt":"2026-06-10T01:00:00.000Z","archivedAt":"2026-06-01T00:00:00.000Z"}
	],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`

	schemaProjectsPage = `{"data":{"projects":{"nodes":[
		{"id":"proj-1","name":"Launch","description":"Ship it","state":"started","url":"https://linear.app/acme/project/launch-1","progress":0.42,"targetDate":"2026-09-30","createdAt":"2024-01-02T03:04:05.000Z","updatedAt":"2026-06-10T01:00:00.000Z","archivedAt":null},
		{"id":"proj-2","name":"Someday","description":"","state":"backlog","url":"https://linear.app/acme/project/someday-2","progress":0,"targetDate":null,"createdAt":"2024-01-02T03:04:05.000Z","updatedAt":"2026-06-10T01:00:00.000Z","archivedAt":"2026-06-02T00:00:00.000Z"}
	],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`

	schemaLabelsPage = `{"data":{"issueLabels":{"nodes":[
		{"id":"label-1","name":"bug","color":"#eb5757","description":"Something broke","createdAt":"2024-01-02T03:04:05.000Z","updatedAt":"2026-06-10T01:00:00.000Z","archivedAt":null,"team":{"id":"team-1","key":"ENG"}},
		{"id":"label-2","name":"customer","color":"#5e6ad2","description":null,"createdAt":"2024-01-02T03:04:05.000Z","updatedAt":"2026-06-10T01:00:00.000Z","archivedAt":null,"team":null}
	],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`

	schemaCyclesPage = `{"data":{"cycles":{"nodes":[
		{"id":"cycle-1","number":7,"name":"Sprint 7","startsAt":"2026-06-01T00:00:00.000Z","endsAt":"2026-06-15T00:00:00.000Z","completedAt":"2026-06-15T00:00:00.000Z","archivedAt":null,"updatedAt":"2026-06-15T00:00:01.000Z","progress":1,"team":{"id":"team-1","key":"ENG"}},
		{"id":"cycle-2","number":8,"name":null,"startsAt":"2026-06-15T00:00:00.000Z","endsAt":"2026-06-29T00:00:00.000Z","completedAt":null,"archivedAt":null,"updatedAt":"2026-06-16T00:00:00.000Z","progress":0.1,"team":{"id":"team-1","key":"ENG"}}
	],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`

	schemaIssuesPage = `{"data":{"issues":{"nodes":[
		{"id":"issue-1","identifier":"ENG-42","title":"Fix login","description":"Steps:\n1. log in","priority":2,"estimate":3,"dueDate":"2026-07-01",
		 "createdAt":"2026-06-01T00:00:00.000Z","updatedAt":"2026-06-10T01:00:00.000Z","completedAt":"2026-06-10T01:00:00.000Z","canceledAt":null,"archivedAt":null,
		 "state":{"id":"state-1","name":"Done","type":"completed"},"team":{"id":"team-1","key":"ENG"},
		 "assignee":{"id":"user-1","name":"Ada Lovelace","email":"ada@example.com"},"creator":{"id":"user-2","name":"Gone User","email":"gone@example.com"},
		 "project":{"id":"proj-1","name":"Launch"},"cycle":{"id":"cycle-1","number":7,"name":"Sprint 7"},
		 "parent":{"id":"issue-0","identifier":"ENG-7","team":{"id":"team-1","key":"ENG"}},
		 "labels":{"nodes":[{"id":"label-1","name":"bug"}]},
		 "attachments":{"nodes":[{"url":"https://github.com/acme/api/pull/12","sourceType":"github"}],"pageInfo":{"hasNextPage":true}},
		 "sortOrder":-12.5},
		{"id":"issue-2","identifier":"ENG-43","title":"Untriaged","description":null,"priority":0,"estimate":null,"dueDate":null,
		 "createdAt":"2026-06-02T00:00:00.000Z","updatedAt":"2026-06-10T02:00:00.000Z","completedAt":null,"canceledAt":null,"archivedAt":null,
		 "state":{"id":"state-0","name":"Triage","type":"triage"},"team":{"id":"team-1","key":"ENG"},
		 "assignee":null,"creator":null,"project":null,"cycle":null,"parent":null,
		 "labels":{"nodes":[]},"attachments":{"nodes":[],"pageInfo":{"hasNextPage":false}}}
	],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`
)

func schemaRespond(query string, _ map[string]any, dest any) error {
	switch {
	case strings.Contains(query, "DiscoverTeams"):
		return unmarshalInto(dest, teamsOnePage)
	case strings.Contains(query, "FetchTeams"):
		return unmarshalInto(dest, schemaTeamsPage)
	case strings.Contains(query, "FetchUsers"):
		return unmarshalInto(dest, schemaUsersPage)
	case strings.Contains(query, "FetchProjects"):
		return unmarshalInto(dest, schemaProjectsPage)
	case strings.Contains(query, "FetchLabels"):
		return unmarshalInto(dest, schemaLabelsPage)
	case strings.Contains(query, "FetchCycles"):
		return unmarshalInto(dest, schemaCyclesPage)
	case strings.Contains(query, "FetchIssues"):
		return unmarshalInto(dest, schemaIssuesPage)
	}
	return unmarshalInto(dest, `{"data":{}}`)
}

// TestPayloadsMatchSchemas runs every Linear operation against the fake
// gateway and validates each emitted payload against
// schemas/linear/<op>.schema.json. A sparse node (only id and updatedAt, as
// the other op tests use) is validated too: the payload keeps every key.
func TestPayloadsMatchSchemas(t *testing.T) {
	for _, op := range []sdk.APIFetchOperation{
		&FetchTeamsOp{}, &FetchUsersOp{}, &FetchProjectsOp{}, &FetchLabelsOp{}, &FetchCyclesOp{}, &FetchIssuesOp{},
	} {
		id := string(op.OperationID())
		t.Run(id, func(t *testing.T) {
			sink := testkit.NewChannelSink()
			_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
				Config: testConfig("org-1"), VendorGateway: &fakeGateway{respond: schemaRespond}, Sink: sink,
			})
			require.NoError(t, err)
			records := sink.Records()
			require.Len(t, records, 2)
			for _, rec := range records {
				assert.Equal(t, "linear", rec.ConnectorID)
				assert.Equal(t, id, rec.OperationID)
				assert.True(t, rec.DataDate.IsZero(), "a current-state record carries no DataDate, got %v", rec.DataDate)
				assert.Empty(t, rec.Metadata, "linear records carry no metadata")
				require.NoError(t, schemas.Validate(rec.ConnectorID, rec.OperationID, rec.Payload), "payload %s", rec.Payload)
			}
		})
	}
}

func TestSparseNodePayloadsMatchSchemas(t *testing.T) {
	for _, tc := range []struct {
		op    sdk.APIFetchOperation
		field string
	}{
		{&FetchTeamsOp{}, "teams"}, {&FetchUsersOp{}, "users"}, {&FetchProjectsOp{}, "projects"},
		{&FetchLabelsOp{}, "issueLabels"}, {&FetchCyclesOp{}, "cycles"}, {&FetchIssuesOp{}, "issues"},
	} {
		id := string(tc.op.OperationID())
		t.Run(id, func(t *testing.T) {
			gw := &fakeGateway{respond: func(query string, _ map[string]any, dest any) error {
				if strings.Contains(query, "DiscoverTeams") {
					return unmarshalInto(dest, teamsOnePage)
				}
				return unmarshalInto(dest, `{"data":{"`+tc.field+`":{"nodes":[{"id":"n-1","identifier":"ENG-1","updatedAt":"2026-06-10T01:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
			}}
			sink := testkit.NewChannelSink()
			_, err := tc.op.Execute(context.Background(), &sdk.ExecutionParams{Config: testConfig("org-1"), VendorGateway: gw, Sink: sink})
			require.NoError(t, err)
			require.Len(t, sink.Records(), 1)
			rec := sink.Records()[0]
			require.NoError(t, schemas.Validate("linear", id, rec.Payload), "payload %s", rec.Payload)
		})
	}
}

// TestSchemasRejectDrift checks the schemas catch a renamed or retyped key.
func TestSchemasRejectDrift(t *testing.T) {
	for _, bad := range []struct{ op, payload string }{
		{"fetch_teams", `{"id":"t","name":"n","key":"K","description":"","timezone":"UTC","createdAt":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","archivedAt":null}`},
		{"fetch_users", `{"id":"u","name":"n","displayName":"d","email":"e","active":"yes","admin":false,"createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z","archivedAt":null}`},
	} {
		assert.Error(t, schemas.Validate("linear", bad.op, []byte(bad.payload)), bad.op)
	}
}
