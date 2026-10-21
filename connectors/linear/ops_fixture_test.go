// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// The four workspace-level operations each page through their connection,
// emit one record per node keyed by the node id, and fail on a page that
// carries GraphQL errors instead of emitting it.
func TestWorkspaceOpsEmitEachNodeAndFailOnAGraphQLError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		op    sdk.APIFetchOperation
		field string
	}{
		{"fetch_teams", &FetchTeamsOp{}, "teams"},
		{"fetch_users", &FetchUsersOp{}, "users"},
		{"fetch_projects", &FetchProjectsOp{}, "projects"},
		{"fetch_labels", &FetchLabelsOp{}, "issueLabels"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page := 0
			gw := &fakeGateway{respond: func(_ string, _ map[string]any, dest any) error {
				page++
				if page == 1 {
					return unmarshalInto(dest, `{"data":{"`+tc.field+`":{"nodes":[{"id":"n-1","updatedAt":"2026-06-10T01:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}}}`)
				}
				return unmarshalInto(dest, `{"data":{"`+tc.field+`":{"nodes":[{"id":"n-2","updatedAt":"2026-06-11T01:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`)
			}}
			sink := testkit.NewChannelSink()
			_, err := tc.op.Execute(context.Background(), &sdk.ExecutionParams{Config: testConfig("org-1"), VendorGateway: gw, Sink: sink})
			require.NoError(t, err)
			var ids []string
			for _, r := range sink.Records() {
				ids = append(ids, r.ExternalID)
			}
			assert.Equal(t, []string{"n-1", "n-2"}, ids, "one record per node, across both pages")

			errSink := testkit.NewChannelSink()
			errGW := &fakeGateway{respond: func(_ string, _ map[string]any, dest any) error {
				return unmarshalInto(dest, `{"data":{"`+tc.field+`":{"nodes":[{"id":"n-x"}],"pageInfo":{"hasNextPage":false}}},"errors":[{"message":"field not accessible"}]}`)
			}}
			_, err = tc.op.Execute(context.Background(), &sdk.ExecutionParams{Config: testConfig("org-1"), VendorGateway: errGW, Sink: errSink})
			require.Error(t, err, "a page with GraphQL errors fails the run")
			assert.Empty(t, errSink.Records(), "nothing from an errored page is emitted")
		})
	}
}
