// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// graphQLProjectsResponse builds a *connectorpb.GitHubGraphQLResponse whose Data
// field carries an organization.projectsV2 connection. Mirrors graphQLDataResponse
// but for the projects-specific response shape.
func graphQLProjectsResponse(nodes []ghGQLProjectV2Node, hasNext bool, endCursor string) *connectorpb.GitHubGraphQLResponse {
	envelope := ghProjectsV2Response{}
	envelope.Organization.ProjectsV2.Nodes = nodes
	envelope.Organization.ProjectsV2.PageInfo = ghPageInfo{HasNextPage: hasNext, EndCursor: endCursor}
	raw, _ := json.Marshal(envelope)
	return &connectorpb.GitHubGraphQLResponse{Data: raw}
}

// TestBuildProjectsQuery asserts the GraphQL query selects exactly the fields
// the consumer's GitHub projects payload reads on the record payload.
// A regression here breaks the consumer-side wire contract.
func TestBuildProjectsQuery(t *testing.T) {
	t.Parallel()

	q := buildProjectsQuery("acme", "", 100)
	tests := []struct {
		name     string
		fragment string
		desc     string
	}{
		{"organization scope", `organization(login: "acme")`, "org-level projects only — same scope as fetch_org_members"},
		{"projectsV2 connection", "projectsV2(first: 100", "page size flows from GitHubPagination.Operations.fetch_projects.PageSize"},
		{"node number", "number\n", "ProjectV2 number → IssueKey suffix downstream"},
		{"node title", "title\n", "displayed name; a project without one is skipped"},
		{"node shortDescription", "shortDescription\n", "the project description a consumer stores"},
		{"node url", "url\n", "the project URL a consumer stores"},
		{"node closed", "closed\n", "drives ProjectState_PROJECT_STATE_ACTIVE vs _CLOSED in parseGitHub"},
		{"node createdAt", "createdAt\n", "the project creation time a consumer stores"},
		{"pageInfo selected", "pageInfo {", "pagination loop reads hasNextPage / endCursor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, q, tt.fragment, tt.desc)
		})
	}

	t.Run("first-page after clause omitted when cursor empty", func(t *testing.T) {
		t.Parallel()
		assert.NotContains(t, q, "after:", "first page must not include after: clause")
	})
	t.Run("subsequent-page after clause includes cursor", func(t *testing.T) {
		t.Parallel()
		paged := buildProjectsQuery("acme", "MTAw", 100)
		assert.Contains(t, paged, `after: "MTAw"`, "cursor flows into the after argument")
	})
	t.Run("page size respects config override", func(t *testing.T) {
		t.Parallel()
		smaller := buildProjectsQuery("acme", "", 25)
		assert.Contains(t, smaller, "projectsV2(first: 25", "non-default page size is honored")
	})
}

// TestFetchProjectsOp_Execute exercises the producer end-to-end against a mock
// VendorGateway: emits one record per node, paginates via pageInfo, skips
// structurally-invalid nodes, and returns a wrapped error on failure paths.
func TestFetchProjectsOp_Execute(t *testing.T) {
	tests := []struct {
		name           string
		executeFunc    func(t *testing.T) func(ctx context.Context, req proto.Message) (proto.Message, error)
		paginationCfg  *config.GitHubPaginationConfig // optional override; nil → package defaults
		wantErr        bool
		wantErrFrag    string // substring asserted on err.Error() when wantErr is true
		wantRecords    int
		wantExternalID []string // expected ExternalID order; empty entries skip the assertion
		validate       func(t *testing.T, records []*sdk.RawRecord)
	}{
		{
			name: "single page emits one record per project",
			executeFunc: func(t *testing.T) func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return func(ctx context.Context, req proto.Message) (proto.Message, error) {
					return graphQLProjectsResponse([]ghGQLProjectV2Node{
						{Number: 7, Title: "Q1 Roadmap", ShortDescription: "objectives", URL: "https://github.com/orgs/acme/projects/7", Closed: false},
						{Number: 8, Title: "Eng Initiatives", URL: "https://github.com/orgs/acme/projects/8", Closed: true},
					}, false, ""), nil
				}
			},
			wantRecords:    2,
			wantExternalID: []string{"project-7", "project-8"},
			validate: func(t *testing.T, records []*sdk.RawRecord) {
				// Round-trip the first record's payload through the consumer-side
				// shape to catch silent renames at the JSON-tag layer.
				var got struct {
					Number           int64  `json:"number"`
					Title            string `json:"title"`
					ShortDescription string `json:"shortDescription"`
					URL              string `json:"url"`
					Closed           bool   `json:"closed"`
				}
				require.NoError(t, json.Unmarshal(records[0].Payload, &got))
				assert.Equal(t, int64(7), got.Number)
				assert.Equal(t, "Q1 Roadmap", got.Title)
				assert.Equal(t, "objectives", got.ShortDescription)
				assert.False(t, got.Closed)
				assert.Equal(t, map[string]string{"organization": "acme"}, records[0].Metadata,
					"metadata stamps the org so consumers can resolve platform_tenant later")
				// A project is current state: no DataDate, so a date-keyed sink
				// keeps one row per project instead of one per run.
				for _, r := range records {
					assert.True(t, r.DataDate.IsZero(), "%s: DataDate must be empty, got %v", r.ExternalID, r.DataDate)
				}
			},
		},
		{
			name: "two pages — second-page cursor flows back into the next query",
			executeFunc: func(t *testing.T) func(ctx context.Context, req proto.Message) (proto.Message, error) {
				calls := 0
				return func(ctx context.Context, req proto.Message) (proto.Message, error) {
					calls++
					gqlReq, ok := req.(*connectorpb.GitHubGraphQLRequest)
					require.True(t, ok)
					switch calls {
					case 1:
						assert.NotContains(t, gqlReq.GetQuery(), "after:", "first call must not include after:")
						return graphQLProjectsResponse([]ghGQLProjectV2Node{
							{Number: 1, Title: "A", URL: "u1"},
						}, true, "CURSOR_PAGE_2"), nil
					case 2:
						assert.Contains(t, gqlReq.GetQuery(), `after: "CURSOR_PAGE_2"`, "second call must use endCursor from page 1")
						return graphQLProjectsResponse([]ghGQLProjectV2Node{
							{Number: 2, Title: "B", URL: "u2"},
						}, false, ""), nil
					default:
						t.Fatalf("unexpected 3rd call to Execute")
						return nil, nil
					}
				}
			},
			wantRecords:    2,
			wantExternalID: []string{"project-1", "project-2"},
		},
		{
			name: "structurally-invalid nodes (zero number / empty title) are skipped",
			executeFunc: func(t *testing.T) func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return func(ctx context.Context, req proto.Message) (proto.Message, error) {
					return graphQLProjectsResponse([]ghGQLProjectV2Node{
						{Number: 0, Title: "missing-number"},   // skipped
						{Number: 9, Title: ""},                 // skipped
						{Number: 10, Title: "valid", URL: "u"}, // kept
					}, false, ""), nil
				}
			},
			wantRecords:    1,
			wantExternalID: []string{"project-10"},
		},
		{
			name: "empty projects list — successful run, no records emitted",
			executeFunc: func(t *testing.T) func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return func(ctx context.Context, req proto.Message) (proto.Message, error) {
					return graphQLProjectsResponse(nil, false, ""), nil
				}
			},
			wantRecords: 0,
		},
		{
			name: "transport failure on the GraphQL call surfaces wrapped error",
			executeFunc: func(t *testing.T) func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return func(ctx context.Context, req proto.Message) (proto.Message, error) {
					return nil, errors.New("boom")
				}
			},
			wantErr:     true,
			wantErrFrag: "fetch projects for acme",
		},
		{
			name: "malformed JSON in response body surfaces parse error",
			executeFunc: func(t *testing.T) func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return func(ctx context.Context, req proto.Message) (proto.Message, error) {
					return &connectorpb.GitHubGraphQLResponse{Data: []byte(`{not json`)}, nil
				}
			},
			wantErr:     true,
			wantErrFrag: "parse projects for acme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sink := testkit.NewChannelSink()
			gw := &mockVendorGateway{executeFunc: tt.executeFunc(t)}
			op := &FetchProjectsOp{paginationCfg: tt.paginationCfg}

			cursor, err := op.Execute(context.Background(), testParams(sink, gw, nil))
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrFrag != "" {
					assert.Contains(t, err.Error(), tt.wantErrFrag)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cursor, "successful run returns a timestamp cursor")

			records := sink.Records()
			require.Len(t, records, tt.wantRecords)
			for i, want := range tt.wantExternalID {
				if want == "" {
					continue
				}
				assert.Equal(t, want, records[i].ExternalID)
				assert.Equal(t, "fetch_projects", records[i].OperationID)
			}
			if tt.validate != nil {
				tt.validate(t, records)
			}
		})
	}
}
