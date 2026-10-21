// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A webhook record's payload is the delivery body, and a payload is JSON. A
// body GitHub sends form-encoded (content type
// application/x-www-form-urlencoded) or otherwise not a JSON object is
// rejected, not stored as opaque bytes.
func TestWebhookEventsOp_RejectsNonJSONBody(t *testing.T) {
	secret := "test-secret-123"
	for name, body := range map[string][]byte{
		"form-encoded": []byte(`payload=%7B%22action%22%3A%22opened%22%7D`),
		"truncated":    []byte(`{"action":"opened"`),
		"empty":        nil,
	} {
		t.Run(name, func(t *testing.T) {
			headers := http.Header{}
			headers.Set("X-Hub-Signature-256", signPayload(secret, body))
			headers.Set("X-GitHub-Event", "pull_request")
			headers.Set("X-GitHub-Delivery", "d-1")
			sink := testkit.NewChannelSink()

			_, err := (&WebhookEventsOp{}).HandleWebhook(context.Background(), &sdk.WebhookParams{
				Config: webhookConfig(secret), Headers: headers, Body: body, Sink: sink,
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "not a JSON object")
			assert.Empty(t, sink.Records(), "a non-JSON body must not become a record")
		})
	}
}

// github_com_user_id arrives as a number or a string; both identify the row.
// A row without one has no identity and is skipped, never stored under a
// shared ExternalID.
func TestFetchCopilotUsageOp_UserIDNumericStringOrMissing(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.CopilotMetricsRequest:
				return &connectorpb.CopilotMetricsResponse{}, nil
			case *connectorpb.CopilotUserUsageRequest:
				return &connectorpb.CopilotUserUsageResponse{UserMetrics: [][]byte{
					[]byte(`{"day":"2024-01-16","github_com_user_id":9001}`),
					[]byte(`{"day":"2024-01-16","github_com_user_id":"9002"}`),
					[]byte(`{"day":"2024-01-16","github_com_user_id":""}`),
					[]byte(`{"day":"2024-01-16"}`),
				}}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	sink := testkit.NewChannelSink()
	_, err := (&FetchCopilotUsageOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)

	var ids []string
	for _, r := range sink.Records() {
		ids = append(ids, r.ExternalID)
		assert.Equal(t, time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC), r.DataDate, "a usage row is dated by its day")
	}
	assert.Equal(t, []string{
		"copilot-user-acme-2024-01-16-9001",
		"copilot-user-acme-2024-01-16-9002",
	}, ids)
}

func TestCopilotUserID_Unmarshal(t *testing.T) {
	for in, want := range map[string]string{`9001`: "9001", `"9001"`: "9001", `null`: ""} {
		var got copilotUserID
		require.NoError(t, json.Unmarshal([]byte(in), &got), in)
		assert.Equal(t, want, string(got), in)
	}
	var bad copilotUserID
	assert.Error(t, json.Unmarshal([]byte(`true`), &bad))
}

// pr_author_id is empty when the PR author has no database id (a deleted
// "ghost" user), on fetch_pr_comments as on fetch_prs and fetch_reviews —
// never "0".
func TestFetchPRCommentsOp_UnknownPRAuthorIDIsEmpty(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitHubGraphQLRequest:
				return graphQLDataResponse(ghGraphQLData{
					Repository: ghGQLRepository{
						DatabaseID: 9876,
						PullRequests: &ghGQLConnection[ghGQLPRNode]{
							Nodes:    []ghGQLPRNode{{Number: 10, CreatedAt: now, UpdatedAt: now}},
							PageInfo: ghPageInfo{},
						},
					},
				}), nil
			case *connectorpb.ListPRCommentsRequest:
				b, _ := json.Marshal(ghPRComment{ID: 1001, User: ghUser{Login: "alice", ID: 300, Type: "User"}, CreatedAt: now})
				return &connectorpb.ListPRCommentsResponse{Comments: [][]byte{b}}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
	sink := testkit.NewChannelSink()
	_, err := (&FetchPRCommentsOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1)
	got, ok := records[0].Metadata["pr_author_id"]
	require.True(t, ok, "pr_author_id is always written")
	assert.Empty(t, got)
}
