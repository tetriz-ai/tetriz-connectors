// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// fetchOnePRRaw runs fetch_prs against one vendor PR node given as raw GraphQL JSON, the
// way GitHub returns it, and returns the emitted records.
func fetchOnePRRaw(t *testing.T, prNode string) []*sdk.RawRecord {
	t.Helper()
	data := `{"repository":{"databaseId":7,"pullRequests":{"nodes":[` + prNode + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
		return &connectorpb.GitHubGraphQLResponse{Data: json.RawMessage(data)}, nil
	}}
	sink := testkit.NewChannelSink()
	_, err := (&FetchPRsOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)
	return sink.Records()
}

func prNodeAt(now time.Time, extra string) string {
	ts := now.Format(time.RFC3339)
	return `{"number":5,"state":"MERGED","createdAt":"` + ts + `","updatedAt":"` + ts + `","mergedAt":"` + ts + `"` + extra + `}`
}

// TestEdge_PRPayloadKeepsRequestedReviewerDatabaseID: buildPRsQuery selects
// requestedReviewer { ... on User { login databaseId } } because the identity layer keys
// GitHub identities by databaseId (TestBuildPRsQuerySelectsRequestedReviewerDatabaseID).
// The emitted payload is a re-marshal of ghGQLPRNode, so a field the struct does not
// declare is dropped between the vendor and the consumer.
func TestEdge_PRPayloadKeepsRequestedReviewerDatabaseID(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	records := fetchOnePRRaw(t, prNodeAt(now,
		`,"reviewRequests":{"totalCount":1,"nodes":[{"requestedReviewer":{"login":"octo","databaseId":9001}}]}`))
	require.NotEmpty(t, records)

	var payload struct {
		ReviewRequests struct {
			Nodes []struct {
				RequestedReviewer map[string]any `json:"requestedReviewer"`
			} `json:"nodes"`
		} `json:"reviewRequests"`
	}
	require.NoError(t, json.Unmarshal(records[0].Payload, &payload))
	require.Len(t, payload.ReviewRequests.Nodes, 1)
	assert.EqualValues(t, 9001, payload.ReviewRequests.Nodes[0].RequestedReviewer["databaseId"],
		"the requested reviewer's databaseId the query selects must survive into the emitted payload")
}

// TestEdge_PRPayloadKeepsMergedBy: the query selects mergedBy { login }, but the emitted
// payload drops it for the same reason.
func TestEdge_PRPayloadKeepsMergedBy(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	records := fetchOnePRRaw(t, prNodeAt(now, `,"mergedBy":{"login":"merger"}`))
	require.NotEmpty(t, records)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(records[0].Payload, &payload))
	assert.Equal(t, map[string]any{"login": "merger"}, payload["mergedBy"],
		"mergedBy is selected by the PR query and must reach the consumer")
}

// TestEdge_PRWithGhostAuthorAndNoCommits: a PR whose author account was deleted comes
// back with "author": null, and a PR whose branch was force-pushed to the base has zero
// commits. Neither may fail the page; the author id is left empty rather than "0".
func TestEdge_PRWithGhostAuthorAndNoCommits(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	records := fetchOnePRRaw(t, prNodeAt(now,
		`,"author":null,"mergedBy":null,"title":"修正 🚀 \"quoted\"","headRefName":"feature/ünïcødé-🚀","commits":{"nodes":[],"pageInfo":{"hasNextPage":false}}`))
	require.Len(t, records, 3, "opened, snapshot and merged")
	for _, r := range records {
		assert.Empty(t, r.Metadata["pr_author_id"], "a null author has no id")
		assert.Equal(t, "feature/ünïcødé-🚀", r.Metadata["head_branch"], "unicode branch names pass through unchanged")
	}
	var payload struct {
		Title string `json:"title"`
	}
	require.NoError(t, json.Unmarshal(records[0].Payload, &payload))
	assert.Equal(t, "修正 🚀 \"quoted\"", payload.Title)
}

// TestEdge_PRTimestampsWithOffsetAndFractionalSeconds: GitHub returns UTC "Z" stamps, but
// the decoder must accept any RFC 3339 form. A mergedAt carrying a +02:00 offset and
// fractional seconds lands its merged record on the same instant.
func TestEdge_PRTimestampsWithOffsetAndFractionalSeconds(t *testing.T) {
	merged := time.Date(2026, 9, 30, 23, 45, 1, 250_000_000, time.UTC)
	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	node := `{"number":9,"state":"MERGED","createdAt":"` + created.Format(time.RFC3339) +
		`","updatedAt":"` + time.Now().In(time.FixedZone("UTC+2", 2*3600)).Format(time.RFC3339Nano) +
		`","mergedAt":"` + merged.In(time.FixedZone("UTC+2", 2*3600)).Format(time.RFC3339Nano) + `"}`
	records := fetchOnePRRaw(t, node)

	var mergedRec *sdk.RawRecord
	for _, r := range records {
		if r.Metadata["event_type"] == "merged" {
			mergedRec = r
		}
	}
	require.NotNil(t, mergedRec)
	assert.True(t, mergedRec.DataDate.Equal(merged), "merged DataDate %s must equal %s", mergedRec.DataDate, merged)
}
