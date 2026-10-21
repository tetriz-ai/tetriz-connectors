// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func fetchIssuesDataResponseRaw(databaseID int64, nodes []ghIssueDescription, hasNext bool, endCursor string) *connectorpb.GitHubGraphQLResponse {
	resp := fetchIssuesDataResponse{}
	resp.Repository.DatabaseID = databaseID
	resp.Repository.Issues = &struct {
		Nodes    []ghIssueDescription `json:"nodes"`
		PageInfo ghPageInfo           `json:"pageInfo"`
	}{
		Nodes:    nodes,
		PageInfo: ghPageInfo{HasNextPage: hasNext, EndCursor: endCursor},
	}
	raw, _ := json.Marshal(resp)
	return &connectorpb.GitHubGraphQLResponse{Data: raw}
}

func TestFetchIssuesDataOp_Execute(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	const (
		platformRepoID = "777"
		repoFullName   = "acme/widgets"
	)

	tests := []struct {
		name      string
		body      string
		wantS3Key string
		wantEmit  bool
	}{
		{
			name:      "real markdown body uploads raw bytes + emits ref with sha",
			body:      "Bug report: fixes PROJ-100\n\n_steps_:\n- a\n- b",
			wantS3Key: fmt.Sprintf("descriptions/org-test/%s/issues/%s/42", connectorID, platformRepoID),
			wantEmit:  true,
		},
		{
			name:     "empty body: no S3 PUT, no emit",
			body:     "",
			wantEmit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
					return fetchIssuesDataResponseRaw(777, []ghIssueDescription{
						{Number: 42, UpdatedAt: now, Body: tt.body},
					}, false, ""), nil
				},
			}

			sink := testkit.NewChannelSink()
			s3, writes := newRecordingS3(t)
			params := testParams(sink, gw, nil)
			params.BlobStore = s3

			op := &FetchIssuesDataOp{}
			cursor, err := op.Execute(context.Background(), params)
			require.NoError(t, err)
			require.NotNil(t, cursor)

			records := sink.Drain()
			if !tt.wantEmit {
				assert.Empty(t, records, "no-blob outcome must not emit")
				assert.Empty(t, *writes, "no-blob outcome must not touch S3")
				return
			}
			require.Len(t, records, 1)

			rec := records[0]
			assert.Equal(t, "fetch_issues_data", rec.OperationID)
			// external_id is repo-qualified — github issue numbers are unique per-repo.
			assert.Equal(t, fmt.Sprintf("%s#I-%d", repoFullName, 42), rec.ExternalID)
			assert.True(t, rec.DataDate.IsZero(), "a description pointer is current state: no DataDate, got %v", rec.DataDate)

			var payload map[string]any
			require.NoError(t, json.Unmarshal(rec.Payload, &payload))
			assert.Equal(t, tt.wantS3Key, payload["s3_key"])
			assert.Equal(t, fmt.Sprintf("%s#%d", repoFullName, 42), payload["issue_key"])
			_, hasNaturalID := payload["parent_natural_id"]
			assert.False(t, hasNaturalID, "parent_natural_id dropped from payload")
			assert.Equal(t, platformRepoID, payload["platform_project_id"])
			assert.Equal(t, repoFullName, payload["repo_full_name"])

			digest := sha256.Sum256([]byte(tt.body))
			wantSha := "sha256:" + hex.EncodeToString(digest[:])
			assert.Equal(t, wantSha, rec.Metadata["description_sha"], "sha must equal sha256(raw body bytes)")

			require.Len(t, *writes, 1)
			assert.Equal(t, tt.wantS3Key, (*writes)[0].Key)
			assert.Equal(t, tt.body, string((*writes)[0].Content), "S3 bytes must equal the raw body bytes")
		})
	}
}

// TestGhGQLIssueNode_PreservesClosedByPullRequestsReferences guards the
// connector half of PR<->Issue linkage: fetch_issues unmarshals each issue
// into ghGQLIssueNode and re-marshals it as the record payload. If the
// struct omits closedByPullRequestsReferences (which buildIssuesQuery requests)
// the field is silently dropped before a downstream consumer can read it.
func TestGhGQLIssueNode_PreservesClosedByPullRequestsReferences(t *testing.T) {
	t.Parallel()
	raw := `{"number":7,"closedByPullRequestsReferences":{"nodes":[{"number":141,"state":"MERGED","repository":{"nameWithOwner":"acme/web"}}]}}`

	var node ghGQLIssueNode
	require.NoError(t, json.Unmarshal([]byte(raw), &node))
	require.Len(t, node.ClosedByPullRequestsReferences.Nodes, 1)
	require.Equal(t, 141, node.ClosedByPullRequestsReferences.Nodes[0].Number)
	require.Equal(t, "acme/web", node.ClosedByPullRequestsReferences.Nodes[0].Repository.NameWithOwner)

	// The re-marshal that produces the emitted payload must retain the field.
	remarshaled, err := json.Marshal(node)
	require.NoError(t, err)
	require.Contains(t, string(remarshaled), "closedByPullRequestsReferences")
	require.Contains(t, string(remarshaled), `"number":141`)
}

// TestGhGQLIssueNode_PreservesIssueTypeAndParent guards the connector half of
// a consumer's issue type and parent issue: fetch_issues unmarshals each issue into
// ghGQLIssueNode and re-marshals it as the record payload. If the struct omits
// issueType / parent (both requested by buildIssuesQuery) the fields are silently
// dropped before a downstream consumer can read them.
func TestGhGQLIssueNode_PreservesIssueTypeAndParent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		raw        string
		wantType   bool // issueType present on the node and in the re-marshaled payload
		wantParent bool // parent present on the node and in the re-marshaled payload
	}{
		{
			name:       "both present survive the round-trip",
			raw:        `{"number":7,"issueType":{"name":"Bug"},"parent":{"number":3,"repository":{"nameWithOwner":"acme/web"}}}`,
			wantType:   true,
			wantParent: true,
		},
		{
			// omitempty must OMIT the keys, not emit "issueType":null / "parent":null —
			// the consumer's nil-check then correctly leaves issue_type / parent_issue_id unset.
			name:       "both absent stay omitted on re-marshal",
			raw:        `{"number":7}`,
			wantType:   false,
			wantParent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var node ghGQLIssueNode
			require.NoError(t, json.Unmarshal([]byte(tt.raw), &node))

			// The re-marshal is what produces the emitted record payload.
			remarshaled, err := json.Marshal(node)
			require.NoError(t, err)
			out := string(remarshaled)

			if tt.wantType {
				require.NotNil(t, node.IssueType)
				require.Equal(t, "Bug", node.IssueType.Name)
				require.Contains(t, out, `"issueType":{"name":"Bug"}`)
			} else {
				require.Nil(t, node.IssueType)
				require.NotContains(t, out, "issueType")
			}

			if tt.wantParent {
				require.NotNil(t, node.Parent)
				require.Equal(t, int64(3), node.Parent.Number)
				require.Equal(t, "acme/web", node.Parent.Repository.NameWithOwner)
				require.Contains(t, out, `"parent":{"number":3`)
			} else {
				require.Nil(t, node.Parent)
				require.NotContains(t, out, `"parent"`)
			}
		})
	}
}
