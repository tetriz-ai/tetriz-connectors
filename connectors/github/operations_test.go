// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// --- Mock VendorGateway ---

type mockVendorGateway struct {
	executeFunc func(ctx context.Context, req proto.Message) (proto.Message, error)
}

func (m *mockVendorGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not expected in new architecture")
}

func (m *mockVendorGateway) DoJSON(ctx context.Context, method, url string, reqBody, respBody any) error {
	return fmt.Errorf("DoJSON not expected in new architecture")
}

func (m *mockVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	return m
}

func (m *mockVendorGateway) ForGroup(group string) sdk.VendorGateway {
	return m
}

func (m *mockVendorGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, req)
	}
	return nil, fmt.Errorf("unexpected Execute call: %T", req)
}

// --- Test helpers ---

func testParams(sink sdk.DataSink, gw sdk.VendorGateway, cursor *sdk.Cursor) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-test",
			Credentials: mustJSON(GitHubCredentials{
				InstallationID: "12345678",
			}),
			Settings: mustJSON(GitHubSettings{
				Organization: "acme",
				Repositories: []string{"acme/widgets"},
			}),
			Enabled: true,
		},
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gw,
	}
}

// graphQLDataResponse builds a *connectorpb.GitHubGraphQLResponse whose Data field
// contains the JSON of a ghGraphQLData struct.
func graphQLDataResponse(data ghGraphQLData) *connectorpb.GitHubGraphQLResponse {
	raw, _ := json.Marshal(data)
	return &connectorpb.GitHubGraphQLResponse{Data: raw}
}

// graphQLPRsData builds a ghGraphQLData for pull requests.
func graphQLPRsData(prs []ghGQLPRNode, hasNext bool, endCursor string) ghGraphQLData {
	return ghGraphQLData{
		Repository: ghGQLRepository{
			PullRequests: &ghGQLConnection[ghGQLPRNode]{
				Nodes:    prs,
				PageInfo: ghPageInfo{HasNextPage: hasNext, EndCursor: endCursor},
			},
		},
	}
}

func graphQLCommitsData(commits []ghGQLCommitNode, hasNext bool, endCursor string) ghGraphQLData {
	return ghGraphQLData{
		Repository: ghGQLRepository{
			Ref: &ghGQLRef{
				Target: ghGQLTarget{
					History: &ghGQLConnection[ghGQLCommitNode]{
						Nodes:    commits,
						PageInfo: ghPageInfo{HasNextPage: hasNext, EndCursor: endCursor},
					},
				},
			},
		},
	}
}

func graphQLIssuesData(issues []ghGQLIssueNode, hasNext bool, endCursor string) ghGraphQLData {
	return ghGraphQLData{
		Repository: ghGQLRepository{
			Issues: &ghGQLConnection[ghGQLIssueNode]{
				Nodes:    issues,
				PageInfo: ghPageInfo{HasNextPage: hasNext, EndCursor: endCursor},
			},
		},
	}
}

// --- FetchPRsOp tests ---

func TestFetchPRsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLPRsData(
				[]ghGQLPRNode{
					{Number: 42, UpdatedAt: now, State: "OPEN", CreatedAt: now.Add(-24 * time.Hour), HeadRefName: "feat/my-feature"},
				},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	// Open PR emits 2 lifecycle records: #opened, #snapshot.
	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "acme/widgets#PR-42#opened", records[0].ExternalID)
	assert.Equal(t, "acme/widgets#PR-42#snapshot", records[1].ExternalID)
	assert.Equal(t, "fetch_prs", records[0].OperationID)
	assert.Equal(t, "github", records[0].ConnectorID)
	assert.Equal(t, "org-test", records[0].OrganizationID)
	assert.Equal(t, "acme/widgets", records[0].Metadata["repo"])
	assert.Equal(t, "opened", records[0].Metadata["event_type"])
	assert.Equal(t, "snapshot", records[1].Metadata["event_type"])
	assert.Equal(t, "feat/my-feature", records[0].Metadata["head_branch"])
	assert.Equal(t, "feat/my-feature", records[1].Metadata["head_branch"])
}

func TestFetchPRsOp_MergedPR(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	createdAt := now.Add(-5 * 24 * time.Hour)
	mergedAt := now.Add(-1 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLPRsData(
				[]ghGQLPRNode{
					{Number: 42, UpdatedAt: now, State: "MERGED", CreatedAt: createdAt, MergedAt: &mergedAt},
				},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	// Merged PR emits 3 records: #opened, #snapshot, #merged.
	records := sink.Records()
	require.Len(t, records, 3)
	assert.Equal(t, "acme/widgets#PR-42#opened", records[0].ExternalID)
	assert.Equal(t, "acme/widgets#PR-42#snapshot", records[1].ExternalID)
	assert.Equal(t, "acme/widgets#PR-42#merged", records[2].ExternalID)
	assert.Equal(t, "merged", records[2].Metadata["event_type"])
	// Opened uses createdAt, merged uses mergedAt; the snapshot is current
	// state and carries no DataDate (ADR-0003 amendment).
	assert.Equal(t, createdAt, records[0].DataDate)
	assert.True(t, records[1].DataDate.IsZero(), "snapshot DataDate must be empty, got %v", records[1].DataDate)
	assert.Equal(t, mergedAt, records[2].DataDate)
}

func TestFetchPRsOp_ClosedPR(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	createdAt := now.Add(-5 * 24 * time.Hour)
	closedAt := now.Add(-2 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLPRsData(
				[]ghGQLPRNode{
					{Number: 42, UpdatedAt: now, State: "CLOSED", CreatedAt: createdAt, ClosedAt: &closedAt},
				},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	// Closed (not merged) PR emits 3 records: #opened, #snapshot, #closed.
	records := sink.Records()
	require.Len(t, records, 3)
	assert.Equal(t, "acme/widgets#PR-42#opened", records[0].ExternalID)
	assert.Equal(t, "acme/widgets#PR-42#snapshot", records[1].ExternalID)
	assert.Equal(t, "acme/widgets#PR-42#closed", records[2].ExternalID)
	assert.Equal(t, "closed", records[2].Metadata["event_type"])
	assert.Equal(t, closedAt, records[2].DataDate)
}

func TestFetchPRsOp_IncrementalSync(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	since := now.Add(-2 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLPRsData(
				[]ghGQLPRNode{
					{Number: 43, UpdatedAt: now, State: "OPEN", CreatedAt: now.Add(-time.Hour)},
					{Number: 42, UpdatedAt: since.Add(-time.Minute)}, // Older than cursor
				},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	cursor := buildStateCursor(map[string]repoCursorState{"acme/widgets": {Timestamp: since}})
	params := testParams(sink, gw, cursor)

	op := &FetchPRsOp{}
	newCursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, newCursor)

	records := sink.Records()
	require.Len(t, records, 2, "should emit #opened + #snapshot for the one PR newer than cursor")
	assert.Equal(t, "acme/widgets#PR-43#opened", records[0].ExternalID)
	assert.Equal(t, "acme/widgets#PR-43#snapshot", records[1].ExternalID)
}

func TestFetchPRsOp_EmptyResponse(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLPRsData(nil, false, "")), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.Empty(t, sink.Records())
	assert.NotNil(t, cursor) // Returns composite cursor with zero-time entry
}

func TestFetchPRsOp_APIError(t *testing.T) {
	// A 401 from the only repository fails the run with that error, so an auth
	// failure is recorded as auth (and, when terminal, pauses the connector).
	// A failure in one of several repositories is skipped instead
	// (TestFetchPRsOp_ContinuesOnRepoError).
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: http.StatusUnauthorized, Terminal: true,
				Cause: errors.New("HTTP 401: Bad credentials")}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.True(t, syncErr.Terminal, "a terminal auth error stays terminal, so the run pauses the connector")
	assert.Empty(t, sink.Records())
}

func TestFetchPRsOp_RateLimitError(t *testing.T) {
	// A rate-limit error on the only repository fails the run with that kind.
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, &sdk.SyncError{
				ConnectorID: connectorID,
				OperationID: "fetch_prs",
				Kind:        sdk.ErrKindRateLimit,
				Retryable:   true,
				Cause:       fmt.Errorf("rate limit exceeded"),
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindRateLimit, syncErr.Kind)
	assert.Empty(t, sink.Records())
}

func TestFetchPRsOp_ContinuesOnRepoError(t *testing.T) {
	// Verify that a 502 error for one repo does not block PR sync for other repos.
	// This is the root cause fix for PRs showing as open because
	// a failing repo prevented snapshot updates for repos processed after it.
	now := time.Now().UTC().Truncate(time.Second)
	createdAt := now.Add(-3 * 24 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			gqlReq, ok := req.(*connectorpb.GitHubGraphQLRequest)
			if !ok {
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
			query := gqlReq.GetQuery()

			// Repo "failing-repo" returns a persistent 502 at all page sizes.
			if strings.Contains(query, "failing-repo") {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: "fetch_prs",
					Kind:        sdk.ErrKindVendorError,
					HTTPStatus:  502,
					Cause:       errors.New("502 Bad Gateway"),
				}
			}
			// Repo "healthy-repo" returns one open PR.
			return graphQLDataResponse(graphQLPRsData(
				[]ghGQLPRNode{
					{
						Number: 99, UpdatedAt: now, State: "OPEN", CreatedAt: createdAt,
						Author: struct {
							Login      string `json:"login"`
							DatabaseID int64  `json:"databaseId"`
						}{Login: "alice"},
					},
				},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-test",
			Credentials: mustJSON(GitHubCredentials{
				InstallationID: "12345678",
			}),
			Settings: mustJSON(GitHubSettings{
				Organization: "acme",
				// failing-repo is listed first so it's processed before healthy-repo.
				Repositories: []string{"acme/failing-repo", "acme/healthy-repo"},
			}),
			Enabled: true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}

	op := &FetchPRsOp{}
	cursor, err := op.Execute(context.Background(), params)

	// Operation should succeed even though one repo failed.
	require.NoError(t, err)
	require.NotNil(t, cursor)

	// healthy-repo's PRs should be emitted despite failing-repo's error.
	records := sink.Records()
	require.GreaterOrEqual(t, len(records), 2, "should emit records from healthy-repo")
	assert.Equal(t, "acme/healthy-repo#PR-99#opened", records[0].ExternalID)
	assert.Equal(t, "acme/healthy-repo#PR-99#snapshot", records[1].ExternalID)
}

// --- FetchCommitsOp tests ---

func TestFetchCommitsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLCommitsData(
				[]ghGQLCommitNode{
					{OID: "abc123", CommittedDate: now},
					{OID: "def456", CommittedDate: now.Add(-time.Hour)},
				},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchCommitsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "acme/widgets#abc123", records[0].ExternalID)
	assert.Equal(t, "acme/widgets#def456", records[1].ExternalID)
	assert.Equal(t, "fetch_commits", records[0].OperationID)
}

// TestFetchCommitsOp_AlwaysDefaultBranch verifies that fetch_commits always
// targets the repo's default branch, regardless of the tracked-branch /
// all-branches setting (which governs only the PR read-filter). The connector
// signals branch selection through the GraphQL query shape: an empty/default
// branch uses defaultBranchRef while a non-default branch would use
// ref(qualifiedName: "refs/heads/<branch>"). We auto-discover a repo whose
// default branch is "main" and assert the query never references the configured
// develop/all settings.
func TestFetchCommitsOp_AlwaysDefaultBranch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name     string
		settings GitHubSettings
	}{
		{
			name: "tracked branch develop ignored",
			settings: GitHubSettings{
				Organization:  "acme",
				TrackedBranch: "develop",
			},
		},
		{
			name: "track all branches ignored",
			settings: GitHubSettings{
				Organization:     "acme",
				TrackAllBranches: true,
			},
		},
		{
			name: "per-repo override develop ignored",
			settings: GitHubSettings{
				Organization:        "acme",
				RepoBranchOverrides: map[string]string{"acme/repo": "develop"},
			},
		},
		{
			name: "per-repo all-branches ignored",
			settings: GitHubSettings{
				Organization:    "acme",
				RepoAllBranches: []string{"acme/repo"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedQuery string
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch r := req.(type) {
					case *connectorpb.ListOrgReposRequest:
						repo := mustJSON(ghRepository{ID: 42, FullName: "acme/repo", DefaultBranch: "main"})
						return &connectorpb.ListOrgReposResponse{Repos: [][]byte{repo}, HasNextPage: false}, nil
					case *connectorpb.GitHubGraphQLRequest:
						capturedQuery = r.GetQuery()
						// Default-branch commits flow through the ref(...) →
						// CustomRef path because branch is non-empty ("main").
						return graphQLDataResponse(ghGraphQLData{
							Repository: ghGQLRepository{
								CustomRef: &ghGQLRef{Target: ghGQLTarget{History: &ghGQLConnection[ghGQLCommitNode]{
									Nodes: []ghGQLCommitNode{{OID: "c1", CommittedDate: now}},
								}}},
							},
						}), nil
					default:
						return nil, fmt.Errorf("unexpected request %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			params := &sdk.ExecutionParams{
				Config: &sdk.ConnectorConfig{
					ConnectorID:    "github",
					OrganizationID: "org-test",
					Credentials:    mustJSON(GitHubCredentials{InstallationID: "12345678"}),
					Settings:       mustJSON(tt.settings),
					Enabled:        true,
				},
				Sink:          sink,
				VendorGateway: gw,
			}

			_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
			require.NoError(t, err)

			assert.Contains(t, capturedQuery, `refs/heads/main`,
				"commit query must target the default branch (main)")
			assert.NotContains(t, capturedQuery, "develop",
				"commit query must not target the tracked/override branch")
			require.Len(t, sink.Records(), 1)
		})
	}
}

// --- FetchIssuesOp tests ---

func TestFetchIssuesOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLIssuesData(
				[]ghGQLIssueNode{
					{Number: 10, UpdatedAt: now, State: "OPEN", CreatedAt: now.Add(-time.Hour)},
				},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchIssuesOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	// Open issue emits 2 records: #opened, #snapshot.
	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "acme/widgets#ISSUE-10#opened", records[0].ExternalID)
	assert.Equal(t, "acme/widgets#ISSUE-10#snapshot", records[1].ExternalID)
	assert.Equal(t, "fetch_issues", records[0].OperationID)
	assert.Equal(t, "opened", records[0].Metadata["event_type"])
}

func TestFetchIssuesOp_ClosedIssue(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	createdAt := now.Add(-3 * 24 * time.Hour)
	closedAt := now.Add(-time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLIssuesData(
				[]ghGQLIssueNode{
					{Number: 10, UpdatedAt: now, State: "CLOSED", CreatedAt: createdAt, ClosedAt: &closedAt},
				},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchIssuesOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	// Closed issue emits 3 records: #opened, #snapshot, #closed.
	records := sink.Records()
	require.Len(t, records, 3)
	assert.Equal(t, "acme/widgets#ISSUE-10#opened", records[0].ExternalID)
	assert.Equal(t, "acme/widgets#ISSUE-10#snapshot", records[1].ExternalID)
	assert.Equal(t, "acme/widgets#ISSUE-10#closed", records[2].ExternalID)
	assert.Equal(t, createdAt, records[0].DataDate)
	assert.True(t, records[1].DataDate.IsZero(), "issue snapshot DataDate must be empty, got %v", records[1].DataDate)
	assert.Equal(t, closedAt, records[2].DataDate)
}

// --- FetchReviewsOp tests ---

func TestFetchReviewsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const platformRepoID int64 = 9876
	const prAuthorID int64 = 200
	prAuthor := struct {
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	}{Login: "prauthor", DatabaseID: prAuthorID}

	tests := []struct {
		name           string
		reviewer       ghUser
		wantIsBot      string
		wantReviewerID int64
	}{
		{
			name:           "human reviewer",
			reviewer:       ghUser{Login: "alice", ID: 300, Type: "User"},
			wantIsBot:      "false",
			wantReviewerID: 300,
		},
		{
			name:           "bot via user.type",
			reviewer:       ghUser{Login: "custombot", ID: 400, Type: "Bot"},
			wantIsBot:      "true",
			wantReviewerID: 400,
		},
		{
			name:           "bot via [bot] login suffix",
			reviewer:       ghUser{Login: "dependabot[bot]", ID: 500, Type: ""},
			wantIsBot:      "true",
			wantReviewerID: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &mockVendorGateway{
				executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.GitHubGraphQLRequest:
						// GraphQL call to fetch PR numbers + author + repo databaseId.
						return graphQLDataResponse(ghGraphQLData{
							Repository: ghGQLRepository{
								DatabaseID: platformRepoID,
								PullRequests: &ghGQLConnection[ghGQLPRNode]{
									Nodes: []ghGQLPRNode{
										{Number: 42, CreatedAt: now, UpdatedAt: now, Author: prAuthor},
									},
									PageInfo: ghPageInfo{HasNextPage: false},
								},
							},
						}), nil
					case *connectorpb.ListPRReviewsRequest:
						// REST call to fetch reviews for PR #42.
						review := ghReview{ID: 100, User: tt.reviewer, State: "APPROVED"}
						b, _ := json.Marshal(review)
						return &connectorpb.ListPRReviewsResponse{
							Reviews:     [][]byte{b},
							HasNextPage: false,
						}, nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			params := testParams(sink, gw, nil)

			op := &FetchReviewsOp{}
			cursor, err := op.Execute(context.Background(), params)

			require.NoError(t, err)
			assert.NotNil(t, cursor)

			records := sink.Records()
			require.Len(t, records, 1)
			rec := records[0]
			assert.Equal(t, "acme/widgets#PR-42#REVIEW-100", rec.ExternalID)
			assert.Equal(t, "fetch_reviews", rec.OperationID)
			assert.Equal(t, "42", rec.Metadata["pr_number"])
			assert.Equal(t, "acme/widgets#PR-42", rec.Metadata["pr_key"])
			assert.Equal(t, "prauthor", rec.Metadata["pr_author_login"])
			assert.Equal(t, "200", rec.Metadata["pr_author_id"])
			assert.Equal(t, "9876", rec.Metadata["platform_repo_id"])
			assert.Equal(t, tt.wantIsBot, rec.Metadata["reviewer_is_bot"])
			// pr_created_at is the parent PR's opened_at — the date anchor
			// a consumer uses to gate cross-day reviews.
			assert.Equal(t, now.Format(time.RFC3339), rec.Metadata["pr_created_at"])
		})
	}
}

func TestFetchReviewsOp_ResumeSkipsProcessedPRs(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Track which pull request review requests were made.
	var requestedPRs []int32

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.GitHubGraphQLRequest:
				return graphQLDataResponse(graphQLPRsData(
					[]ghGQLPRNode{
						{Number: 10, UpdatedAt: now.Add(-2 * time.Hour)},
						{Number: 20, UpdatedAt: now.Add(-1 * time.Hour)},
						{Number: 30, UpdatedAt: now},
					},
					false, "",
				)), nil
			case *connectorpb.ListPRReviewsRequest:
				requestedPRs = append(requestedPRs, r.PrNumber)
				review := ghReview{ID: int64(r.PrNumber * 100), User: ghUser{Login: "rev"}, State: "APPROVED"}
				b, _ := json.Marshal(review)
				return &connectorpb.ListPRReviewsResponse{Reviews: [][]byte{b}, HasNextPage: false}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	// Simulate a cursor from a previous interrupted run where PR 10 was already processed.
	prevState := map[string]repoCursorState{
		"acme/widgets": {
			Timestamp:    now.Add(-3 * time.Hour),
			Since:        now.Add(-3 * time.Hour),
			ProcessedPRs: []int{10},
		},
	}
	prevCursor := buildStateCursor(prevState)

	var checkpointCount int
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, prevCursor)
	params.Checkpoint = func(ctx context.Context, c *sdk.Cursor) error {
		checkpointCount++
		return nil
	}

	op := &FetchReviewsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	// PR 10 should be skipped — only PRs 20 and 30 should have REST calls.
	assert.Equal(t, []int32{20, 30}, requestedPRs)

	// 2 records emitted (one per remaining PR).
	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "acme/widgets#PR-20#REVIEW-2000", records[0].ExternalID)
	assert.Equal(t, "acme/widgets#PR-30#REVIEW-3000", records[1].ExternalID)

	// Checkpoints: 1 per processed PR (20, 30) + 1 final repo-complete = 3.
	assert.Equal(t, 3, checkpointCount)
}

// --- FetchCopilotUsageOp tests ---

func TestFetchCopilotUsageOp_Execute(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.CopilotMetricsRequest:
				metrics := []ghCopilotMetrics{
					{Date: "2024-01-15"},
					{Date: "2024-01-16"},
				}
				metricBytes := make([][]byte, len(metrics))
				for i, m := range metrics {
					metricBytes[i], _ = json.Marshal(m)
				}
				return &connectorpb.CopilotMetricsResponse{Metrics: metricBytes}, nil
			case *connectorpb.CopilotUserUsageRequest:
				userMetrics := []ghCopilotUsageResponse{
					{Day: "2024-01-16", GitHubComUserID: "user1"},
				}
				umBytes := make([][]byte, len(userMetrics))
				for i, um := range userMetrics {
					umBytes[i], _ = json.Marshal(um)
				}
				return &connectorpb.CopilotUserUsageResponse{UserMetrics: umBytes}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchCopilotUsageOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	records := sink.Records()
	require.Len(t, records, 3) // 2 org metrics + 1 user metric
	assert.Equal(t, "copilot-acme-2024-01-15", records[0].ExternalID)
	assert.Equal(t, "copilot-acme-2024-01-16", records[1].ExternalID)
	assert.Equal(t, "copilot-user-acme-2024-01-16-user1", records[2].ExternalID)
}

func TestFetchCopilotUsageOp_IncrementalSync(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.CopilotMetricsRequest:
				metrics := []ghCopilotMetrics{
					{Date: "2024-01-15"},
					{Date: "2024-01-16"},
				}
				metricBytes := make([][]byte, len(metrics))
				for i, m := range metrics {
					metricBytes[i], _ = json.Marshal(m)
				}
				return &connectorpb.CopilotMetricsResponse{Metrics: metricBytes}, nil
			case *connectorpb.CopilotUserUsageRequest:
				return &connectorpb.CopilotUserUsageResponse{}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	cursorData, _ := json.Marshal(map[string]string{"last_date": "2024-01-15"})
	cursor := &sdk.Cursor{Data: cursorData}
	params := testParams(sink, gw, cursor)

	op := &FetchCopilotUsageOp{}
	_, err := op.Execute(context.Background(), params)

	require.NoError(t, err)

	records := sink.Records()
	// With a 2-day finalization lag the skip floor is 2024-01-13, so the
	// watermark day (2024-01-15) re-emits alongside the newer 2024-01-16 — both
	// re-pulled to pick up finalized counts.
	require.Len(t, records, 2, "should re-emit the trailing window, not just records newer than the watermark")
	assert.Equal(t, "copilot-acme-2024-01-15", records[0].ExternalID)
	assert.Equal(t, "copilot-acme-2024-01-16", records[1].ExternalID)
}

func TestFetchCopilotUsageOp_RefetchesTrailingWindow(t *testing.T) {
	// The most-recent Copilot day is partial when first captured; its counts
	// finalize a day or two later. A same-/next-day run must re-pull the
	// trailing window so the day at (and just under) the prior watermark is
	// re-emitted with finalized counts — not skipped by the <= watermark filter.
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.CopilotMetricsRequest:
				metrics := []ghCopilotMetrics{
					{Date: "2024-01-14"},
					{Date: "2024-01-15"},
					{Date: "2024-01-16"},
					{Date: "2024-01-17"},
				}
				metricBytes := make([][]byte, len(metrics))
				for i, m := range metrics {
					metricBytes[i], _ = json.Marshal(m)
				}
				return &connectorpb.CopilotMetricsResponse{Metrics: metricBytes}, nil
			case *connectorpb.CopilotUserUsageRequest:
				return &connectorpb.CopilotUserUsageResponse{}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	// Prior run's watermark sat on 2024-01-16 (then a partial day).
	cursorData, _ := json.Marshal(map[string]string{"last_date": "2024-01-16"})
	cursor := &sdk.Cursor{Data: cursorData}
	params := testParams(sink, gw, cursor)

	op := &FetchCopilotUsageOp{}
	newCursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	ids := make([]string, 0)
	for _, r := range sink.Records() {
		ids = append(ids, r.ExternalID)
	}
	// With a 2-day finalization lag, the floor is 2024-01-14: that day is
	// skipped, while 15/16/17 re-emit. The key assertion is that 2024-01-16
	// (== prior watermark) is re-pulled, not locked out.
	assert.Equal(t, []string{
		"copilot-acme-2024-01-15",
		"copilot-acme-2024-01-16",
		"copilot-acme-2024-01-17",
	}, ids)

	// The persisted watermark must still advance to the true max — the trailing
	// re-fetch is a read-window concern, not a held-back cursor.
	var persisted struct {
		LastDate string `json:"last_date"`
	}
	require.NoError(t, json.Unmarshal(newCursor.Data, &persisted))
	assert.Equal(t, "2024-01-17", persisted.LastDate)
}

func TestFetchCopilotUsageOp_NotAvailable(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.CopilotMetricsRequest:
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: "fetch_copilot_usage",
					Kind:        sdk.ErrKindNotFound,
					Cause:       fmt.Errorf("not found"),
				}
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchCopilotUsageOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.Nil(t, cursor, "should return nil cursor when API not available")
	assert.Empty(t, sink.Records())
}

// --- Config error tests ---

func TestFetchPRsOp_InvalidConfig(t *testing.T) {
	gw := &mockVendorGateway{}
	sink := testkit.NewChannelSink()

	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-test",
			Credentials:    json.RawMessage(`{invalid}`),
			Settings:       mustJSON(GitHubSettings{Organization: "acme"}),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)

	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindConfigInvalid, syncErr.Kind)
}

// --- FetchPRCommentsOp tests ---

func TestFetchPRCommentsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const platformRepoID int64 = 9876
	const prAuthorID int64 = 200
	prAuthor := struct {
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	}{Login: "prauthor", DatabaseID: prAuthorID}

	tests := []struct {
		name      string
		commenter ghUser
		wantIsBot string
	}{
		{
			name:      "human commenter",
			commenter: ghUser{Login: "alice", ID: 300, Type: "User"},
			wantIsBot: "false",
		},
		{
			name:      "bot commenter via user.type",
			commenter: ghUser{Login: "custombot", ID: 400, Type: "Bot"},
			wantIsBot: "true",
		},
		{
			name:      "bot commenter via [bot] login suffix",
			commenter: ghUser{Login: "renovate[bot]", ID: 500, Type: ""},
			wantIsBot: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &mockVendorGateway{
				executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.GitHubGraphQLRequest:
						// GraphQL call to collect PR numbers + author + repo databaseId.
						return graphQLDataResponse(ghGraphQLData{
							Repository: ghGQLRepository{
								DatabaseID: platformRepoID,
								PullRequests: &ghGQLConnection[ghGQLPRNode]{
									Nodes: []ghGQLPRNode{
										{Number: 10, CreatedAt: now, UpdatedAt: now, Author: prAuthor},
									},
									PageInfo: ghPageInfo{HasNextPage: false},
								},
							},
						}), nil
					case *connectorpb.ListPRCommentsRequest:
						comment := ghPRComment{ID: 1001, User: tt.commenter, CreatedAt: now}
						b, _ := json.Marshal(comment)
						return &connectorpb.ListPRCommentsResponse{
							Comments:    [][]byte{b},
							HasNextPage: false,
						}, nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			params := testParams(sink, gw, nil)

			op := &FetchPRCommentsOp{}
			cursor, err := op.Execute(context.Background(), params)

			require.NoError(t, err)
			assert.NotNil(t, cursor)

			records := sink.Records()
			require.Len(t, records, 1)
			rec := records[0]
			assert.Equal(t, "acme/widgets#PR-10/COMMENT-1001", rec.ExternalID)
			assert.Equal(t, "fetch_pr_comments", rec.OperationID)
			assert.Equal(t, "acme/widgets#PR-10", rec.Metadata["pr_key"])
			assert.Equal(t, "prauthor", rec.Metadata["pr_author_login"])
			assert.Equal(t, "200", rec.Metadata["pr_author_id"])
			assert.Equal(t, "9876", rec.Metadata["platform_repo_id"])
			assert.Equal(t, tt.wantIsBot, rec.Metadata["reviewer_is_bot"])
			// pr_created_at is the parent PR's opened_at — the date anchor
			// a consumer uses to gate cross-day comments.
			assert.Equal(t, now.Format(time.RFC3339), rec.Metadata["pr_created_at"])
		})
	}
}

func TestFetchPRCommentsOp_SkipsOldComments(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-48 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitHubGraphQLRequest:
				return graphQLDataResponse(graphQLPRsData(
					[]ghGQLPRNode{
						{Number: 10, UpdatedAt: now, Author: struct {
							Login      string `json:"login"`
							DatabaseID int64  `json:"databaseId"`
						}{Login: "author", DatabaseID: 200}},
					},
					false, "",
				)), nil
			case *connectorpb.ListPRCommentsRequest:
				comments := []ghPRComment{
					{ID: 1001, User: ghUser{Login: "old-reviewer"}, CreatedAt: old},
					{ID: 1002, User: ghUser{Login: "new-reviewer"}, CreatedAt: now},
				}
				commentBytes := make([][]byte, len(comments))
				for i, c := range comments {
					commentBytes[i], _ = json.Marshal(c)
				}
				return &connectorpb.ListPRCommentsResponse{
					Comments:    commentBytes,
					HasNextPage: false,
				}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	// Set cursor to 24h ago — old comment should be skipped.
	since := now.Add(-24 * time.Hour)
	cursorData, _ := json.Marshal(map[string]any{
		"acme/widgets": map[string]any{"ts": since.Format(time.RFC3339Nano)},
	})
	cursor := &sdk.Cursor{Data: cursorData}
	params := testParams(sink, gw, cursor)

	op := &FetchPRCommentsOp{}
	_, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	records := sink.Records()
	require.Len(t, records, 1, "should only emit the new comment")
	assert.Equal(t, "acme/widgets#PR-10/COMMENT-1002", records[0].ExternalID)
}

func TestFetchPRCommentsOp_EmptyResponse(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitHubGraphQLRequest:
				return graphQLDataResponse(graphQLPRsData(nil, false, "")), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRCommentsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)
	assert.Empty(t, sink.Records())
}

// --- Helper function tests ---

func TestResolveRepoOwner(t *testing.T) {
	// Full "owner/repo" → owner from the name itself (the PAT + cross-owner fix).
	owner, repo := resolveRepoOwner("acme/widgets", "fallback-org")
	assert.Equal(t, "acme", owner)
	assert.Equal(t, "widgets", repo)

	// A bare name takes settings.organization as its owner.
	owner, repo = resolveRepoOwner("widgets", "acme-org")
	assert.Equal(t, "acme-org", owner)
	assert.Equal(t, "widgets", repo)

	// Bare name with no fallback (a PAT, which never stores bare names) → empty
	// owner, full string as repo; that repo simply yields nothing rather than
	// pulling under a wrong owner.
	owner, repo = resolveRepoOwner("widgets", "")
	assert.Empty(t, owner)
	assert.Equal(t, "widgets", repo)
}

// --- Integration test: Orchestrator + GitHub connector ---

func TestOrchestrator_GitHubConnector_Integration(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Set up a mock VG that responds to GraphQL PR queries.
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLPRsData(
				[]ghGQLPRNode{
					{Number: 1, UpdatedAt: now},
				},
				false, "",
			)), nil
		},
	}

	// Register connector.
	registry := sdk.NewConnectorRegistry()
	registry.Register(NewConnector(nil, 0))

	// Verify we can look up the connector and operation.
	conn, err := registry.Get("github")
	require.NoError(t, err)
	assert.Equal(t, sdk.ConnectorID("github"), conn.ID())

	opDesc, err := registry.GetOperation("github", "fetch_prs")
	require.NoError(t, err)
	assert.Equal(t, sdk.OperationID("fetch_prs"), opDesc.ID)

	// Create and execute the operation.
	op := opDesc.Factory()
	fetchOp, ok := op.(sdk.APIFetchOperation)
	require.True(t, ok, "fetch_prs should implement APIFetchOperation")

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	cursor, err := fetchOp.Execute(context.Background(), params)
	require.NoError(t, err)
	assert.NotNil(t, cursor)
	// Open PR emits 2 lifecycle records: #opened, #snapshot.
	assert.Len(t, sink.Records(), 2)
	assert.Equal(t, "acme/widgets#PR-1#opened", sink.Records()[0].ExternalID)
	assert.Equal(t, "acme/widgets#PR-1#snapshot", sink.Records()[1].ExternalID)
}

func TestFetchPRsOp_MetadataIncludesAuthorAndFiles(t *testing.T) {
	tests := []struct {
		name             string
		authorDatabaseID int64
		filePaths        []string
		wantFilesJSON    string
	}{
		{
			name:             "stores author id and file list",
			authorDatabaseID: 12345,
			filePaths:        []string{"src/auth/handler.go", "api/auth.proto"},
			wantFilesJSON:    `["src/auth/handler.go","api/auth.proto"]`,
		},
		{
			name:             "stores empty files list when pr has no files",
			authorDatabaseID: 67890,
			filePaths:        nil,
			wantFilesJSON:    `[]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			nodes := make([]struct {
				Path      string `json:"path"`
				Additions int    `json:"additions"`
				Deletions int    `json:"deletions"`
			}, len(tt.filePaths))
			for i, p := range tt.filePaths {
				nodes[i].Path = p
				nodes[i].Additions = 10 + i
				nodes[i].Deletions = 2 + i
			}
			pr := ghGQLPRNode{
				Number:      42,
				State:       "OPEN",
				CreatedAt:   now,
				UpdatedAt:   now,
				HeadRefName: "feat/test-branch",
				Author: struct {
					Login      string `json:"login"`
					DatabaseID int64  `json:"databaseId"`
				}{Login: "user", DatabaseID: tt.authorDatabaseID},
				Files: struct {
					Nodes []struct {
						Path      string `json:"path"`
						Additions int    `json:"additions"`
						Deletions int    `json:"deletions"`
					} `json:"nodes"`
					PageInfo struct {
						HasNextPage bool `json:"hasNextPage"`
					} `json:"pageInfo"`
				}{Nodes: nodes},
			}
			sink := testkit.NewChannelSink()
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					return graphQLDataResponse(graphQLPRsData([]ghGQLPRNode{pr}, false, "")), nil
				},
			}
			op := &FetchPRsOp{}
			_, err := op.Execute(context.Background(), testParams(sink, gw, nil))
			require.NoError(t, err)

			records := sink.Records()
			require.NotEmpty(t, records)
			for _, r := range records {
				meta := r.Metadata
				assert.Equal(t, fmt.Sprintf("%d", tt.authorDatabaseID), meta["pr_author_id"])
				assert.Equal(t, tt.wantFilesJSON, meta["files_changed"])
				assert.Equal(t, "feat/test-branch", meta["head_branch"])

				// files_line_stats should be valid JSON.
				var lineStats []json.RawMessage
				require.NoError(t, json.Unmarshal([]byte(meta["files_line_stats"]), &lineStats),
					"files_line_stats should be valid JSON array")
				assert.Len(t, lineStats, len(tt.filePaths),
					"files_line_stats should have one entry per file")
			}
		})
	}
}

func TestFetchPRsOp_RESTFallbackFailureUsesGraphQLData(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Build a PR where GraphQL truncated files (HasNextPage: true) and includes 2 partial files.
	pr := ghGQLPRNode{
		Number:      99,
		State:       "OPEN",
		CreatedAt:   now,
		UpdatedAt:   now,
		HeadRefName: "feat/large-pr",
		Author: struct {
			Login      string `json:"login"`
			DatabaseID int64  `json:"databaseId"`
		}{Login: "dev", DatabaseID: 555},
		Files: struct {
			Nodes []struct {
				Path      string `json:"path"`
				Additions int    `json:"additions"`
				Deletions int    `json:"deletions"`
			} `json:"nodes"`
			PageInfo struct {
				HasNextPage bool `json:"hasNextPage"`
			} `json:"pageInfo"`
		}{
			Nodes: []struct {
				Path      string `json:"path"`
				Additions int    `json:"additions"`
				Deletions int    `json:"deletions"`
			}{
				{Path: "file_a.go", Additions: 5, Deletions: 1},
				{Path: "file_b.go", Additions: 3, Deletions: 2},
			},
			PageInfo: struct {
				HasNextPage bool `json:"hasNextPage"`
			}{HasNextPage: true},
		},
	}

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitHubGraphQLRequest:
				return graphQLDataResponse(graphQLPRsData([]ghGQLPRNode{pr}, false, "")), nil
			case *connectorpb.ListPRFilesRequest:
				// REST fallback fails.
				return nil, fmt.Errorf("REST HTTP 502: Bad Gateway")
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	meta := records[0].Metadata

	// REST failed, so collectPRFileData should fall back to the 2 GraphQL files.
	var filePaths []string
	require.NoError(t, json.Unmarshal([]byte(meta["files_changed"]), &filePaths))
	assert.Equal(t, []string{"file_a.go", "file_b.go"}, filePaths)

	var lineStats []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(meta["files_line_stats"]), &lineStats))
	assert.Len(t, lineStats, 2, "files_line_stats should contain the 2 GraphQL files")
}

// --- Adaptive page size tests ---

func TestDoGraphQLAdaptive(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	since := now.Add(-24 * time.Hour)
	successResponse := graphQLDataResponse(graphQLPRsData(
		[]ghGQLPRNode{{Number: 99, UpdatedAt: now, State: "OPEN", CreatedAt: since}},
		false, "",
	))
	vendor502 := &sdk.SyncError{
		Kind: sdk.ErrKindVendorError, HTTPStatus: http.StatusBadGateway,
		Cause: errors.New("HTTP 502: Bad Gateway"), Retryable: true,
	}
	testPageCfg := &config.OperationPaginationConfig{
		PageSize:       25,
		MinPageSize:    1,
		BackoffDivisor: 2,
	}
	buildQuery := func(ps int) string {
		return buildPRsQuery("acme", "widgets", since, "", ps, nil, false)
	}

	t.Run("succeeds on first try without retry", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				callCount.Add(1)
				return successResponse, nil
			},
		}
		body, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.NoError(t, err)
		require.NotNil(t, body)
		assert.Equal(t, int32(1), callCount.Load())
	})

	t.Run("recovers after single 502", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				n := callCount.Add(1)
				if n == 1 {
					return nil, vendor502
				}
				return successResponse, nil
			},
		}
		body, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.NoError(t, err)
		require.NotNil(t, body)
		assert.Equal(t, int32(2), callCount.Load())
	})

	t.Run("halving sequence is 25 12 6 3 1", func(t *testing.T) {
		// Capture the query from each call to extract the page size.
		var queries []string
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				gqlReq := req.(*connectorpb.GitHubGraphQLRequest)
				queries = append(queries, gqlReq.GetQuery())
				return nil, vendor502
			},
		}
		_, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.Error(t, err)
		// Should have tried 5 times: 25, 12, 6, 3, 1
		assert.Len(t, queries, 5, "expected 5 attempts (page sizes: 25, 12, 6, 3, 1)")

		expectedSizes := []int{25, 12, 6, 3, 1}
		for i, expected := range expectedSizes {
			expectedQuery := buildPRsQuery("acme", "widgets", since, "", expected, nil, false)
			assert.Equal(t, expectedQuery, queries[i], "attempt %d should use page size %d", i+1, expected)
		}
	})

	t.Run("recovers at minimum page size 1", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				n := callCount.Add(1)
				// Fail on attempts 1-4 (sizes 25, 12, 6, 3), succeed on attempt 5 (size 1)
				if n < 5 {
					return nil, vendor502
				}
				return successResponse, nil
			},
		}
		body, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.NoError(t, err)
		require.NotNil(t, body)
		assert.Equal(t, int32(5), callCount.Load())
	})

	t.Run("gives up after all 5 attempts exhausted", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				callCount.Add(1)
				return nil, vendor502
			},
		}
		_, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.Error(t, err)
		assert.Equal(t, int32(5), callCount.Load(), "should attempt exactly 5 times before giving up")
		var syncErr *sdk.SyncError
		require.ErrorAs(t, err, &syncErr)
		assert.Equal(t, http.StatusBadGateway, syncErr.HTTPStatus)
	})

	t.Run("does not retry on 401 auth error", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				callCount.Add(1)
				return nil, &sdk.SyncError{
					Kind: sdk.ErrKindAuth, HTTPStatus: http.StatusUnauthorized,
					Cause: errors.New("HTTP 401: Unauthorized"),
				}
			},
		}
		_, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.Error(t, err)
		assert.Equal(t, int32(1), callCount.Load(), "auth error should not trigger retry")
	})

	t.Run("does not retry on 429 rate limit", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				callCount.Add(1)
				return nil, &sdk.SyncError{
					Kind: sdk.ErrKindRateLimit, HTTPStatus: http.StatusTooManyRequests,
					Cause: errors.New("HTTP 429: Too Many Requests"), Retryable: true,
				}
			},
		}
		_, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.Error(t, err)
		assert.Equal(t, int32(1), callCount.Load(), "rate limit should not trigger page size retry")
	})

	t.Run("does not retry on non-SyncError", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				callCount.Add(1)
				return nil, errors.New("network unreachable")
			},
		}
		_, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.Error(t, err)
		assert.Equal(t, int32(1), callCount.Load(), "generic error should not trigger retry")
	})

	t.Run("retries on 500 internal server error", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				n := callCount.Add(1)
				if n == 1 {
					return nil, &sdk.SyncError{
						Kind: sdk.ErrKindVendorError, HTTPStatus: http.StatusInternalServerError,
						Cause: errors.New("HTTP 500"), Retryable: true,
					}
				}
				return successResponse, nil
			},
		}
		body, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.NoError(t, err)
		require.NotNil(t, body)
		assert.Equal(t, int32(2), callCount.Load())
	})

	t.Run("retries on 503 service unavailable", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				n := callCount.Add(1)
				if n == 1 {
					return nil, &sdk.SyncError{
						Kind: sdk.ErrKindVendorError, HTTPStatus: http.StatusServiceUnavailable,
						Cause: errors.New("HTTP 503"), Retryable: true,
					}
				}
				return successResponse, nil
			},
		}
		body, err := doGraphQLAdaptive(context.Background(), gw, testPageCfg, buildQuery, "acme/widgets", false)
		require.NoError(t, err)
		require.NotNil(t, body)
		assert.Equal(t, int32(2), callCount.Load())
	})
}

func TestFetchPRsOp_AdaptivePageSizeEndToEnd(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	t.Run("502 then success emits records", func(t *testing.T) {
		var callCount atomic.Int32
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				n := callCount.Add(1)
				if n == 1 {
					return nil, &sdk.SyncError{
						Kind: sdk.ErrKindVendorError, HTTPStatus: http.StatusBadGateway,
						Cause: errors.New("HTTP 502: Bad Gateway"), Retryable: true,
					}
				}
				return graphQLDataResponse(graphQLPRsData(
					[]ghGQLPRNode{
						{Number: 99, UpdatedAt: now, State: "OPEN", CreatedAt: now.Add(-24 * time.Hour)},
					},
					false, "",
				)), nil
			},
		}

		sink := testkit.NewChannelSink()
		params := testParams(sink, gw, nil)
		op := &FetchPRsOp{}
		cursor, err := op.Execute(context.Background(), params)

		require.NoError(t, err)
		assert.NotNil(t, cursor)
		assert.GreaterOrEqual(t, int(callCount.Load()), 2)

		records := sink.Records()
		require.NotEmpty(t, records)
		assert.Equal(t, "acme/widgets#PR-99#opened", records[0].ExternalID)
	})

	t.Run("permanent 502 on the only repo fails the run as vendor_error", func(t *testing.T) {
		gw := &mockVendorGateway{
			executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return nil, &sdk.SyncError{
					Kind: sdk.ErrKindVendorError, HTTPStatus: http.StatusBadGateway,
					Cause: errors.New("HTTP 502: Bad Gateway"), Retryable: true,
				}
			},
		}

		sink := testkit.NewChannelSink()
		params := testParams(sink, gw, nil)
		op := &FetchPRsOp{}
		_, err := op.Execute(context.Background(), params)

		var syncErr *sdk.SyncError
		require.ErrorAs(t, err, &syncErr)
		assert.Equal(t, sdk.ErrKindVendorError, syncErr.Kind)
		assert.Empty(t, sink.Records())
	})
}

func TestIsVendorServerError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"502 Bad Gateway", &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 502}, true},
		{"500 Internal Server Error", &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 500}, true},
		{"503 Service Unavailable", &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 503}, true},
		{"401 Unauthorized", &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401}, false},
		{"429 Rate Limit", &sdk.SyncError{Kind: sdk.ErrKindRateLimit, HTTPStatus: 429}, false},
		{"404 Not Found", &sdk.SyncError{Kind: sdk.ErrKindNotFound, HTTPStatus: 404}, false},
		{"SyncError with no HTTP status", &sdk.SyncError{Kind: sdk.ErrKindInternal, HTTPStatus: 0}, false},
		{"wrapped SyncError 502", fmt.Errorf("graphql request: %w", &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 502}), true},
		{"wrapped SyncError 401", fmt.Errorf("graphql request: %w", &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401}), false},
		{"plain error", errors.New("something broke"), false},
		{"nil error", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isVendorServerError(tt.err))
		})
	}
}

func TestHasTimeRemaining(t *testing.T) {
	t.Parallel()

	t.Run("no reserve configured returns true", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()
		assert.True(t, hasTimeRemaining(ctx, 0))
	})

	t.Run("no deadline returns true", func(t *testing.T) {
		t.Parallel()
		assert.True(t, hasTimeRemaining(context.Background(), 30*time.Second))
	})

	t.Run("plenty of time returns true", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		assert.True(t, hasTimeRemaining(ctx, 30*time.Second))
	})

	t.Run("under reserve returns false", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		assert.False(t, hasTimeRemaining(ctx, 30*time.Second))
	})
}

func TestPrAlreadyProcessed(t *testing.T) {
	t.Parallel()

	assert.False(t, prAlreadyProcessed(nil, 42))
	assert.False(t, prAlreadyProcessed([]int{1, 2, 3}, 42))
	assert.True(t, prAlreadyProcessed([]int{1, 42, 3}, 42))
}

func TestOpPageConfig(t *testing.T) {
	t.Parallel()

	t.Run("returns defaults when config is nil", func(t *testing.T) {
		t.Parallel()
		cfg := opPageConfig(nil, opFetchPRs)
		assert.Equal(t, 25, cfg.PageSize)
		assert.Equal(t, 1, cfg.MinPageSize)
		assert.Equal(t, 2, cfg.BackoffDivisor)
	})

	t.Run("returns config value when set", func(t *testing.T) {
		t.Parallel()
		paginationCfg := &config.GitHubPaginationConfig{
			Operations: map[string]*config.OperationPaginationConfig{
				opFetchPRs: {PageSize: 10, MinPageSize: 2, BackoffDivisor: 3},
			},
		}
		cfg := opPageConfig(paginationCfg, opFetchPRs)
		assert.Equal(t, 10, cfg.PageSize)
		assert.Equal(t, 2, cfg.MinPageSize)
		assert.Equal(t, 3, cfg.BackoffDivisor)
	})

	t.Run("fixes zero BackoffDivisor", func(t *testing.T) {
		t.Parallel()
		paginationCfg := &config.GitHubPaginationConfig{
			Operations: map[string]*config.OperationPaginationConfig{
				opFetchPRs: {PageSize: 10, MinPageSize: 1, BackoffDivisor: 0},
			},
		}
		cfg := opPageConfig(paginationCfg, opFetchPRs)
		assert.Equal(t, 2, cfg.BackoffDivisor)
	})
}

// TestBuildIssuesQuery asserts that the GraphQL query produced by
// buildIssuesQuery contains the fields a consumer needs:
// the issue body and the first 25 comments with bodies, authors, and
// pagination signal.
func TestBuildIssuesQuery(t *testing.T) {
	since := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	q := buildIssuesQuery("acme", "widgets", since, "", 25, 25)

	tests := []struct {
		name     string
		fragment string
		desc     string
	}{
		{"body field present", "body\n", "issue body required by a consumer"},
		{"comments first 25", "comments(first: 25)", "fetch first 25 comments inline; pageInfo signals truncation"},
		{"comment id", "id\n", "node id required for stable comment identity"},
		{"comment body", "body\n", "comment body content required by issue-quality scoring"},
		{"comment author", "author { login ... on User { databaseId } }", "author login + databaseId for identity resolution"},
		{"comment createdAt", "createdAt\n", "comment timestamp required for ordering"},
		{"comment pageInfo", "pageInfo { hasNextPage endCursor }", "pageInfo signals truncation when issue has >25 comments"},
		{"comments totalCount preserved", "totalCount", "totalCount kept so any current consumer keeps reading the same value"},
		{"repository nameWithOwner per node", "repository { nameWithOwner }", "a consumer builds the issue key from repo.nameWithOwner + number"},
		{"milestone number", "milestone { number title state dueOn }", "milestone.number is the sprint key a consumer reads"},
		{"issueType selected", "issueType { name }", "the native issue type is a consumer's issue type; dropping it regresses issue_type to NULL"},
		{"parent selected", "parent { number repository { nameWithOwner } }", "sub-issue parent is a consumer's parent-issue link"},
		{"subIssues totalCount", "subIssues { totalCount }", "metadata.sub_issues_count a consumer reads"},
		{"existing field title preserved", "title\n", "existing fields must remain"},
		{"existing field stateReason preserved", "stateReason", "existing fields must remain"},
		{"existing field projectItems preserved", "projectItems(first: 10)", "existing fields must remain"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Contains(t, q, tt.fragment, tt.desc)
		})
	}
}

// TestBuildIssueCommentsTailQuery asserts the tail-fetch GraphQL query
// targets a specific issue and selects the same comment node fields the
// initial issues query selects, so downstream code can append seamlessly.
func TestBuildIssueCommentsTailQuery(t *testing.T) {
	q := buildIssueCommentsTailQuery("acme", "widgets", 42, "MTAw", 25)

	tests := []struct {
		name     string
		fragment string
	}{
		{"targets issue by number", "issue(number: 42)"},
		{"uses owner/repo path", `repository(owner: "acme", name: "widgets")`},
		{"selects comments page", "comments(first: 25, after: \"MTAw\")"},
		{"comment id selected", "id\n"},
		{"comment body selected", "body\n"},
		{"comment author selected", "author { login ... on User { databaseId } }"},
		{"comment createdAt selected", "createdAt\n"},
		{"pageInfo selected for chained pagination", "pageInfo { hasNextPage endCursor }"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Contains(t, q, tt.fragment)
		})
	}

	t.Run("first-page cursor is null when empty", func(t *testing.T) {
		q := buildIssueCommentsTailQuery("acme", "widgets", 42, "", 25)
		assert.Contains(t, q, "after: null")
		assert.NotContains(t, q, `after: ""`)
	})
}

// TestFetchIssuesOp_TailFetchComments verifies that when an issue's initial
// comments page reports hasNextPage=true, the connector issues a tail-fetch
// query and the resulting the record store payload contains comments from BOTH
// the initial page and the tail page concatenated.
func TestFetchIssuesOp_TailFetchComments(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	createdAt := now.Add(-time.Hour)

	// Build an issue that came back from the initial fetch with 1 comment
	// and hasNextPage=true to signal more comments exist.
	initialIssue := ghGQLIssueNode{
		Number: 7, UpdatedAt: now, State: "OPEN", CreatedAt: createdAt,
		Title: "Issue with overflow comments", Body: "issue body",
	}
	initialIssue.Comments.TotalCount = 27
	initialIssue.Comments.Nodes = []ghGQLIssueComment{
		{ID: "c1", Body: "first inline comment"},
	}
	initialIssue.Comments.PageInfo = ghPageInfo{HasNextPage: true, EndCursor: "TAIL_CURSOR_1"}

	// Tail page returns 2 more comments and signals end of pagination.
	tailPageJSON := []byte(`{
  "repository": {
    "issue": {
      "comments": {
        "nodes": [
          {"id": "c2", "body": "second comment", "author": {"login": "alice"}, "createdAt": "2026-05-03T07:00:00Z", "updatedAt": "2026-05-03T07:00:00Z"},
          {"id": "c3", "body": "third comment", "author": {"login": "bob"}, "createdAt": "2026-05-03T07:30:00Z", "updatedAt": "2026-05-03T07:30:00Z"}
        ],
        "pageInfo": {"hasNextPage": false, "endCursor": "TAIL_CURSOR_2"}
      }
    }
  }
}`)

	var capturedQueries []string
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			gqlReq := req.(*connectorpb.GitHubGraphQLRequest)
			capturedQueries = append(capturedQueries, gqlReq.GetQuery())

			if strings.Contains(gqlReq.GetQuery(), "issue(number: 7)") {
				return &connectorpb.GitHubGraphQLResponse{Data: tailPageJSON}, nil
			}
			return graphQLDataResponse(graphQLIssuesData(
				[]ghGQLIssueNode{initialIssue},
				false, "",
			)), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchIssuesOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(capturedQueries), 2, "expected initial issues query plus at least one tail query")

	// The emitted opened/snapshot records should both have all 3 comment bodies.
	records := sink.Records()
	require.Len(t, records, 2)
	for _, rec := range records {
		var emitted ghGQLIssueNode
		require.NoError(t, json.Unmarshal(rec.Payload, &emitted))
		require.Len(t, emitted.Comments.Nodes, 3, "all three comments (1 inline + 2 tail) should be in the emitted payload")
		assert.Equal(t, "c1", emitted.Comments.Nodes[0].ID)
		assert.Equal(t, "c2", emitted.Comments.Nodes[1].ID)
		assert.Equal(t, "c3", emitted.Comments.Nodes[2].ID)
		assert.Equal(t, "issue body", emitted.Body, "issue body must round-trip through the typed struct into the downstream record store")
		assert.False(t, emitted.Comments.PageInfo.HasNextPage, "PageInfo must reflect the final tail-page state, not the stale initial signal")
	}
}

// TestFetchPRs_CapturesCrossRepository verifies that isCrossRepository and
// headRepositoryOwner from the GitHub GraphQL PR node survive the connector
// round-trip and appear in the emitted record payload.
func TestFetchPRs_CapturesCrossRepository(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	pr := ghGQLPRNode{
		Number:            1,
		UpdatedAt:         now,
		State:             "OPEN",
		CreatedAt:         now.Add(-time.Hour),
		IsCrossRepository: true,
	}
	pr.HeadRepositoryOwner.Login = "forker"

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLPRsData([]ghGQLPRNode{pr}, false, "")), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records, "at least one PR record must be emitted")

	// Payload is the marshaled ghGQLPRNode — verify cross-repo fields survive.
	var payload struct {
		IsCrossRepository   bool `json:"isCrossRepository"`
		HeadRepositoryOwner struct {
			Login string `json:"login"`
		} `json:"headRepositoryOwner"`
	}
	require.NoError(t, json.Unmarshal(records[0].Payload, &payload))
	assert.True(t, payload.IsCrossRepository, "isCrossRepository must be true in the emitted payload")
	assert.Equal(t, "forker", payload.HeadRepositoryOwner.Login, "headRepositoryOwner.login must survive the round-trip")
}

// --- Composite-cursor readback shape-discriminator ---

// TestDecodeCursorStateEntry pins the readback shape-discriminator: a struct
// entry whose high-water timestamp is the zero value must decode as a struct
// with its ProcessedPRs intact (ok=true → no "unparseable" warning), while a
// genuinely non-JSON entry is rejected (ok=false → the warning is real). The
// prior "!Timestamp.IsZero()" discriminator misread the zero-ts struct as
// corrupt and discarded its resume state.
func TestDecodeCursorStateEntry(t *testing.T) {
	ts := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name          string
		raw           string
		wantOK        bool
		wantErr       bool
		wantTS        time.Time
		wantProcessed []int
	}{
		{
			name:          "struct with non-zero timestamp",
			raw:           `{"ts":"2025-01-02T03:04:05Z","prs":[10,20]}`,
			wantOK:        true,
			wantTS:        ts,
			wantProcessed: []int{10, 20},
		},
		{
			name:          "zero-timestamp struct keeps ProcessedPRs",
			raw:           `{"ts":"0001-01-01T00:00:00Z","sf":"2025-01-02T03:04:05Z","prs":[10]}`,
			wantOK:        true,
			wantTS:        time.Time{},
			wantProcessed: []int{10},
		},
		{
			name:   "a bare timestamp string is not an entry",
			raw:    `"2025-01-02T03:04:05Z"`,
			wantOK: false,
		},
		{
			name:    "struct shape but corrupt payload returns the decode error",
			raw:     `{"ts":"not-a-time"}`,
			wantOK:  false,
			wantErr: true,
		},
		{
			name:   "genuinely corrupt non-JSON (no shape match)",
			raw:    `garbage`,
			wantOK: false,
		},
		{
			name:   "empty entry",
			raw:    ``,
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, ok, err := decodeCursorStateEntry(json.RawMessage(tt.raw))
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.True(t, tt.wantTS.Equal(state.Timestamp), "timestamp: got %v want %v", state.Timestamp, tt.wantTS)
			assert.Equal(t, tt.wantProcessed, state.ProcessedPRs)
		})
	}
}

// TestFetchReviewsOp_ResumeSkipsProcessedPRs_ZeroTimestamp verifies the
// end-to-end resume: when a first-sync checkpoint carries ProcessedPRs but its
// high-water timestamp is still the zero value, only the unprocessed PRs get
// REST calls.
func TestFetchReviewsOp_ResumeSkipsProcessedPRs_ZeroTimestamp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	var requestedPRs []int32
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.GitHubGraphQLRequest:
				return graphQLDataResponse(graphQLPRsData(
					[]ghGQLPRNode{
						{Number: 10, UpdatedAt: now.Add(-2 * time.Hour)},
						{Number: 20, UpdatedAt: now.Add(-1 * time.Hour)},
						{Number: 30, UpdatedAt: now},
					},
					false, "",
				)), nil
			case *connectorpb.ListPRReviewsRequest:
				requestedPRs = append(requestedPRs, r.PrNumber)
				review := ghReview{ID: int64(r.PrNumber * 100), User: ghUser{Login: "rev"}, State: "APPROVED"}
				b, _ := json.Marshal(review)
				return &connectorpb.ListPRReviewsResponse{Reviews: [][]byte{b}, HasNextPage: false}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	// Timestamp deliberately left at the zero value — a struct-shape entry that
	// must still decode with its ProcessedPRs preserved.
	prevCursor := buildStateCursor(map[string]repoCursorState{
		"acme/widgets": {
			Since:        now.Add(-3 * time.Hour),
			ProcessedPRs: []int{10},
		},
	})

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, prevCursor)

	op := &FetchReviewsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, []int32{20, 30}, requestedPRs, "PR 10 must stay skipped despite the zero high-water timestamp")
}

// fakeDeadlineCtx reports a deadline the test flips from far-future to past on
// demand, so FetchPRCommentsOp's mid-repo deadline check trips exactly when the
// test decides — a deterministic stand-in for a wall-clock timeout.
type fakeDeadlineCtx struct {
	context.Context
	expired bool
}

func (c *fakeDeadlineCtx) Deadline() (time.Time, bool) {
	if c.expired {
		return time.Now().Add(-time.Hour), true
	}
	return time.Now().Add(time.Hour), true
}

func commentsGatewayFor(prs []ghGQLPRNode) *mockVendorGateway {
	return &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.GitHubGraphQLRequest:
				return graphQLDataResponse(ghGraphQLData{
					Repository: ghGQLRepository{
						DatabaseID:   9876,
						PullRequests: &ghGQLConnection[ghGQLPRNode]{Nodes: prs, PageInfo: ghPageInfo{HasNextPage: false}},
					},
				}), nil
			case *connectorpb.ListPRCommentsRequest:
				// One comment per PR, dated at the PR's own UpdatedAt so it always
				// clears the since floor the PR itself passed.
				var created time.Time
				for _, pr := range prs {
					if pr.Number == int(r.PrNumber) {
						created = pr.UpdatedAt
					}
				}
				comment := ghPRComment{ID: int64(r.PrNumber), User: ghUser{Login: "alice", Type: "User"}, CreatedAt: created}
				b, _ := json.Marshal(comment)
				return &connectorpb.ListPRCommentsResponse{Comments: [][]byte{b}, HasNextPage: false}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}
}

// Guards the highWater invariant documented on collectRecentPRsWithAuthor: a repo that
// yields no PRs must leave its watermark unset rather than persist the since floor.
func TestFetchPRCommentsAndReviews_DormantRepoDoesNotPersistSinceFloor(t *testing.T) {
	const repoKey = "acme/widgets"

	tests := []struct {
		name string
		run  func(*sdk.ExecutionParams) (*sdk.Cursor, error)
	}{
		{
			name: "fetch_pr_comments",
			run: func(p *sdk.ExecutionParams) (*sdk.Cursor, error) {
				return (&FetchPRCommentsOp{}).Execute(context.Background(), p)
			},
		},
		{
			name: "fetch_reviews",
			run: func(p *sdk.ExecutionParams) (*sdk.Cursor, error) {
				return (&FetchReviewsOp{}).Execute(context.Background(), p)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First sync (nil cursor) against a repo with no PRs at all.
			sink := testkit.NewChannelSink()
			params := testParams(sink, commentsGatewayFor(nil), nil)

			cursor, err := tt.run(params)
			require.NoError(t, err)
			require.NotNil(t, cursor)

			got := getRepoCursorState(cursor, repoKey)
			assert.True(t, got.Timestamp.IsZero(),
				"a repo that yielded no PRs must leave the high-water unset, not persist the since floor; got %s", got.Timestamp)

			// The floor is roughly one lookback old, so persisting it is exactly what
			// ages an entry past the cursor-age threshold with nothing able to move it.
			if !got.Timestamp.IsZero() {
				assert.Less(t, time.Since(got.Timestamp), 24*time.Hour,
					"persisted watermark is a stale synthetic floor, not observed data")
			}
		})
	}
}

// TestFetchPRCommentsOp_DeadlineDoesNotAdvanceCursorPastUnprocessedPRs guards the
// in-progress checkpoint invariant: when a run checkpoints one PR then hits the
// deadline mid-repo, the cursor must retain the old high-water (and the frozen
// since floor) so PRs still below latestUpdated are re-collected on resume rather
// than skipped by the next run's since filter.
func TestFetchPRCommentsOp_DeadlineDoesNotAdvanceCursorPastUnprocessedPRs(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const repoKey = "acme/widgets"
	prAuthor := struct {
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	}{Login: "prauthor", DatabaseID: 200}

	// Returned UPDATED_AT DESC, so PR 10 is the high-water; PR 20 sits below it
	// and is the one lost if the cursor advances past it.
	prs := []ghGQLPRNode{
		{Number: 10, CreatedAt: now, UpdatedAt: now, Author: prAuthor},
		{Number: 20, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour), Author: prAuthor},
	}

	// Run 1: trip the deadline mid-repo. The per-PR checkpoint (after PR 10)
	// expires the context, so the next per-PR deadline check fails before PR 20.
	dctx := &fakeDeadlineCtx{Context: context.Background()}
	run1Sink := testkit.NewChannelSink()
	run1Params := testParams(run1Sink, commentsGatewayFor(prs), nil)
	run1Params.Checkpoint = func(_ context.Context, _ *sdk.Cursor) error {
		dctx.expired = true
		return nil
	}

	run1Cursor, err := (&FetchPRCommentsOp{checkpointReserve: time.Minute}).Execute(dctx, run1Params)
	require.NoError(t, err)

	// PR 10 processed, PR 20 not — the deadline tripped between them.
	run1Records := run1Sink.Records()
	require.Len(t, run1Records, 1, "only PR 10's comment should be fetched before the deadline")
	assert.Equal(t, repoKey+"#PR-10/COMMENT-10", run1Records[0].ExternalID)

	// The in-progress checkpoint must keep the old (zero) high-water and persist
	// the frozen since floor, so the resume never advances past unprocessed PRs.
	run1State := getRepoCursorState(run1Cursor, repoKey)
	assert.Equal(t, []int{10}, run1State.ProcessedCommentPRs)
	assert.True(t, run1State.Timestamp.IsZero(), "watermark must not advance while PRs remain unprocessed")
	assert.False(t, run1State.Since.IsZero(), "the since floor must be frozen into the in-progress checkpoint")

	// Run 2: resume. PR 10 is skipped (already processed); PR 20 is re-collected
	// and emitted. A cursor that skipped PR 20 would make this run emit nothing.
	run2Sink := testkit.NewChannelSink()
	run2Params := testParams(run2Sink, commentsGatewayFor(prs), run1Cursor)
	_, err = (&FetchPRCommentsOp{}).Execute(context.Background(), run2Params)
	require.NoError(t, err)

	run2Records := run2Sink.Records()
	require.Len(t, run2Records, 1, "resume must re-collect the unprocessed PR 20's comment")
	assert.Equal(t, repoKey+"#PR-20/COMMENT-20", run2Records[0].ExternalID)
	assert.Equal(t, "20", run2Records[0].Metadata["pr_number"])
}

// TestFetchPRCommentsOp_ResumeUsesFrozenSinceFloor guards the first-sync variant
// of the same loss: a first sync leaves the high-water at zero, so recomputing
// the since floor each resume would slide it forward with wall-clock time and
// drop PRs near the lookback boundary. The resume must reuse the floor frozen
// into the in-progress checkpoint instead.
func TestFetchPRCommentsOp_ResumeUsesFrozenSinceFloor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const repoKey = "acme/widgets"
	prAuthor := struct {
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	}{Login: "prauthor", DatabaseID: 200}

	// PR 30 sits below a freshly-recomputed floor (default lookback 365d) but
	// above the frozen floor stored from the interrupted first sync — so it is
	// re-collected only if the resume reads state.Since rather than recomputing.
	frozenSince := now.Add(-400 * 24 * time.Hour)
	prUpdated := now.Add(-380 * 24 * time.Hour)
	prs := []ghGQLPRNode{{Number: 30, CreatedAt: prUpdated, UpdatedAt: prUpdated, Author: prAuthor}}

	// Interrupted first sync: high-water still zero, floor frozen at frozenSince.
	prevCursor := buildStateCursor(map[string]repoCursorState{
		repoKey: {Since: frozenSince, ProcessedCommentPRs: []int{10}},
	})

	sink := testkit.NewChannelSink()
	params := testParams(sink, commentsGatewayFor(prs), prevCursor)
	_, err := (&FetchPRCommentsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "PR 30 (below a fresh floor, above the frozen floor) must be re-collected via state.Since")
	assert.Equal(t, repoKey+"#PR-30/COMMENT-30", records[0].ExternalID)
}

// A repository that fails keeps its stored cursor state, so the next run
// fetches what this one could not; only the repositories that succeeded
// advance.
func TestFetchPRsOp_FailedRepoKeepsItsCursorState(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prior := now.Add(-48 * time.Hour)
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if strings.Contains(req.(*connectorpb.GitHubGraphQLRequest).GetQuery(), "failing-repo") {
				return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 502, Cause: errors.New("502 Bad Gateway")}
			}
			return graphQLDataResponse(graphQLPRsData([]ghGQLPRNode{{Number: 7, UpdatedAt: now, State: "OPEN", CreatedAt: now.Add(-time.Hour)}}, false, "")), nil
		},
	}
	params := testParams(testkit.NewChannelSink(), gw, buildStateCursor(map[string]repoCursorState{
		"acme/failing-repo": {Timestamp: prior},
		"acme/healthy-repo": {Timestamp: prior},
	}))
	params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/failing-repo", "acme/healthy-repo"}})

	cursor, err := (&FetchPRsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.True(t, getRepoCursorState(cursor, "acme/failing-repo").Timestamp.Equal(prior), "the failed repository's watermark is unchanged")
	assert.True(t, getRepoCursorState(cursor, "acme/healthy-repo").Timestamp.Equal(now), "the healthy repository advances")
}
