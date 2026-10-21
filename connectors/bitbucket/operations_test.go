// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	bitbucketpb "github.com/tetriz-ai/tetriz-connectors/gen/bitbucket/v1"
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
			ConnectorID:    "bitbucket",
			OrganizationID: "org-test",
			Credentials: mustJSON(BitbucketCredentials{
				OAuthAccessToken: "test-oauth-token",
			}),
			Settings: mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{"myworkspace/myrepo"},
			}),
			Enabled: true,
		},
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gw,
	}
}

func bbReposResponse(repos []bbRepository, hasNext bool) *connectorpb.BitbucketListReposResponse {
	repoBytes := make([][]byte, len(repos))
	for i, r := range repos {
		repoBytes[i], _ = json.Marshal(r)
	}
	nextPage := ""
	if hasNext {
		nextPage = "https://api.bitbucket.org/2.0/repositories/myworkspace?page=2"
	}
	return &connectorpb.BitbucketListReposResponse{
		Repos:       repoBytes,
		NextPageUrl: nextPage,
	}
}

func bbPRsResponse(prs []bbPullRequest, hasNext bool) *connectorpb.BitbucketListPullRequestsResponse {
	prBytes := make([][]byte, len(prs))
	for i, pr := range prs {
		prBytes[i], _ = json.Marshal(pr)
	}
	nextPage := ""
	if hasNext {
		nextPage = "https://api.bitbucket.org/2.0/repositories/myworkspace/myrepo/pullrequests?page=2"
	}
	return &connectorpb.BitbucketListPullRequestsResponse{
		PullRequests: prBytes,
		NextPageUrl:  nextPage,
	}
}

func bbCommitsResponse(commits []bbCommit, hasNext bool) *connectorpb.BitbucketListCommitsResponse {
	commitBytes := make([][]byte, len(commits))
	for i, c := range commits {
		commitBytes[i], _ = json.Marshal(c)
	}
	nextPage := ""
	if hasNext {
		nextPage = "https://api.bitbucket.org/2.0/repositories/myworkspace/myrepo/commits?page=2"
	}
	return &connectorpb.BitbucketListCommitsResponse{
		Commits:     commitBytes,
		NextPageUrl: nextPage,
	}
}

// --- FetchReposOp tests ---
// NOTE: FetchReposOp now emits proto-marshaled RepositoryRecord payloads with
// platform_repo_id (UUID) as ExternalID and full_name in metadata. See fetch_repos.go.

// --- FetchPRsOp tests ---

func bbDiffstatResponse(added, removed int) *connectorpb.BitbucketGetPRDiffstatResponse {
	entry := bbDiffstatEntry{LinesAdded: added, LinesRemoved: removed, Status: "modified"}
	entryBytes, _ := json.Marshal(entry)
	return &connectorpb.BitbucketGetPRDiffstatResponse{
		DiffstatEntries: [][]byte{entryBytes},
	}
}

func bbMembersResponse(members []bbWorkspaceMember, hasNext bool) *connectorpb.BitbucketListWorkspaceMembersResponse {
	memberBytes := make([][]byte, len(members))
	for i, m := range members {
		memberBytes[i], _ = json.Marshal(m)
	}
	nextPage := ""
	if hasNext {
		nextPage = "https://api.bitbucket.org/2.0/workspaces/myworkspace/members?page=2"
	}
	return &connectorpb.BitbucketListWorkspaceMembersResponse{
		Members:     memberBytes,
		NextPageUrl: nextPage,
	}
}

func bbPRCommitsResponse(commits []bbCommit, hasNext bool) *connectorpb.BitbucketListPRCommitsResponse {
	commitBytes := make([][]byte, len(commits))
	for i, c := range commits {
		commitBytes[i], _ = json.Marshal(c)
	}
	nextPage := ""
	if hasNext {
		nextPage = "https://api.bitbucket.org/2.0/repositories/myworkspace/myrepo/pullrequests/42/commits?page=2"
	}
	return &connectorpb.BitbucketListPRCommitsResponse{
		Commits:     commitBytes,
		NextPageUrl: nextPage,
	}
}

func TestFetchPRsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	firstCommit := now.Add(-3 * time.Hour)
	laterCommit := now.Add(-90 * time.Minute)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 42, Title: "Add feature", State: "OPEN", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
					{ID: 41, Title: "Fix bug", State: "MERGED", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now.Add(-time.Hour)},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return bbDiffstatResponse(10, 5), nil
			case *connectorpb.BitbucketListPRCommitsRequest:
				return bbPRCommitsResponse([]bbCommit{
					{Hash: "later", Date: laterCommit},
					{Hash: "first", Date: firstCommit},
				}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	records := sink.Records()
	// PR-42 (OPEN): opened + snapshot = 2 records
	// PR-41 (MERGED): opened + snapshot + merged = 3 records
	// Total = 5 records
	require.Len(t, records, 5)
	assert.Equal(t, "myworkspace/myrepo#PR-42#opened", records[0].ExternalID)
	assert.Equal(t, "myworkspace/myrepo#PR-42#snapshot", records[1].ExternalID)
	assert.Equal(t, "myworkspace/myrepo#PR-41#opened", records[2].ExternalID)
	assert.Equal(t, "myworkspace/myrepo#PR-41#snapshot", records[3].ExternalID)
	assert.Equal(t, "myworkspace/myrepo#PR-41#merged", records[4].ExternalID)
	// Events are dated by the event; the snapshot is current state and has no
	// DataDate, so it stays one row in a date-keyed sink (ADR-0003 amendment).
	assert.False(t, records[0].DataDate.IsZero(), "#opened is dated by created_on")
	assert.True(t, records[1].DataDate.IsZero(), "#snapshot has no DataDate")
	assert.True(t, records[3].DataDate.IsZero(), "#snapshot has no DataDate")
	assert.False(t, records[4].DataDate.IsZero(), "#merged is dated")
	assert.Equal(t, "fetch_prs", records[0].OperationID)
	assert.Equal(t, "bitbucket", records[0].ConnectorID)
	assert.Equal(t, "org-test", records[0].OrganizationID)
	assert.Equal(t, "myworkspace/myrepo", records[0].Metadata["repo"])
	assert.Equal(t, "opened", records[0].Metadata["event_type"])
	assert.Equal(t, "merged", records[4].Metadata["event_type"])
	// All records for each PR share the same baseMeta, so first_commit_at is present on all.
	assert.Equal(t, firstCommit.UTC().Format(time.RFC3339), records[0].Metadata["first_commit_at"],
		"opened record should carry earliest first_commit_at from the chained commits page")
}

func TestFetchPRsOp_IncrementalSync(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	since := now.Add(-2 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 43, Title: "New PR", State: "OPEN", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
					{ID: 42, Title: "Old PR", State: "MERGED", CreatedOn: since.Add(-2 * time.Hour), UpdatedOn: since.Add(-time.Minute)},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return bbDiffstatResponse(3, 1), nil
			case *connectorpb.BitbucketListPRCommitsRequest:
				return bbPRCommitsResponse([]bbCommit{{Hash: "xyz", Date: now.Add(-90 * time.Minute)}}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	cursor := prsCursor(map[string]time.Time{"myworkspace/myrepo": since})
	params := testParams(sink, gw, cursor)

	op := &FetchPRsOp{}
	newCursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, newCursor)

	records := sink.Records()
	// PR-43 (OPEN, newer than cursor): opened + snapshot = 2 records
	// PR-42 (older than cursor): skipped
	require.Len(t, records, 2, "should only emit PRs newer than cursor")
	assert.Equal(t, "myworkspace/myrepo#PR-43#opened", records[0].ExternalID)
	assert.Equal(t, "myworkspace/myrepo#PR-43#snapshot", records[1].ExternalID)
}

func TestFetchPRsOp_EmptyResponse(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.Empty(t, sink.Records())
	// A first sync that observed nothing records nothing: with no incoming
	// cursor and an empty vendor response there is no watermark to persist, so the
	// incoming (nil) cursor comes back — writing the synthetic `since` floor here is
	// exactly the pin the cursor loaders exist to drop.
	assert.Nil(t, cursor)
}

func TestFetchPRsOp_APIError(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, fmt.Errorf("REST HTTP 401: Unauthorized")
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestFetchPRsOp_RateLimitError(t *testing.T) {
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

	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limit")
}

// TestFetchPRsOp_WaitsOutLocalRateLimit pins, at the op level, that the
// local limiter does not starve the run: a rejection
// from OUR OWN limiter on the first PR-list page must be waited out and the run
// must complete, while a vendor 429 must still abort the run (the vendor is
// pacing us — waiting would stack two waits for one rejection). Mirrors
// fetch_repo_tree_test.go's TestGetJSONWaitsOutLocalRateLimit /
// TestGetJSONAbortsOnVendorRateLimit at the operation level.
func TestFetchPRsOp_WaitsOutLocalRateLimit(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Shaped exactly like the local limiter's rejection: kind rate_limit, cause
	// wrapping the ErrLocalRateLimit sentinel, carrying the sized wait.
	localLimit := &sdk.SyncError{
		ConnectorID:       connectorID,
		OperationID:       "fetch_prs",
		Kind:              sdk.ErrKindRateLimit,
		Retryable:         true,
		Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
		RetryAfterSeconds: 1,
	}
	// A vendor 429 carries no local sentinel — the vendor wants us to stop.
	vendor429 := &sdk.SyncError{
		ConnectorID:       connectorID,
		OperationID:       "fetch_prs",
		Kind:              sdk.ErrKindRateLimit,
		HTTPStatus:        429,
		Cause:             fmt.Errorf("bitbucket: 429 Too Many Requests"),
		RetryAfterSeconds: 60,
	}

	tests := []struct {
		name         string
		firstPageErr error // returned on the first PR-list call, then success
		wantErr      bool
		wantPRCalls  int // expected PR-list Execute attempts
	}{
		{
			name:         "local limit on first page is waited out and the run completes",
			firstPageErr: localLimit,
			wantErr:      false,
			wantPRCalls:  2, // rejected once, retried after the wait
		},
		{
			name:         "vendor 429 aborts the run on the first attempt",
			firstPageErr: vendor429,
			wantErr:      true,
			wantPRCalls:  1, // no retry
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prListCalls := 0
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.BitbucketListPullRequestsRequest:
						prListCalls++
						if prListCalls == 1 {
							return nil, tt.firstPageErr
						}
						return bbPRsResponse([]bbPullRequest{
							{ID: 42, Title: "Add feature", State: "OPEN", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
						}, false), nil
					case *connectorpb.BitbucketGetPRDiffstatRequest:
						return bbDiffstatResponse(10, 5), nil
					case *connectorpb.BitbucketListPRCommitsRequest:
						return bbPRCommitsResponse([]bbCommit{{Hash: "first", Date: now.Add(-3 * time.Hour)}}, false), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			params := testParams(sink, gw, nil)

			// A deadline ample for the 1s local-limit wait (which needs > 1s plus
			// the 90s safety margin); the wait is real but tiny.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			op := &FetchPRsOp{}
			_, err := op.Execute(ctx, params)

			assert.Equal(t, tt.wantPRCalls, prListCalls)
			if tt.wantErr {
				require.Error(t, err, "a vendor 429 must fail the run")
				assert.Empty(t, sink.Records())
			} else {
				require.NoError(t, err, "a local-limiter rejection must be waited out, not fail the run")
				assert.NotEmpty(t, sink.Records(), "the retried page's PRs must be emitted")
			}
		})
	}
}

// fetchIssuesForRepo must treat ONLY a 404 (issue tracker disabled per-repo) as
// "no data"; a rate-limit/auth/5xx has to surface as an error rather than be
// reported as a clean run that fetched nothing.
func TestFetchIssuesForRepo_SyncErrorKindHandling(t *testing.T) {
	t.Parallel()
	since := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		kind    sdk.SyncErrorKind
		wantErr bool
	}{
		{name: "not found is swallowed as no data", kind: sdk.ErrKindNotFound, wantErr: false},
		{name: "rate limit surfaces", kind: sdk.ErrKindRateLimit, wantErr: true},
		{name: "auth failure surfaces", kind: sdk.ErrKindAuth, wantErr: true},
		{name: "vendor 5xx surfaces", kind: sdk.ErrKindVendorError, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := &mockVendorGateway{
				executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
					return nil, &sdk.SyncError{
						ConnectorID: connectorID,
						OperationID: "fetch_issues",
						Kind:        tt.kind,
						Cause:       fmt.Errorf("boom"),
					}
				},
			}
			sink := testkit.NewChannelSink()

			got, err := fetchIssuesForRepo(context.Background(), gw, sink, "fetch_issues",
				"org-test", "myworkspace", "myrepo", "myworkspace/myrepo", since)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "fetch issues for myworkspace/myrepo")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, since, got)
		})
	}
}

// --- FetchCommitsOp tests ---

func TestFetchCommitsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return bbCommitsResponse([]bbCommit{
				{Hash: "abc123def456", Date: now, Message: "Fix bug"},
				{Hash: "789ghi012jkl", Date: now.Add(-time.Hour), Message: "Add feature"},
			}, false), nil
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
	assert.Equal(t, "myworkspace/myrepo#abc123def456", records[0].ExternalID)
	assert.Equal(t, "myworkspace/myrepo#789ghi012jkl", records[1].ExternalID)
	assert.Equal(t, "fetch_commits", records[0].OperationID)
}

func TestFetchCommitsOp_IncrementalSync(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	since := now.Add(-2 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return bbCommitsResponse([]bbCommit{
				{Hash: "new123", Date: now, Message: "New commit"},
				{Hash: "old456", Date: since.Add(-time.Minute), Message: "Old commit"},
			}, false), nil
		},
	}

	sink := testkit.NewChannelSink()
	cursor := buildCompositeCursor(map[string]time.Time{"myworkspace/myrepo": since})
	params := testParams(sink, gw, cursor)

	op := &FetchCommitsOp{}
	newCursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, newCursor)

	records := sink.Records()
	require.Len(t, records, 1, "should only emit commits newer than cursor")
	assert.Equal(t, "myworkspace/myrepo#new123", records[0].ExternalID)
}

// --- Config error tests ---

func TestFetchPRsOp_InvalidConfig(t *testing.T) {
	gw := &mockVendorGateway{}
	sink := testkit.NewChannelSink()

	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-test",
			Credentials:    json.RawMessage(`{invalid}`),
			Settings:       mustJSON(BitbucketSettings{Workspace: "myworkspace"}),
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

// --- Helper function tests ---

func TestSplitFullName(t *testing.T) {
	workspace, repo, err := splitFullName("myworkspace/myrepo")
	require.NoError(t, err)
	assert.Equal(t, "myworkspace", workspace)
	assert.Equal(t, "myrepo", repo)

	_, _, err = splitFullName("invalid")
	assert.Error(t, err)
}

// --- Integration test: Orchestrator + Bitbucket connector ---

func TestOrchestrator_BitbucketConnector_Integration(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 1, Title: "First PR", State: "OPEN", UpdatedOn: now},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return bbDiffstatResponse(7, 2), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	registry := sdk.NewConnectorRegistry()
	registry.Register(NewConnector(nil))

	conn, err := registry.Get("bitbucket")
	require.NoError(t, err)
	assert.Equal(t, sdk.ConnectorID("bitbucket"), conn.ID())

	opDesc, err := registry.GetOperation("bitbucket", "fetch_prs")
	require.NoError(t, err)
	assert.Equal(t, sdk.OperationID("fetch_prs"), opDesc.ID)

	op := opDesc.Factory()
	fetchOp, ok := op.(sdk.APIFetchOperation)
	require.True(t, ok, "fetch_prs should implement APIFetchOperation")

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	cursor, err := fetchOp.Execute(context.Background(), params)
	require.NoError(t, err)
	assert.NotNil(t, cursor)
	// PR-1 (OPEN): opened + snapshot = 2 records
	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "myworkspace/myrepo#PR-1#opened", records[0].ExternalID)
	assert.Equal(t, "myworkspace/myrepo#PR-1#snapshot", records[1].ExternalID)
}

// --- FetchPRsOp enrichment tests ---

func TestFetchPRsOp_EnrichmentIncludesAdditionsDeletions(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 99, Title: "Enriched PR", State: "OPEN", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return bbDiffstatResponse(20, 8), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 2)

	// Verify enriched payload contains additions and deletions.
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(records[0].Payload, &payload))
	assert.Contains(t, payload, "additions")
	assert.Contains(t, payload, "deletions")

	var additions, deletions int
	require.NoError(t, json.Unmarshal(payload["additions"], &additions))
	require.NoError(t, json.Unmarshal(payload["deletions"], &deletions))
	assert.Equal(t, 20, additions)
	assert.Equal(t, 8, deletions)
}

func TestFetchPRsOp_EnrichmentFailureRetainsOriginalPayload(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 55, Title: "PR with failed enrichment", State: "OPEN", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return nil, fmt.Errorf("diffstat API unavailable")
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	// Enrichment failure should NOT fail the overall sync.
	require.NoError(t, err)

	records := sink.Records()
	// PR-55 (OPEN): opened + snapshot = 2 records (with original payload)
	require.Len(t, records, 2)
	assert.Equal(t, "myworkspace/myrepo#PR-55#opened", records[0].ExternalID)
	// files_line_stats MUST be omitted on enrichment failure — an "[]" here is
	// indistinguishable from a real empty diff and the DAO COALESCEs it over the
	// stored stats, so a transient diffstat error would erase real path coverage.
	for _, r := range records {
		_, present := r.Metadata["files_line_stats"]
		assert.Falsef(t, present, "enrichment failure must omit files_line_stats (record %s)", r.ExternalID)
	}
}

func TestFetchPRsOp_EmptyDiffstatStampsFilesLineStats(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 56, Title: "PR with empty diffstat", State: "OPEN", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				// Diffstat succeeds with no entries — a genuinely empty diff.
				return &connectorpb.BitbucketGetPRDiffstatResponse{DiffstatEntries: nil}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	// A nil-error empty diffstat IS an authoritative empty answer — the key is stamped
	// as "[]" (measured), unlike the omit-on-failure and omit-on-degraded cases.
	v, present := records[0].Metadata["files_line_stats"]
	require.True(t, present, "a successful empty diffstat must stamp files_line_stats so the PR is measured, not unmeasurable")
	assert.Equal(t, "[]", v, "an empty successful diffstat stamps an empty array")
}

func TestFetchPRsOp_DegradedEnrichmentOmitsFilesLineStats(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 57, Title: "PR with degraded diffstat", State: "OPEN", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				// One valid entry + one unparseable entry → enrichPR returns a
				// nil error but marks the aggregate Degraded (incomplete file list).
				valid, _ := json.Marshal(bbDiffstatEntry{
					LinesAdded: 5, LinesRemoved: 1, Status: "modified",
					New: &bbDiffstatFile{Path: "a.go"},
				})
				return &connectorpb.BitbucketGetPRDiffstatResponse{
					DiffstatEntries: [][]byte{valid, []byte("{not json")},
				}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	// A degraded (incomplete) aggregate is not trustworthy enough to persist: the
	// partial list would COALESCE over complete stored stats. Omit the key, mirroring
	// the diffstat cache's `enrichErr == nil && !Degraded` trust guard.
	_, present := records[0].Metadata["files_line_stats"]
	assert.False(t, present, "a degraded enrichment must omit files_line_stats so a partial list cannot overwrite stored stats")
}

func TestFetchPRsOp_MetadataIncludesAuthorBranchAndFiles(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{
						ID:        77,
						Title:     "Feature PR",
						State:     "OPEN",
						CreatedOn: now.Add(-time.Hour),
						UpdatedOn: now,
						Author:    &bbUser{AccountID: "557058:test-author", DisplayName: "Test Author"},
						Source:    &bbPRRef{Branch: &bbBranch{Name: "feat/my-feature"}},
					},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				// Return a diffstat with file-level entries including paths.
				entries := []bbDiffstatEntry{
					{
						LinesAdded: 15, LinesRemoved: 3, Status: "modified",
						New: &bbDiffstatFile{Path: "src/handler.go"},
						Old: &bbDiffstatFile{Path: "src/handler.go"},
					},
					{
						LinesAdded: 8, LinesRemoved: 0, Status: "added",
						New: &bbDiffstatFile{Path: "api/service.proto"},
					},
				}
				entryBytes := make([][]byte, len(entries))
				for i, e := range entries {
					entryBytes[i], _ = json.Marshal(e)
				}
				return &connectorpb.BitbucketGetPRDiffstatResponse{
					DiffstatEntries: entryBytes,
				}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	meta := records[0].Metadata
	assert.Equal(t, "557058:test-author", meta["pr_author_id"])
	assert.Equal(t, "feat/my-feature", meta["head_branch"])
	assert.NotEmpty(t, meta["files_changed"])
	assert.NotEmpty(t, meta["files_line_stats"])

	// Verify files_changed is a valid JSON array with expected paths.
	var filePaths []string
	require.NoError(t, json.Unmarshal([]byte(meta["files_changed"]), &filePaths))
	assert.Equal(t, []string{"src/handler.go", "api/service.proto"}, filePaths)

	// Verify files_line_stats is a valid JSON array with per-file stats.
	var lineStats []bbPRFileLineStat
	require.NoError(t, json.Unmarshal([]byte(meta["files_line_stats"]), &lineStats))
	require.Len(t, lineStats, 2)
	assert.Equal(t, "src/handler.go", lineStats[0].Path)
	assert.Equal(t, 15, lineStats[0].Additions)
	assert.Equal(t, 3, lineStats[0].Deletions)
	assert.Equal(t, "api/service.proto", lineStats[1].Path)
	assert.Equal(t, 8, lineStats[1].Additions)
	assert.Equal(t, 0, lineStats[1].Deletions)
}

func TestFetchPRsOp_CommitsFetchFailureDoesNotDropPR(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 42, Title: "Add feature", State: "OPEN", CreatedOn: now.Add(-time.Hour), UpdatedOn: now},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return bbDiffstatResponse(5, 2), nil
			case *connectorpb.BitbucketListPRCommitsRequest:
				return nil, fmt.Errorf("REST HTTP 500: Internal Server Error")
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err, "Execute should not fail when chained commits call errors — graceful degradation")

	records := sink.Records()
	// PR-42 is OPEN: opened + snapshot = 2 records, all without first_commit_at.
	require.NotEmpty(t, records, "PR should still be emitted even when chained commits call fails")
	for _, r := range records {
		if r.OperationID == "fetch_prs" {
			assert.Empty(t, r.Metadata["first_commit_at"],
				"PR record should NOT carry first_commit_at when chained commits call failed")
		}
	}
}

// --- FetchWorkspaceMembersOp tests ---

func TestFetchWorkspaceMembersOp_Execute(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListWorkspaceMembersRequest:
				return bbMembersResponse([]bbWorkspaceMember{
					{User: &bbUser{AccountID: "acc-1", DisplayName: "Alice", Nickname: "alice"}},
					{User: &bbUser{AccountID: "acc-2", DisplayName: "Bob", Nickname: "bob"}},
				}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorkspaceMembersOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "member-acc-1", records[0].ExternalID)
	assert.Equal(t, "member-acc-2", records[1].ExternalID)
	assert.Equal(t, "fetch_workspace_members", records[0].OperationID)
	assert.Equal(t, "bitbucket", records[0].ConnectorID)
	assert.Equal(t, "org-test", records[0].OrganizationID)
	assert.Equal(t, "myworkspace", records[0].Metadata["workspace"])
	for _, r := range records {
		assert.True(t, r.DataDate.IsZero(), "a member is current state: no DataDate, or every run is a new row")
	}
}

func TestFetchWorkspaceMembersOp_EmptyResponse(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return bbMembersResponse(nil, false), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorkspaceMembersOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)
	assert.Empty(t, sink.Records())
}

func TestFetchWorkspaceMembersOp_APIError(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, fmt.Errorf("REST HTTP 403: Forbidden")
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorkspaceMembersOp{}
	_, err := op.Execute(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
}

// --- FetchPRCommentsOp tests ---

func bbCommentsResponse(comments []bbComment, hasNext bool) *connectorpb.BitbucketListPRCommentsResponse {
	commentBytes := make([][]byte, len(comments))
	for i, c := range comments {
		commentBytes[i], _ = json.Marshal(c)
	}
	nextPage := ""
	if hasNext {
		nextPage = "https://api.bitbucket.org/2.0/repositories/myworkspace/myrepo/pullrequests/1/comments?page=2"
	}
	return &connectorpb.BitbucketListPRCommentsResponse{
		Comments:    commentBytes,
		NextPageUrl: nextPage,
	}
}

func TestFetchPRCommentsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{
						ID:        1,
						CreatedOn: now.Add(-time.Hour),
						UpdatedOn: now,
						Author:    &bbUser{AccountID: "557058:author", DisplayName: "Author"},
						Destination: &bbPRRef{
							Repository: &bbRepository{UUID: "{repo-uuid-1}"},
						},
					},
				}, false), nil
			case *connectorpb.BitbucketListPRCommentsRequest:
				return bbCommentsResponse([]bbComment{
					{ID: 100, CreatedOn: now, User: &bbUser{AccountID: "557058:reviewer", DisplayName: "Reviewer"}},
					{ID: 101, CreatedOn: now, User: &bbUser{AccountID: "557058:reviewer2", DisplayName: "Reviewer2"}},
				}, false), nil
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
	require.Len(t, records, 2)
	assert.Equal(t, "myworkspace/myrepo#PR-1/COMMENT-100", records[0].ExternalID)
	assert.Equal(t, "myworkspace/myrepo#PR-1/COMMENT-101", records[1].ExternalID)
	assert.Equal(t, "fetch_pr_comments", records[0].OperationID)
	assert.Equal(t, "557058:author", records[0].Metadata["pr_author_account_id"])

	// Reviewer-load metadata fields.
	for _, rec := range records {
		assert.Equal(t, "Author", rec.Metadata["pr_author_login"], "pr_author_login should mirror PR author display name")
		assert.Equal(t, "557058:author", rec.Metadata["pr_author_id"], "pr_author_id should be PR author account_id")
		assert.Equal(t, "{repo-uuid-1}", rec.Metadata["platform_repo_id"], "platform_repo_id should be destination repo UUID")
		assert.Equal(t, "false", rec.Metadata["reviewer_is_bot"], "reviewer_is_bot defaults to false — Bitbucket has no clean vendor bot signal")
		// pr_created_at = parent PR opened_at (date anchor).
		assert.Equal(t, now.Add(-time.Hour).Format(time.RFC3339), rec.Metadata["pr_created_at"], "pr_created_at")
	}
}

func TestFetchPRCommentsOp_SkipsOldComments(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-48 * time.Hour)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{ID: 1, UpdatedOn: now, Author: &bbUser{AccountID: "557058:author"}},
				}, false), nil
			case *connectorpb.BitbucketListPRCommentsRequest:
				return bbCommentsResponse([]bbComment{
					{ID: 100, CreatedOn: old, User: &bbUser{AccountID: "557058:old"}},
					{ID: 101, CreatedOn: now, User: &bbUser{AccountID: "557058:new"}},
				}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	since := now.Add(-24 * time.Hour)
	cursorData, _ := json.Marshal(map[string]string{
		"myworkspace/myrepo": since.Format(time.RFC3339Nano),
	})
	cursor := &sdk.Cursor{Data: cursorData}
	params := testParams(sink, gw, cursor)

	op := &FetchPRCommentsOp{}
	_, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	records := sink.Records()
	require.Len(t, records, 1, "should only emit the new comment")
	assert.Equal(t, "myworkspace/myrepo#PR-1/COMMENT-101", records[0].ExternalID)
}

func TestFetchPRCommentsOp_EmptyResponse(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse(nil, false), nil
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
	// A repo with no PRs observed nothing, so the op records no watermark and returns
	// the incoming cursor untouched — nil here. The orchestrator skips SaveCursor on a
	// nil return, so the stored cursor is left alone rather than overwritten. Asserting
	// a non-nil cursor would be asserting the pin: the only value available to return
	// is `since`, which on a first sync is the synthetic lookback floor.
	assert.Nil(t, cursor)
	assert.Empty(t, sink.Records())
}

// TestFetchPRCommentsOp_ColdStartRepoDiscoveryPacesLocalLimit locks the efficacy
// gap this change closes: on a cold org (no cached DiscoveredRepos, no explicit
// Repositories -> live listing) the FIRST bitbucket-rest call is the repo-discovery
// listing, so a local-limit rejection there must be waited out and retried, not
// turned into a run failure. This test does a real ~1s sleep (the sized hint), so
// it is deliberately not run in parallel.
func TestFetchPRCommentsOp_ColdStartRepoDiscoveryPacesLocalLimit(t *testing.T) {
	var listReposCalls int
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListReposRequest:
				listReposCalls++
				if listReposCalls == 1 {
					// First discovery call: OUR OWN limiter rejects with a tiny sized
					// hint so the wait is ~1s, not a minute.
					return nil, &sdk.SyncError{
						Kind:              sdk.ErrKindRateLimit,
						Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
						Retryable:         true,
						RetryAfterSeconds: 1,
					}
				}
				return bbReposResponse([]bbRepository{
					{UUID: "{uuid-1}", FullName: "myworkspace/repo1", Slug: "repo1"},
				}, false), nil
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse(nil, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-test",
			Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "test-oauth-token"}),
			// No Repositories and no DiscoveredRepos -> fetchAllRepos takes the live
			// listing path, the call site this pacing protects.
			Settings: mustJSON(BitbucketSettings{Workspace: "myworkspace"}),
			Enabled:  true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}

	// The wait needs room under the deadline (1s hint + 90s safety margin), so give
	// the run an ample deadline; it still sleeps only the ~1s hint.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	op := &FetchPRCommentsOp{}
	_, err := op.Execute(ctx, params)

	require.NoError(t, err, "a paced local-limit rejection during repo discovery must not fail the run")
	assert.Equal(t, 2, listReposCalls, "discovery must retry after waiting out the local-limit rejection")
}

func TestFetchPRsOp_NilSourceAndAuthorEmitsEmptyMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{
						ID:        88,
						Title:     "PR with nil author and source",
						State:     "OPEN",
						CreatedOn: now.Add(-time.Hour),
						UpdatedOn: now,
						Author:    nil,
						Source:    nil,
					},
				}, false), nil
			case *connectorpb.BitbucketGetPRDiffstatRequest:
				return bbDiffstatResponse(0, 0), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.NotEmpty(t, records)

	meta := records[0].Metadata
	// Contains guards key presence — assert.Empty alone passes on an absent key
	// (a missing map entry reads as nil), which would hide a dropped-key regression.
	require.Contains(t, meta, "pr_author_id")
	assert.Empty(t, meta["pr_author_id"], "nil author should yield empty pr_author_id")
	require.Contains(t, meta, "head_branch")
	assert.Empty(t, meta["head_branch"], "nil source should yield empty head_branch")
}

// --- FetchReviewsOp tests ---

func bbActivityResponse(activities []bbActivity, hasNext bool) *connectorpb.BitbucketListPRActivityResponse {
	bytes := make([][]byte, len(activities))
	for i, a := range activities {
		bytes[i], _ = json.Marshal(a)
	}
	nextPage := ""
	if hasNext {
		nextPage = "https://api.bitbucket.org/2.0/repositories/myworkspace/myrepo/pullrequests/1/activity?page=2"
	}
	return &connectorpb.BitbucketListPRActivityResponse{
		Activities:  bytes,
		NextPageUrl: nextPage,
	}
}

func TestFetchReviewsOp_EmitsMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name           string
		prAuthor       *bbUser
		destRepoUUID   string
		approval       *bbApproval
		comment        *bbComment
		wantPRAuthorID string
		wantPRAuthor   string
		wantRepoID     string
		wantIsBot      string
		wantReviewType string
	}{
		{
			name:           "approval emits reviewer-load metadata",
			prAuthor:       &bbUser{AccountID: "557058:author", DisplayName: "Author"},
			destRepoUUID:   "{repo-uuid-1}",
			approval:       &bbApproval{Date: now.Format(time.RFC3339), User: &bbUser{AccountID: "557058:reviewer", DisplayName: "Reviewer"}},
			wantPRAuthorID: "557058:author",
			wantPRAuthor:   "Author",
			wantRepoID:     "{repo-uuid-1}",
			wantIsBot:      "false",
			wantReviewType: "approval",
		},
		{
			name:           "comment emits reviewer-load metadata",
			prAuthor:       &bbUser{AccountID: "557058:author", DisplayName: "Author"},
			destRepoUUID:   "{repo-uuid-2}",
			comment:        &bbComment{ID: 7, CreatedOn: now, User: &bbUser{AccountID: "557058:reviewer", DisplayName: "Reviewer"}},
			wantPRAuthorID: "557058:author",
			wantPRAuthor:   "Author",
			wantRepoID:     "{repo-uuid-2}",
			wantIsBot:      "false",
			wantReviewType: "comment",
		},
		{
			name:           "missing PR author yields empty author metadata",
			prAuthor:       nil,
			destRepoUUID:   "{repo-uuid-3}",
			approval:       &bbApproval{Date: now.Format(time.RFC3339), User: &bbUser{AccountID: "557058:reviewer"}},
			wantPRAuthorID: "",
			wantPRAuthor:   "",
			wantRepoID:     "{repo-uuid-3}",
			wantIsBot:      "false",
			wantReviewType: "approval",
		},
		{
			name:           "missing destination repo yields empty platform_repo_id",
			prAuthor:       &bbUser{AccountID: "557058:author", DisplayName: "Author"},
			destRepoUUID:   "",
			comment:        &bbComment{ID: 9, CreatedOn: now, User: &bbUser{AccountID: "557058:reviewer"}},
			wantPRAuthorID: "557058:author",
			wantPRAuthor:   "Author",
			wantRepoID:     "",
			wantIsBot:      "false",
			wantReviewType: "comment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := bbPullRequest{ID: 1, CreatedOn: now.Add(-time.Hour), UpdatedOn: now, Author: tt.prAuthor}
			if tt.destRepoUUID != "" {
				pr.Destination = &bbPRRef{Repository: &bbRepository{UUID: tt.destRepoUUID}}
			}
			activity := bbActivity{Approval: tt.approval, Comment: tt.comment}

			gw := &mockVendorGateway{
				executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.BitbucketListPullRequestsRequest:
						return bbPRsResponse([]bbPullRequest{pr}, false), nil
					case *connectorpb.BitbucketListPRActivityRequest:
						return bbActivityResponse([]bbActivity{activity}, false), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			params := testParams(sink, gw, nil)

			op := &FetchReviewsOp{}
			_, err := op.Execute(context.Background(), params)
			require.NoError(t, err)

			records := sink.Records()
			require.Len(t, records, 1)
			rec := records[0]

			assert.Equal(t, "fetch_reviews", rec.OperationID)
			assert.Equal(t, tt.wantReviewType, rec.Metadata["review_type"])
			assert.Equal(t, "myworkspace/myrepo#PR-1", rec.Metadata["pr_key"])
			assert.Equal(t, tt.wantPRAuthor, rec.Metadata["pr_author_login"], "pr_author_login")
			assert.Equal(t, tt.wantPRAuthorID, rec.Metadata["pr_author_id"], "pr_author_id")
			assert.Equal(t, tt.wantRepoID, rec.Metadata["platform_repo_id"], "platform_repo_id")
			assert.Equal(t, tt.wantIsBot, rec.Metadata["reviewer_is_bot"], "reviewer_is_bot")
			// pr_created_at = parent PR opened_at (date anchor).
			assert.Equal(t, now.Add(-time.Hour).Format(time.RFC3339), rec.Metadata["pr_created_at"], "pr_created_at")
		})
	}
}

// TestFetchReviewsOp_BotDetectionDefaultsFalse asserts the connector-side bot
// detection is conservative for Bitbucket: with no clean vendor signal
// available, reviewer_is_bot is always "false". The org-level allow-list
// (BitbucketSettings.BotAccountIDs) is the override path applied at the
// consumer, not here.
func TestFetchReviewsOp_BotDetectionDefaultsFalse(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Reviewer that a naive heuristic might flag — bot-ish display name and
	// "team" type — must still come through as "false" at the connector layer.
	reviewer := &bbUser{AccountID: "557058:dependabot", DisplayName: "dependabot[bot]"}

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{
						ID:        1,
						CreatedOn: now.Add(-time.Hour),
						UpdatedOn: now,
						Author:    &bbUser{AccountID: "557058:author", DisplayName: "Author"},
						Destination: &bbPRRef{
							Repository: &bbRepository{UUID: "{repo-uuid-1}"},
						},
					},
				}, false), nil
			case *connectorpb.BitbucketListPRActivityRequest:
				return bbActivityResponse([]bbActivity{
					{Approval: &bbApproval{Date: now.Format(time.RFC3339), User: reviewer}},
					{Comment: &bbComment{ID: 5, CreatedOn: now, User: reviewer}},
				}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchReviewsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 2, "should emit both approval and comment records")
	for _, rec := range records {
		assert.Equal(t, "false", rec.Metadata["reviewer_is_bot"],
			"connector defaults reviewer_is_bot=false for Bitbucket; allow-list is the override path")
	}
}

func TestFetchPRCommentsOp_BotDetectionDefaultsFalse(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListPullRequestsRequest:
				return bbPRsResponse([]bbPullRequest{
					{
						ID:        1,
						CreatedOn: now.Add(-time.Hour),
						UpdatedOn: now,
						Author:    &bbUser{AccountID: "557058:author", DisplayName: "Author"},
						Destination: &bbPRRef{
							Repository: &bbRepository{UUID: "{repo-uuid-1}"},
						},
					},
				}, false), nil
			case *connectorpb.BitbucketListPRCommentsRequest:
				return bbCommentsResponse([]bbComment{
					{ID: 200, CreatedOn: now, User: &bbUser{AccountID: "557058:dependabot", DisplayName: "dependabot[bot]"}},
				}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchPRCommentsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1)
	assert.Equal(t, "false", records[0].Metadata["reviewer_is_bot"],
		"connector defaults reviewer_is_bot=false for Bitbucket; allow-list is the override path")
}

// --- Tests for commit-diffstat enrichment + PR participant/reviewer counts ---

func bbCommitDiffstatResponse(entries []bbDiffstatEntry, nextURL string) *connectorpb.BitbucketGetCommitDiffstatResponse {
	resp := &connectorpb.BitbucketGetCommitDiffstatResponse{NextPageUrl: nextURL}
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			panic(err)
		}
		resp.DiffstatEntries = append(resp.DiffstatEntries, b)
	}
	return resp
}

func TestCountReviewersRequested(t *testing.T) {
	tests := []struct {
		name string
		pr   bbPullRequest
		want int
	}{
		{
			name: "explicit reviewers wins over participants",
			pr: bbPullRequest{
				Reviewers: []bbUser{{AccountID: "a"}, {AccountID: "b"}},
				Participants: []bbPRParticipant{
					{Role: "REVIEWER", User: &bbUser{AccountID: "a"}},
					{Role: "PARTICIPANT", User: &bbUser{AccountID: "c"}},
				},
			},
			want: 2,
		},
		{
			name: "falls back to participants role=REVIEWER",
			pr: bbPullRequest{
				Participants: []bbPRParticipant{
					{Role: "REVIEWER", User: &bbUser{AccountID: "a"}},
					{Role: "REVIEWER", User: &bbUser{AccountID: "b"}},
					{Role: "PARTICIPANT", User: &bbUser{AccountID: "c"}},
				},
			},
			want: 2,
		},
		{
			name: "no reviewers no participants",
			pr:   bbPullRequest{},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, countReviewersRequested(tt.pr))
		})
	}
}

func TestCountFilePathsInJSON(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want int
	}{
		{"empty", "", 0},
		{"empty array", "[]", 0},
		{"three files", `["a.go","b.go","c.go"]`, 3},
		{"malformed", `not-json`, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, countFilePathsInJSON(tt.in))
		})
	}
}

func TestInjectCommitLineCounts(t *testing.T) {
	original := json.RawMessage(`{"hash":"abc","message":"fix"}`)
	out := injectCommitLineCounts(original, 12, 3, 4)

	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	assert.EqualValues(t, 12, got["additions"])
	assert.EqualValues(t, 3, got["deletions"])
	assert.EqualValues(t, 4, got["files_changed"])
	assert.Equal(t, "abc", got["hash"])
	assert.Equal(t, "fix", got["message"])
}

func TestInjectCommitLineCounts_PreservesPayloadOnMalformedInput(t *testing.T) {
	bad := json.RawMessage(`not-json`)
	out := injectCommitLineCounts(bad, 1, 2, 3)
	assert.Equal(t, bad, out, "fallback must return the original payload unchanged")
}

func TestInjectPRCounts(t *testing.T) {
	original := json.RawMessage(`{"id":1,"state":"OPEN"}`)
	out := injectPRCounts(original, 7, 3)

	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	assert.EqualValues(t, 7, got["files_changed"])
	assert.EqualValues(t, 3, got["reviews_requested"])
	assert.EqualValues(t, 1, got["id"])
	assert.Equal(t, "OPEN", got["state"])
}

func TestEnrichCommit_AggregatesAndInjects(t *testing.T) {
	called := 0
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			called++
			r, ok := req.(*connectorpb.BitbucketGetCommitDiffstatRequest)
			require.True(t, ok)
			assert.Equal(t, "ws", r.Workspace)
			assert.Equal(t, "repo", r.RepoSlug)
			assert.Equal(t, "deadbeef", r.CommitSha)
			return bbCommitDiffstatResponse([]bbDiffstatEntry{
				{LinesAdded: 10, LinesRemoved: 2, New: &bbDiffstatFile{Path: "a.go"}},
				{LinesAdded: 5, LinesRemoved: 1, New: &bbDiffstatFile{Path: "b.go"}},
			}, ""), nil
		},
	}

	original := json.RawMessage(`{"hash":"deadbeef"}`)
	got, err := enrichCommit(context.Background(), gw, nil, "ws", "repo", "deadbeef", original, nil, "fetch_commits", "org-test")
	require.NoError(t, err)
	assert.Equal(t, 15, got.Additions)
	assert.Equal(t, 3, got.Deletions)
	assert.Equal(t, 2, got.FilesChanged)
	assert.Equal(t, 1, called)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(got.Payload, &payload))
	assert.EqualValues(t, 15, payload["additions"])
	assert.EqualValues(t, 3, payload["deletions"])
	assert.EqualValues(t, 2, payload["files_changed"])
}

func TestEnrichCommit_ErrorReturnsOriginalPayload(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, fmt.Errorf("vendor 500")
		},
	}
	original := json.RawMessage(`{"hash":"x"}`)
	got, err := enrichCommit(context.Background(), gw, nil, "ws", "repo", "x", original, nil, "fetch_commits", "org-test")
	require.Error(t, err)
	assert.Equal(t, original, got.Payload)
	assert.Equal(t, 0, got.Additions)
}

func TestFetchCommitsOp_EnrichesAndCachesPerSHA(t *testing.T) {
	now := time.Now().UTC()
	listCalls, diffstatCalls := 0, 0
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.BitbucketListReposRequest:
				return &connectorpb.BitbucketListReposResponse{
					Repos: [][]byte{mustMarshalJSON(t, bbRepository{FullName: "ws/repo", Slug: "repo"})},
				}, nil
			case *connectorpb.BitbucketListCommitsRequest:
				listCalls++
				return &connectorpb.BitbucketListCommitsResponse{
					Commits: [][]byte{
						mustMarshalJSON(t, bbCommit{Hash: "sha-1", Date: now}),
						mustMarshalJSON(t, bbCommit{Hash: "sha-2", Date: now}),
					},
				}, nil
			case *connectorpb.BitbucketGetCommitDiffstatRequest:
				diffstatCalls++
				return bbCommitDiffstatResponse([]bbDiffstatEntry{
					{LinesAdded: 4, LinesRemoved: 1, New: &bbDiffstatFile{Path: r.GetCommitSha() + ".go"}},
				}, ""), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	// Pre-warm the fetch-cache table for sha-1 so only sha-2 should hit the API.
	fake := newFakeFetchCache()
	fake.seed(CacheTypeCommitDiffstat, "ws/repo#sha-1", CommitDiffstatCacheEntry{Additions: 99, Deletions: 0, FilesChanged: 1})
	cfg := validConfig()
	cfg.Settings = mustJSON(BitbucketSettings{Workspace: "ws"})
	params := &sdk.ExecutionParams{Config: cfg, Sink: sink, VendorGateway: gw, FetchCache: fake}

	op := &FetchCommitsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, 1, listCalls)
	assert.Equal(t, 1, diffstatCalls, "cached sha-1 must not trigger /diffstat; sha-2 must trigger exactly one")

	records := sink.Records()
	require.Len(t, records, 2)

	bySha := map[string]map[string]any{}
	for _, r := range records {
		var p map[string]any
		require.NoError(t, json.Unmarshal(r.Payload, &p))
		bySha[p["hash"].(string)] = p
	}
	assert.EqualValues(t, 99, bySha["sha-1"]["additions"], "cached entry must be injected on emit")
	assert.EqualValues(t, 1, bySha["sha-1"]["files_changed"])
	assert.EqualValues(t, 4, bySha["sha-2"]["additions"], "live enrichment must populate additions")
	assert.EqualValues(t, 1, bySha["sha-2"]["files_changed"])
}

// TestFetchCommitsOp_WaitsOutLocalRateLimit pins the op-level guard: a local-limit
// rejection on the commit-list walk is waited out so the per-repo watermark
// advances instead of the run aborting and re-listing (and re-emitting) the same
// commits; a vendor 429 must still abort on the first attempt. Mirrors
// TestFetchPRsOp_WaitsOutLocalRateLimit.
func TestFetchCommitsOp_WaitsOutLocalRateLimit(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// Shaped exactly like the local limiter's rejection: kind rate_limit, cause
	// wrapping the ErrLocalRateLimit sentinel, carrying the sized wait.
	localLimit := &sdk.SyncError{
		ConnectorID:       connectorID,
		OperationID:       "fetch_commits",
		Kind:              sdk.ErrKindRateLimit,
		Retryable:         true,
		Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
		RetryAfterSeconds: 1,
	}
	// A vendor 429 carries no local sentinel — the vendor wants us to stop.
	vendor429 := &sdk.SyncError{
		ConnectorID:       connectorID,
		OperationID:       "fetch_commits",
		Kind:              sdk.ErrKindRateLimit,
		HTTPStatus:        429,
		Cause:             fmt.Errorf("bitbucket: 429 Too Many Requests"),
		RetryAfterSeconds: 60,
	}

	tests := []struct {
		name          string
		firstListErr  error // returned on the first commit-list call, then success
		wantErr       bool
		wantListCalls int // expected commit-list Execute attempts
	}{
		{
			name:          "local limit on first commit page is waited out and the run completes",
			firstListErr:  localLimit,
			wantErr:       false,
			wantListCalls: 2, // rejected once, retried after the wait
		},
		{
			name:          "vendor 429 aborts the run on the first attempt",
			firstListErr:  vendor429,
			wantErr:       true,
			wantListCalls: 1, // no retry
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listCalls := 0
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.BitbucketListReposRequest:
						return &connectorpb.BitbucketListReposResponse{
							Repos: [][]byte{mustMarshalJSON(t, bbRepository{FullName: "ws/repo", Slug: "repo"})},
						}, nil
					case *connectorpb.BitbucketListCommitsRequest:
						listCalls++
						if listCalls == 1 {
							return nil, tt.firstListErr
						}
						return &connectorpb.BitbucketListCommitsResponse{
							Commits: [][]byte{mustMarshalJSON(t, bbCommit{Hash: "sha-1", Date: now})},
						}, nil
					case *connectorpb.BitbucketGetCommitDiffstatRequest:
						return bbCommitDiffstatResponse([]bbDiffstatEntry{
							{LinesAdded: 4, LinesRemoved: 1, New: &bbDiffstatFile{Path: "a.go"}},
						}, ""), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			cfg := validConfig()
			cfg.Settings = mustJSON(BitbucketSettings{Workspace: "ws"})
			params := &sdk.ExecutionParams{Config: cfg, Sink: sink, VendorGateway: gw, FetchCache: newFakeFetchCache()}

			// A deadline ample for the 1s local-limit wait (which needs > 1s plus
			// the 90s safety margin); the wait is real but tiny.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			op := &FetchCommitsOp{}
			cur, err := op.Execute(ctx, params)

			assert.Equal(t, tt.wantListCalls, listCalls)
			if tt.wantErr {
				require.Error(t, err, "a vendor 429 must fail the run")
				assert.Empty(t, sink.Records())
			} else {
				require.NoError(t, err, "a local-limiter rejection must be waited out, not fail the run")
				assert.NotEmpty(t, sink.Records(), "the retried page's commits must be emitted")
				assert.NotNil(t, cur, "a completed walk must return an advanced cursor")
			}
		})
	}
}

// TestFetchCommitsOp_PacesDefaultBranchResolution guards the cursor-correctness
// hazard in default-branch resolution: on the explicit-Repositories path (no
// DiscoveredRepos cache) fetch_commits resolves each repo's default branch via a
// GetRepo call, and that branch decides which branch the commit walk — and thus
// the per-repo watermark — follows. If a local-limit rejection there fell back to
// "main", a repo whose real default is "master" would advance its cursor on main's
// commits and later skip real master commits. So GetRepo is paced too: the local
// limit is waited out and the walk runs on the resolved "master", never the
// fallback.
func TestFetchCommitsOp_PacesDefaultBranchResolution(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	localLimit := &sdk.SyncError{
		ConnectorID:       connectorID,
		OperationID:       "fetch_commits",
		Kind:              sdk.ErrKindRateLimit,
		Retryable:         true,
		Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
		RetryAfterSeconds: 1,
	}

	getRepoCalls := 0
	var listedBranch string
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.BitbucketGetRepoRequest:
				getRepoCalls++
				if getRepoCalls == 1 {
					return nil, localLimit // waited out, then retried
				}
				return &connectorpb.BitbucketGetRepoResponse{
					Repo: mustMarshalJSON(t, bbRepository{FullName: "ws/repo", Slug: "repo", Mainbranch: &bbBranchRef{Name: "master"}}),
				}, nil
			case *connectorpb.BitbucketListCommitsRequest:
				listedBranch = r.GetInclude()
				return &connectorpb.BitbucketListCommitsResponse{
					Commits: [][]byte{mustMarshalJSON(t, bbCommit{Hash: "sha-1", Date: now})},
				}, nil
			case *connectorpb.BitbucketGetCommitDiffstatRequest:
				return bbCommitDiffstatResponse(nil, ""), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	cfg := validConfig()
	// Explicit-Repositories path: no DiscoveredRepos cache, so the default branch
	// is resolved per-repo via GetRepo — the paced path under test.
	cfg.Settings = mustJSON(BitbucketSettings{Workspace: "ws", Repositories: []string{"ws/repo"}})
	params := &sdk.ExecutionParams{Config: cfg, Sink: sink, VendorGateway: gw, FetchCache: newFakeFetchCache()}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	op := &FetchCommitsOp{}
	_, err := op.Execute(ctx, params)
	require.NoError(t, err)

	assert.Equal(t, 2, getRepoCalls, "the local-limited GetRepo must be waited out and retried, not swallowed")
	assert.Equal(t, "master", listedBranch, `the commit walk must follow the resolved default branch, never the "main" fallback`)
	assert.NotEmpty(t, sink.Records())
}

// TestFetchCommitsOp_SkipsRepoWhenBranchResolutionGivesUp completes the
// branch-resolution guard: when pacing GIVES UP resolving the default branch
// (here a ctx with no deadline, so the limiter's give-up is immediate), the repo
// is skipped — no commit walk on a guessed "main", no cursor advance — rather than
// risk advancing the per-repo watermark on the wrong branch. The next run
// re-resolves once capacity returns.
func TestFetchCommitsOp_SkipsRepoWhenBranchResolutionGivesUp(t *testing.T) {
	localLimit := &sdk.SyncError{
		ConnectorID:       connectorID,
		OperationID:       "fetch_commits",
		Kind:              sdk.ErrKindRateLimit,
		Retryable:         true,
		Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
		RetryAfterSeconds: 1,
	}

	getRepoCalls, listCalls := 0, 0
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketGetRepoRequest:
				getRepoCalls++
				return nil, localLimit
			case *connectorpb.BitbucketListCommitsRequest:
				listCalls++
				return &connectorpb.BitbucketListCommitsResponse{}, nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	cfg := validConfig()
	cfg.Settings = mustJSON(BitbucketSettings{Workspace: "ws", Repositories: []string{"ws/repo"}})
	params := &sdk.ExecutionParams{Config: cfg, Sink: sink, VendorGateway: gw, FetchCache: newFakeFetchCache()}

	// No deadline: canAfford refuses to wait (give-up reason no_deadline), so
	// branch resolution gives up on the first local-limit rejection — the same
	// give-up the caller must treat as "branch unknown, skip" as a budget_cap or
	// deadline_too_near give-up in real use.
	op := &FetchCommitsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err, "a skipped repo is not a run failure")

	assert.Equal(t, 1, getRepoCalls, "no deadline -> pacing gives up immediately, no retry")
	assert.Equal(t, 0, listCalls, "the repo must be skipped, never walked on a guessed branch")
	assert.Empty(t, sink.Records())
}

func TestFetchCommitsOp_DisableFlagSkipsEnrichment(t *testing.T) {
	now := time.Now().UTC()
	diffstatCalls := 0
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.BitbucketListReposRequest:
				return &connectorpb.BitbucketListReposResponse{
					Repos: [][]byte{mustMarshalJSON(t, bbRepository{FullName: "ws/repo", Slug: "repo"})},
				}, nil
			case *connectorpb.BitbucketListCommitsRequest:
				return &connectorpb.BitbucketListCommitsResponse{
					Commits: [][]byte{mustMarshalJSON(t, bbCommit{Hash: "sha-only", Date: now})},
				}, nil
			case *connectorpb.BitbucketGetCommitDiffstatRequest:
				diffstatCalls++
				return bbCommitDiffstatResponse(nil, ""), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	cfg := validConfig()
	cfg.Settings = mustJSON(BitbucketSettings{Workspace: "ws", DisableCommitDiffstat: true})
	params := &sdk.ExecutionParams{Config: cfg, Sink: sink, VendorGateway: gw}

	op := &FetchCommitsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, 0, diffstatCalls, "DisableCommitDiffstat=true must suppress /diffstat calls")
	require.Len(t, sink.Records(), 1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(sink.Records()[0].Payload, &payload))
	_, hasAdditions := payload["additions"]
	assert.False(t, hasAdditions, "disabled path must not inject additions field")
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// bbPRDiffstatResponse mirrors bbCommitDiffstatResponse but emits the PR
// diffstat shape — same field name (DiffstatEntries) but a different proto
// type so the URL builder + dispatcher pick the right path.
func bbPRDiffstatResponse(entries []bbDiffstatEntry, nextURL string) *connectorpb.BitbucketGetPRDiffstatResponse {
	resp := &connectorpb.BitbucketGetPRDiffstatResponse{NextPageUrl: nextURL}
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			panic(err)
		}
		resp.DiffstatEntries = append(resp.DiffstatEntries, b)
	}
	return resp
}

// TestEnrichPR_PaginatesAcrossAllPages verifies the PR diffstat loop walks
// every page of the response, so a large PR (more than diffstatPagelen
// files) emits accurate aggregates instead of silently truncating at page 1:
// getPRDiffstatVendorResponse keeps `next`, and enrichPR follows it.
func TestEnrichPR_PaginatesAcrossAllPages(t *testing.T) {
	calls := 0
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			calls++
			r, ok := req.(*connectorpb.BitbucketGetPRDiffstatRequest)
			require.True(t, ok)
			require.Equal(t, int32(500), r.GetPagelen(), "default pagelen must be propagated")
			switch r.GetPage() {
			case 1:
				return bbPRDiffstatResponse([]bbDiffstatEntry{
					{LinesAdded: 100, LinesRemoved: 10, New: &bbDiffstatFile{Path: "a.go"}},
				}, "https://api.bitbucket.org/2.0/.../diffstat?page=2"), nil
			case 2:
				return bbPRDiffstatResponse([]bbDiffstatEntry{
					{LinesAdded: 50, LinesRemoved: 5, New: &bbDiffstatFile{Path: "b.go"}},
				}, ""), nil
			default:
				return nil, fmt.Errorf("unexpected page %d", r.GetPage())
			}
		},
	}

	original := json.RawMessage(`{"id":1,"state":"OPEN"}`)
	got, err := enrichPR(context.Background(), gw, nil, "ws", "repo", 1, original, "org-test", "fetch_prs", &pacing.PacingBudget{})
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "two pages expected")
	assert.Equal(t, 150, got.Additions)
	assert.Equal(t, 15, got.Deletions)
	assert.JSONEq(t, `["a.go","b.go"]`, got.FilesStr)
}

// TestEnrichPR_MaxPagesGuardBounds walks the loop indefinitely from the
// mock side and verifies enrichPR stops after `maxDiffstatPages`. Without
// the guard a pathological `next` URL would run the sync forever.
func TestEnrichPR_MaxPagesGuardBounds(t *testing.T) {
	calls := 0
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			calls++
			return bbPRDiffstatResponse(nil, "https://api.bitbucket.org/2.0/.../diffstat?page=next"), nil
		},
	}
	original := json.RawMessage(`{"id":1}`)
	_, err := enrichPR(context.Background(), gw, nil, "ws", "repo", 1, original, "org-test", "fetch_prs", &pacing.PacingBudget{})
	require.NoError(t, err)
	assert.Equal(t, int(maxDiffstatPages), calls, "loop must stop at maxDiffstatPages")
}

// TestResolveDiffstatPagination_RespectsOverrides confirms per-op YAML
// overrides take precedence over the package-level defaults, and the
// resolver is nil-safe at every level.
func TestResolveDiffstatPagination_RespectsOverrides(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *config.BitbucketPaginationConfig
		op          string
		wantPagelen int32
		wantMax     int32
	}{
		{"nil cfg uses defaults", nil, opPaginationPRDiffstat, diffstatPagelen, maxDiffstatPages},
		{
			name:        "max-pages override applied even without per-op entry",
			cfg:         &config.BitbucketPaginationConfig{MaxDiffstatPages: 50},
			op:          opPaginationCommitDiffstat,
			wantPagelen: diffstatPagelen,
			wantMax:     50,
		},
		{
			name: "per-op pagesize override applied",
			cfg: &config.BitbucketPaginationConfig{
				Operations: map[string]*config.OperationPaginationConfig{
					opPaginationPRDiffstat: {PageSize: 100},
				},
			},
			op:          opPaginationPRDiffstat,
			wantPagelen: 100,
			wantMax:     maxDiffstatPages,
		},
		{
			name: "missing op entry falls back to default",
			cfg: &config.BitbucketPaginationConfig{
				Operations: map[string]*config.OperationPaginationConfig{
					opPaginationPRDiffstat: {PageSize: 100},
				},
			},
			op:          opPaginationCommitDiffstat,
			wantPagelen: diffstatPagelen,
			wantMax:     maxDiffstatPages,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pagelen, maxPages := resolveDiffstatPagination(tt.cfg, tt.op)
			assert.Equal(t, tt.wantPagelen, pagelen)
			assert.Equal(t, tt.wantMax, maxPages)
		})
	}
}

// TestFetchAllRepos_PrefersDiscoveredReposCache verifies the rate-limit-budget
// optimisation: when settings.Repositories is empty but DiscoveredRepos is
// populated, fetchAllRepos returns the cached list without issuing any
// BitbucketListReposRequest calls, so the three pull ops (fetch_prs,
// fetch_reviews, fetch_pr_comments) do not each re-paginate the repo list and
// saturate the per-org bitbucket-rest budget on every sync tick.
func TestFetchAllRepos_PrefersDiscoveredReposCache(t *testing.T) {
	tests := []struct {
		name         string
		settings     BitbucketSettings
		wantRepos    []string
		wantGWCalled bool
	}{
		{
			name: "manual override wins over discovered cache",
			settings: BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{"myworkspace/manual"},
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "myworkspace/cached", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE},
				},
			},
			wantRepos:    []string{"myworkspace/manual"},
			wantGWCalled: false,
		},
		{
			name: "discovered cache used when no manual override",
			settings: BitbucketSettings{
				Workspace: "myworkspace",
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "myworkspace/repo-a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE},
					{FullName: "myworkspace/repo-b", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE},
				},
			},
			wantRepos:    []string{"myworkspace/repo-a", "myworkspace/repo-b"},
			wantGWCalled: false,
		},
		{
			name: "live pagination only when no override and no cache",
			settings: BitbucketSettings{
				Workspace: "myworkspace",
			},
			wantRepos:    []string{"myworkspace/live"},
			wantGWCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			gw := &mockVendorGateway{
				executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
					called = true
					return bbReposResponse([]bbRepository{
						{UUID: "{uuid-live}", FullName: "myworkspace/live", Slug: "live"},
					}, false), nil
				},
			}

			repos, err := fetchAllRepos(context.Background(), gw, tt.settings, "fetch_repos", "org-test", &pacing.PacingBudget{})

			require.NoError(t, err)
			assert.Equal(t, tt.wantRepos, repos)
			assert.Equal(t, tt.wantGWCalled, called,
				"VendorGateway.Execute should only fire on live-pagination path")
		})
	}
}

// --- Per-PR change-cache tests (fetch_reviews + fetch_pr_comments) ---

// stubConfigUpdater captures the most recent settings JSON written via
// PatchSettings so tests can assert the persisted activity / comments cache.
type stubConfigUpdater struct {
	current  json.RawMessage
	patchErr error
}

func (s *stubConfigUpdater) UpdateSettings(_ context.Context, _ string, _ sdk.ConnectorID, settings json.RawMessage) error {
	s.current = settings
	return nil
}

func (s *stubConfigUpdater) PatchSettings(_ context.Context, _ string, _ sdk.ConnectorID, mutator func(json.RawMessage) (json.RawMessage, error)) error {
	updated, err := mutator(s.current)
	if err != nil {
		return err
	}
	s.current = updated
	return s.patchErr
}

func TestFetchWatermarkCodec(t *testing.T) {
	// subSecond mirrors the real Bitbucket source precision (microsecond digits).
	// A watermark stored at whole-second precision and compared against the raw
	// sub-second UpdatedOn would make every PR look changed on every run, so the
	// codec keeps the sub-second digits.
	subSecond := time.Date(2026, 5, 24, 12, 0, 15, 39870*1000, time.UTC) // …15.039870Z
	tests := []struct {
		name        string
		cached      string
		prUpdatedOn time.Time
		want        bool
	}{
		// A sub-second watermark stored via the codec must hit when re-checked
		// against the same UpdatedOn. Encoding as RFC3339 fails exactly this case.
		{name: "sub-second watermark round-trips and hits", cached: encodeFetchWatermark(subSecond), prUpdatedOn: subSecond, want: true},
		{name: "unchanged whole-second instant hits", cached: encodeFetchWatermark(subSecond.Truncate(time.Second)), prUpdatedOn: subSecond.Truncate(time.Second), want: true},
		{name: "advanced by a microsecond is a miss", cached: encodeFetchWatermark(subSecond), prUpdatedOn: subSecond.Add(time.Microsecond), want: false},
		// Sub-microsecond advance sits below the watermark's resolution, so it hits.
		// Two distinct PR activities inside one microsecond cannot occur, so this is
		// a boundary the design accepts rather than a reachable staleness window.
		{name: "advance within the same microsecond hits (below precision)", cached: encodeFetchWatermark(subSecond), prUpdatedOn: subSecond.Add(500 * time.Nanosecond), want: true},
		{name: "cached older is a miss", cached: encodeFetchWatermark(subSecond.Add(-time.Hour)), prUpdatedOn: subSecond, want: false},
		// A watermark written at whole-second precision forces exactly one
		// refetch against a sub-second UpdatedOn, then re-stores at microsecond and hits.
		{name: "a whole-second cached value refetches once", cached: subSecond.Truncate(time.Second).Format(time.RFC3339), prUpdatedOn: subSecond, want: false},
		{name: "unparseable cached value is a miss", cached: "not-a-time", prUpdatedOn: subSecond, want: false},
		{name: "zero updated_on is a miss", cached: encodeFetchWatermark(subSecond), prUpdatedOn: time.Time{}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fetchWatermarkFresh(context.Background(), tt.cached, tt.prUpdatedOn)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFetchReviewsOp_PerPRChangeCache(t *testing.T) {
	// Sub-second UpdatedOn: a whole-second value would hit at whole-second precision too and hide a precision bug.
	now := time.Now().UTC().Truncate(time.Second).Add(39870 * time.Microsecond)
	t.Run("cache hit skips /activity call", func(t *testing.T) {
		var prListCalls, activityCalls int
		gw := &mockVendorGateway{
			executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				switch req.(type) {
				case *connectorpb.BitbucketListPullRequestsRequest:
					prListCalls++
					return bbPRsResponse([]bbPullRequest{
						{ID: 42, Title: "Cached", State: "OPEN", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now},
					}, false), nil
				case *connectorpb.BitbucketListPRActivityRequest:
					activityCalls++
					return bbActivityResponse([]bbActivity{}, false), nil
				default:
					return nil, fmt.Errorf("unexpected request type: %T", req)
				}
			},
		}

		sink := testkit.NewChannelSink()
		params := testParams(sink, gw, nil)
		params.Config.Settings = mustJSON(BitbucketSettings{
			Workspace:    "myworkspace",
			Repositories: []string{"myworkspace/myrepo"},
		})
		// Seed the activity watermark in the table so PR-42's UpdatedOn matches → hit.
		fake := newFakeFetchCache()
		fake.seed(CacheTypePRActivity, "myworkspace/myrepo#42", newPRProbeEntry(now, time.Time{}))
		params.FetchCache = fake

		_, err := (&FetchReviewsOp{}).Execute(context.Background(), params)
		require.NoError(t, err)

		assert.Equal(t, 1, prListCalls, "list call still required")
		assert.Equal(t, 0, activityCalls, "cached PR should skip /activity")
	})

	t.Run("cache miss fetches /activity and updates cache", func(t *testing.T) {
		var activityCalls int
		gw := &mockVendorGateway{
			executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				switch req.(type) {
				case *connectorpb.BitbucketListPullRequestsRequest:
					return bbPRsResponse([]bbPullRequest{
						{ID: 42, Title: "Updated", State: "OPEN", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now},
					}, false), nil
				case *connectorpb.BitbucketListPRActivityRequest:
					activityCalls++
					return bbActivityResponse([]bbActivity{}, false), nil
				default:
					return nil, fmt.Errorf("unexpected request type: %T", req)
				}
			},
		}

		sink := testkit.NewChannelSink()
		params := testParams(sink, gw, nil)
		params.Config.Settings = mustJSON(BitbucketSettings{
			Workspace:    "myworkspace",
			Repositories: []string{"myworkspace/myrepo"},
		})
		// Seed the table with an OLDER watermark than pr.UpdatedOn → miss.
		fake := newFakeFetchCache()
		fake.seed(CacheTypePRActivity, "myworkspace/myrepo#42", newPRProbeEntry(now.Add(-2*time.Hour), time.Time{}))
		params.FetchCache = fake

		_, err := (&FetchReviewsOp{}).Execute(context.Background(), params)
		require.NoError(t, err)

		assert.Equal(t, 1, activityCalls, "stale cache forces refetch")

		put := fake.putEntries(CacheTypePRActivity)
		require.Contains(t, put, "myworkspace/myrepo#42")
		assert.Equal(t, encodeFetchWatermark(now), probeEntryFrom(t, put["myworkspace/myrepo#42"]).Watermark,
			"cache must advance to the PR's UpdatedOn after successful fetch")
	})
}

func TestFetchPRCommentsOp_PerPRChangeCache(t *testing.T) {
	// Sub-second UpdatedOn: a whole-second value would hit at whole-second precision too and hide a precision bug.
	now := time.Now().UTC().Truncate(time.Second).Add(39870 * time.Microsecond)
	t.Run("cache hit skips /comments call", func(t *testing.T) {
		var prListCalls, commentsCalls int
		gw := &mockVendorGateway{
			executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				switch req.(type) {
				case *connectorpb.BitbucketListPullRequestsRequest:
					prListCalls++
					return bbPRsResponse([]bbPullRequest{
						{ID: 42, Title: "Cached", State: "OPEN", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now},
					}, false), nil
				case *connectorpb.BitbucketListPRCommentsRequest:
					commentsCalls++
					return bbCommentsResponse([]bbComment{}, false), nil
				default:
					return nil, fmt.Errorf("unexpected request type: %T", req)
				}
			},
		}

		sink := testkit.NewChannelSink()
		params := testParams(sink, gw, nil)
		params.Config.Settings = mustJSON(BitbucketSettings{
			Workspace:    "myworkspace",
			Repositories: []string{"myworkspace/myrepo"},
		})
		// Seed the comments watermark in the table so PR-42's UpdatedOn matches → hit.
		fake := newFakeFetchCache()
		fake.seed(CacheTypePRComments, "myworkspace/myrepo#42", newPRProbeEntry(now, time.Time{}))
		params.FetchCache = fake

		_, err := (&FetchPRCommentsOp{}).Execute(context.Background(), params)
		require.NoError(t, err)

		assert.Equal(t, 1, prListCalls)
		assert.Equal(t, 0, commentsCalls, "cached PR should skip /comments")
	})

	t.Run("cache miss fetches /comments and updates cache", func(t *testing.T) {
		var commentsCalls int
		gw := &mockVendorGateway{
			executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				switch req.(type) {
				case *connectorpb.BitbucketListPullRequestsRequest:
					return bbPRsResponse([]bbPullRequest{
						{ID: 42, Title: "Updated", State: "OPEN", CreatedOn: now.Add(-2 * time.Hour), UpdatedOn: now},
					}, false), nil
				case *connectorpb.BitbucketListPRCommentsRequest:
					commentsCalls++
					return bbCommentsResponse([]bbComment{}, false), nil
				default:
					return nil, fmt.Errorf("unexpected request type: %T", req)
				}
			},
		}

		sink := testkit.NewChannelSink()
		params := testParams(sink, gw, nil)
		params.Config.Settings = mustJSON(BitbucketSettings{
			Workspace:    "myworkspace",
			Repositories: []string{"myworkspace/myrepo"},
		})
		// Seed the table with an OLDER watermark than pr.UpdatedOn → miss.
		fake := newFakeFetchCache()
		fake.seed(CacheTypePRComments, "myworkspace/myrepo#42", newPRProbeEntry(now.Add(-2*time.Hour), time.Time{}))
		params.FetchCache = fake

		_, err := (&FetchPRCommentsOp{}).Execute(context.Background(), params)
		require.NoError(t, err)

		assert.Equal(t, 1, commentsCalls, "stale cache forces refetch")

		put := fake.putEntries(CacheTypePRComments)
		require.Contains(t, put, "myworkspace/myrepo#42")
		assert.Equal(t, encodeFetchWatermark(now), probeEntryFrom(t, put["myworkspace/myrepo#42"]).Watermark,
			"cache must advance to the PR's UpdatedOn after successful fetch")
	})
}

// --- Per-repo failure isolation ---

func TestClassifyRepoError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantKind   repoErrorKind
		wantStatus int
	}{
		// classifyRepoError works off SyncError.Kind, which vendorapi sets
		// for every vendor-derived error. Tests construct typed SyncErrors
		// matching what ClassifyHTTPErrorWithHeaders would produce in
		// production.
		{name: "ErrKindNotFound → not_found", err: &sdk.SyncError{Kind: sdk.ErrKindNotFound, HTTPStatus: 404}, wantKind: repoErrNotFound, wantStatus: 404},
		{name: "ErrKindNotFound no status → not_found, infers 404", err: &sdk.SyncError{Kind: sdk.ErrKindNotFound}, wantKind: repoErrNotFound, wantStatus: 404},
		{name: "ErrKindAuth 401 → auth", err: &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401}, wantKind: repoErrAuth, wantStatus: 401},
		{name: "ErrKindAuth 403 → forbidden (that repository's failure)", err: &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 403}, wantKind: repoErrForbidden, wantStatus: 403},
		{name: "ErrKindAuth no status → auth, infers 401", err: &sdk.SyncError{Kind: sdk.ErrKindAuth}, wantKind: repoErrAuth, wantStatus: 401},
		{name: "ErrKindRateLimit 429 → transient", err: &sdk.SyncError{Kind: sdk.ErrKindRateLimit, HTTPStatus: 429}, wantKind: repoErrTransient, wantStatus: 429},
		{name: "ErrKindRateLimit no status → transient, infers 429", err: &sdk.SyncError{Kind: sdk.ErrKindRateLimit}, wantKind: repoErrTransient, wantStatus: 429},
		{name: "ErrKindRateLimit 403 (anti-abuse) → transient", err: &sdk.SyncError{Kind: sdk.ErrKindRateLimit, HTTPStatus: 403}, wantKind: repoErrTransient, wantStatus: 403},
		{name: "ErrKindVendorError 500 → transient", err: &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 500}, wantKind: repoErrTransient, wantStatus: 500},
		{name: "ErrKindVendorError 503 → transient", err: &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 503}, wantKind: repoErrTransient, wantStatus: 503},
		{name: "ErrKindTimeout → transient (504)", err: &sdk.SyncError{Kind: sdk.ErrKindTimeout, HTTPStatus: 504}, wantKind: repoErrTransient, wantStatus: 504},
		{name: "ErrKindCtxExhausted → transient", err: &sdk.SyncError{Kind: sdk.ErrKindCtxExhausted}, wantKind: repoErrTransient, wantStatus: 0},
		// Internal-class kinds report `internal` in the log field, so an operator can
		// tell a transient failure from one that needs a fix.
		{name: "ErrKindSinkFailure → internal (queue infra bug, not vendor)", err: &sdk.SyncError{Kind: sdk.ErrKindSinkFailure}, wantKind: repoErrInternal, wantStatus: 0},
		{name: "ErrKindConfigInvalid → internal", err: &sdk.SyncError{Kind: sdk.ErrKindConfigInvalid}, wantKind: repoErrInternal, wantStatus: 0},
		{name: "ErrKindInternal → internal", err: &sdk.SyncError{Kind: sdk.ErrKindInternal}, wantKind: repoErrInternal, wantStatus: 0},
		{name: "ErrKindInvalidInput → internal (400-class, caller bug)", err: &sdk.SyncError{Kind: sdk.ErrKindInvalidInput, HTTPStatus: 400}, wantKind: repoErrInternal, wantStatus: 400},
		{name: "plain error → transient zero status (no typed SyncError)", err: fmt.Errorf("generic"), wantKind: repoErrTransient, wantStatus: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			kind, status := classifyRepoError(tt.err)
			assert.Equal(t, tt.wantKind, kind)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

func TestShouldFailRun(t *testing.T) {
	t.Parallel()
	mk := func(kinds ...repoErrorKind) []repoFailure {
		out := make([]repoFailure, len(kinds))
		for i, k := range kinds {
			out[i] = repoFailure{name: fmt.Sprintf("r%d", i), kind: k}
		}
		return out
	}
	tests := []struct {
		name     string
		failures []repoFailure
		want     bool
	}{
		{name: "empty", failures: nil, want: false},
		{name: "only 404s", failures: mk(repoErrNotFound, repoErrNotFound), want: false},
		{name: "404 + auth → fail", failures: mk(repoErrNotFound, repoErrAuth), want: true},
		{name: "404 + transient → fail", failures: mk(repoErrNotFound, repoErrTransient), want: true},
		{name: "404 + internal → fail", failures: mk(repoErrNotFound, repoErrInternal), want: true},
		{name: "only auth → fail", failures: mk(repoErrAuth), want: true},
		{name: "only transient → fail", failures: mk(repoErrTransient), want: true},
		{name: "only internal → fail", failures: mk(repoErrInternal), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, shouldFailRun(tt.failures))
		})
	}
}

func TestRepoStatusForKind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		kind       repoErrorKind
		wantStatus bitbucketpb.BitbucketRepoStatus
		wantOk     bool
	}{
		{name: "404 → deleted", kind: repoErrNotFound, wantStatus: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED, wantOk: true},
		{name: "auth → no transition", kind: repoErrAuth, wantStatus: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_UNSPECIFIED, wantOk: false},
		{name: "transient → no transition", kind: repoErrTransient, wantStatus: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_UNSPECIFIED, wantOk: false},
		{name: "internal → no transition", kind: repoErrInternal, wantStatus: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_UNSPECIFIED, wantOk: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			status, ok := repoStatusForKind(tt.kind)
			assert.Equal(t, tt.wantStatus, status)
			assert.Equal(t, tt.wantOk, ok)
		})
	}
}

func TestMergeRepoStateUpdates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		live    []BitbucketDiscoveredRepo
		updates map[string]BitbucketDiscoveredRepo
		want    []BitbucketDiscoveredRepo
	}{
		{
			name: "update existing repo in place preserves DefaultBranch + Private + LastSeenAt",
			live: []BitbucketDiscoveredRepo{
				{FullName: "ws/a", DefaultBranch: "main", Private: true, Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE, LastSeenAt: "2026-05-01T00:00:00Z"},
			},
			updates: map[string]BitbucketDiscoveredRepo{
				"ws/a": {FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED, LastErrorAt: "2026-05-02T00:00:00Z", LastHTTPStatus: 404},
			},
			want: []BitbucketDiscoveredRepo{
				{FullName: "ws/a", DefaultBranch: "main", Private: true, Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED, LastSeenAt: "2026-05-01T00:00:00Z", LastErrorAt: "2026-05-02T00:00:00Z", LastHTTPStatus: 404},
			},
		},
		{
			// Appending failure-only entries would create a DiscoveredRepos
			// cache containing only failed repos, which then blocks discovery
			// because fetchAllRepos filters non-active entries and returns an
			// empty list. The merge helper skips updates for unknown repos;
			// fetch_repos owns the canonical population pass.
			name: "drops update for repo not yet in live cache",
			live: []BitbucketDiscoveredRepo{{FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE}},
			updates: map[string]BitbucketDiscoveredRepo{
				"ws/b": {FullName: "ws/b", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED, LastErrorAt: "2026-05-02T00:00:00Z", LastHTTPStatus: 404},
			},
			want: []BitbucketDiscoveredRepo{{FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE}},
		},
		{
			name:    "empty updates no-op",
			live:    []BitbucketDiscoveredRepo{{FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE}},
			updates: nil,
			want:    []BitbucketDiscoveredRepo{{FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE}},
		},
		{
			// Diagnostic-only update: an auth or transient failure stages
			// LastErrorAt + LastHTTPStatus with Status=UNSPECIFIED so the
			// merge preserves the existing lifecycle (the run will fail
			// and asynq will retry — we don't want to flip Status to
			// DELETED for a 401 / 503).
			name: "Status=UNSPECIFIED on update preserves existing Status; diagnostic fields still overwritten",
			live: []BitbucketDiscoveredRepo{
				{FullName: "ws/a", DefaultBranch: "main", Private: true, Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE, LastSeenAt: "2026-05-01T00:00:00Z"},
			},
			updates: map[string]BitbucketDiscoveredRepo{
				"ws/a": {FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_UNSPECIFIED, LastErrorAt: "2026-05-02T00:00:00Z", LastHTTPStatus: 401},
			},
			want: []BitbucketDiscoveredRepo{
				{FullName: "ws/a", DefaultBranch: "main", Private: true, Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE, LastSeenAt: "2026-05-01T00:00:00Z", LastErrorAt: "2026-05-02T00:00:00Z", LastHTTPStatus: 401},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := mergeRepoStateUpdates(tt.live, tt.updates)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFetchAllRepos_SkipsNonActiveRepos(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		settings BitbucketSettings
		want     []string
	}{
		{
			name: "all active",
			settings: BitbucketSettings{
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE},
					{FullName: "ws/b", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE},
				},
			},
			want: []string{"ws/a", "ws/b"},
		},
		{
			name: "deleted skipped",
			settings: BitbucketSettings{
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE},
					{FullName: "ws/b", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED},
					{FullName: "ws/c", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE},
				},
			},
			want: []string{"ws/a", "ws/c"},
		},
		{
			// IsActive treats UNSPECIFIED, an entry with no status, as active. fetch_repos will overwrite the
			// zero value with an explicit ACTIVE on its next reconciliation.
			name: "unspecified included (an entry without status)",
			settings: BitbucketSettings{
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_UNSPECIFIED},
					{FullName: "ws/b", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE},
				},
			},
			want: []string{"ws/a", "ws/b"},
		},
		{
			// Pins the no-fallback behaviour: a fully-deleted cache returns
			// an empty slice rather than falling through to live REST
			// pagination. Protects against accidental regressions if a
			// future change moves the fallback inside the cache path.
			name: "all deleted returns empty (does not fall back to live REST)",
			settings: BitbucketSettings{
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/a", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED},
					{FullName: "ws/b", Status: bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED},
				},
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			repos, err := fetchAllRepos(context.Background(), &mockVendorGateway{}, tt.settings, "fetch_repos", "org-test", &pacing.PacingBudget{})
			require.NoError(t, err)
			assert.Equal(t, tt.want, repos)
		})
	}
}

// TestSaveDiscoveredRepos_Reconciliation pins the three reconciliation
// branches and the two early-return guards. saveDiscoveredRepos is the
// canonical writer of BitbucketSettings.DiscoveredRepos and the only path
// that promotes a DELETED entry back to ACTIVE, so the
// branches need explicit coverage even though the helper-level pieces are
// tested above.
func TestSaveDiscoveredRepos_Reconciliation(t *testing.T) {
	t.Parallel()
	active := bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE
	deleted := bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED

	mkRepo := func(fullName, mainbranch string, private bool) bbRepository {
		r := bbRepository{FullName: fullName, IsPrivate: private}
		if mainbranch != "" {
			r.Mainbranch = &bbBranchRef{Name: mainbranch}
		}
		return r
	}

	tests := []struct {
		name            string
		seedSettings    BitbucketSettings
		nilUpdater      bool
		liveRepos       []bbRepository
		expectNoWrite   bool
		assertPersisted func(t *testing.T, got BitbucketSettings)
	}{
		{
			// Explicit Repositories short-circuits auto-discovery — the
			// helper must not touch DiscoveredRepos.
			name: "explicit Repositories list short-circuits",
			seedSettings: BitbucketSettings{
				Workspace:    "ws",
				Repositories: []string{"ws/manual"},
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/old", Status: active},
				},
			},
			liveRepos:     []bbRepository{mkRepo("ws/live", "main", false)},
			expectNoWrite: true,
		},
		{
			name:          "nil ConfigUpdater is a no-op",
			seedSettings:  BitbucketSettings{Workspace: "ws"},
			nilUpdater:    true,
			liveRepos:     []bbRepository{mkRepo("ws/live", "main", false)},
			expectNoWrite: true,
		},
		{
			name:         "appends brand-new repo as active",
			seedSettings: BitbucketSettings{Workspace: "ws"},
			liveRepos:    []bbRepository{mkRepo("ws/new", "develop", true)},
			assertPersisted: func(t *testing.T, got BitbucketSettings) {
				require.Len(t, got.DiscoveredRepos, 1)
				dr := got.DiscoveredRepos[0]
				assert.Equal(t, "ws/new", dr.FullName)
				assert.Equal(t, active, dr.Status)
				assert.Equal(t, "develop", dr.DefaultBranch)
				assert.True(t, dr.Private)
				assert.NotEmpty(t, dr.LastSeenAt)
				assert.Empty(t, dr.LastErrorAt)
				assert.Zero(t, dr.LastHTTPStatus)
			},
		},
		{
			// LastHTTPStatus stays 0 because fetch_repos noticed the repo
			// was absent from the workspace listing — no per-repo HTTP
			// request was made. Distinguishes this signal from a fetch_prs
			// 404, which records 404.
			name: "marks cache-only repo as deleted; LastHTTPStatus stays 0 (no per-repo request)",
			seedSettings: BitbucketSettings{
				Workspace: "ws",
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/gone", Status: active, DefaultBranch: "main", LastSeenAt: "2026-05-01T00:00:00Z"},
				},
			},
			liveRepos: nil,
			assertPersisted: func(t *testing.T, got BitbucketSettings) {
				require.Len(t, got.DiscoveredRepos, 1)
				dr := got.DiscoveredRepos[0]
				assert.Equal(t, deleted, dr.Status)
				assert.Zero(t, dr.LastHTTPStatus, "no per-repo HTTP request was made — status stays 0")
				assert.NotEmpty(t, dr.LastErrorAt)
				assert.Equal(t, "main", dr.DefaultBranch, "DefaultBranch preserved across deletion")
				assert.Equal(t, "2026-05-01T00:00:00Z", dr.LastSeenAt, "LastSeenAt frozen at the last live observation")
			},
		},
		{
			// Restored after a prior 404 → promoted back to ACTIVE,
			// LastErrorAt + LastHTTPStatus cleared so monitoring does not keep
			// surfacing the stale failure stamp.
			name: "promotes a deleted repo back to active and clears error stamps",
			seedSettings: BitbucketSettings{
				Workspace: "ws",
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/restored", Status: deleted, DefaultBranch: "main", LastErrorAt: "2026-05-10T00:00:00Z", LastHTTPStatus: 404},
				},
			},
			liveRepos: []bbRepository{mkRepo("ws/restored", "main", false)},
			assertPersisted: func(t *testing.T, got BitbucketSettings) {
				require.Len(t, got.DiscoveredRepos, 1)
				dr := got.DiscoveredRepos[0]
				assert.Equal(t, active, dr.Status)
				assert.Empty(t, dr.LastErrorAt, "LastErrorAt cleared on restore")
				assert.Zero(t, dr.LastHTTPStatus, "LastHTTPStatus cleared on restore")
				assert.NotEmpty(t, dr.LastSeenAt, "LastSeenAt refreshed on restore")
			},
		},
		{
			// Repo already DELETED observed missing again → status stays
			// DELETED and the ORIGINAL LastErrorAt is preserved. The guard
			// at operations.go:477 prevents the timestamp from being
			// refreshed every reconciliation pass, which would erase "how
			// long has this been gone".
			name: "preserves original LastErrorAt on repeated deletion observation",
			seedSettings: BitbucketSettings{
				Workspace: "ws",
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/stale", Status: deleted, LastErrorAt: "2026-05-10T00:00:00Z", LastHTTPStatus: 404},
				},
			},
			liveRepos: nil,
			assertPersisted: func(t *testing.T, got BitbucketSettings) {
				require.Len(t, got.DiscoveredRepos, 1)
				dr := got.DiscoveredRepos[0]
				assert.Equal(t, deleted, dr.Status)
				assert.Equal(t, "2026-05-10T00:00:00Z", dr.LastErrorAt, "original deletion stamp must not be refreshed")
				assert.Equal(t, 404, dr.LastHTTPStatus)
			},
		},
		{
			// Mixed run pins that the three branches compose correctly in
			// a single PatchSettings closure.
			name: "mixed run: restore + append + delete in one call",
			seedSettings: BitbucketSettings{
				Workspace: "ws",
				DiscoveredRepos: []BitbucketDiscoveredRepo{
					{FullName: "ws/restored", Status: deleted, LastErrorAt: "2026-05-10T00:00:00Z", LastHTTPStatus: 404},
					{FullName: "ws/going", Status: active, DefaultBranch: "main", LastSeenAt: "2026-05-10T00:00:00Z"},
				},
			},
			liveRepos: []bbRepository{
				mkRepo("ws/restored", "main", false),
				mkRepo("ws/fresh", "trunk", true),
			},
			assertPersisted: func(t *testing.T, got BitbucketSettings) {
				require.Len(t, got.DiscoveredRepos, 3)
				byName := make(map[string]BitbucketDiscoveredRepo, len(got.DiscoveredRepos))
				for _, dr := range got.DiscoveredRepos {
					byName[dr.FullName] = dr
				}
				assert.Equal(t, active, byName["ws/restored"].Status)
				assert.Empty(t, byName["ws/restored"].LastErrorAt)
				assert.Equal(t, active, byName["ws/fresh"].Status)
				assert.Equal(t, "trunk", byName["ws/fresh"].DefaultBranch)
				assert.True(t, byName["ws/fresh"].Private)
				assert.Equal(t, deleted, byName["ws/going"].Status)
				assert.Zero(t, byName["ws/going"].LastHTTPStatus, "fetch_repos cache-only-missing path leaves LastHTTPStatus=0")
				assert.NotEmpty(t, byName["ws/going"].LastErrorAt)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			updater := &stubConfigUpdater{}
			params := testParams(testkit.NewChannelSink(), &mockVendorGateway{}, nil)
			params.Config.Settings = mustJSON(tt.seedSettings)
			updater.current = params.Config.Settings
			if !tt.nilUpdater {
				params.ConfigUpdater = updater
			}

			saveDiscoveredRepos(context.Background(), params, tt.seedSettings, tt.liveRepos)

			if tt.expectNoWrite {
				assert.JSONEq(t, string(params.Config.Settings), string(updater.current),
					"settings must be untouched for early-return cases")
				return
			}

			var got BitbucketSettings
			require.NoError(t, json.Unmarshal(updater.current, &got))
			tt.assertPersisted(t, got)
		})
	}
}

// TestFetchCommitsOp_AlwaysDefaultBranch verifies fetch_commits always passes
// the repo's default branch as the Include ref, regardless of the tracked-branch
// / all-branches setting (which governs only the PR read-filter). The default
// branch is read from the DiscoveredRepos cache (populated by fetch_repos), so
// fetch_commits stays on the cached repo list without an extra listing call.
// The mock captures Include from the BitbucketListCommitsRequest.
func TestFetchCommitsOp_AlwaysDefaultBranch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const repoFullName = "myworkspace/myrepo"

	// Discovered-repos cache always carries DefaultBranch=main and ACTIVE status,
	// so fetchAllRepos returns it without a live listing call.
	discovered := []BitbucketDiscoveredRepo{{
		FullName:      repoFullName,
		DefaultBranch: "main",
		Status:        bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_ACTIVE,
	}}

	tests := []struct {
		name     string
		settings BitbucketSettings
	}{
		{
			name:     "tracked branch develop ignored",
			settings: BitbucketSettings{Workspace: "myworkspace", DisableCommitDiffstat: true, DiscoveredRepos: discovered, TrackedBranch: "develop"},
		},
		{
			name:     "track all branches ignored",
			settings: BitbucketSettings{Workspace: "myworkspace", DisableCommitDiffstat: true, DiscoveredRepos: discovered, TrackAllBranches: true},
		},
		{
			name:     "per-repo override develop ignored",
			settings: BitbucketSettings{Workspace: "myworkspace", DisableCommitDiffstat: true, DiscoveredRepos: discovered, RepoBranchOverrides: map[string]string{repoFullName: "develop"}},
		},
		{
			name:     "per-repo all-branches ignored",
			settings: BitbucketSettings{Workspace: "myworkspace", DisableCommitDiffstat: true, DiscoveredRepos: discovered, RepoAllBranches: []string{repoFullName}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedInclude string
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch r := req.(type) {
					case *connectorpb.BitbucketListCommitsRequest:
						capturedInclude = r.GetInclude()
						return bbCommitsResponse([]bbCommit{{Hash: "c1", Date: now}}, false), nil
					default:
						return nil, fmt.Errorf("unexpected request %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			params := &sdk.ExecutionParams{
				Config: &sdk.ConnectorConfig{
					ConnectorID:    "bitbucket",
					OrganizationID: "org-test",
					Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "t"}),
					Settings:       mustJSON(tt.settings),
					Enabled:        true,
				},
				Sink:          sink,
				VendorGateway: gw,
			}

			_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
			require.NoError(t, err)
			assert.Equal(t, "main", capturedInclude,
				"commit fetch must use the default branch, not the tracked/override branch")
			require.Len(t, sink.Records(), 1)
		})
	}
}

// TestFetchCommitsOp_ExplicitRepoListFallsBackToMain covers the explicit
// repo-list path (settings.Repositories set, DiscoveredRepos empty): there is
// no cached default branch, so resolveCommitBranch must fall back to a concrete
// "main" rather than emitting an empty Include — an empty Include on the
// Bitbucket commits endpoint is `git log --all` (every branch), which would
// silently ingest all-branch commits and break the default-branch-only
// contract. This is the failure mode the cache-hit cases above cannot exercise.
func TestFetchCommitsOp_ExplicitRepoListFallsBackToMain(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const repoFullName = "myworkspace/myrepo"

	settings := BitbucketSettings{
		Workspace:             "myworkspace",
		DisableCommitDiffstat: true,
		Repositories:          []string{repoFullName},
		DiscoveredRepos:       nil,
	}

	var capturedInclude string
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.BitbucketListCommitsRequest:
				capturedInclude = r.GetInclude()
				return bbCommitsResponse([]bbCommit{{Hash: "c1", Date: now}}, false), nil
			default:
				return nil, fmt.Errorf("unexpected request %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-test",
			Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "t"}),
			Settings:       mustJSON(settings),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}

	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Equal(t, "main", capturedInclude,
		"explicit repo-list with no cached default branch must fall back to a concrete branch, never an empty (all-branches) Include")
	require.Len(t, sink.Records(), 1)
}

// TestFetchPRs_DerivesCrossRepoFromSourceDestination verifies that when a PR's
// source repository differs from its destination repository, the emitted records
// carry is_cross_repository="true" and head_repo_owner set to the source workspace.
// Same-repo PRs must not set the field.
func TestFetchPRs_DerivesCrossRepoFromSourceDestination(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name              string
		pr                bbPullRequest
		wantCrossRepo     bool
		wantHeadRepoOwner string
	}{
		{
			name: "fork_pr_sets_cross_repository_and_head_repo_owner",
			pr: bbPullRequest{
				ID:        11,
				Title:     "Fork PR",
				State:     "OPEN",
				CreatedOn: now.Add(-time.Hour),
				UpdatedOn: now,
				Source: &bbPRRef{
					Branch:     &bbBranch{Name: "feat/fork-branch"},
					Repository: &bbRepository{FullName: "fork-workspace/myrepo", UUID: "{fork-uuid}"},
				},
				Destination: &bbPRRef{
					Branch:     &bbBranch{Name: "main"},
					Repository: &bbRepository{FullName: "myworkspace/myrepo", UUID: "{dest-uuid}"},
				},
			},
			wantCrossRepo:     true,
			wantHeadRepoOwner: "fork-workspace",
		},
		{
			name: "same_repo_pr_does_not_set_cross_repository",
			pr: bbPullRequest{
				ID:        12,
				Title:     "Same-repo PR",
				State:     "OPEN",
				CreatedOn: now.Add(-time.Hour),
				UpdatedOn: now,
				Source: &bbPRRef{
					Branch:     &bbBranch{Name: "feat/internal-branch"},
					Repository: &bbRepository{FullName: "myworkspace/myrepo", UUID: "{dest-uuid}"},
				},
				Destination: &bbPRRef{
					Branch:     &bbBranch{Name: "main"},
					Repository: &bbRepository{FullName: "myworkspace/myrepo", UUID: "{dest-uuid}"},
				},
			},
			wantCrossRepo:     false,
			wantHeadRepoOwner: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &mockVendorGateway{
				executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.BitbucketListPullRequestsRequest:
						return bbPRsResponse([]bbPullRequest{tt.pr}, false), nil
					case *connectorpb.BitbucketGetPRDiffstatRequest:
						return &connectorpb.BitbucketGetPRDiffstatResponse{}, nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}

			sink := testkit.NewChannelSink()
			params := testParams(sink, gw, nil)
			_, err := (&FetchPRsOp{}).Execute(context.Background(), params)
			require.NoError(t, err)

			records := sink.Records()
			require.NotEmpty(t, records)

			// All lifecycle records for a single PR carry the same baseMeta;
			// verify every emitted record, not just the first.
			for idx, rec := range records {
				meta := rec.Metadata
				if tt.wantCrossRepo {
					assert.Equal(t, "true", meta["is_cross_repository"],
						"record[%d] (%s): fork PR must set is_cross_repository=true", idx, rec.ExternalID)
					assert.Equal(t, tt.wantHeadRepoOwner, meta["head_repo_owner"],
						"record[%d] (%s): fork PR must set head_repo_owner to the source workspace", idx, rec.ExternalID)
				} else {
					assert.NotEqual(t, "true", meta["is_cross_repository"],
						"record[%d] (%s): same-repo PR must not set is_cross_repository", idx, rec.ExternalID)
					assert.Empty(t, meta["head_repo_owner"],
						"record[%d] (%s): same-repo PR must not set head_repo_owner", idx, rec.ExternalID)
				}
			}
		})
	}
}
