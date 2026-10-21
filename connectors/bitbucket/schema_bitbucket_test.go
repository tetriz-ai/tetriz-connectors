// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// The simulator run in schemas/simulator_test.go cannot produce these record
// kinds: webhooks, file content (host work queue), the branching model and
// branch restrictions (the simulator answers 404), changes-requested reviews,
// no-change trees, member groups, and vendor-only PR fields. Each case below
// builds them through the connector's own code and validates them.

// validateBitbucketRecords validates every record against its schema.
func validateBitbucketRecords(t *testing.T, recs []*sdk.RawRecord) {
	t.Helper()
	require.NotEmpty(t, recs, "the fixture emitted no records")
	for _, r := range recs {
		if err := schemas.Validate("bitbucket", r.OperationID, r.Payload); err != nil {
			t.Errorf("%s: record %s: %v", r.OperationID, r.ExternalID, err)
		}
	}
}

func TestSchema_BitbucketWebhookEvents(t *testing.T) {
	secret := "s"
	body := []byte(`{"actor":{"type":"user","uuid":"{u}"},"repository":{"full_name":"ws/repo","uuid":"{r}"},"pullrequest":{"id":7,"state":"OPEN"}}`)
	headers := http.Header{}
	headers.Set("X-Hub-Signature", signPayload(secret, body))
	headers.Set("X-Event-Key", "pullrequest:created")
	headers.Set("X-Hook-UUID", "hook-1")
	headers.Set("X-Request-UUID", "req-1")
	sink := testkit.NewChannelSink()
	_, err := (&WebhookEventsOp{}).HandleWebhook(context.Background(), &sdk.WebhookParams{
		Config: webhookConfig(secret), Headers: headers, Body: body, Sink: sink,
	})
	require.NoError(t, err)
	validateBitbucketRecords(t, sink.Records())
}

func TestSchema_BitbucketFileContent(t *testing.T) {
	ctrl := gomock.NewController(t)
	srv := bbfcContentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": []byte("# skill\n")}, nil)
	tomb := bbfcOneFileRecord()
	tomb.SourcePath = ".claude/skills/old/SKILL.md"
	tomb.IsTombstone = true
	tomb.Files = nil
	queue := sdkmocks.NewMockWorkQueue(ctrl)
	queue.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{bbfcOneFileRecord(), tomb}}, nil)
	s3, _, _ := bbfcCaptureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := bbfcNewParams(newBBFCMockGateway(srv.URL), s3, bbfcNewTestBudget(t), sink)
	params.Host.WorkQueue = queue
	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	require.Len(t, sink.Records(), 2)
	validateBitbucketRecords(t, sink.Records())
}

func TestSchema_BitbucketBranchesAndBranchingModel(t *testing.T) {
	branch := `{"name":"main","type":"branch","target":{"hash":"0123456789abcdef0123456789abcdef01234567","date":"2026-01-05T10:00:00+00:00","message":"m","type":"commit"},"merge_strategies":["merge_commit","squash"],"default_merge_strategy":"squash","links":{"self":{"href":"https://api.bitbucket.org/x"}}}`
	gw := &bbBranchGateway{
		repoUUID:           "{uuid-bm}",
		mainbranch:         "main",
		branchBodyByURL:    func(string) string { return `{"values":[` + branch + `],"next":""}` },
		branchingModelBody: `{"type":"branching_model","development":{"name":"develop","use_mainbranch":false,"branch":{"name":"develop","type":"branch"}},"production":{"name":"main","use_mainbranch":true},"branch_types":[{"kind":"feature","prefix":"feature/"},{"kind":"release","prefix":"release/"}],"links":{"self":{"href":"https://api.bitbucket.org/x"}}}`,
	}
	sink := testkit.NewChannelSink()
	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsBB(sink, gw))
	require.NoError(t, err)
	ops := map[string]bool{}
	for _, r := range sink.Records() {
		ops[r.OperationID] = true
	}
	require.True(t, ops["fetch_branches"] && ops["fetch_branching_model"], "both record kinds: %v", ops)
	validateBitbucketRecords(t, sink.Records())
}

func TestSchema_BitbucketChangesRequestedReview(t *testing.T) {
	pr := bbPullRequest{
		ID:        42,
		CreatedOn: time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC),
		Participants: []bbPRParticipant{
			{User: &bbUser{AccountID: "cr", DisplayName: "CR"}, Role: "REVIEWER", State: "changes_requested"},
		},
	}
	sink := testkit.NewChannelSink()
	require.NoError(t, emitChangesRequestedReviews(context.Background(), sink, "fetch_reviews", "org", "ws/repo", bbPRInfo{ID: 42, CreatedOn: pr.CreatedOn}, pr))
	validateBitbucketRecords(t, sink.Records())
}

func TestSchema_BitbucketRepoTreeNoChange(t *testing.T) {
	const sha = "aabbcc112233"
	ctrl := gomock.NewController(t)
	gw := &bbTreeGateway{doJSONHandlers: []bbTreeHandler{bbBranchHandler(sha)}}
	shaJSON, _ := json.Marshal(sha)
	cursor := sdk.NewCompositeCursor(map[string]json.RawMessage{"myworkspace/myrepo": shaJSON})
	sink := testkit.NewChannelSink()
	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), bbTreeTestParams(t, sink, gw, sdkmocks.NewMockBlobStore(ctrl), cursor))
	require.NoError(t, err)
	validateBitbucketRecords(t, sink.Records())
}

func TestSchema_BitbucketConfigWithRestrictions(t *testing.T) {
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.BitbucketGetRepoRequest:
			return &connectorpb.BitbucketGetRepoResponse{Repo: []byte(`{"uuid":"{repo-uuid}","slug":"api","full_name":"ws/api","mainbranch":{"name":"main","type":"branch"}}`)}, nil
		case *connectorpb.BitbucketGetBranchRequest:
			return &connectorpb.BitbucketGetBranchResponse{RawJson: []byte(`{"name":"main","default_merge_strategy":"squash","merge_strategies":["merge_commit","squash"]}`)}, nil
		case *connectorpb.BitbucketListBranchRestrictionsRequest:
			return &connectorpb.BitbucketListBranchRestrictionsResponse{Restrictions: [][]byte{
				[]byte(`{"id":1,"kind":"require_approvals_to_merge","branch_match_kind":"glob","pattern":"main","value":2,"users":[],"groups":[],"type":"branchrestriction"}`),
				[]byte(`{"id":2,"kind":"push","branch_match_kind":"branching_model","branch_type":"production","pattern":"","value":null,"users":[],"groups":[]}`),
			}}, nil
		}
		return nil, nil
	}}
	sink := testkit.NewChannelSink()
	_, err := (&FetchConfigOp{}).Execute(context.Background(), &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID: "bitbucket", OrganizationID: "org", Enabled: true,
			Credentials: mustJSON(BitbucketCredentials{OAuthAccessToken: "tok"}),
			Settings:    mustJSON(BitbucketSettings{Workspace: "ws", DiscoveredRepos: []BitbucketDiscoveredRepo{{FullName: "ws/api", DefaultBranch: "main"}}}),
		},
		Sink: sink, VendorGateway: gw,
	})
	require.NoError(t, err)
	require.Contains(t, string(sink.Records()[0].Payload), "branch_restrictions")
	validateBitbucketRecords(t, sink.Records())
}

// TestSchema_BitbucketVendorShapes validates vendor-shaped bodies, with the
// fields and nulls the simulator omits, through the connector's injection code.
func TestSchema_BitbucketVendorShapes(t *testing.T) {
	user := `{"type":"user","uuid":"{u1}","account_id":"557058:abc","display_name":"Ann","nickname":"ann","links":{"avatar":{"href":"https://x"}}}`
	pr := `{"id":12,"title":"t","description":"","state":"DECLINED","created_on":"2026-01-05T10:00:00.123456+00:00","updated_on":"2026-01-06T11:00:00.654321+00:00",` +
		`"author":` + user + `,"closed_by":null,"merge_commit":null,"comment_count":0,"task_count":0,"close_source_branch":false,"type":"pullrequest","reason":"",` +
		`"source":{"branch":{"name":"feature/x"},"commit":{"hash":"abc1234","type":"commit"},"repository":{"type":"repository","full_name":"ws/repo","name":"repo","uuid":"{r}"}},` +
		`"destination":{"branch":{"name":"main"},"commit":{"hash":"def5678"},"repository":{"full_name":"ws/repo","name":"repo","uuid":"{r}"}},` +
		`"reviewers":[` + user + `],"participants":[{"type":"participant","user":` + user + `,"role":"REVIEWER","approved":false,"state":null,"participated_on":null}],"links":{"html":{"href":"https://x"}},"summary":{"raw":""}}`
	prPayload := injectPRCounts(injectLineCounts(json.RawMessage(pr), 3, 1), 1, 1)
	commit := `{"type":"commit","hash":"0123456789abcdef0123456789abcdef01234567","date":"2026-01-05T10:00:00+00:00","message":"fix\n","author":{"type":"author","raw":"Ann <a@x>","user":` + user + `},"parents":[{"hash":"1111111","type":"commit"}],"links":{},"summary":{"raw":"fix"},"repository":{"full_name":"ws/repo"}}`
	commentBody := `{"id":5,"created_on":"2026-01-05T10:00:00.1+00:00","updated_on":"2026-01-05T10:00:00.1+00:00","user":` + user + `,"content":{"type":"rendered","raw":"hi","markup":"markdown","html":"<p>hi</p>"},"inline":{"from":null,"to":3,"path":"a.go"},"deleted":false,"pending":false,"type":"pullrequest_comment","pullrequest":{"id":12},"links":{}}`
	member, err := json.Marshal(WorkspaceMemberRecord{AccountID: "a", DisplayName: "A", HintSource: "workspace_members_api", Groups: []string{"devs"}})
	require.NoError(t, err)
	repo := bbRepository{UUID: "{r}", Slug: "repo", FullName: "ws/repo", IsPrivate: true, Size: 4096, SCM: "git",
		CreatedOn: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Workspace: &bbWorkspace{UUID: "{w}", Slug: "ws"},
		Mainbranch: &bbBranchRef{Name: "main", Type: "branch"}, Project: &bbProject{Key: "P", UUID: "{p}"},
		Parent: &bbRepository{UUID: "{parent}"}, Links: &bbRepoLinks{HTML: &bbLink{Href: "https://bitbucket.org/ws/repo"}, Clone: []bbClone{{Name: "https", Href: "https://x"}, {Name: "ssh", Href: "git@x"}}}}
	repoPayload, err := protojson.Marshal(mapBitbucketRepoToRecord(&repo, BitbucketSettings{}))
	require.NoError(t, err)

	validateBitbucketRecords(t, []*sdk.RawRecord{
		{OperationID: "fetch_prs", ExternalID: "ws/repo#PR-12#closed", Payload: prPayload},
		{OperationID: "fetch_commits", ExternalID: "ws/repo#0123", Payload: injectCommitLineCounts(json.RawMessage(commit), 1, 2, 3)},
		{OperationID: "fetch_commits", ExternalID: "ws/repo#0123 unenriched", Payload: json.RawMessage(commit)},
		{OperationID: "fetch_pr_comments", ExternalID: "ws/repo#PR-12/COMMENT-5", Payload: json.RawMessage(commentBody)},
		{OperationID: "fetch_workspace_members", ExternalID: "member-a", Payload: member},
		{OperationID: "fetch_repos", ExternalID: "{r}", Payload: repoPayload},
	})
}

// TestSchema_BitbucketRejectsWrongShapes checks the schemas are not vacuous.
func TestSchema_BitbucketRejectsWrongShapes(t *testing.T) {
	wrappedBlob := `{"platformRepoId":"{r}","name":"r","fullName":"ws/r","visibility":"REPOSITORY_VISIBILITY_PRIVATE","stats":{},"vendorBlobJson":"eyJiaXRidWNrZXQiOnt9fQ=="}` // {"bitbucket":{}}
	for _, c := range []struct{ op, payload string }{
		{"fetch_repos", wrappedBlob},
		{"fetch_reviews", `{"user":{"account_id":"a","display_name":"","nickname":""},"state":"DISMISSED","pr_id":"1","created_on":"2026-01-01T00:00:00Z"}`},
		{"fetch_prs", `{"id":1,"state":"OPEN"}`},
		{"fetch_repo_tree", `{"platformRepoId":"{r}","treeSha":"a","treeBranch":"main","fetchedAt":"2026-01-01T00:00:00Z"}`},
		{"fetch_prs_data", `{"s3_key":"x","pr_number":1,"platform_repo_id":"{r}","repo_full_name":"ws/r"}`},
	} {
		require.Error(t, schemas.Validate("bitbucket", c.op, []byte(c.payload)), "%s accepted %s", c.op, c.payload)
	}
}
