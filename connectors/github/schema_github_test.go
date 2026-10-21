// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// The tests below validate the payloads of the operations the in-process
// simulator run in schemas/simulator_test.go cannot produce: webhook
// deliveries, branch listings, harness file content, issue bodies and branch
// rules. Each runs the real operation against this package's fakes.

// validateRecords checks every record against its schema and that op emitted
// at least want records.
func validateRecords(t *testing.T, op string, recs []*sdk.RawRecord, want int) {
	t.Helper()
	require.GreaterOrEqual(t, len(recs), want, "%s emitted too few records", op)
	for _, r := range recs {
		require.Equal(t, "github", r.ConnectorID)
		if err := schemas.Validate(r.ConnectorID, r.OperationID, r.Payload); err != nil {
			t.Errorf("%s: record %s (%s): %v", op, r.ExternalID, r.OperationID, err)
		}
	}
}

func TestSchema_WebhookEventsAndPRSyncEvents(t *testing.T) {
	const secret = "schema-secret"
	deliveries := []struct {
		event, id, body string
		want            int
	}{
		{"pull_request", "d-1", `{
			"action": "synchronize",
			"number": 42,
			"before": "1111111111111111111111111111111111111111",
			"after": "2222222222222222222222222222222222222222",
			"pull_request": {"id": 1, "number": 42, "state": "open", "updated_at": "2026-10-01T10:00:00Z",
				"user": {"login": "carol", "id": 7, "type": "User"},
				"base": {"ref": "main", "repo": {"id": 55, "full_name": "acme/widgets"}}},
			"repository": {"id": 55, "full_name": "acme/widgets"},
			"sender": {"login": "carol", "id": 7, "type": "User"}
		}`, 2},
		{"push", "d-2", `{
			"ref": "refs/heads/main",
			"before": "1111111111111111111111111111111111111111",
			"after": "3333333333333333333333333333333333333333",
			"commits": [{"id": "3333333333333333333333333333333333333333", "message": "fix"}],
			"repository": {"id": 55, "full_name": "acme/widgets"},
			"sender": {"login": "carol", "id": 7}
		}`, 1},
		{"issues", "d-3", `{"action": "closed", "issue": {"number": 9, "state": "closed"}, "repository": {"id": 55}}`, 1},
	}
	for _, d := range deliveries {
		h := http.Header{}
		h.Set("X-Hub-Signature-256", signPayload(secret, []byte(d.body)))
		h.Set("X-GitHub-Event", d.event)
		h.Set("X-GitHub-Delivery", d.id)
		sink := testkit.NewChannelSink()
		_, err := (&WebhookEventsOp{}).HandleWebhook(context.Background(), &sdk.WebhookParams{
			Config: webhookConfig(secret), Headers: h, Body: []byte(d.body), Sink: sink,
		})
		require.NoError(t, err)
		recs := sink.Records()
		require.Len(t, recs, d.want, d.event)
		validateRecords(t, "webhook_events", recs, d.want)
	}
}

func TestSchema_FetchBranches(t *testing.T) {
	body := `[
		{"name":"main","commit":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","url":"https://api.github.com/repos/testorg/myrepo/commits/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"protected":true},
		{"name":"feature/x","commit":{"sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","url":"https://api.github.com/repos/testorg/myrepo/commits/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"protected":false}
	]`
	sink := testkit.NewChannelSink()
	gw := &branchDoGateway{pages: map[int]string{1: body}, lastPage: 1}
	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchTestParams(sink, gw))
	require.NoError(t, err)
	validateRecords(t, "fetch_branches", sink.Records(), 2)
}

func TestSchema_FetchFileContent(t *testing.T) {
	ctrl := gomock.NewController(t)
	live := oneFileRecord()
	tomb := oneFileRecord()
	tomb.SourcePath = ".claude/skills/old/SKILL.md"
	tomb.IsTombstone = true
	tomb.Files = nil
	srv := contentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": []byte("# Deploy\n")}, nil)
	queue := sdkmocks.NewMockWorkQueue(ctrl)
	queue.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{live, tomb}}, nil)
	s3, _, _ := captureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := newParams(newDiffMockGateway(srv.URL), s3, newTestBudget(t), sink)
	params.Host.WorkQueue = queue

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	validateRecords(t, "fetch_file_content", sink.Records(), 2)
}

func TestSchema_FetchIssuesData(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			return fetchIssuesDataResponseRaw(777, []ghIssueDescription{
				{Number: 42, UpdatedAt: time.Now().UTC(), Body: "Steps to reproduce:\n- open the app"},
			}, false, ""), nil
		},
	}
	sink := testkit.NewChannelSink()
	s3, _ := newRecordingS3(t)
	params := testParams(sink, gw, nil)
	params.BlobStore = s3
	_, err := (&FetchIssuesDataOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	validateRecords(t, "fetch_issues_data", sink.Drain(), 1)
}

func TestSchema_FetchConfigWithBranchRules(t *testing.T) {
	rules := `[
		{"type":"pull_request","ruleset_source_type":"Repository","ruleset_source":"acme/widgets","ruleset_id":7,
		 "parameters":{"required_approving_review_count":2,"allowed_merge_methods":["squash"]}},
		{"type":"non_fast_forward","ruleset_source_type":"Organization","ruleset_source":"acme","ruleset_id":8}
	]`
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *vgpb.GetRepositoryRequest:
				return &vgpb.GetRepositoryResponse{RawJson: []byte(`{"id":12345,"full_name":"acme/widgets","default_branch":"main"}`)}, nil
			case *vgpb.GetBranchRulesRequest:
				return &vgpb.GetBranchRulesResponse{RawJson: []byte(rules)}, nil
			}
			return nil, nil
		},
	}
	sink := testkit.NewChannelSink()
	_, err := (&FetchConfigOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)
	validateRecords(t, "fetch_config", sink.Records(), 1)
}

// TestSchema_RejectsDrift checks the schemas are strict where the connector
// builds the payload: a key the struct does not have, or a wrong type, fails.
func TestSchema_RejectsDrift(t *testing.T) {
	for _, tc := range []struct{ op, payload string }{
		{"fetch_prs_data", `{"s3_key":"descriptions/o/github/pull_requests/1/2","pr_number":"2","platform_repo_id":"1","repo_full_name":"a/b"}`},
		{"fetch_org_members", `{"numeric_user_id":1,"login":"a","hint_source":"org_members_api","extra":true}`},
		{"fetch_repos", `{"platformRepoId":"1","name":"b","fullName":"a/b","vendorBlobJson":"e30="}`},
	} {
		require.Error(t, schemas.Validate("github", tc.op, []byte(tc.payload)), tc.op)
	}
}
