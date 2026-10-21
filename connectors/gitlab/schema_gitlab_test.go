// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// The simulator cannot run webhook_events (webhook-only) or fetch_file_content
// (host-only), so these tests build their records through the real handlers
// and validate them against schemas/gitlab. schemas/simulator_test.go covers
// the other operations.

// validateRecords fails for each record whose payload does not match the
// schema of its operationId.
func validateRecords(t *testing.T, op string, recs []*sdk.RawRecord) {
	t.Helper()
	for _, r := range recs {
		if err := schemas.Validate(string(connectorID), r.OperationID, r.Payload); err != nil {
			t.Errorf("%s: record %s (operationId %s): %v", op, r.ExternalID, r.OperationID, err)
		}
	}
}

func TestSchema_WebhookRecordsMatchSchemas(t *testing.T) {
	const secret = "s3cret"
	hooks := []struct {
		event string
		body  string
		want  []string // operationIds the hook must emit
	}{
		{"Push Hook", `{"object_kind":"push","ref":"refs/heads/main","before":"a","after":"b","project":{"id":1,"path_with_namespace":"acme/widgets"},"commits":[{"id":"0123456789abcdef0123456789abcdef01234567","message":"fix","timestamp":"2026-03-01T10:00:00Z"},{"id":"1123456789abcdef0123456789abcdef01234567","timestamp":"2026-03-01T11:00:00+02:00"}]}`, []string{"webhook_events", "webhook_events"}},
		{"Tag Push Hook", `{"object_kind":"tag_push","ref":"refs/tags/v1.0","before":"0000","after":"b","checkout_sha":"b","project":{"path_with_namespace":"acme/widgets"},"commits":[]}`, []string{"webhook_events"}},
		{"Push Hook", `{"object_kind":"push","ref":"refs/heads/gone","checkout_sha":null,"project":{"path_with_namespace":"acme/widgets"},"commits":[]}`, []string{"webhook_events"}},
		{"Merge Request Hook", `{"object_kind":"merge_request","user":{"id":5,"username":"dev"},"object_attributes":{"iid":7,"project_id":1,"state":"merged","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-02T00:00:00Z","merged_at":"2026-01-02T00:00:00Z","closed_at":null}}`, []string{"webhook_events", "webhook_events"}},
		{"Merge Request Hook", `{"object_kind":"merge_request","object_attributes":{"iid":9,"project_id":1,"action":"open","state":"opened","created_at":"2026-01-01T00:00:00Z"}}`, []string{"webhook_events", "webhook_events"}},
		{"Merge Request Hook", `{"object_kind":"merge_request","object_attributes":{"iid":8,"project_id":1,"state":"closed","created_at":"2026-01-01T00:00:00Z","closed_at":"2026-01-03T00:00:00Z"}}`, []string{"webhook_events", "webhook_events"}},
		{"Issue Hook", `{"object_kind":"issue","project":{"path_with_namespace":"acme/widgets"},"labels":[{"title":"bug"}],"object_attributes":{"iid":3,"project_id":1,"title":"Broken","author_id":9,"assignee_ids":[9,10],"created_at":"2026-02-01T00:00:00Z","updated_at":"2026-02-02T00:00:00Z","closed_at":"2026-02-02T00:00:00Z","state":"closed","due_date":"2026-04-30","type":"Test Case"}}`, []string{"webhook_events", "webhook_events"}},
		{"Issue Hook", `{"object_kind":"issue","project":{"path_with_namespace":"acme/widgets"},"object_attributes":{"iid":4,"project_id":1,"title":"New","created_at":"2026-02-01T00:00:00Z","state":"opened","type":"Issue"}}`, []string{"webhook_events"}},
		{"Note Hook", `{"object_kind":"note","user":{"id":9,"username":"rev","bot":false},"project":{"id":1},"merge_request":{"iid":7,"author":{"id":5,"username":"dev"}},"object_attributes":{"id":77,"project_id":1,"created_at":"2026-01-01T12:00:00Z","updated_at":"2026-01-01T12:00:00Z","system":false}}`, []string{"webhook_events"}},
	}
	for _, h := range hooks {
		headers := http.Header{}
		headers.Set("X-Gitlab-Token", secret)
		headers.Set("X-Gitlab-Event", h.event)
		sink := testkit.NewChannelSink()
		_, err := (&WebhookEventsOp{}).HandleWebhook(context.Background(), &sdk.WebhookParams{
			Config: webhookConfig(secret), Headers: headers, Body: []byte(h.body), Sink: sink,
		})
		require.NoError(t, err, h.event)
		recs := sink.Records()
		got := make([]string, 0, len(recs))
		for _, r := range recs {
			got = append(got, r.OperationID)
		}
		require.Equal(t, h.want, got, "%s: emitted operationIds", h.event)
		validateRecords(t, "webhook_events "+h.event, recs)
	}
}

func TestSchema_FileContentRecordsMatchSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	srv := glContentServer(t, map[string][]byte{".claude/skills/deploy/SKILL.md": []byte("# Deploy\n")}, nil, nil)

	removal := fcOneFileRecord()
	removal.SourcePath = ".claude/skills/old/SKILL.md"
	removal.Files = nil
	removal.RemovedPaths = []string{"scripts/old.sh"}
	tombstone := &workqueuev1.FileContentRecord{
		RepositoryId: "repo-1", IdeType: fcOneFileRecord().GetIdeType(),
		SourcePath: ".cursor/rules/gone.mdc", Generation: 43, IsTombstone: true,
	}
	queue := sdkmocks.NewMockWorkQueue(ctrl)
	queue.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(
		&workqueuev1.ListPendingFileFetchesResponse{Records: []*workqueuev1.FileContentRecord{fcOneFileRecord(), removal, tombstone}}, nil)
	blobs, _, _ := fcCaptureStagingWrites(t, ctrl)
	sink := testkit.NewChannelSink()
	params := fcNewParams(newFCMockGateway(srv.URL), blobs, fcNewTestBudget(t), sink)
	params.Host.WorkQueue = queue

	_, err := (&FetchFileContentOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	recs := sink.Records()
	require.Len(t, recs, 3, "a fetched artifact, a removal-only artifact and a tombstone")
	validateRecords(t, opFetchFileContent, recs)
}

// TestSchema_StructPayloadsMatchSchemas builds the payloads the connector
// marshals itself from fully populated inputs, which the seeded simulator
// leaves partly empty.
func TestSchema_StructPayloadsMatchSchemas(t *testing.T) {
	lastActivity := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	p := glProject{
		ID: 12, PathWithNamespace: "acme/widgets", Name: "Widgets", Path: "widgets",
		DefaultBranch: "main", Visibility: "internal", CreatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Description: "d", Archived: true, LastActivityAt: &lastActivity, StarCount: 3, ForksCount: 1,
		OpenIssuesCount: 2, IssuesEnabled: true, WikiEnabled: true, WebURL: "https://gitlab.example.com/acme/widgets",
		SSHURLToRepo: "git@gitlab.example.com:acme/widgets.git", HTTPURLToRepo: "https://gitlab.example.com/acme/widgets.git",
		Topics: []string{"go"}, ImportStatus: "none", EmptyRepo: false,
		ForkedFromProject: &struct {
			ID int `json:"id"`
		}{ID: 11},
		Namespace: &struct {
			ID   int    `json:"id"`
			Path string `json:"path"`
			Kind string `json:"kind"`
		}{ID: 4, Path: "acme", Kind: "group"},
	}
	repo, err := protojson.Marshal(mapGitLabProjectToRecord(&p, GitLabSettings{TrackedBranch: "develop"}))
	require.NoError(t, err)
	member, err := json.Marshal(GroupMemberRecord{NumericUserID: 5, Login: "dev", Email: "dev@example.com", Name: "Dev", AvatarURL: "https://example.com/a.png", HintSource: HintSourceGroupMembersAPI, Groups: []string{"acme/backend"}})
	require.NoError(t, err)
	validateRecords(t, "struct", []*sdk.RawRecord{
		{OperationID: opFetchRepos, ExternalID: "12", Payload: repo},
		{OperationID: "fetch_org_members", ExternalID: "member-5", Payload: member},
	})

	// The schemas are not vacuous: a blob with an unknown key and an approval
	// with a wrong state are rejected.
	var rec map[string]any
	require.NoError(t, json.Unmarshal(repo, &rec))
	rec["vendorBlobJson"] = base64.StdEncoding.EncodeToString([]byte(`{"gitlab":{}}`))
	bad, err := json.Marshal(rec)
	require.NoError(t, err)
	require.Error(t, schemas.Validate("gitlab", opFetchRepos, bad), "a vendor blob of the wrong shape")
	require.Error(t, schemas.Validate("gitlab", "fetch_reviews", []byte(`{"user":{"id":1,"username":"a","bot":false},"state":"COMMENTED","created_at":"2026-01-01T00:00:00Z"}`)), "an approval that is not APPROVED")
}
