// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func webhookConfig(secret string) *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID:    "gitlab",
		OrganizationID: "org-test",
		Credentials: mustJSON(GitLabCredentials{
			OAuthRefreshToken: "test-refresh-token",
			WebhookSecret:     secret,
		}),
		Settings: mustJSON(GitLabSettings{
			GroupID:   42,
			GroupName: "mygroup",
		}),
		Enabled: true,
	}
}

func TestWebhookEventsOp_HandleWebhook_ValidTokenAndEvent(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"object_kind":"merge_request","object_attributes":{"iid":7,"project_id":1,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-03-09T10:00:00Z"}}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Merge Request Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-abc-123")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	require.Len(t, records, 1)
	assert.Equal(t, "mr-1!7#snapshot", records[0].ExternalID)
	assert.Equal(t, "webhook_events", records[0].OperationID)
	assert.Equal(t, "gitlab", records[0].ConnectorID)
	assert.Equal(t, "org-test", records[0].OrganizationID)
}

func TestWebhookEventsOp_HandleWebhook_WrongToken(t *testing.T) {
	secret := "correct-secret"
	body := []byte(`{"object_kind":"push"}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", "wrong-secret")
	headers.Set("X-Gitlab-Event", "Push Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-wrong-456")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	_, err := op.HandleWebhook(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid X-Gitlab-Token")
	assert.Empty(t, sink.Records())
}

func TestWebhookEventsOp_HandleWebhook_EmptyTokenHeader(t *testing.T) {
	secret := "correct-secret"
	body := []byte(`{"object_kind":"push"}`)

	headers := http.Header{}
	// X-Gitlab-Token not set — empty string received
	headers.Set("X-Gitlab-Event", "Push Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-empty-789")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	_, err := op.HandleWebhook(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid X-Gitlab-Token")
	assert.Empty(t, sink.Records())
}

func TestWebhookEventsOp_HandleWebhook_NoWebhookSecret(t *testing.T) {
	body := []byte(`{"object_kind":"push"}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", "some-token")
	headers.Set("X-Gitlab-Event", "Push Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-nosec-000")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		// Empty webhook secret in config
		Config:  webhookConfig(""),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	_, err := op.HandleWebhook(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no webhook secret configured")
	assert.Empty(t, sink.Records())
}

func TestWebhookEventsOp_HandleWebhook_EmptyEventType(t *testing.T) {
	secret := "test-secret"
	body := []byte(`{}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	// X-Gitlab-Event intentionally not set

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Acknowledged)
	assert.Empty(t, sink.Records(), "should not emit for empty event type")
}

func TestWebhookEventsOp_HandleWebhook_UnsupportedEvent(t *testing.T) {
	secret := "test-secret"
	body := []byte(`{"object_kind":"unknown"}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Some Future Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-unknown-111")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Acknowledged, "should acknowledge unsupported events")
	assert.Empty(t, sink.Records(), "unknown events should be dropped")
}

func TestWebhookEventsOp_HandleWebhook_PushHook_FansOutCommits(t *testing.T) {
	secret := "push-secret"
	body := []byte(`{
		"object_kind": "push",
		"project": {"path_with_namespace": "mygroup/myrepo"},
		"commits": [
			{"id": "aaa111", "timestamp": "2026-03-09T10:00:00Z"},
			{"id": "bbb222", "timestamp": "2026-03-09T11:00:00Z"}
		]
	}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Push Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-push-222")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)

	require.NoError(t, err)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "webhook_events", records[0].OperationID)
	assert.Equal(t, "mygroup/myrepo#aaa111", records[0].ExternalID)
	assert.Equal(t, "mygroup/myrepo", records[0].Metadata["repo"])
	assert.Equal(t, "webhook_events", records[1].OperationID)
	assert.Equal(t, "mygroup/myrepo#bbb222", records[1].ExternalID)
}

func TestWebhookEventsOp_HandleWebhook_PushHook_NoCommits_FallsBackToRaw(t *testing.T) {
	secret := "push-secret"
	// Tag Push Hook has no commits array — uses project@ref as ExternalID.
	body := []byte(`{"object_kind":"tag_push","ref":"v1.2.3","project":{"path_with_namespace":"mygroup/myrepo"}}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Tag Push Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-tag-999")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)

	require.NoError(t, err)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	require.Len(t, records, 1)
	assert.Equal(t, "mygroup/myrepo@v1.2.3", records[0].ExternalID)
	assert.Equal(t, "webhook_events", records[0].OperationID)
	assert.Equal(t, "Tag Push Hook", records[0].Metadata["event_type"])
}

func TestWebhookEventsOp_HandleWebhook_MergeRequestHook_Open(t *testing.T) {
	secret := "mr-secret"
	body := []byte(`{"object_kind":"merge_request","object_attributes":{"iid":42,"project_id":10,"state":"opened","created_at":"2026-01-15T08:00:00Z","updated_at":"2026-03-09T10:00:00Z"}}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Merge Request Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-mr-333")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)

	require.NoError(t, err)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	// An MR hook without the open action: only #snapshot (no #opened, #merged or #closed).
	require.Len(t, records, 1)
	assert.Equal(t, "mr-10!42#snapshot", records[0].ExternalID)
	assert.Equal(t, "webhook_events", records[0].OperationID)
	assert.Equal(t, "snapshot", records[0].Metadata["event_type"])
}

func TestWebhookEventsOp_HandleWebhook_MergeRequestHook_Merged(t *testing.T) {
	secret := "mr-secret"
	body := []byte(`{"object_kind":"merge_request","object_attributes":{"iid":42,"project_id":10,"state":"merged","created_at":"2026-01-15T08:00:00Z","updated_at":"2026-03-09T10:00:00Z","merged_at":"2026-03-09T10:00:00Z"}}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Merge Request Hook")
	headers.Set("X-Gitlab-Event-UUID", "uuid-mr-merged")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)

	require.NoError(t, err)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	// Merged MR: #snapshot + #merged.
	require.Len(t, records, 2)
	assert.Equal(t, "mr-10!42#snapshot", records[0].ExternalID)
	assert.Equal(t, "snapshot", records[0].Metadata["event_type"])
	assert.Equal(t, "mr-10!42#merged", records[1].ExternalID)
	assert.Equal(t, "merged", records[1].Metadata["event_type"])
}

func TestWebhookEventsOp_HandleWebhook_NoteHook_ReviewerLoadMetadata(t *testing.T) {
	secret := "note-secret"

	tests := []struct {
		name       string
		body       []byte
		assertMeta func(t *testing.T, meta map[string]string)
	}{
		{
			name: "human reviewer — six fields populated including pr_key and mr_iid",
			body: []byte(`{
				"object_kind": "note",
				"object_attributes": {"id": 9876, "project_id": 5, "created_at": "2026-03-09T10:00:00Z", "updated_at": "2026-03-09T10:00:00Z"},
				"user": {"id": 7001, "username": "alice", "bot": false},
				"merge_request": {"iid": 42, "author": {"id": 99001, "username": "gluser"}},
				"project": {"id": 5}
			}`),
			assertMeta: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "mr-5!42", meta["pr_key"])
				assert.Equal(t, "42", meta["mr_iid"])
				assert.Equal(t, "42", meta["pr_number"])
				assert.Equal(t, "note", meta["review_type"])
				assert.Equal(t, "gluser", meta["pr_author_login"])
				assert.Equal(t, "99001", meta["pr_author_id"])
				assert.Equal(t, "5", meta["platform_repo_id"])
				assert.Equal(t, "false", meta["reviewer_is_bot"])
				assert.Equal(t, "note", meta["record_kind"])
				assert.Len(t, meta, 9)
			},
		},
		{
			name: "bot reviewer — reviewer_is_bot=true with pr_key and mr_iid still emitted",
			body: []byte(`{
				"object_kind": "note",
				"object_attributes": {"id": 9877, "project_id": 5, "created_at": "2026-03-09T10:00:00Z", "updated_at": "2026-03-09T10:00:00Z"},
				"user": {"id": 8001, "username": "review-bot", "bot": true},
				"merge_request": {"iid": 42, "author": {"id": 99001, "username": "gluser"}},
				"project": {"id": 5}
			}`),
			assertMeta: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "mr-5!42", meta["pr_key"])
				assert.Equal(t, "42", meta["mr_iid"])
				assert.Equal(t, "true", meta["reviewer_is_bot"])
				assert.Equal(t, "gluser", meta["pr_author_login"])
				assert.Equal(t, "5", meta["platform_repo_id"])
			},
		},
		{
			name: "missing user/mr/project blocks — fields fall back to empty/zero strings without panic",
			body: []byte(`{
				"object_kind": "note",
				"object_attributes": {"id": 9878, "project_id": 5, "created_at": "2026-03-09T10:00:00Z", "updated_at": "2026-03-09T10:00:00Z"}
			}`),
			assertMeta: func(t *testing.T, meta map[string]string) {
				assert.Equal(t, "mr-0!0", meta["pr_key"])
				assert.Equal(t, "0", meta["mr_iid"])
				assert.Empty(t, meta["pr_author_login"])
				assert.Empty(t, meta["pr_author_id"])
				assert.Equal(t, "0", meta["platform_repo_id"])
				assert.Equal(t, "false", meta["reviewer_is_bot"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Set("X-Gitlab-Token", secret)
			headers.Set("X-Gitlab-Event", "Note Hook")
			headers.Set("X-Gitlab-Event-UUID", "550e8400-e29b-41d4-a716-446655440000")

			sink := testkit.NewChannelSink()
			params := &sdk.WebhookParams{
				Config:  webhookConfig(secret),
				Headers: headers,
				Body:    tt.body,
				Sink:    sink,
			}

			op := &WebhookEventsOp{}
			result, err := op.HandleWebhook(context.Background(), params)
			require.NoError(t, err)
			assert.True(t, result.Acknowledged)

			records := sink.Records()
			require.Len(t, records, 1)
			assert.Equal(t, "webhook_events", records[0].OperationID)
			tt.assertMeta(t, records[0].Metadata)
		})
	}
}

func TestWebhookEventsOp_HandleWebhook_NoteHook_SkipsSystemNotes(t *testing.T) {
	secret := "note-secret"

	tests := []struct {
		name        string
		body        []byte
		wantRecords int
	}{
		{
			name: "system note (label change) emits nothing",
			body: []byte(`{
				"object_kind": "note",
				"object_attributes": {"id": 9900, "project_id": 5, "created_at": "2026-03-09T10:00:00Z", "updated_at": "2026-03-09T10:00:00Z", "system": true},
				"user": {"id": 7001, "username": "release-bot", "bot": true},
				"merge_request": {"iid": 42, "author": {"id": 99001, "username": "gluser"}},
				"project": {"id": 5}
			}`),
			wantRecords: 0,
		},
		{
			name: "human note (system=false) still emits a review record",
			body: []byte(`{
				"object_kind": "note",
				"object_attributes": {"id": 9901, "project_id": 5, "created_at": "2026-03-09T10:00:00Z", "updated_at": "2026-03-09T10:00:00Z", "system": false},
				"user": {"id": 7002, "username": "alice", "bot": false},
				"merge_request": {"iid": 42, "author": {"id": 99001, "username": "gluser"}},
				"project": {"id": 5}
			}`),
			wantRecords: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Set("X-Gitlab-Token", secret)
			headers.Set("X-Gitlab-Event", "Note Hook")
			headers.Set("X-Gitlab-Event-UUID", "550e8400-e29b-41d4-a716-446655440000")

			sink := testkit.NewChannelSink()
			params := &sdk.WebhookParams{
				Config:  webhookConfig(secret),
				Headers: headers,
				Body:    tt.body,
				Sink:    sink,
			}

			op := &WebhookEventsOp{}
			result, err := op.HandleWebhook(context.Background(), params)
			require.NoError(t, err)
			assert.True(t, result.Acknowledged)
			assert.Len(t, sink.Records(), tt.wantRecords)
		})
	}
}

func TestWebhookEventsOp_HandleWebhook_IssueHook_Open(t *testing.T) {
	secret := "issue-secret"
	body := []byte(`{"object_kind":"issue","object_attributes":{"iid":12,"project_id":7,"state":"opened","created_at":"2026-02-01T00:00:00Z","updated_at":"2026-03-09T10:00:00Z"}}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Issue Hook")
	headers.Set("X-Gitlab-Event-UUID", "550e8400-e29b-41d4-a716-000000000001")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)
	require.NoError(t, err)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	// Open issue: only #snapshot.
	require.Len(t, records, 1)
	assert.Equal(t, "issue-7!12#snapshot", records[0].ExternalID)
	assert.Equal(t, "webhook_events", records[0].OperationID)
	assert.Equal(t, "snapshot", records[0].Metadata["event_type"])
}

func TestWebhookEventsOp_HandleWebhook_IssueHook_Closed(t *testing.T) {
	secret := "issue-secret"
	body := []byte(`{"object_kind":"issue","object_attributes":{"iid":12,"project_id":7,"state":"closed","created_at":"2026-02-01T00:00:00Z","updated_at":"2026-03-09T10:00:00Z","closed_at":"2026-03-09T10:00:00Z"}}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Issue Hook")
	headers.Set("X-Gitlab-Event-UUID", "550e8400-e29b-41d4-a716-000000000002")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)
	require.NoError(t, err)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	// Closed issue: #snapshot + #closed.
	require.Len(t, records, 2)
	assert.Equal(t, "issue-7!12#snapshot", records[0].ExternalID)
	assert.Equal(t, "snapshot", records[0].Metadata["event_type"])
	assert.Equal(t, "issue-7!12#closed", records[1].ExternalID)
	assert.Equal(t, "closed", records[1].Metadata["event_type"])
}

// TestWebhookEventsOp_HandleWebhook_IssueHook_ReshapedToTheRESTShape pins the
// payload shape: a consumer's GitLab issue reader
// expects the REST fetch_issues shape (top-level iid, references.full), but the
// hook body has neither — everything lives under object_attributes/project.
// Without the reshape in handleIssueHook, a reader that needs iid rejects every
// webhook-sourced issue. Body shape matches GitLab's real Issue Hook payload (verified against
// docs.gitlab.com/user/project/integrations/webhook_events).
func TestWebhookEventsOp_HandleWebhook_IssueHook_ReshapedToTheRESTShape(t *testing.T) {
	secret := "issue-secret"
	body := []byte(`{
		"object_kind": "issue",
		"user": {"id": 1, "username": "root"},
		"project": {"id": 7, "path_with_namespace": "acme/web"},
		"object_attributes": {
			"iid": 12,
			"project_id": 7,
			"title": "Login crashes",
			"author_id": 51,
			"assignee_ids": [200, 300],
			"state": "opened",
			"type": "Incident",
			"created_at": "2026-02-01T00:00:00Z",
			"updated_at": "2026-03-09T10:00:00Z"
		},
		"labels": [{"id": 206, "title": "bug"}],
		"assignees": [{"username": "bob"}, {"username": "carol"}]
	}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Issue Hook")
	headers.Set("X-Gitlab-Event-UUID", "550e8400-e29b-41d4-a716-000000000003")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)
	require.NoError(t, err)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	require.Len(t, records, 1)

	var reshaped struct {
		IID       int64    `json:"iid"`
		Title     string   `json:"title"`
		State     string   `json:"state"`
		IssueType string   `json:"issue_type"`
		Labels    []string `json:"labels"`
		Assignees []struct {
			ID       int64  `json:"id"`
			Username string `json:"username"`
		} `json:"assignees"`
		Author struct {
			ID int64 `json:"id"`
		} `json:"author"`
		References struct {
			Full string `json:"full"`
		} `json:"references"`
	}
	require.NoError(t, json.Unmarshal(records[0].Payload, &reshaped))

	assert.Equal(t, int64(12), reshaped.IID)
	assert.Equal(t, "Login crashes", reshaped.Title)
	assert.Equal(t, "opened", reshaped.State)
	assert.Equal(t, "incident", reshaped.IssueType, "hook's Title-cased type must be normalized to match the REST shape")
	assert.Equal(t, []string{"bug"}, reshaped.Labels)
	assert.Equal(t, "acme/web#12", reshaped.References.Full)
	assert.Equal(t, int64(51), reshaped.Author.ID)
	require.Len(t, reshaped.Assignees, 2)
	assert.Equal(t, int64(200), reshaped.Assignees[0].ID)
	assert.Equal(t, int64(300), reshaped.Assignees[1].ID)
	// No display-name hint on webhook-sourced assignees: GitLab gives no
	// shared key to pair object_attributes.assignee_ids against the
	// top-level assignees array, and a wrong pairing would mis-attribute a
	// real user's display name.
	assert.Empty(t, reshaped.Assignees[0].Username)
	assert.Empty(t, reshaped.Assignees[1].Username)
}

// TestWebhookEventsOp_HandleWebhook_IssueHook_MultiWordTypeAndDueDate pins the
// reshape's issue_type/due_date handling: ToLower alone mangles a multi-word type ("Test Case" -> "test case",
// not the REST shape's "test_case"), and omitting the issue's own due_date
// NULLs a poll-set due date on every webhook delivery (due_date
// is an unconditionally-overwritten consumer column, not mask-gated).
func TestWebhookEventsOp_HandleWebhook_IssueHook_MultiWordTypeAndDueDate(t *testing.T) {
	secret := "issue-secret"
	body := []byte(`{
		"object_kind": "issue",
		"project": {"id": 7, "path_with_namespace": "acme/web"},
		"object_attributes": {
			"iid": 40,
			"project_id": 7,
			"title": "regression case",
			"state": "opened",
			"type": "Test Case",
			"due_date": "2026-04-30",
			"created_at": "2026-02-01T00:00:00Z",
			"updated_at": "2026-03-09T10:00:00Z"
		}
	}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Issue Hook")
	headers.Set("X-Gitlab-Event-UUID", "550e8400-e29b-41d4-a716-000000000004")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)
	require.NoError(t, err)
	assert.True(t, result.Acknowledged)

	records := sink.Records()
	require.Len(t, records, 1)

	var reshaped struct {
		IssueType string `json:"issue_type"`
		DueDate   string `json:"due_date"`
	}
	require.NoError(t, json.Unmarshal(records[0].Payload, &reshaped))
	assert.Equal(t, "test_case", reshaped.IssueType)
	assert.Equal(t, "2026-04-30", reshaped.DueDate)
}

func TestWebhookEventsOp_HandleWebhook_PipelineHook_Dropped(t *testing.T) {
	secret := "uuid-secret"
	body := []byte(`{"object_kind":"pipeline","object_attributes":{"id":9876,"updated_at":"2026-03-09T10:00:00Z"}}`)

	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", "Pipeline Hook")
	headers.Set("X-Gitlab-Event-UUID", "550e8400-e29b-41d4-a716-446655440000")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(secret),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	result, err := op.HandleWebhook(context.Background(), params)
	require.NoError(t, err)
	assert.True(t, result.Acknowledged, "should acknowledge dropped events")
	assert.Empty(t, sink.Records(), "Pipeline Hook should be dropped")
}

func TestWebhookEventsOp_ImplementsInterface(t *testing.T) {
	var _ sdk.WebhookOperation = (*WebhookEventsOp)(nil)
}

func deliverHook(t *testing.T, event, body string) []*sdk.RawRecord {
	t.Helper()
	const secret = "s3cret"
	headers := http.Header{}
	headers.Set("X-Gitlab-Token", secret)
	headers.Set("X-Gitlab-Event", event)
	sink := testkit.NewChannelSink()
	_, err := (&WebhookEventsOp{}).HandleWebhook(context.Background(), &sdk.WebhookParams{
		Config: webhookConfig(secret), Headers: headers, Body: []byte(body), Sink: sink,
	})
	require.NoError(t, err)
	return sink.Records()
}

// A hook body is smaller than the polled object. Every webhook record carries
// operationId webhook_events, never a polling operation's id, so it cannot
// overwrite the polled record that shares its ExternalID.
func TestWebhookEventsOp_NeverEmitsUnderAPollingOperationID(t *testing.T) {
	polled := map[string]bool{}
	for _, op := range (&GitLabConnector{}).Operations() {
		if op.ID != opWebhookEvents {
			polled[string(op.ID)] = true
		}
	}
	require.NotEmpty(t, polled)
	hooks := []struct{ event, body string }{
		{"Push Hook", `{"object_kind":"push","project":{"path_with_namespace":"g/p"},"commits":[{"id":"aaa","timestamp":"2026-03-09T10:00:00Z"}]}`},
		{"Tag Push Hook", `{"object_kind":"tag_push","ref":"refs/tags/v1","project":{"path_with_namespace":"g/p"},"commits":[]}`},
		{"Merge Request Hook", `{"object_kind":"merge_request","object_attributes":{"iid":1,"project_id":1,"action":"merge","state":"merged","created_at":"2026-01-01T00:00:00Z","merged_at":"2026-01-02T00:00:00Z"}}`},
		{"Issue Hook", `{"object_kind":"issue","project":{"path_with_namespace":"g/p"},"object_attributes":{"iid":1,"project_id":1,"state":"closed","created_at":"2026-01-01T00:00:00Z","closed_at":"2026-01-02T00:00:00Z"}}`},
		{"Note Hook", `{"object_kind":"note","user":{"id":9,"username":"rev"},"project":{"id":1},"merge_request":{"iid":1},"object_attributes":{"id":7,"project_id":1,"created_at":"2026-01-01T12:00:00Z"}}`},
	}
	for _, h := range hooks {
		recs := deliverHook(t, h.event, h.body)
		require.NotEmpty(t, recs, h.event)
		for _, r := range recs {
			assert.Equal(t, "webhook_events", r.OperationID, "%s %s", h.event, r.ExternalID)
			assert.False(t, polled[r.OperationID], "%s emitted under polling operation %s", h.event, r.OperationID)
			assert.NotEmpty(t, r.Metadata["record_kind"], "%s %s names its shape", h.event, r.ExternalID)
		}
	}
}

// The open action is the merge request's creation: the hook emits #opened,
// dated by created_at, beside the undated #snapshot.
func TestWebhookEventsOp_MergeRequestHook_OpenActionEmitsOpened(t *testing.T) {
	recs := deliverHook(t, "Merge Request Hook", `{"object_kind":"merge_request","object_attributes":{"iid":42,"project_id":10,"action":"open","state":"opened","created_at":"2026-01-15T08:00:00Z","updated_at":"2026-01-15T08:00:00Z"}}`)
	require.Len(t, recs, 2)
	assert.Equal(t, "mr-10!42#opened", recs[0].ExternalID)
	assert.Equal(t, "opened", recs[0].Metadata["event_type"])
	assert.True(t, recs[0].DataDate.Equal(time.Date(2026, 1, 15, 8, 0, 0, 0, time.UTC)))
	assert.Equal(t, "mr-10!42#snapshot", recs[1].ExternalID)
	assert.True(t, recs[1].DataDate.IsZero(), "#snapshot is current state")

	// A reopen is not the creation event.
	reopened := deliverHook(t, "Merge Request Hook", `{"object_kind":"merge_request","object_attributes":{"iid":42,"project_id":10,"action":"reopen","state":"opened","created_at":"2026-01-15T08:00:00Z"}}`)
	require.Len(t, reopened, 1)
	assert.Equal(t, "mr-10!42#snapshot", reopened[0].ExternalID)
}

// A push with no commits is a "push" record of webhook_events, with no
// DataDate: the body carries no event time and the record is the ref's
// latest push.
func TestWebhookEventsOp_PushWithoutCommitsIsAnUndatedPushRecord(t *testing.T) {
	recs := deliverHook(t, "Tag Push Hook", `{"object_kind":"tag_push","ref":"refs/tags/v1","project":{"path_with_namespace":"g/p"},"commits":[]}`)
	require.Len(t, recs, 1)
	assert.Equal(t, "webhook_events", recs[0].OperationID)
	assert.Equal(t, "g/p@refs/tags/v1", recs[0].ExternalID)
	assert.Equal(t, "push", recs[0].Metadata["record_kind"])
	assert.True(t, recs[0].DataDate.IsZero())
}
