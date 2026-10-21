// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func webhookConfig(secret string) *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID:    "github",
		OrganizationID: "org-test",
		Credentials: mustJSON(GitHubCredentials{
			InstallationID: "12345678",
			WebhookSecret:  secret,
		}),
		Settings: mustJSON(GitHubSettings{
			Organization: "acme",
		}),
		Enabled: true,
	}
}

func signPayload(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestWebhookEventsOp_HandleWebhook(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"action":"opened","pull_request":{"number":42}}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature-256", sig)
	headers.Set("X-GitHub-Event", "pull_request")
	headers.Set("X-GitHub-Delivery", "abc-123")

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
	assert.Equal(t, "webhook-abc-123", records[0].ExternalID)
	assert.Equal(t, "webhook_events", records[0].OperationID)
	assert.Equal(t, "github", records[0].ConnectorID)
	assert.Equal(t, "org-test", records[0].OrganizationID)
	assert.Equal(t, "pull_request", records[0].Metadata["event_type"])
	assert.Equal(t, "opened", records[0].Metadata["action"])
}

func TestWebhookEventsOp_InvalidSignature(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"action":"opened"}`)
	badSig := "sha256=0000000000000000000000000000000000000000000000000000000000000000"

	headers := http.Header{}
	headers.Set("X-Hub-Signature-256", badSig)
	headers.Set("X-GitHub-Event", "pull_request")
	headers.Set("X-GitHub-Delivery", "abc-123")

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
	assert.Contains(t, err.Error(), "signature mismatch")
	assert.Empty(t, sink.Records())
}

func TestWebhookEventsOp_MissingSignature(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-GitHub-Event", "pull_request")
	headers.Set("X-GitHub-Delivery", "abc-123")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig("test-secret"),
		Headers: headers,
		Body:    []byte(`{}`),
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	_, err := op.HandleWebhook(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing X-Hub-Signature-256")
}

func TestWebhookEventsOp_PingEvent(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"zen":"Approachable is better than simple.","hook_id":12345}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature-256", sig)
	headers.Set("X-GitHub-Event", "ping")
	headers.Set("X-GitHub-Delivery", "ping-123")

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
	assert.Equal(t, "application/json", result.VerificationContentType)

	var pong map[string]string
	require.NoError(t, json.Unmarshal(result.VerificationResponse, &pong))
	assert.Equal(t, "pong", pong["msg"])

	// Ping events should NOT emit records.
	assert.Empty(t, sink.Records())
}

func TestWebhookEventsOp_UnrecognizedEvent(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"action":"created"}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature-256", sig)
	headers.Set("X-GitHub-Event", "star") // Not a supported event
	headers.Set("X-GitHub-Delivery", "star-123")

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
	assert.Empty(t, sink.Records(), "should not emit records for unsupported events")
}

func TestWebhookEventsOp_PREvent(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"action":"opened","pull_request":{"number":99,"title":"Add feature"}}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature-256", sig)
	headers.Set("X-GitHub-Event", "pull_request")
	headers.Set("X-GitHub-Delivery", "pr-456")

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
	assert.Equal(t, "webhook-pr-456", records[0].ExternalID)
	assert.Equal(t, "pull_request", records[0].Metadata["event_type"])
	assert.Equal(t, "opened", records[0].Metadata["action"])

	// Verify the full payload is preserved.
	assert.JSONEq(t, string(body), string(records[0].Payload))
}

func TestWebhookEventsOp_PushEvent(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"ref":"refs/heads/main","commits":[{"id":"abc123","message":"fix bug"}]}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature-256", sig)
	headers.Set("X-GitHub-Event", "push")
	headers.Set("X-GitHub-Delivery", "push-789")

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
	assert.Equal(t, "webhook-push-789", records[0].ExternalID)
	assert.Equal(t, "push", records[0].Metadata["event_type"])
}

func TestWebhookEventsOp_SecretGate(t *testing.T) {
	body := []byte(`{"action":"opened","pull_request":{"number":42}}`)

	tests := []struct {
		name        string
		secret      string
		signWith    string // secret that signs X-Hub-Signature-256; "" omits the header
		wantErr     bool
		wantErrMsg  string
		wantRecords int
	}{
		{
			name:        "empty secret rejected — fail closed",
			secret:      "",
			signWith:    "attacker-secret", // forged signature; irrelevant once secret is empty
			wantErr:     true,
			wantErrMsg:  "no webhook_secret configured",
			wantRecords: 0,
		},
		{
			name:        "valid secret with correct signature accepted",
			secret:      "test-secret-123",
			signWith:    "test-secret-123",
			wantErr:     false,
			wantRecords: 1,
		},
		{
			name:        "valid secret with wrong signature rejected",
			secret:      "test-secret-123",
			signWith:    "wrong-secret",
			wantErr:     true,
			wantErrMsg:  "signature mismatch",
			wantRecords: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Set("X-GitHub-Event", "pull_request")
			headers.Set("X-GitHub-Delivery", "gate-123")
			if tt.signWith != "" {
				headers.Set("X-Hub-Signature-256", signPayload(tt.signWith, body))
			}

			sink := testkit.NewChannelSink()
			op := &WebhookEventsOp{}
			result, err := op.HandleWebhook(context.Background(), &sdk.WebhookParams{
				Config:  webhookConfig(tt.secret),
				Headers: headers,
				Body:    body,
				Sink:    sink,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
			} else {
				require.NoError(t, err)
				assert.True(t, result.Acknowledged)
			}
			assert.Len(t, sink.Records(), tt.wantRecords)
		})
	}
}

func TestVerifySignature(t *testing.T) {
	secret := "mysecret"
	body := []byte("hello world")

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	tests := []struct {
		name      string
		sig       string
		expectErr bool
	}{
		{"valid", validSig, false},
		{"empty", "", true},
		{"no prefix", "abc123", true},
		{"bad hex", "sha256=zzzz", true},
		{"wrong hash", "sha256=0000000000000000000000000000000000000000000000000000000000000000", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifySignature(secret, body, tt.sig)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestWebhookEventsOp_PRSynchronize(t *testing.T) {
	secret := "test-secret-123"
	// Realistic pull_request.synchronize payload: repo, PR number, before/after SHA.
	body := []byte(`{
		"action": "synchronize",
		"before": "aabbcc112233",
		"after":  "ddeeff445566",
		"pull_request": {
			"number": 42,
			"base": {
				"repo": {
					"full_name": "acme/myrepo"
				}
			}
		}
	}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature-256", sig)
	headers.Set("X-GitHub-Event", "pull_request")
	headers.Set("X-GitHub-Delivery", "sync-999")

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
	// synchronize emits two records: the generic webhook_events and a pr_sync_events record.
	require.Len(t, records, 2)

	// First record: generic webhook_events (unchanged path).
	webhook := records[0]
	assert.Equal(t, "webhook_events", webhook.OperationID)
	assert.Equal(t, "webhook-sync-999", webhook.ExternalID)
	assert.Equal(t, "pull_request", webhook.Metadata["event_type"])
	assert.Equal(t, "synchronize", webhook.Metadata["action"])

	// Second record: pr_sync_events for squash-push survivability.
	sync := records[1]
	assert.Equal(t, "pr_sync_events", sync.OperationID)
	assert.Equal(t, "acme/myrepo#PR-42#SYNC-ddeeff445566", sync.ExternalID)
	assert.Equal(t, "acme/myrepo", sync.Metadata["repo"])
	assert.Equal(t, "42", sync.Metadata["pr_number"])
	assert.Equal(t, "acme/myrepo#PR-42", sync.Metadata["pr_key"])
	assert.Equal(t, "ddeeff445566", sync.Metadata["after_sha"])
	assert.Equal(t, "aabbcc112233", sync.Metadata["before_sha"])
	assert.JSONEq(t, string(body), string(sync.Payload))
}

func TestWebhookEventsOp_PRSynchronize_MissingPR(t *testing.T) {
	// If the synchronize payload is missing pull_request data, only the
	// generic webhook_events record is emitted — no pr_sync_events.
	secret := "test-secret-123"
	body := []byte(`{"action":"synchronize","after":"ddeeff445566"}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature-256", sig)
	headers.Set("X-GitHub-Event", "pull_request")
	headers.Set("X-GitHub-Delivery", "sync-bad")

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
	assert.Equal(t, "webhook_events", records[0].OperationID)
}

// Verify WebhookEventsOp implements the WebhookOperation interface.
func TestWebhookEventsOp_ImplementsInterface(t *testing.T) {
	var _ sdk.WebhookOperation = (*WebhookEventsOp)(nil)
}
