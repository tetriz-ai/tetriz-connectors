// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

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
		ConnectorID:    "bitbucket",
		OrganizationID: "org-test",
		Credentials: mustJSON(BitbucketCredentials{
			OAuthAccessToken: "test-oauth-token",
			WebhookSecret:    secret,
		}),
		Settings: mustJSON(BitbucketSettings{
			Workspace: "myworkspace",
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
	body := []byte(`{"pullrequest":{"id":42,"title":"Add feature"},"repository":{"full_name":"myworkspace/myrepo"}}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature", sig)
	headers.Set("X-Event-Key", "pullrequest:created")
	headers.Set("X-Hook-UUID", "hook-abc-123")
	headers.Set("X-Request-UUID", "req-001")

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
	assert.Equal(t, "webhook-pullrequest:created-req-001", records[0].ExternalID)
	assert.Equal(t, "webhook_events", records[0].OperationID)
	assert.Equal(t, "bitbucket", records[0].ConnectorID)
	assert.Equal(t, "org-test", records[0].OrganizationID)
	assert.Equal(t, "pullrequest:created", records[0].Metadata["event_type"])
}

func TestWebhookEventsOp_InvalidSignature(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"pullrequest":{"id":42}}`)
	badSig := "sha256=0000000000000000000000000000000000000000000000000000000000000000"

	headers := http.Header{}
	headers.Set("X-Hub-Signature", badSig)
	headers.Set("X-Event-Key", "pullrequest:created")
	headers.Set("X-Hook-UUID", "hook-abc-123")

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
	headers.Set("X-Event-Key", "pullrequest:created")
	headers.Set("X-Hook-UUID", "hook-abc-123")

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
	assert.Contains(t, err.Error(), "missing X-Hub-Signature")
}

func TestWebhookEventsOp_PingEvent(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{}`)
	headers := http.Header{}
	headers.Set("X-Hub-Signature", signPayload(secret, body))
	headers.Set("X-Event-Key", "diagnostics:ping")
	headers.Set("X-Hook-UUID", "ping-123")

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

	assert.Empty(t, sink.Records())
}

func TestWebhookEventsOp_UnrecognizedEvent(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{}`)
	headers := http.Header{}
	headers.Set("X-Hub-Signature", signPayload(secret, body))
	headers.Set("X-Event-Key", "repo:fork")
	headers.Set("X-Hook-UUID", "fork-123")

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

func TestWebhookEventsOp_PushEvent(t *testing.T) {
	secret := "test-secret-123"
	body := []byte(`{"push":{"changes":[{"new":{"name":"main","type":"branch"}}]},"repository":{"full_name":"myworkspace/myrepo"}}`)
	sig := signPayload(secret, body)

	headers := http.Header{}
	headers.Set("X-Hub-Signature", sig)
	headers.Set("X-Event-Key", "repo:push")
	headers.Set("X-Hook-UUID", "push-789")
	headers.Set("X-Request-UUID", "req-push-1")

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
	assert.Equal(t, "webhook-repo:push-req-push-1", records[0].ExternalID)
	assert.Equal(t, "repo:push", records[0].Metadata["event_type"])
}

func TestWebhookEventsOp_NoSecret(t *testing.T) {
	body := []byte(`{"pullrequest":{"id":42}}`)

	headers := http.Header{}
	headers.Set("X-Event-Key", "pullrequest:created")
	headers.Set("X-Hook-UUID", "nosec-123")

	sink := testkit.NewChannelSink()
	params := &sdk.WebhookParams{
		Config:  webhookConfig(""),
		Headers: headers,
		Body:    body,
		Sink:    sink,
	}

	op := &WebhookEventsOp{}
	_, err := op.HandleWebhook(context.Background(), params)

	// Fail closed: with no secret the HMAC cannot be verified, so the event is
	// rejected and nothing is emitted. Bind to the gate's message so the test
	// cannot pass on an unrelated error.
	require.ErrorContains(t, err, "no webhook_secret configured")
	assert.Empty(t, sink.Records())
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

func TestWebhookEventsOp_ImplementsInterface(t *testing.T) {
	var _ sdk.WebhookOperation = (*WebhookEventsOp)(nil)
}

// deliver sends one signed delivery of eventType from hook with the given
// request UUID and body.
func deliver(t *testing.T, sink sdk.DataSink, eventType, hook, request string, body []byte) error {
	t.Helper()
	secret := "test-secret-123"
	headers := http.Header{}
	headers.Set("X-Hub-Signature", signPayload(secret, body))
	headers.Set("X-Event-Key", eventType)
	headers.Set("X-Hook-UUID", hook)
	if request != "" {
		headers.Set("X-Request-UUID", request)
	}
	_, err := (&WebhookEventsOp{}).HandleWebhook(context.Background(), &sdk.WebhookParams{
		Config: webhookConfig(secret), Headers: headers, Body: body, Sink: sink,
	})
	return err
}

// Two deliveries of one event type from one hook are two records: the
// ExternalID names the delivery (X-Request-UUID), not the subscription
// (X-Hook-UUID), so a sink keyed on it keeps both.
func TestWebhookEventsOp_DeliveriesFromOneHookStayDistinct(t *testing.T) {
	sink := testkit.NewChannelSink()
	require.NoError(t, deliver(t, sink, "pullrequest:updated", "hook-1", "req-a", []byte(`{"pullrequest":{"id":1}}`)))
	require.NoError(t, deliver(t, sink, "pullrequest:updated", "hook-1", "req-b", []byte(`{"pullrequest":{"id":2}}`)))
	recs := sink.Records()
	require.Len(t, recs, 2)
	assert.Equal(t, "webhook-pullrequest:updated-req-a", recs[0].ExternalID)
	assert.Equal(t, "webhook-pullrequest:updated-req-b", recs[1].ExternalID)
}

// A redelivery repeats X-Request-UUID, so it keeps the ExternalID and a sink
// deduplicates it.
func TestWebhookEventsOp_RedeliveryKeepsExternalID(t *testing.T) {
	sink := testkit.NewChannelSink()
	body := []byte(`{"pullrequest":{"id":1}}`)
	require.NoError(t, deliver(t, sink, "pullrequest:updated", "hook-1", "req-a", body))
	require.NoError(t, deliver(t, sink, "pullrequest:updated", "hook-1", "req-a", body))
	recs := sink.Records()
	require.Len(t, recs, 2)
	assert.Equal(t, recs[0].ExternalID, recs[1].ExternalID)
}

// Without X-Request-UUID the delivery has no identity; it is refused rather
// than emitted under an ExternalID every such delivery would share.
func TestWebhookEventsOp_MissingRequestUUIDIsRejected(t *testing.T) {
	sink := testkit.NewChannelSink()
	err := deliver(t, sink, "pullrequest:updated", "hook-1", "", []byte(`{"pullrequest":{"id":1}}`))
	require.ErrorContains(t, err, "missing X-Request-UUID")
	assert.Empty(t, sink.Records())
}

// A payload is JSON: a signed body that is not a JSON object is refused, not
// emitted as opaque bytes.
func TestWebhookEventsOp_NonJSONBodyIsRejected(t *testing.T) {
	for name, body := range map[string][]byte{
		"form encoded": []byte(`payload=%7B%22pullrequest%22%3A%7B%7D%7D`),
		"truncated":    []byte(`{"pullrequest":`),
		"array":        []byte(`[1,2]`),
	} {
		t.Run(name, func(t *testing.T) {
			sink := testkit.NewChannelSink()
			err := deliver(t, sink, "pullrequest:created", "hook-1", "req-a", body)
			require.ErrorContains(t, err, "not a JSON object")
			assert.Empty(t, sink.Records())
		})
	}
}
