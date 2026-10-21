// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Supported Bitbucket webhook event types that produce records.
// See: https://support.atlassian.com/bitbucket-cloud/docs/event-payloads/
var supportedWebhookEvents = map[string]bool{
	"repo:push":                   true,
	"pullrequest:created":         true,
	"pullrequest:updated":         true,
	"pullrequest:approved":        true,
	"pullrequest:unapproved":      true,
	"pullrequest:fulfilled":       true,
	"pullrequest:rejected":        true,
	"pullrequest:comment_created": true,
	"issue:created":               true,
	"issue:updated":               true,
	"issue:comment_created":       true,
}

// WebhookEventsOp processes inbound Bitbucket webhook payloads.
// It implements sdk.WebhookOperation. No RPC in this module calls it; a host
// calls it with the payload it received.
type WebhookEventsOp struct{}

func (op *WebhookEventsOp) OperationID() sdk.OperationID { return "webhook_events" }

func (op *WebhookEventsOp) HandleWebhook(ctx context.Context, params *sdk.WebhookParams) (*sdk.WebhookResult, error) {
	// Step 1: Parse credentials to get webhook secret for HMAC verification.
	var creds BitbucketCredentials
	if err := json.Unmarshal(params.Config.Credentials, &creds); err != nil {
		return nil, fmt.Errorf("webhook: parse credentials: %w", err)
	}

	// Step 2: Verify HMAC-SHA256 signature. Fail closed: without a secret the
	// HMAC cannot be verified, so any caller could forge events. A config may
	// legitimately carry no webhook secret (polling-only sync), but such a
	// config must not accept unsigned inbound webhooks — mirrors the GitHub
	// handler. Bitbucket sends the signature in the X-Hub-Signature header.
	if creds.WebhookSecret == "" {
		return nil, fmt.Errorf("webhook: no webhook_secret configured; rejecting unverifiable event")
	}
	sig := params.Headers.Get("X-Hub-Signature")
	if err := verifySignature(creds.WebhookSecret, params.Body, sig); err != nil {
		return nil, fmt.Errorf("webhook: %w", err)
	}

	// Step 3: Parse event type from headers.
	// Bitbucket sends event type in X-Event-Key header.
	eventType := params.Headers.Get("X-Event-Key")
	// X-Request-UUID names this delivery; X-Hook-UUID names the subscription,
	// which every delivery from the hook shares. A Bitbucket redelivery reuses
	// the request UUID, so a retried delivery keeps its ExternalID.
	requestUUID := params.Headers.Get("X-Request-UUID")

	// Step 4: Handle test/ping event.
	if eventType == "diagnostics:ping" || eventType == "" {
		return &sdk.WebhookResult{
			Acknowledged:            true,
			VerificationResponse:    []byte(`{"msg":"pong"}`),
			VerificationContentType: "application/json",
		}, nil
	}

	// Step 5: Check if this event type is supported.
	if !supportedWebhookEvents[eventType] {
		return &sdk.WebhookResult{Acknowledged: true}, nil
	}

	// Step 6: Refuse what cannot be keyed or read. Without the delivery id every
	// delivery of one event type would share an ExternalID; a body that is not
	// JSON would reach the sink as a payload no consumer can parse.
	if requestUUID == "" {
		return nil, fmt.Errorf("webhook: missing X-Request-UUID header; rejecting %s event", eventType)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(params.Body, &object); err != nil {
		return nil, fmt.Errorf("webhook: %s body is not a JSON object; rejecting: %w", eventType, err)
	}

	// Step 7: Emit the webhook payload as a RawRecord.
	externalID := fmt.Sprintf("webhook-%s-%s", eventType, requestUUID)
	metadata := map[string]string{
		"event_type": eventType,
	}

	record := sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    string(op.OperationID()),
		OrganizationID: params.Config.OrganizationID,
		ExternalID:     externalID,
		Payload:        params.Body,
		FetchedAt:      time.Now().UTC(),
		Metadata:       metadata,
	}

	if err := params.Sink.Emit(ctx, &record); err != nil {
		return nil, fmt.Errorf("webhook: emit: %w", err)
	}

	return &sdk.WebhookResult{Acknowledged: true}, nil
}

// verifySignature checks the HMAC-SHA256 signature.
// The signature header format is "sha256=<hex-digest>".
func verifySignature(secret string, body []byte, signature string) error {
	if signature == "" {
		return fmt.Errorf("missing X-Hub-Signature header")
	}

	if !strings.HasPrefix(signature, "sha256=") {
		return fmt.Errorf("invalid signature format: expected sha256= prefix")
	}

	sigHex := strings.TrimPrefix(signature, "sha256=")
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return fmt.Errorf("invalid signature hex: %w", err)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := mac.Sum(nil)

	if !hmac.Equal(sigBytes, expected) {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}
