// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const opIDPRSyncEvents = "pr_sync_events"

// Supported GitHub webhook event types that produce records.
var supportedWebhookEvents = map[string]bool{
	"push":                true,
	"pull_request":        true,
	"pull_request_review": true,
	"issues":              true,
	"issue_comment":       true,
	"create":              true,
	"delete":              true,
}

// WebhookEventsOp processes inbound GitHub webhook payloads.
// It implements sdk.WebhookOperation. No RPC in this module calls it; a host
// calls it with the payload it received.
type WebhookEventsOp struct{}

func (op *WebhookEventsOp) OperationID() sdk.OperationID { return "webhook_events" }

func (op *WebhookEventsOp) HandleWebhook(ctx context.Context, params *sdk.WebhookParams) (*sdk.WebhookResult, error) {
	var creds GitHubCredentials
	if err := json.Unmarshal(params.Config.Credentials, &creds); err != nil {
		return nil, fmt.Errorf("webhook: parse credentials: %w", err)
	}

	// Fail closed: without a secret the HMAC cannot be verified, so any caller
	// could forge events. A config may legitimately carry no webhook secret
	// (polling-only sync — validateCredentials requires only installation_id),
	// but such a config must not accept unsigned inbound webhooks.
	if creds.WebhookSecret == "" {
		return nil, fmt.Errorf("webhook: no webhook_secret configured; rejecting unverifiable event")
	}
	sig := params.Headers.Get("X-Hub-Signature-256")
	if err := verifySignature(creds.WebhookSecret, params.Body, sig); err != nil {
		return nil, fmt.Errorf("webhook: %w", err)
	}

	eventType := params.Headers.Get("X-GitHub-Event")
	deliveryID := params.Headers.Get("X-GitHub-Delivery")

	// Ping is GitHub's webhook-configured handshake — respond pong so the UI
	// confirms delivery, no record to emit.
	if eventType == "ping" {
		return &sdk.WebhookResult{
			Acknowledged:            true,
			VerificationResponse:    []byte(`{"msg":"pong"}`),
			VerificationContentType: "application/json",
		}, nil
	}

	// Acknowledge unsupported event types without emitting — graceful ignore.
	if !supportedWebhookEvents[eventType] {
		return &sdk.WebhookResult{Acknowledged: true}, nil
	}

	// The body is the record's payload, and a payload is JSON. A delivery
	// configured as application/x-www-form-urlencoded, or any other body that
	// is not a JSON object, is rejected rather than stored as opaque bytes.
	var payload ghWebhookPayload
	if err := json.Unmarshal(params.Body, &payload); err != nil {
		logger.WarnWithCtx(ctx, "webhook: rejecting delivery whose body is not a JSON object",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.String("delivery_id", deliveryID),
			zap.String("event_type", eventType),
			zap.Error(err),
		)
		return nil, fmt.Errorf("webhook: body is not a JSON object (set the webhook content type to application/json): %w", err)
	}

	now := time.Now().UTC()
	externalID := fmt.Sprintf("webhook-%s", deliveryID)
	metadata := map[string]string{
		"event_type": eventType,
	}
	if payload.Action != "" {
		metadata["action"] = payload.Action
	}

	record := sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    string(op.OperationID()),
		OrganizationID: params.Config.OrganizationID,
		ExternalID:     externalID,
		Payload:        params.Body,
		FetchedAt:      now,
		Metadata:       metadata,
	}
	if err := params.Sink.Emit(ctx, &record); err != nil {
		return nil, fmt.Errorf("webhook: emit: %w", err)
	}

	// For pull_request.synchronize, also emit a pr_sync_events record.
	// pull_request.synchronize fires on every push to the PR branch, including
	// force-push. This survives commit squashing — polling fetch_pr_commits would
	// only see the latest squashed commit, missing intermediate pushes entirely.
	if eventType == "pull_request" && payload.Action == "synchronize" &&
		payload.PullRequest != nil && payload.After != "" {
		pr := payload.PullRequest
		repoFullName := pr.Base.Repo.FullName
		prKey := fmt.Sprintf("%s#PR-%d", repoFullName, pr.Number)
		syncRecord := sdk.RawRecord{
			ConnectorID:    string(connectorID),
			OperationID:    opIDPRSyncEvents,
			OrganizationID: params.Config.OrganizationID,
			ExternalID:     fmt.Sprintf("%s#SYNC-%s", prKey, payload.After),
			Payload:        params.Body,
			FetchedAt:      now,
			Metadata: map[string]string{
				"repo":       repoFullName,
				"pr_number":  fmt.Sprintf("%d", pr.Number),
				"pr_key":     prKey,
				"after_sha":  payload.After,
				"before_sha": payload.Before,
			},
		}
		if err := params.Sink.Emit(ctx, &syncRecord); err != nil {
			return nil, fmt.Errorf("webhook: emit pr_sync_events: %w", err)
		}
	}

	return &sdk.WebhookResult{Acknowledged: true}, nil
}

// verifySignature checks the HMAC-SHA256 signature from GitHub.
// The signature header format is "sha256=<hex-digest>".
func verifySignature(secret string, body []byte, signature string) error {
	if signature == "" {
		return fmt.Errorf("missing X-Hub-Signature-256 header")
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
