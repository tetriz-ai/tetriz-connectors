// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"context"
	"net/http"
)

// WebhookOperation handles inbound webhook payloads.
// A connector supporting webhooks can implement BOTH APIFetchOperation (for backfill/polling)
// AND WebhookOperation (for real-time push) on the same struct.
type WebhookOperation interface {
	Operation

	// HandleWebhook processes an inbound webhook payload.
	HandleWebhook(ctx context.Context, params *WebhookParams) (*WebhookResult, error)
}

// WebhookParams contains everything needed to process an inbound webhook.
type WebhookParams struct {
	Config  *ConnectorConfig
	Headers http.Header
	Body    []byte
	Sink    DataSink
}

// WebhookResult is the outcome of processing a webhook.
type WebhookResult struct {
	// Acknowledged indicates the webhook was successfully processed.
	Acknowledged bool

	// VerificationResponse is for webhook verification/challenge requests
	// (e.g., Slack URL verification, GitHub ping events).
	VerificationResponse    []byte
	VerificationContentType string
}

// StreamOperation is for long-lived connections (SSE, WebSocket, GraphQL subscriptions).
type StreamOperation interface {
	Operation

	// Start opens the stream and emits records to the sink.
	// This is a blocking call — runs until ctx is cancelled or the stream disconnects.
	// The framework manages reconnection by calling Start again after backoff.
	Start(ctx context.Context, params *ExecutionParams) error
}

// BulkImportOperation is for file-based or bulk data imports.
type BulkImportOperation interface {
	Operation

	// Import processes a bulk data source (CSV, JSON, database dump, S3 export, etc.)
	Import(ctx context.Context, params *BulkImportParams) (*Cursor, error)
}

// BulkImportParams contains everything needed for a bulk import.
type BulkImportParams struct {
	Config *ConnectorConfig
	Cursor *Cursor
	Sink   DataSink
	Source string // file path, S3 URL, etc.
}
