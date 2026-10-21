// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package descriptions shares the description-capture emit pipeline across
// per-vendor fetch_issues_data ops. Lives at the connector layer because the
// helper takes a sdk.DataSink.
package descriptions

import (
	"context"
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

// S3Writer matches the narrow surface description emitters need.
type S3Writer interface {
	Write(ctx context.Context, dst string, content []byte, aclString string) error
}

// IssueDescriptionPayload is the wire shape the producer writes and a consumer
// reads — one definition so the two sides can't drift.
type IssueDescriptionPayload struct {
	S3Key             string `json:"s3_key"`
	IssueKey          string `json:"issue_key"`
	PlatformProjectID string `json:"platform_project_id"`
	RepoFullName      string `json:"repo_full_name,omitempty"`
}

// EmitIssueDescriptionInput carries the per-call parameters. Raw is the raw
// vendor body (markdown for github, ADF JSON for jira) — whitespace-only skips.
type EmitIssueDescriptionInput struct {
	ConnectorID       sdk.ConnectorID
	OperationID       sdk.OperationID
	OrgID             string
	IssueKey          string
	ExternalID        string
	PlatformProjectID string
	RepoFullName      string
	Raw               []byte
	Meta              map[string]string
	S3Key             string
}

// EmitIssueDescription runs the S3-write → sha → payload → emit pipeline. The
// record is the issue's current description, so it carries no DataDate.
// Whitespace-only bodies skip silently; empty PlatformProjectID logs a warn so the
// next sync with a resolved id retries.
func EmitIssueDescription(ctx context.Context, s3 S3Writer, sink sdk.DataSink, in EmitIssueDescriptionInput) error {
	if len(in.Raw) == 0 || strings.TrimSpace(string(in.Raw)) == "" {
		return nil
	}

	if in.PlatformProjectID == "" {
		logger.WarnWithCtx(ctx, "fetch_issues_data: skipping issue without resolved platform_project_id",
			zap.String(logger.ConnectorID, string(in.ConnectorID)),
			zap.String(logger.IssueKey, in.IssueKey))
		return nil
	}

	if err := s3.Write(ctx, in.S3Key, in.Raw, ""); err != nil {
		return fmt.Errorf("s3 write %s: %w", in.S3Key, err)
	}

	digest := sha256.Sum256(in.Raw)
	if in.Meta == nil {
		in.Meta = map[string]string{}
	}
	in.Meta["description_sha"] = "sha256:" + hex.EncodeToString(digest[:])

	payload, err := json.Marshal(IssueDescriptionPayload{
		S3Key:             in.S3Key,
		IssueKey:          in.IssueKey,
		PlatformProjectID: in.PlatformProjectID,
		RepoFullName:      in.RepoFullName,
	})
	if err != nil {
		return fmt.Errorf("marshal description payload: %w", err)
	}

	if err := sink.Emit(ctx, &sdk.RawRecord{
		ConnectorID:    string(in.ConnectorID),
		OperationID:    string(in.OperationID),
		OrganizationID: in.OrgID,
		ExternalID:     in.ExternalID,
		Payload:        payload,
		FetchedAt:      time.Now().UTC(),
		Metadata:       in.Meta,
	}); err != nil {
		return fmt.Errorf("emitting description record: %w", err)
	}
	return nil
}
