// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// extractArray pulls a nested JSON array field (commits, notes, diffs) out of a
// stored PR payload. A missing or malformed field yields an empty slice, never
// an error: payloads are seed-controlled, so a shape mismatch is a seed bug that
// should surface as an empty page, not a 500 the connector would treat as transient.
func extractArray(ctx context.Context, payload []byte, field, handler string) []json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		logger.WarnWithCtx(ctx, "simulator: gitlab "+handler+": malformed PR payload", zap.Error(err))
		return []json.RawMessage{}
	}
	raw, ok := obj[field]
	if !ok {
		return []json.RawMessage{}
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		logger.WarnWithCtx(ctx, "simulator: gitlab "+handler+": malformed "+field+" array", zap.Error(err))
		return []json.RawMessage{}
	}
	return arr
}

// defaultApprovals is the GitLab approvals object when the PR payload omits
// the field. The route returns an object, not an array, so a missing field
// cannot become [].
var defaultApprovals = json.RawMessage(`{"approved":false,"approved_by":[]}`)

// extractObject pulls a nested JSON object field (approvals) out of a stored
// PR payload. A missing or malformed field yields defaultApprovals, never an
// error — same seed-bug rationale as extractArray.
func extractObject(ctx context.Context, payload []byte, field, handler string) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		logger.WarnWithCtx(ctx, "simulator: gitlab "+handler+": malformed PR payload", zap.Error(err))
		return defaultApprovals
	}
	raw, ok := obj[field]
	if !ok || len(raw) == 0 || raw[0] != '{' {
		return defaultApprovals
	}
	return raw
}
