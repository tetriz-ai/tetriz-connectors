// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// extractArray pulls a nested JSON array field (diffstat, commits) out of a stored
// PR payload. A missing or malformed field yields an empty slice, never an error:
// payloads are seed-controlled, so a shape mismatch is a seed bug that should surface
// as an empty page, not a 500 the connector would treat as a transient failure.
func extractArray(ctx context.Context, payload []byte, field, handler string) []json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		logger.WarnWithCtx(ctx, "simulator: bitbucket "+handler+": malformed PR payload", zap.Error(err))
		return []json.RawMessage{}
	}
	raw, ok := obj[field]
	if !ok {
		return []json.RawMessage{}
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		logger.WarnWithCtx(ctx, "simulator: bitbucket "+handler+": malformed "+field+" array", zap.Error(err))
		return []json.RawMessage{}
	}
	return arr
}

// sortCommitsByDateAsc returns commit objects ordered oldest-first by their RFC3339
// "date" field. Entries with a missing or unparseable date sort last, keeping their
// relative order (stable sort). This is the one route that departs from the
// simulator's newest-first default: fetch_prs's first-commit enrichment reads the
// oldest commit via sort=date, and a wrong order would set first_commit_at to the
// newest commit.
func sortCommitsByDateAsc(values []json.RawMessage) []json.RawMessage {
	type dated struct {
		raw    json.RawMessage
		date   time.Time
		parsed bool
	}
	items := make([]dated, 0, len(values))
	for _, v := range values {
		d := dated{raw: v}
		var obj struct {
			Date string `json:"date"`
		}
		// Best-effort date read: a commit that doesn't unmarshal or whose date
		// isn't RFC3339 stays unparsed (sorts last). Undated commits are an
		// expected, non-error state (direct pushes), so both parse failures are
		// intentionally dropped rather than logged.
		_ = json.Unmarshal(v, &obj)                                  //nolint:errcheck // failure → empty date → unparsed below
		if t, _ := time.Parse(time.RFC3339, obj.Date); !t.IsZero() { //nolint:errcheck // non-RFC3339/empty → zero time → unparsed
			d.date, d.parsed = t, true
		}
		items = append(items, d)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].parsed != items[j].parsed {
			return items[i].parsed // parsed dates sort ahead of undated entries
		}
		return items[i].date.Before(items[j].date)
	})
	out := make([]json.RawMessage, len(items))
	for i, it := range items {
		out[i] = it.raw
	}
	return out
}
