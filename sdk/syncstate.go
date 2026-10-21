// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// Cursor is an opaque checkpoint that an operation uses to track incremental progress.
// The framework persists this between executions but never inspects its contents.
type Cursor struct {
	// Data holds the serialized cursor state. Each operation defines its own schema.
	Data json.RawMessage `json:"data"`

	// UpdatedAt is set by the framework when persisting.
	UpdatedAt time.Time `json:"updated_at"`

	// Version is the store's version of the saved cursor: the store sets it
	// when it reads or saves one, and a save succeeds only while the stored
	// version still equals it (zero: no cursor is stored). It is not part of
	// the serialized cursor, and an operation leaves it alone.
	Version int64 `json:"-"`
}

// --- Timestamp cursor helpers ---

type timestampCursor struct {
	LastUpdatedAt time.Time `json:"last_updated_at"`
}

// NewTimestampCursor creates a cursor from a timestamp (most common pattern).
// The signature stays error-free for its many call sites; a marshal failure
// (not reachable for a time.Time) leaves Data empty, which the matching
// extractor rejects as an empty cursor.
func NewTimestampCursor(t time.Time) *Cursor {
	data, err := json.Marshal(timestampCursor{LastUpdatedAt: t.UTC()})
	if err != nil {
		logger.InfoNoCtx("syncstate: failed to marshal timestamp cursor; emitting empty cursor", zap.Error(err))
		data = nil
	}
	return &Cursor{Data: data, UpdatedAt: time.Now().UTC()}
}

// TimestampFromCursor extracts a timestamp from a cursor.
func TimestampFromCursor(c *Cursor) (time.Time, error) {
	if isEmptyCursor(c) {
		return time.Time{}, errors.New("cursor is nil or empty")
	}
	var tc timestampCursor
	if err := json.Unmarshal(c.Data, &tc); err != nil {
		return time.Time{}, fmt.Errorf("unmarshaling: %w", err)
	}
	return tc.LastUpdatedAt, nil
}

// --- Page cursor helpers ---

type pageCursor struct {
	Page      int    `json:"page"`
	PageToken string `json:"page_token"`
}

// NewPageCursor creates a cursor for page/offset pagination.
// A marshal failure (not reachable for these scalar fields) leaves Data empty,
// which the matching extractor rejects as an empty cursor.
func NewPageCursor(page int, pageToken string) *Cursor {
	data, err := json.Marshal(pageCursor{Page: page, PageToken: pageToken})
	if err != nil {
		logger.InfoNoCtx("syncstate: failed to marshal page cursor; emitting empty cursor", zap.Error(err))
		data = nil
	}
	return &Cursor{Data: data, UpdatedAt: time.Now().UTC()}
}

// PageFromCursor extracts page info from a cursor.
func PageFromCursor(c *Cursor) (page int, pageToken string, err error) {
	if isEmptyCursor(c) {
		return 0, "", errors.New("cursor is nil or empty")
	}
	var pc pageCursor
	if err := json.Unmarshal(c.Data, &pc); err != nil {
		return 0, "", fmt.Errorf("unmarshaling: %w", err)
	}
	return pc.Page, pc.PageToken, nil
}

// --- Composite cursor helpers ---

// NewCompositeCursor creates a cursor with multiple named sub-cursors.
// Useful when a single operation fetches from multiple endpoints.
func NewCompositeCursor(parts map[string]json.RawMessage) *Cursor {
	data, err := json.Marshal(parts)
	if err != nil {
		// A part carrying invalid JSON yields an empty cursor, which
		// CompositeFromCursor rejects rather than persisting a corrupt checkpoint.
		logger.ErrorNoCtx("syncstate: failed to marshal composite cursor; emitting empty cursor", zap.Error(err))
		data = nil
	}
	return &Cursor{Data: data, UpdatedAt: time.Now().UTC()}
}

// CompositeFromCursor extracts named parts from a composite cursor.
func CompositeFromCursor(c *Cursor) (map[string]json.RawMessage, error) {
	if isEmptyCursor(c) {
		return nil, errors.New("cursor is nil or empty")
	}
	var parts map[string]json.RawMessage
	if err := json.Unmarshal(c.Data, &parts); err != nil {
		return nil, fmt.Errorf("unmarshaling: %w", err)
	}
	return parts, nil
}

// isEmptyCursor reports a cursor that carries no position: nil, no bytes, or the
// JSON value null, which is how the store hands back an empty cursor.
func isEmptyCursor(c *Cursor) bool {
	return c == nil || len(bytes.TrimSpace(c.Data)) == 0 || string(bytes.TrimSpace(c.Data)) == "null"
}
