// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import "context"

// syncRunIDKey is the context key carrying the id of the SyncRun that is
// executing the current operation.
type syncRunIDKey struct{}

// WithSyncRunID returns a context carrying the executing SyncRun's id, so the
// DataSink can stamp it onto every record the operation emits without each
// operation having to thread it by hand. Must be set before the operation runs,
// since the sink reads it per emitted record. An id absent from the context leaves
// records unattributed.
func WithSyncRunID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, syncRunIDKey{}, id)
}

// SyncRunIDFromContext returns the executing SyncRun's id, or "" when the
// context carries none.
func SyncRunIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(syncRunIDKey{}).(string); ok {
		return v
	}
	return ""
}
