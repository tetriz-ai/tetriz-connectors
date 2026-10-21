// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import "context"

// DataSink is the output interface for fetched data. The runner hands one to
// each operation at execution time. Connectors emit records; the sink decides
// how they are delivered (a queue, a file, a table). The sink package holds
// the implementations.
type DataSink interface {
	// Emit publishes a single record to downstream destinations.
	// Thread-safe — can be called concurrently within an operation.
	Emit(ctx context.Context, record *RawRecord) error

	// EmitBatch publishes multiple records at once. Preferred for bulk fetches.
	EmitBatch(ctx context.Context, records []*RawRecord) error

	// Flush delivers every record emitted so far, or returns an error. The
	// runner calls it before each checkpoint and before it saves the final
	// cursor.
	Flush(ctx context.Context) error
}
