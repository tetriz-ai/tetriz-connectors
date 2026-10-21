// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// LoggingSink is a DataSink that writes one log entry per record to a
// slog.Logger and keeps nothing. It never fails an emit, except for a nil
// record.
type LoggingSink struct {
	log *slog.Logger
}

// NewLoggingSink returns a LoggingSink writing to l, or to slog.Default()
// when l is nil.
func NewLoggingSink(l *slog.Logger) *LoggingSink {
	if l == nil {
		l = slog.Default()
	}
	return &LoggingSink{log: l}
}

// Emit logs the record's identity and payload size.
func (s *LoggingSink) Emit(ctx context.Context, record *sdk.RawRecord) error {
	if record == nil {
		return fmt.Errorf("logging sink: record must not be nil")
	}
	s.log.InfoContext(ctx, "record",
		slog.String("connector_id", record.ConnectorID),
		slog.String("operation_id", record.OperationID),
		slog.String("org_id", record.OrganizationID),
		slog.String("external_id", record.ExternalID),
		slog.Int("payload_bytes", len(record.Payload)),
		slog.String("sync_run_id", sdk.SyncRunIDFromContext(ctx)),
	)
	return nil
}

// EmitBatch logs each record in order and stops at the first error.
func (s *LoggingSink) EmitBatch(ctx context.Context, records []*sdk.RawRecord) error {
	for _, r := range records {
		if err := s.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// Flush does nothing; entries are written on Emit.
func (s *LoggingSink) Flush(_ context.Context) error { return nil }
