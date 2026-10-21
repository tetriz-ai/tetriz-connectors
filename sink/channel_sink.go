// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"sync"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ChannelSink is an in-memory DataSink implementation for testing.
// It stores emitted records in a mutex-protected slice.
type ChannelSink struct {
	mu      sync.Mutex
	records []*sdk.RawRecord
}

// NewChannelSink creates a new ChannelSink.
func NewChannelSink() *ChannelSink {
	return &ChannelSink{}
}

// Emit appends a single record. Thread-safe.
func (s *ChannelSink) Emit(_ context.Context, record *sdk.RawRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)
	return nil
}

// EmitBatch appends multiple records. Thread-safe.
func (s *ChannelSink) EmitBatch(_ context.Context, records []*sdk.RawRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, records...)
	return nil
}

// Flush is a no-op for the in-memory sink.
func (s *ChannelSink) Flush(_ context.Context) error {
	return nil
}

// Records returns a copy of all emitted records.
func (s *ChannelSink) Records() []*sdk.RawRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*sdk.RawRecord, len(s.records))
	copy(out, s.records)
	return out
}

// Drain returns all emitted records and clears the internal buffer.
func (s *ChannelSink) Drain() []*sdk.RawRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.records
	s.records = nil
	return out
}
