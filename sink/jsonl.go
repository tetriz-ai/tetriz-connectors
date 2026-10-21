// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
)

// JSONLPublisher writes each envelope as one line of protojson to w. Writes
// are serialised, so one publisher is safe for concurrent emitters. It
// returns no message id.
type JSONLPublisher struct {
	mu sync.Mutex
	w  io.Writer
}

// NewJSONLPublisher returns a publisher writing to w, typically an
// *os.File opened for append.
func NewJSONLPublisher(w io.Writer) *JSONLPublisher {
	return &JSONLPublisher{w: w}
}

// Publish implements Publisher.
func (p *JSONLPublisher) Publish(_ context.Context, envelope *connectorpb.ConnectorDataEnvelope) (string, error) {
	raw, err := protojson.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("jsonl publisher: marshal envelope: %w", err)
	}
	// protojson varies its whitespace between runs; compacting makes each
	// line byte-stable for the same envelope.
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return "", fmt.Errorf("jsonl publisher: compact envelope: %w", err)
	}
	b := buf.Bytes()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.w.Write(append(b, '\n')); err != nil {
		return "", fmt.Errorf("jsonl publisher: write: %w", err)
	}
	return "", nil
}
