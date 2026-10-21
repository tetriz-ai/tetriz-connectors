// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/async/goroutine"
	"github.com/tetriz-ai/tetriz-connectors/internal/async/waitgroup"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestToProcessConnectorDataEventRequestStampsSyncRunID(t *testing.T) {
	t.Parallel()

	newRecord := func(meta map[string]string) *sdk.RawRecord {
		return &sdk.RawRecord{
			ConnectorID:    "github",
			OperationID:    "fetch_branches",
			OrganizationID: "org_1",
			ExternalID:     "42:main",
			Payload:        json.RawMessage(`{}`),
			FetchedAt:      time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC),
			Metadata:       meta,
		}
	}

	const runID = "3f1a9c7e-4b2d-4a5f-9c8e-1d2b3a4c5d6e"

	tests := []struct {
		name    string
		ctx     context.Context
		meta    map[string]string
		wantRun string
	}{
		{
			name:    "run id from context lands on the event",
			ctx:     sdk.WithSyncRunID(context.Background(), runID),
			meta:    map[string]string{"branch_name": "main"},
			wantRun: runID,
		},
		{
			// Outside an orchestrated run (a test, a manual invocation, webhook
			// ingestion) the field must be empty rather than a sentinel — the consumer
			// keys "attributed vs not" off exactly this, and stores empty as NULL.
			name:    "no run in context leaves the field empty",
			ctx:     context.Background(),
			meta:    map[string]string{"branch_name": "main"},
			wantRun: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			record := newRecord(tt.meta)

			req := ToEnvelope(tt.ctx, record)

			require.Len(t, req.Events, 1)
			event := req.Events[0]
			assert.Equal(t, tt.wantRun, event.GetSyncRunId())

			// The run id rides its own typed field, so it must not also appear in the
			// metadata map the operation owns.
			assert.NotContains(t, event.GetMetadata(), "sync_run_id",
				"the run id belongs to its typed field, not the metadata blob")
			assert.Equal(t, "main", event.GetMetadata()["branch_name"],
				"operation metadata must be passed through untouched")
		})
	}
}

// mockQueuePublisher implements Publisher for testing.
type mockQueuePublisher struct {
	mu       sync.Mutex
	messages []proto.Message
	err      error
}

func (m *mockQueuePublisher) Publish(_ context.Context, msg *connectorpb.ConnectorDataEnvelope) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	if m.err != nil {
		return "", m.err
	}
	return "msg-id", nil
}

func (m *mockQueuePublisher) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

func newTestRecord(connectorID, opID, orgID, externalID string) *sdk.RawRecord {
	return &sdk.RawRecord{
		ConnectorID:    connectorID,
		OperationID:    opID,
		OrganizationID: orgID,
		ExternalID:     externalID,
		Payload:        json.RawMessage(`{"key":"value"}`),
		FetchedAt:      time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		Metadata:       map[string]string{"source": "test"},
	}
}

func TestToProcessConnectorDataEventRequest(t *testing.T) {
	t.Parallel()

	record := &sdk.RawRecord{
		ConnectorID:    "github",
		OperationID:    "fetch_prs",
		OrganizationID: "org_1",
		ExternalID:     "pr-42",
		Payload:        json.RawMessage(`{"title":"fix bug"}`),
		FetchedAt:      time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC),
		Metadata:       map[string]string{"page": "1"},
	}

	req := ToEnvelope(context.Background(), record)

	require.Len(t, req.Events, 1)
	event := req.Events[0]
	assert.Equal(t, "github", event.ConnectorId)
	assert.Equal(t, "fetch_prs", event.OperationId)
	assert.Equal(t, "org_1", event.OrganizationId)
	assert.Equal(t, "pr-42", event.ExternalId)
	assert.JSONEq(t, `{"title":"fix bug"}`, string(event.Payload))
	assert.Equal(t, record.FetchedAt.Unix(), event.FetchedAt.AsTime().Unix())
	assert.Equal(t, "1", event.Metadata["page"])
}

func TestConnectorSink_Emit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		record        *sdk.RawRecord
		publishErr    error
		wantErr       bool
		wantErrSubstr string
		wantCalls     int
	}{
		{
			name:      "publishes record immediately",
			record:    newTestRecord("github", "fetch_prs", "org_1", "pr-1"),
			wantCalls: 1,
		},
		{
			name:          "returns error on publish failure",
			record:        newTestRecord("github", "fetch_prs", "org_1", "pr-1"),
			publishErr:    fmt.Errorf("SQS unavailable"),
			wantErr:       true,
			wantErrSubstr: "SQS unavailable",
			wantCalls:     1,
		},
		{
			name:          "rejects nil record",
			record:        nil,
			wantErr:       true,
			wantErrSubstr: "must not be nil",
			wantCalls:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &mockQueuePublisher{err: tt.publishErr}
			sink := NewConnectorSink(pub)

			err := sink.Emit(context.Background(), tt.record)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrSubstr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCalls, pub.callCount())
		})
	}

	t.Run("verifies published proto fields", func(t *testing.T) {
		t.Parallel()
		pub := &mockQueuePublisher{}
		sink := NewConnectorSink(pub)

		record := newTestRecord("github", "fetch_prs", "org_1", "pr-42")
		require.NoError(t, sink.Emit(context.Background(), record))

		require.Equal(t, 1, pub.callCount())
		req := pub.messages[0].(*connectorpb.ConnectorDataEnvelope)
		require.Len(t, req.Events, 1)
		event := req.Events[0]
		assert.Equal(t, "github", event.ConnectorId)
		assert.Equal(t, "fetch_prs", event.OperationId)
		assert.Equal(t, "org_1", event.OrganizationId)
		assert.Equal(t, "pr-42", event.ExternalId)
	})
}

func TestConnectorSink_EmitBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		batchSize     int
		publishErr    error
		wantErr       bool
		wantErrSubstr string
		wantCalls     int
	}{
		{
			name:      "publishes all records immediately",
			batchSize: 5,
			wantCalls: 5,
		},
		{
			name:      "empty batch is a no-op",
			batchSize: 0,
			wantCalls: 0,
		},
		{
			name:          "stops on first publish error",
			batchSize:     3,
			publishErr:    fmt.Errorf("SQS unavailable"),
			wantErr:       true,
			wantErrSubstr: "SQS unavailable",
			wantCalls:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &mockQueuePublisher{err: tt.publishErr}
			sink := NewConnectorSink(pub)

			var records []*sdk.RawRecord
			for i := range tt.batchSize {
				records = append(records, newTestRecord("github", "fetch_prs", "org_1", fmt.Sprintf("pr-%d", i)))
			}

			err := sink.EmitBatch(context.Background(), records)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrSubstr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCalls, pub.callCount())
		})
	}
}

func TestConnectorSink_ConcurrentEmit(t *testing.T) {
	t.Parallel()

	pub := &mockQueuePublisher{}
	sink := NewConnectorSink(pub)

	const numGoroutines = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		goroutine.RunWithDefaultTimeout(context.Background(), func(_ context.Context) {
			defer wg.Done()
			_ = sink.Emit(context.Background(),
				newTestRecord("github", "fetch_prs", "org_1", fmt.Sprintf("pr-%d", i)))
		})
	}
	waitgroup.SafeWaitWithDefaultTimeout(&wg)

	assert.Equal(t, numGoroutines, pub.callCount(), "all concurrent emits should be captured")
}
