// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/timestamppb"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

var (
	sinkEmitTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_sink_emit_total",
			Help: "Total records emitted through the connector sink.",
		},
		[]string{"org_id", "connector_id", "operation_id", "status"},
	)
)

// Publisher sends one envelope to the host's transport (a queue, a log, a
// file) and returns the transport's message id, which may be empty.
type Publisher interface {
	Publish(ctx context.Context, envelope *connectorpb.ConnectorDataEnvelope) (string, error)
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(ctx context.Context, envelope *connectorpb.ConnectorDataEnvelope) (string, error)

// Publish implements Publisher.
func (f PublisherFunc) Publish(ctx context.Context, envelope *connectorpb.ConnectorDataEnvelope) (string, error) {
	return f(ctx, envelope)
}

// ConnectorSink implements sdk.DataSink.
//
// Each record is converted to a ConnectorDataEvent in its own
// ConnectorDataEnvelope and published immediately. There is no batching,
// ordering or deduplication: a consumer deduplicates on the record's
// (organization, connector, operation, external id, data date).
type ConnectorSink struct {
	publisher Publisher
}

// NewConnectorSink creates a ConnectorSink that publishes through publisher.
func NewConnectorSink(publisher Publisher) *ConnectorSink {
	return &ConnectorSink{
		publisher: publisher,
	}
}

// Emit publishes a single record immediately.
func (s *ConnectorSink) Emit(ctx context.Context, record *sdk.RawRecord) error {
	if record == nil {
		return fmt.Errorf("connector sink: record must not be nil")
	}
	if record.ExternalID == "" {
		return fmt.Errorf("connector sink: record of %s %s has no ExternalID, which is its identity", record.ConnectorID, record.OperationID)
	}
	req := ToEnvelope(ctx, record)
	metricOrgID := tenantlabel.OrgID(record.OrganizationID)
	if _, err := s.publisher.Publish(ctx, req); err != nil {
		sinkEmitTotal.WithLabelValues(metricOrgID, record.ConnectorID, record.OperationID, "error").Inc()
		return fmt.Errorf("connector sink: publish failed: %w", err)
	}
	sinkEmitTotal.WithLabelValues(metricOrgID, record.ConnectorID, record.OperationID, "success").Inc()
	return nil
}

// EmitBatch publishes multiple records immediately.
func (s *ConnectorSink) EmitBatch(ctx context.Context, records []*sdk.RawRecord) error {
	for _, record := range records {
		if err := s.Emit(ctx, record); err != nil {
			return fmt.Errorf("emitting: %w", err)
		}
	}
	return nil
}

// Flush is a no-op — records are published immediately on Emit/EmitBatch.
func (s *ConnectorSink) Flush(_ context.Context) error {
	return nil
}

// ToEnvelope converts a RawRecord to an envelope and stamps the sync-run id
// from ctx. Operations cannot set that field. Records emitted outside a run
// carry no id.
func ToEnvelope(ctx context.Context, r *sdk.RawRecord) *connectorpb.ConnectorDataEnvelope {
	event := &connectorpb.ConnectorDataEvent{
		ConnectorId:    r.ConnectorID,
		OperationId:    r.OperationID,
		OrganizationId: r.OrganizationID,
		ExternalId:     r.ExternalID,
		Payload:        r.Payload,
		FetchedAt:      timestamppb.New(r.FetchedAt),
		Metadata:       r.Metadata,
		SyncRunId:      sdk.SyncRunIDFromContext(ctx),
	}
	if !r.DataDate.IsZero() {
		event.DataDate = timestamppb.New(r.DataDate)
	}
	return &connectorpb.ConnectorDataEnvelope{
		Events:         []*connectorpb.ConnectorDataEvent{event},
		OrganizationId: r.OrganizationID,
	}
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		sinkEmitTotal,
	}
}
