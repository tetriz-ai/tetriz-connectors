// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sink_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tetriz-ai/tetriz-connectors/sink"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The envelope keeps the field numbers of the host request it replaces, so
// bytes written by either decode as the other.
func TestEnvelopeFieldNumbers(t *testing.T) {
	d := (&connectorpb.ConnectorDataEnvelope{}).ProtoReflect().Descriptor()
	assert.Equal(t, protoreflect.FieldNumber(2), d.Fields().ByName("events").Number())
	assert.Equal(t, protoreflect.FieldNumber(3), d.Fields().ByName("organization_id").Number())
	assert.True(t, d.ReservedRanges().Has(1))
}

// The JSONL publisher writes one compact protojson object per line.
func TestJSONLPublisherWritesOneLinePerEnvelope(t *testing.T) {
	var buf bytes.Buffer
	p := sink.NewJSONLPublisher(&buf)
	for _, org := range []string{"a", "b"} {
		_, err := p.Publish(context.Background(), &connectorpb.ConnectorDataEnvelope{OrganizationId: org})
		require.NoError(t, err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, `{"organizationId":"a"}`, lines[0])
}

// The sink refuses a record with an empty ExternalID, its identity, and
// publishes nothing.
func TestEmptyExternalIDIsRefused(t *testing.T) {
	var got []*connectorpb.ConnectorDataEnvelope
	s := sink.NewConnectorSink(sink.PublisherFunc(func(_ context.Context, e *connectorpb.ConnectorDataEnvelope) (string, error) {
		got = append(got, e)
		return "1", nil
	}))
	err := s.Emit(context.Background(), &sdk.RawRecord{ConnectorID: "c", OperationID: "op", OrganizationID: "o"})
	require.ErrorContains(t, err, "has no ExternalID")
	require.NoError(t, s.Flush(context.Background()))
	assert.Empty(t, got)
}
