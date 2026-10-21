// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// schemaEmitters maps each contract endpoint to the operation that stores it.
var schemaEmitters = map[string]struct {
	op   sdk.OperationID
	emit func(*testing.T, string) []*sdk.RawRecord
}{
	"activity": {opFetchAnalyticsUsers, emitActivityRow},
	"skills":   {opFetchAnalyticsSkills, emitSkillsRow},
	"cost":     {opFetchAnalyticsUserCost, emitCostRow},
	"usage":    {opFetchAnalyticsUserUsage, emitUsageRow},
	"roster":   {opFetchOrganizationMembers, emitRosterRow},
}

// validateAll checks every emitted payload against the schema for its record's operation.
func validateAll(t *testing.T, want sdk.OperationID, records []*sdk.RawRecord) {
	t.Helper()
	require.NotEmpty(t, records, "the operation emitted nothing to validate")
	for _, r := range records {
		assert.Equal(t, string(connectorID), r.ConnectorID)
		assert.Equal(t, string(want), r.OperationID)
		assert.NoErrorf(t, schemas.Validate(r.ConnectorID, r.OperationID, r.Payload),
			"payload of %s does not match its schema: %s", r.ExternalID, r.Payload)
	}
}

// TestPayloadsMatchTheirSchemas runs each operation on the documented vendor row and
// validates what it emits.
func TestPayloadsMatchTheirSchemas(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	for endpoint, e := range schemaEmitters {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			row := rowDatedFor(endpoint, documentedRows[endpoint].row, yesterday)
			validateAll(t, e.op, e.emit(t, row))
		})
	}
}

// TestMinimalPayloadsMatchTheirSchemas runs each operation on a row holding only the keys
// the connector requires, so the schema demands nothing more than the code guarantees.
func TestMinimalPayloadsMatchTheirSchemas(t *testing.T) {
	t.Parallel()

	bucket := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour).Format(time.RFC3339)
	minimal := map[string]string{
		"activity": `{"user":{"id":"user_A"}}`,
		"skills":   `{"user":{"id":"user_A"}}`,
		"cost":     `{"actor":{"user_id":"user_A"},"starting_at":"` + bucket + `"}`,
		"usage":    `{"actor":{"user_id":"user_A"},"starting_at":"` + bucket + `"}`,
		"roster":   `{"id":"user_A"}`,
	}
	for endpoint, e := range schemaEmitters {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			validateAll(t, e.op, e.emit(t, minimal[endpoint]))
		})
	}
}

// TestGroupedPayloadsMatchTheirSchemas covers the array payloads with more than one row:
// two skills for one actor-day and two models for one actor-bucket-product.
func TestGroupedPayloadsMatchTheirSchemas(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.AddDate(0, 0, -1)

	t.Run("skills", func(t *testing.T) {
		t.Parallel()
		cur, err := encodeCursor(activityPosition{CoveredThrough: yesterday.AddDate(0, 0, -1).Format(dateLayout)})
		require.NoError(t, err)
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
				if req.(*vgpb.AnthropicAnalyticsSkillsRequest).GetDate() != yesterday.Format(dateLayout) {
					return &vgpb.AnthropicAnalyticsSkillsResponse{}, nil
				}
				return &vgpb.AnthropicAnalyticsSkillsResponse{Data: [][]byte{
					[]byte(documentedRows["skills"].row),
					[]byte(`{"user":{"id":"user_A"},"skill_name":"other","distinct_user_count":1}`),
				}}, nil
			}).AnyTimes()
		sink := &collectingSink{}
		_, err = (&FetchAnalyticsSkillsOp{}).Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		validateAll(t, opFetchAnalyticsSkills, sink.records)
	})

	t.Run("cost", func(t *testing.T) {
		t.Parallel()
		cur, err := encodeCursor(reportPosition{
			CoveredThrough: yesterday.Format(dateLayout), DataRefreshedAt: today.Format(time.RFC3339),
		})
		require.NoError(t, err)
		row := rowDatedFor("cost", documentedRows["cost"].row, yesterday)
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(&vgpb.AnthropicAnalyticsUserCostReportResponse{
				Data: [][]byte{
					[]byte(row),
					[]byte(`{"actor":{"user_id":"user_A"},"starting_at":"` + yesterday.Format(time.RFC3339) +
						`","product":"claude_code","model":"Claude Sonnet 4","amount":"1.5","cost_type":null}`),
				},
				DataRefreshedAt: timestamppb.New(today),
			}, nil).AnyTimes()
		sink := &collectingSink{}
		_, err = (&FetchAnalyticsUserCostOp{}).Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		validateAll(t, opFetchAnalyticsUserCost, sink.records)
	})
}

// TestSchemasRejectAPayloadWithoutItsIdentity keeps the schemas from going vacuous: a payload
// missing the key the identity is built from must fail.
func TestSchemasRejectAPayloadWithoutItsIdentity(t *testing.T) {
	t.Parallel()

	for op, payload := range map[sdk.OperationID]string{
		opFetchAnalyticsUsers:      `{"user":{}}`,
		opFetchAnalyticsSkills:     `[{"skill_name":"s"}]`,
		opFetchAnalyticsUserCost:   `[{"actor":{"user_id":"user_A"}}]`,
		opFetchAnalyticsUserUsage:  `[{"starting_at":"2026-08-25T00:00:00Z"}]`,
		opFetchOrganizationMembers: `{"email":"a@example.com"}`,
	} {
		assert.Errorf(t, schemas.Validate(string(connectorID), string(op), []byte(payload)),
			"%s accepted a payload without its identity key", op)
	}
}
