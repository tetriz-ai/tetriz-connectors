// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// Only a 404 means the organization has no Copilot data. Any other vendor
// failure fails the run, so a revoked token pauses the connector and the
// cursor is not advanced.
func TestFetchCopilotUsageOp_OnlyNotFoundMeansNoData(t *testing.T) {
	for _, kind := range []sdk.SyncErrorKind{sdk.ErrKindAuth, sdk.ErrKindRateLimit, sdk.ErrKindVendorError, sdk.ErrKindCtxExhausted} {
		t.Run(kind.String(), func(t *testing.T) {
			gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				if _, ok := req.(*connectorpb.CopilotMetricsRequest); ok {
					return nil, &sdk.SyncError{Kind: kind, Terminal: kind == sdk.ErrKindAuth, Cause: fmt.Errorf("vendor said no")}
				}
				return nil, fmt.Errorf("unexpected %T", req)
			}}
			cur, err := (&FetchCopilotUsageOp{}).Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, nil))
			var syncErr *sdk.SyncError
			require.ErrorAs(t, err, &syncErr, "a %s error must fail the run, got cursor=%v", kind, cur)
			assert.Equal(t, kind, syncErr.Kind)
			assert.Nil(t, cur)
		})
	}
	t.Run("not_found", func(t *testing.T) {
		gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			return nil, &sdk.SyncError{Kind: sdk.ErrKindNotFound, HTTPStatus: 404, Cause: fmt.Errorf("HTTP 404")}
		}}
		cur, err := (&FetchCopilotUsageOp{}).Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, nil))
		require.NoError(t, err, "a 404 means no Copilot data")
		assert.Nil(t, cur)
	})
	t.Run("user usage auth error", func(t *testing.T) {
		gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if _, ok := req.(*connectorpb.CopilotMetricsRequest); ok {
				return &connectorpb.CopilotMetricsResponse{}, nil
			}
			return nil, &sdk.SyncError{Kind: sdk.ErrKindAuth, Terminal: true, Cause: fmt.Errorf("HTTP 401")}
		}}
		_, err := (&FetchCopilotUsageOp{}).Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, nil))
		var syncErr *sdk.SyncError
		require.ErrorAs(t, err, &syncErr, "the per-user request fails the run too")
		assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	})
	t.Run("user usage plain error", func(t *testing.T) {
		gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if _, ok := req.(*connectorpb.CopilotMetricsRequest); ok {
				return &connectorpb.CopilotMetricsResponse{}, nil
			}
			return nil, fmt.Errorf("connection reset")
		}}
		cur, err := (&FetchCopilotUsageOp{}).Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, nil))
		require.Error(t, err, "an error that is not a SyncError fails the run, got cursor=%v", cur)
		assert.Contains(t, err.Error(), "connection reset")
		assert.Nil(t, cur, "the cursor does not advance")
	})
}
