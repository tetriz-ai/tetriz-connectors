// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A GraphQL answer whose error carries Linear's RATELIMITED code fails the
// run as a retryable rate_limit error, not internal. Linear's documented
// shape: https://linear.app/developers/rate-limiting.
func TestGraphQLRatelimitedIsARateLimit(t *testing.T) {
	gw := &fakeGateway{respond: func(_ string, _ map[string]any, dest any) error {
		return unmarshalInto(dest, `{"errors":[{"message":"Rate limit exceeded","extensions":{"code":"RATELIMITED"}}]}`)
	}}
	_, err := (&FetchTeamsOp{}).Execute(context.Background(), &sdk.ExecutionParams{
		Config: testConfig("org-1"), VendorGateway: gw, Sink: testkit.NewChannelSink(),
	})
	var se *sdk.SyncError
	require.True(t, errors.As(err, &se), "err=%v", err)
	assert.Equal(t, sdk.ErrKindRateLimit, se.Kind)
	assert.True(t, se.Retryable)
}
