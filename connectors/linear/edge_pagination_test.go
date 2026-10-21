// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// advancePage refuses a cursor that repeats the one just sent, but a connection
// that cycles between two cursors (A -> B -> A ...) never repeats immediately and
// walks forever, re-emitting the same users each lap. The test cancels the
// context after 100 pages so a looping implementation terminates.
func TestEdge_FetchUsersStopsOnACursorCycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const budget = 100
	pages := 0
	gw := &fakeGateway{respond: func(_ string, vars map[string]any, dest any) error {
		pages++
		if pages >= budget {
			cancel()
		}
		next := "A"
		if vars["after"] == "A" {
			next = "B"
		}
		return unmarshalInto(dest, fmt.Sprintf(
			`{"data":{"users":{"nodes":[{"id":"u-%d","updatedAt":"2026-01-01T00:00:00Z"}],"pageInfo":{"hasNextPage":true,"endCursor":%q}}}}`,
			pages, next))
	}}

	_, err := (&FetchUsersOp{}).Execute(ctx, &sdk.ExecutionParams{
		Config: testConfig("org-1"), Sink: testkit.NewChannelSink(), VendorGateway: gw,
	})
	require.Error(t, err)
	assert.Less(t, pages, budget, "the walk cycled A/B for %d pages and only stopped because the context ended", pages)
}

// The end of a connection is hasNextPage=false, whatever endCursor says, and a
// RATELIMITED code anywhere in the error list classifies as a rate limit.
func TestEdge_PageEndAndRateLimitClassification(t *testing.T) {
	next, done, err := advancePage(PageInfo{HasNextPage: false, EndCursor: "leftover"}, nil)
	require.NoError(t, err)
	assert.True(t, done)
	assert.Nil(t, next)

	var errs []graphqlError
	errs = append(errs, graphqlError{Message: "complexity"})
	rl := graphqlError{Message: "slow down"}
	rl.Extensions.Code = "RATELIMITED"
	errs = append(errs, rl)
	var se *sdk.SyncError
	require.ErrorAs(t, graphQLErr("p", errs), &se)
	assert.Equal(t, sdk.ErrKindRateLimit, se.Kind)
}

// sinceArg renders a non-UTC high-water in UTC, rewound by the overlap, across a
// year boundary.
func TestEdge_SinceArgIsUTCAndRewindsAcrossTheYear(t *testing.T) {
	zone := time.FixedZone("UTC+2", 2*3600)
	hw := time.Date(2026, 1, 1, 2, 2, 0, 0, zone) // 2026-01-01T00:02Z
	assert.Equal(t, "2025-12-31T23:57:00.000Z", sinceArg(hw))
	assert.Equal(t, linearBackfillSentinel, sinceArg(time.Time{}))
}
