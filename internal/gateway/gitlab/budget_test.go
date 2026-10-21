// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/inprocess"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

// A GitLab response carrying RateLimit-Remaining, RateLimit-Limit and
// RateLimit-Reset (the headers GitLabAdapter reads) updates the
// organization's budget, and a request that checks the budget is then held
// back while it is below the floor, as fetch_mr_diffs and fetch_file_content
// check it before their requests.
func TestGatewayFeedsGitLabRateLimitHeadersIntoTheBudget(t *testing.T) {
	stores := inprocess.New()
	budget := stores.BudgetLimiter(ratelimiter.GitLabAdapter{})
	reset := time.Now().Add(time.Hour).Unix()
	h := http.Header{}
	h.Set("RateLimit-Remaining", strconv.Itoa(ratelimiter.GitLabCoreFloor-1))
	h.Set("RateLimit-Limit", "2000")
	h.Set("RateLimit-Reset", strconv.FormatInt(reset, 10))
	doer := &mockDoer{statusCode: http.StatusOK, body: `[]`, headers: h}

	gw, err := NewGitLabVendorGateway(doer, stores.RateLimits, nil, budget)
	require.NoError(t, err)
	ctx := reqctx.WithOrgID(context.Background(), testOrgID)

	allowed, _, err := budget.Allow(ctx, testOrgID, ratelimiter.BucketDefault, ratelimiter.GitLabCoreFloor)
	require.NoError(t, err)
	require.True(t, allowed, "no snapshot before the first response")

	_, err = gw.ForGroup(constants.GroupGitLabREST).Execute(ctx, &connectorpb.GitLabListProjectsRequest{GroupId: 1})
	require.NoError(t, err)

	limit, _, ok, err := budget.GetLimit(ctx, testOrgID, ratelimiter.BucketDefault)
	require.NoError(t, err)
	require.True(t, ok, "the response's headers were recorded")
	assert.Equal(t, 2000, limit)

	allowed, wait, err := budget.Allow(ctx, testOrgID, ratelimiter.BucketDefault, ratelimiter.GitLabCoreFloor)
	require.NoError(t, err)
	assert.False(t, allowed, "the budget is below its floor, so the request is held back")
	assert.Positive(t, wait)
}
