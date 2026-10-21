// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// runawayCallCap is how many vendor calls a test allows before the fake gives up on the
// connector stopping by itself. A connector with a loop guard stops after the second
// page that repeats the cursor it was sent.
const runawayCallCap = 25

var errRunaway = errors.New("test fake: connector kept paging past the call cap")

// TestEdge_FetchPRsStopsWhenEndCursorDoesNotAdvance drives a vendor page that claims
// hasNextPage while handing back the same endCursor it was queried with. Following it
// re-reads the same page forever (re-emitting its records each time) until the run's
// deadline; a pagination loop must notice the cursor did not move and stop.
func TestEdge_FetchPRsStopsWhenEndCursorDoesNotAdvance(t *testing.T) {
	for _, tc := range []struct {
		name      string
		endCursor string
	}{
		{name: "same non-empty endCursor every page", endCursor: "Y3Vyc29yOjE="},
		// An empty endCursor with hasNextPage true sends the next query back to the first page.
		{name: "empty endCursor with hasNextPage", endCursor: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			var calls atomic.Int32
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					if _, ok := req.(*connectorpb.GitHubGraphQLRequest); !ok {
						return nil, fmt.Errorf("unexpected request %T", req)
					}
					if calls.Add(1) > runawayCallCap {
						return nil, errRunaway
					}
					return graphQLDataResponse(graphQLPRsData(
						[]ghGQLPRNode{{Number: 1, UpdatedAt: now, CreatedAt: now.Add(-time.Hour), State: "OPEN"}},
						true, tc.endCursor)), nil
				},
			}
			sink := testkit.NewChannelSink()
			_, _ = (&FetchPRsOp{}).Execute(context.Background(), testParams(sink, gw, nil))

			assert.LessOrEqual(t, int(calls.Load()), 3,
				"a page whose endCursor does not advance must stop pagination, not be re-requested until the deadline")
		})
	}
}

// TestEdge_FetchCommitsStopsWhenEndCursorDoesNotAdvance is the same guard on the commit
// history walk.
func TestEdge_FetchCommitsStopsWhenEndCursorDoesNotAdvance(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var calls atomic.Int32
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if _, ok := req.(*connectorpb.GitHubGraphQLRequest); !ok {
				return nil, fmt.Errorf("unexpected request %T", req)
			}
			if calls.Add(1) > runawayCallCap {
				return nil, errRunaway
			}
			return graphQLDataResponse(graphQLCommitsData(
				[]ghGQLCommitNode{{OID: "abc123", CommittedDate: now}}, true, "stuck")), nil
		},
	}
	sink := testkit.NewChannelSink()
	_, _ = (&FetchCommitsOp{}).Execute(context.Background(), testParams(sink, gw, nil))

	assert.LessOrEqual(t, int(calls.Load()), 3,
		"a history page whose endCursor does not advance must stop pagination")
}

// TestEdge_AdaptivePageSizeTerminatesWithDivisorOne pins that a BackoffDivisor of 1 — a
// value opPageConfig accepts unchanged, since it only replaces values <= 0 — cannot turn a
// persistent 5xx into an endless retry at the same page size. pageSize/1 never drops
// below MinPageSize, so without a guard the loop never exits.
func TestEdge_AdaptivePageSizeTerminatesWithDivisorOne(t *testing.T) {
	var calls atomic.Int32
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
			if calls.Add(1) > runawayCallCap {
				return nil, errRunaway
			}
			return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 502, Cause: errors.New("502 Bad Gateway")}
		},
	}
	cfg := opPageConfig(&config.GitHubPaginationConfig{Operations: map[string]*config.OperationPaginationConfig{
		opFetchPRs: {PageSize: 50, MinPageSize: 10, BackoffDivisor: 1},
	}}, opFetchPRs)

	_, err := doGraphQLAdaptive(context.Background(), gw, cfg, func(ps int) string {
		return buildPRsQuery("acme", "widgets", time.Time{}, "", ps, nil, false)
	}, "acme/widgets", true)

	assert.Error(t, err)
	assert.NotErrorIs(t, err, errRunaway,
		"the adaptive retry must give up on its own, not keep retrying the same page size")
	assert.LessOrEqual(t, int(calls.Load()), 5)
}
