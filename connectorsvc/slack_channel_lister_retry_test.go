// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	vgconnectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestIsBudgetExhaustionDistinguishesCtxErrorFromRealVendorError: a permanent
// vendor error that happens to land after ctx has separately expired must NOT
// be classified as benign budget exhaustion. Only an error that is (or wraps)
// ctx's own cancellation or deadline error may be.
func TestIsBudgetExhaustionDistinguishesCtxErrorFromRealVendorError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.True(t, isBudgetExhaustion(fmt.Errorf("conversations.list: %w", ctx.Err())),
		"an error that wraps ctx's own Err() must be treated as budget exhaustion")
	require.False(t, isBudgetExhaustion(errors.New("slack API error: invalid_auth")),
		"a real vendor error must never be misclassified as budget exhaustion just because ctx happens to also be expired by the time it's checked")
	require.False(t, isBudgetExhaustion(&sdk.SyncError{Kind: sdk.ErrKindRateLimit, Cause: errors.New("HTTP 429")}),
		"a real SyncError from the vendor gateway must never be misclassified as budget exhaustion")
}

// TestCanResumeFromRequiresRealResumePosition: a resumable short-circuit is
// only safe with a real cursor or real gathered channels to resume from;
// otherwise it would return (nil, "", nil), the same signature as "pagination
// genuinely completed with zero channels".
func TestCanResumeFromRequiresRealResumePosition(t *testing.T) {
	t.Parallel()
	require.False(t, canResumeFrom("", nil), "zero progress on a fresh cycle has nothing to resume from")
	require.True(t, canResumeFrom("page-2", nil), "a non-empty cursor from a resumed cycle is a real resume position")
	require.True(t, canResumeFrom("", []*connectorpb.SlackChannel{{Id: "C1"}}), "channels gathered this call, even with an unset cursor, is real progress")
}

// scriptedVendorGateway scripts a sequence of (response, error) results for
// successive Execute calls, enough to drive the retry helper's branches
// without any real HTTP or rate-limiter plumbing.
type scriptedVendorGateway struct {
	sdk.VendorGateway
	results []struct {
		resp proto.Message
		err  error
	}
	calls []time.Time
}

func (f *scriptedVendorGateway) Execute(_ context.Context, _ proto.Message) (proto.Message, error) {
	f.calls = append(f.calls, time.Now())
	i := len(f.calls) - 1
	if i >= len(f.results) {
		return nil, errors.New("scriptedVendorGateway: no scripted result for call")
	}
	r := f.results[i]
	return r.resp, r.err
}

// TestFetchConversationsPageWithRetryHonorsRetryAfter: a rate-limit error
// carrying RetryAfterSeconds waits that duration (not a blind exponential
// guess) before the next attempt.
func TestFetchConversationsPageWithRetryHonorsRetryAfter(t *testing.T) {
	want := &vgconnectorpb.SlackListConversationsResponse{Ok: true}
	gw := &scriptedVendorGateway{results: []struct {
		resp proto.Message
		err  error
	}{
		{nil, &sdk.SyncError{Kind: sdk.ErrKindRateLimit, RetryAfterSeconds: 1, Cause: errors.New("HTTP 429")}},
		{want, nil},
	}}

	start := time.Now()
	got, err := fetchConversationsPageWithRetry(context.Background(), gw, &vgconnectorpb.SlackListConversationsRequest{})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.True(t, proto.Equal(want, got))
	require.Len(t, gw.calls, 2)
	require.GreaterOrEqual(t, elapsed, 1*time.Second, "must wait at least the vendor's Retry-After hint before retrying")
}

// TestFetchConversationsPageWithRetryGivesUpAfterMaxAttempts: the retry loop
// is bounded, and a persistently failing page fetch returns the last error.
func TestFetchConversationsPageWithRetryGivesUpAfterMaxAttempts(t *testing.T) {
	persistentErr := errors.New("boom")
	gw := &scriptedVendorGateway{results: []struct {
		resp proto.Message
		err  error
	}{
		{nil, persistentErr}, {nil, persistentErr}, {nil, persistentErr}, {nil, persistentErr},
	}}

	_, err := fetchConversationsPageWithRetry(context.Background(), gw, &vgconnectorpb.SlackListConversationsRequest{})
	require.Error(t, err)
	require.ErrorIs(t, err, persistentErr)
	require.Len(t, gw.calls, 4, "must stop after the fixed attempt cap, not retry forever")
}

// TestFetchConversationsPageWithRetrySucceedsFirstTry: the common case makes
// exactly one call and returns immediately.
func TestFetchConversationsPageWithRetrySucceedsFirstTry(t *testing.T) {
	want := &vgconnectorpb.SlackListConversationsResponse{Ok: true}
	gw := &scriptedVendorGateway{results: []struct {
		resp proto.Message
		err  error
	}{{want, nil}}}

	got, err := fetchConversationsPageWithRetry(context.Background(), gw, &vgconnectorpb.SlackListConversationsRequest{})
	require.NoError(t, err)
	require.True(t, proto.Equal(want, got))
	require.Len(t, gw.calls, 1)
}
