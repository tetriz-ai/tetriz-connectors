// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/connectorsvc"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	connectormocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// newTestRedisClient returns a client backed by an in-process miniredis
// instance — the same pattern as slack_channel_cache_test.go's newTestCache,
// exposed here as a raw client for tests that build the cache directly.

// commsConnectorID is the slack_comms connector's id.
const commsConnectorID = "slack_comms"

func newTestRedisClient(t *testing.T) *redis.Client {
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

const brokerOrg = "11111111-1111-1111-1111-111111111111"

func commsGrant(enabled bool) *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID: commsConnectorID, OrganizationID: brokerOrg, Enabled: enabled,
		Credentials: json.RawMessage(`{"bot_token":"xoxb-test-1"}`),
	}
}

// TestGetSecret proves the broker returns the connector grant's bot token + id.
func TestGetSecret(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
		Return(commsGrant(true), nil)
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SecretAuthorizer: connectorsvc.AllowConnectors(commsConnectorID)}

	resp, err := svc.GetSecret(context.Background(), &connectorpb.GetSecretRequest{OrganizationId: brokerOrg, ConnectorId: commsConnectorID})
	require.NoError(t, err)
	require.Equal(t, "xoxb-test-1", resp.GetToken())
	require.Equal(t, commsConnectorID, resp.GetConnectorId())
}

// TestGetConnectionStatus proves the three fields stay consistent across every
// grant shape. The revoked row is the one that matters: enablement and the
// ability to authenticate are independent, so it is connected and unusable at
// once — reading connected alone is what made a dead connector look healthy.
func TestGetConnectionStatus(t *testing.T) {
	revoked := commsGrant(true)
	revoked.HealthState = sdk.HealthStateAuthRevoked

	tests := []struct {
		name               string
		grant              *sdk.ConnectorConfig
		wantConnected      bool
		wantNeedsReconnect bool
		wantUsable         bool
	}{
		{"enabled and healthy", commsGrant(true), true, false, true},
		{"disabled", commsGrant(false), false, false, false},
		{"enabled but auth-revoked", revoked, true, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
			cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
				Return(tt.grant, nil)
			svc := &connectorsvc.Service{ConfigDAO: cfgDAO}

			resp, err := svc.GetConnectionStatus(context.Background(),
				&connectorpb.GetConnectionStatusRequest{OrganizationId: brokerOrg, ConnectorId: commsConnectorID})
			require.NoError(t, err)
			require.Equal(t, tt.wantConnected, resp.GetConnected(), "connected")
			require.Equal(t, tt.wantNeedsReconnect, resp.GetNeedsReconnect(), "needs_reconnect")
			require.Equal(t, tt.wantUsable, resp.GetUsable(), "usable")
			require.Equal(t, resp.GetConnected() && !resp.GetNeedsReconnect(), resp.GetUsable(),
				"usable must stay derivable from the other two")
		})
	}
}

// TestReportGrantInvalid proves an auth failure disables the grant.
func TestReportGrantInvalid(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
		Return(commsGrant(true), nil)
	grant := commsGrant(true)
	cfgDAO.EXPECT().DisableIfCredentialsUnchanged(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID), grant.CredentialsUpdatedAt).
		Return(true, nil) // ReportGrantInvalid must disable the grant, fenced on what it read
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO}

	_, err := svc.ReportGrantInvalid(context.Background(), &connectorpb.ReportGrantInvalidRequest{
		OrganizationId: brokerOrg, ConnectorId: commsConnectorID, Reason: "invalid_auth",
	})
	require.NoError(t, err)
}

// TestGetSecretNotFound proves an unresolved connector returns NotFound, not a token.
func TestGetSecretNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
		Return(nil, gorm.ErrRecordNotFound)
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SecretAuthorizer: connectorsvc.AllowConnectors(commsConnectorID)}

	_, err := svc.GetSecret(context.Background(), &connectorpb.GetSecretRequest{OrganizationId: brokerOrg, ConnectorId: commsConnectorID})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestPurgeOrgGrants proves the org-lifecycle purge fan-out deletes the org's
// connector grant and reports a count of 1.
func TestPurgeOrgGrants(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	gomock.InOrder(
		cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
			Return(commsGrant(true), nil),
		cfgDAO.EXPECT().Delete(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Return(nil),
	)
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO}

	resp, err := svc.PurgeOrgGrants(context.Background(), &connectorpb.PurgeOrgGrantsRequest{OrganizationId: brokerOrg, ConnectorId: commsConnectorID})
	require.NoError(t, err)
	require.Equal(t, int64(1), resp.GetDeletedCount())
}

// TestPurgeOrgGrantsMissingIsNotFound proves a store that returns no row and
// no error is a missing grant too: NotFound, and nothing is deleted.
func TestPurgeOrgGrantsMissingIsNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Return(nil, nil)
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO}

	resp, err := svc.PurgeOrgGrants(context.Background(), &connectorpb.PurgeOrgGrantsRequest{OrganizationId: brokerOrg, ConnectorId: commsConnectorID})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Zero(t, resp.GetDeletedCount())
}

// TestPurgeOrgGrantsRequiresConnectorID proves an empty connector_id is rejected.
func TestPurgeOrgGrantsRequiresConnectorID(t *testing.T) {
	svc := &connectorsvc.Service{}
	_, err := svc.PurgeOrgGrants(context.Background(), &connectorpb.PurgeOrgGrantsRequest{OrganizationId: brokerOrg})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// fakeChannelLister returns canned channels (or an error), records the token it
// was lent, and counts calls — so ListCommsSlackChannels' token-plumbing and its
// cache-aside orchestration (whether the lister is consulted at all) are both
// testable without a gateway. By default (pages nil) it completes in one
// call, returning channels/err with an empty nextCursor — the shape every
// existing single-shot test wants. Set pages (keyed by the cursor the caller
// passes in) to simulate a multi-call resumable fetch instead.
type fakeChannelLister struct {
	gotToken string
	channels []*connectorpb.SlackChannel
	pages    map[string]fakeChannelPage
	err      error
	calls    int
}

type fakeChannelPage struct {
	channels   []*connectorpb.SlackChannel
	nextCursor string
	err        error
}

func (f *fakeChannelLister) ListChannels(_ context.Context, token, cursor string) ([]*connectorpb.SlackChannel, string, error) {
	f.calls++
	f.gotToken = token
	if f.pages != nil {
		page, ok := f.pages[cursor]
		if !ok {
			return nil, "", fmt.Errorf("fakeChannelLister: no page configured for cursor %q", cursor)
		}
		return page.channels, page.nextCursor, page.err
	}
	if f.err != nil {
		return nil, "", f.err
	}
	return f.channels, "", nil
}

// TestListCommsSlackChannels proves the RPC resolves the slack_comms grant's bot
// token, lends it to the lister, and returns the channels name-ascending.
func TestListCommsSlackChannels(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
		Return(commsGrant(true), nil)
	lister := &fakeChannelLister{channels: []*connectorpb.SlackChannel{
		{Id: "C2", Name: "zebra"},
		{Id: "C1", Name: "alpha"},
	}}
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: lister}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err)
	require.Equal(t, "xoxb-test-1", lister.gotToken, "must lend the slack_comms grant's bot token")
	require.Len(t, resp.GetChannels(), 2)
	require.Equal(t, "alpha", resp.GetChannels()[0].GetName(), "must be sorted name-ascending")
	require.Equal(t, "zebra", resp.GetChannels()[1].GetName())
}

// TestListCommsSlackChannelsNoGrant proves a missing grant is FailedPrecondition
// (the no-connection signal a caller passes through to its connect prompt).
func TestListCommsSlackChannelsNoGrant(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
		Return(nil, gorm.ErrRecordNotFound)
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: &fakeChannelLister{}}

	_, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestListCommsSlackChannelsMissingTokenFailsFast proves a live, enabled grant
// with no usable bot token (malformed/missing credentials) surfaces
// FailedPrecondition synchronously on the triggered path, rather than looping
// on syncing:true forever while fetchChannelsFromSlack's own token check fails
// silently inside the background goroutine with no way back to the caller.
func TestListCommsSlackChannelsMissingTokenFailsFast(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	noTokenGrant := &sdk.ConnectorConfig{
		ConnectorID: commsConnectorID, OrganizationID: brokerOrg, Enabled: true,
		Credentials: json.RawMessage(`{}`),
	}
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Return(noTokenGrant, nil)

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: &fakeChannelLister{}, ChannelCache: cache}

	_, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestListCommsSlackChannelsUnconfigured proves a nil lister returns Unimplemented
// rather than panicking.
func TestListCommsSlackChannelsUnconfigured(t *testing.T) {
	svc := &connectorsvc.Service{}
	_, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

// TestListCommsSlackChannelsServesFreshCacheWithoutFetching proves a fresh
// cache entry is returned without calling the lister at all, and without
// consulting liveGrant either — a fresh, non-forced hit must skip the
// synchronous Postgres check entirely (see ListCommsSlackChannels' doc
// comment for why that's still safe).
func TestListCommsSlackChannelsServesFreshCacheWithoutFetching(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Times(0)

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	stamp := time.Now().Add(-1 * time.Minute) // well inside the 15-minute soft window
	require.NoError(t, cache.Set(context.Background(), brokerOrg, []*connectorpb.SlackChannel{{Id: "C1", Name: "general"}}, stamp))

	lister := &fakeChannelLister{}
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err)
	require.Len(t, resp.GetChannels(), 1)
	require.Equal(t, 0, lister.calls, "a fresh cache hit must not call the lister")
	require.True(t, stamp.Equal(resp.GetRefreshedAt().AsTime()))
	require.False(t, resp.GetSyncing(), "a fresh cache hit must not report syncing")
}

// TestListCommsSlackChannelsForceRefreshReturnsImmediatelyAndSchedulesRefresh
// proves force_refresh never blocks on a live Slack call: it returns
// immediately with whatever's currently cached (even if fresh — force_refresh
// overrides freshness) and reports syncing so the caller polls for the new
// data. This RPC must never itself call the lister synchronously; the
// background refresh is fire-and-forget and not observed directly by this
// test (asserting on it would require a real Redis + sleep — see
// TestListCommsSlackChannelsStaleCacheServesImmediatelyAndSchedulesRefresh's
// comment for why this repo's tests don't do that).
func TestListCommsSlackChannelsForceRefreshReturnsImmediatelyAndSchedulesRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	// The background refresh may or may not have resolved the grant by the
	// time this test's assertions run (it's async) — allow any number of calls.
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Return(commsGrant(true), nil).AnyTimes()

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	stamp := time.Now() // fresh — force_refresh must override freshness anyway
	require.NoError(t, cache.Set(context.Background(), brokerOrg, []*connectorpb.SlackChannel{{Id: "STALE", Name: "stale"}}, stamp))

	lister := &fakeChannelLister{channels: []*connectorpb.SlackChannel{{Id: "FRESH", Name: "fresh"}}}
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg, ForceRefresh: true})
	require.NoError(t, err)
	require.True(t, resp.GetSyncing(), "force_refresh must report syncing so the caller polls for the new data")
	require.Len(t, resp.GetChannels(), 1)
	require.Equal(t, "STALE", resp.GetChannels()[0].GetId(), "force_refresh must return immediately with whatever's cached now, not block on the new fetch")
}

// TestListCommsSlackChannelsColdStartReturnsImmediatelyAndSchedulesRefresh
// proves a cache miss returns immediately (empty channels, no refreshed_at,
// syncing:true) rather than blocking on a live Slack call. A blocking read
// would fail on a cold start: a synchronous pagination sweep killed by an
// upstream timeout before completing would leave the cache empty for good.
func TestListCommsSlackChannelsColdStartReturnsImmediatelyAndSchedulesRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Return(commsGrant(true), nil).AnyTimes()

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	lister := &fakeChannelLister{channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "general"}}}
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err)
	require.True(t, resp.GetSyncing(), "a cache miss must report syncing so the caller polls until the background fetch lands")
	require.Empty(t, resp.GetChannels(), "a cache miss has nothing to return yet — the response must not block on the live fetch")
	require.Nil(t, resp.GetRefreshedAt(), "no cache entry yet means no refreshed_at")
}

// TestListCommsSlackChannelsColdStartNoGrantFailsFast proves a cache miss for
// an org with no live grant returns FailedPrecondition synchronously — it must
// not fall through to syncing:true, since the background refresh's own
// liveGrant failure has no way to reach the caller.
func TestListCommsSlackChannelsColdStartNoGrantFailsFast(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
		Return(nil, gorm.ErrRecordNotFound)

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: &fakeChannelLister{}, ChannelCache: cache}

	_, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestListCommsSlackChannelsStaleCacheServesImmediatelyAndSchedulesRefresh
// proves a stale (but present) cache entry is returned immediately without
// blocking on a live fetch — the stale-while-revalidate contract. The
// background refresh itself is fire-and-forget and not observed directly by
// this test (asserting on it would require a real Redis + sleep, which is
// exactly the kind of flaky-by-timing test this repo's conventions avoid);
// TestListCommsSlackChannelsServesFreshCacheWithoutFetching proves the
// lister is not called on a fresh cache hit.
func TestListCommsSlackChannelsStaleCacheServesImmediatelyAndSchedulesRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	// The background refresh may or may not have resolved the grant by the
	// time this test asserts (it's async) — allow any number of calls.
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Return(commsGrant(true), nil).AnyTimes()

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	staleStamp := time.Now().Add(-20 * time.Minute) // past the 15-minute soft window
	require.NoError(t, cache.Set(context.Background(), brokerOrg, []*connectorpb.SlackChannel{{Id: "STALE", Name: "stale"}}, staleStamp))

	lister := &fakeChannelLister{channels: []*connectorpb.SlackChannel{{Id: "FRESH", Name: "fresh"}}}
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err)
	require.Len(t, resp.GetChannels(), 1)
	require.Equal(t, "STALE", resp.GetChannels()[0].GetId(), "must return the stale entry immediately, not block on the refresh")
	require.True(t, staleStamp.Equal(resp.GetRefreshedAt().AsTime()))
	require.True(t, resp.GetSyncing(), "a stale cache read must report syncing while the background refresh is in flight")
}

// TestListCommsSlackChannelsFreshReadDuringInFlightRefreshStaysSyncing proves a
// plain (non-forced) read still reports syncing:true while an earlier
// force_refresh's background fetch is still running, even though the cache's
// own timestamp is (still) within the fresh window — otherwise the caller's
// poll would stop before the new data lands.
func TestListCommsSlackChannelsFreshReadDuringInFlightRefreshStaysSyncing(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	// A fresh, non-forced read must not consult liveGrant at all (same
	// invariant TestListCommsSlackChannelsServesFreshCacheWithoutFetching
	// locks in) — this test's own subject is the in-flight-lock peek, not
	// that invariant, but asserting it here too costs nothing.
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Times(0)

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	require.NoError(t, cache.Set(context.Background(), brokerOrg, []*connectorpb.SlackChannel{{Id: "C1", Name: "general"}}, time.Now()))
	// Simulate an in-flight background refresh (e.g. kicked off by an earlier
	// force_refresh) by acquiring the lock directly and never releasing it.
	_, ok, err := cache.TryLockRefresh(context.Background(), brokerOrg)
	require.NoError(t, err)
	require.True(t, ok)

	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: &fakeChannelLister{}, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err)
	require.True(t, resp.GetSyncing(), "a fresh read must still report syncing while a refresh is in flight, even though the cache's own timestamp looks fresh")
}

// TestListCommsSlackChannelsCacheReadErrorTriggersBackgroundRefresh proves a
// cache-read failure (e.g. a Redis blip) is treated as not-fresh rather than
// failing the request — the RPC still returns immediately (syncing:true, no
// data) instead of surfacing an Internal error for a transient cache problem.
func TestListCommsSlackChannelsCacheReadErrorTriggersBackgroundRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Return(commsGrant(true), nil).AnyTimes()

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	cache := connectorsvc.NewSlackChannelCache(redisClient)
	mr.Close() // force every subsequent Redis call to error; MaxRetries: -1 makes that fail fast and quiet

	lister := &fakeChannelLister{channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "general"}}}
	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err, "a cache-read error must not fail the whole request")
	require.True(t, resp.GetSyncing())
	require.Empty(t, resp.GetChannels())
}

// TestListCommsSlackChannelsTransientGrantCheckErrorServesStaleCache proves a
// transient failure resolving the grant (anything other than "no live
// grant") falls back to the stale cache instead of failing a request that
// has real data to show, so a database blip does not leave the picker
// failing.
func TestListCommsSlackChannelsTransientGrantCheckErrorServesStaleCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).
		Return(nil, errors.New("connection reset by peer"))

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	staleStamp := time.Now().Add(-20 * time.Minute) // past the 15-minute soft window
	require.NoError(t, cache.Set(context.Background(), brokerOrg, []*connectorpb.SlackChannel{{Id: "STALE", Name: "stale"}}, staleStamp))

	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: &fakeChannelLister{}, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err, "a transient grant-check error must not fail a request that has stale data to serve")
	require.Len(t, resp.GetChannels(), 1)
	require.Equal(t, "STALE", resp.GetChannels()[0].GetId())
	require.True(t, resp.GetSyncing())
}

// TestListCommsSlackChannelsSkipsGrantCheckWhileRefreshInFlight proves a
// triggered read (stale cache) skips the synchronous grant/token check
// entirely when a refresh for this org is already in flight — avoiding a
// redundant Postgres round trip on every poll during a multi-minute
// background fetch.
func TestListCommsSlackChannelsSkipsGrantCheckWhileRefreshInFlight(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Times(0)

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	staleStamp := time.Now().Add(-20 * time.Minute)
	require.NoError(t, cache.Set(context.Background(), brokerOrg, []*connectorpb.SlackChannel{{Id: "STALE", Name: "stale"}}, staleStamp))
	// Simulate an in-flight background refresh from an earlier trigger.
	_, ok, err := cache.TryLockRefresh(context.Background(), brokerOrg)
	require.NoError(t, err)
	require.True(t, ok)

	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: &fakeChannelLister{}, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err)
	require.True(t, resp.GetSyncing())
	require.Len(t, resp.GetChannels(), 1, "must still serve the stale cache")
}

// TestListCommsSlackChannelsResumeCursorKeepsSyncingOnFreshCache proves a
// leftover resume cursor (a prior background-refresh attempt ran out of
// budget before finishing pagination) forces syncing:true and keeps
// triggering follow-up attempts even though the cache's own timestamp looks
// fresh — freshness alone can't distinguish a completed sync from a partial
// one still being resumed.
func TestListCommsSlackChannelsResumeCursorKeepsSyncingOnFreshCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), brokerOrg, sdk.ConnectorID(commsConnectorID)).Return(commsGrant(true), nil).AnyTimes()

	cache := connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
	require.NoError(t, cache.Set(context.Background(), brokerOrg, []*connectorpb.SlackChannel{{Id: "PAGE1", Name: "page1"}}, time.Now()))
	require.NoError(t, cache.SetResumeState(context.Background(), brokerOrg, nil, "cursor-for-page-2", 0))

	svc := &connectorsvc.Service{ConfigDAO: cfgDAO, SlackChannels: &fakeChannelLister{}, ChannelCache: cache}

	resp, err := svc.ListCommsSlackChannels(context.Background(), &connectorpb.ListCommsSlackChannelsRequest{OrganizationId: brokerOrg})
	require.NoError(t, err)
	require.True(t, resp.GetSyncing(), "a leftover resume cursor must keep reporting syncing even on an otherwise-fresh cache")
	require.Len(t, resp.GetChannels(), 1, "must still serve the partial channels gathered so far")
}
