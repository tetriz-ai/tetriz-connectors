// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"gorm.io/gorm"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	connectormocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const internalTestOrg = "44444444-4444-4444-4444-444444444444"

// newTestSlackChannelCache builds a cache over an in-process miniredis
// client — the same pattern as secret_broker_test.go's newTestRedisClient,
// duplicated here since this file is an internal (white-box) test in package
// connector and cannot import that helper from the external connector_test
// package.
func newTestSlackChannelCache(t *testing.T) *SlackChannelCache {
	t.Helper()
	mr := miniredis.RunT(t)
	return NewSlackChannelCache(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
}

func liveInternalGrant() *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID: slackCommsConnectorID, OrganizationID: internalTestOrg, Enabled: true,
		Credentials: []byte(`{"bot_token":"xoxb-resume-test"}`),
	}
}

// TestWriteChannelsIfGrantStillLiveSkipsWriteWhenGrantGone proves a
// definitively absent grant (errNoLiveGrant) skips the cache write — the
// mid-flight-purge race triggerBackgroundChannelRefresh's fetch window can
// otherwise hit.
func TestWriteChannelsIfGrantStillLiveSkipsWriteWhenGrantGone(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(nil, gorm.ErrRecordNotFound)

	cache := newTestSlackChannelCache(t)
	svc := &Service{ConfigDAO: cfgDAO, ChannelCache: cache}

	err := svc.writeChannelsIfGrantStillLive(context.Background(), internalTestOrg,
		[]*connectorpb.SlackChannel{{Id: "FRESH", Name: "fresh"}}, time.Time{})
	require.Error(t, err, "a definitively absent grant must skip the write")

	_, _, found, getErr := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, getErr)
	require.False(t, found, "channels must not be written when the grant is gone")
}

// TestWriteChannelsIfGrantStillLiveWritesOnTransientCheckError proves a
// transient liveness-check error (a DB blip, not a confirmed disconnect)
// does not discard a successful fetch — conflating the two would confuse
// transient and definitive failures on this async write path, as the synchronous
// ListCommsSlackChannels path also must not.
func TestWriteChannelsIfGrantStillLiveWritesOnTransientCheckError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(nil, errors.New("connection reset by peer"))

	cache := newTestSlackChannelCache(t)
	svc := &Service{ConfigDAO: cfgDAO, ChannelCache: cache}

	err := svc.writeChannelsIfGrantStillLive(context.Background(), internalTestOrg,
		[]*connectorpb.SlackChannel{{Id: "FRESH", Name: "fresh"}}, time.Time{})
	require.NoError(t, err, "a transient liveness-check error must not discard a successful fetch")

	channels, refreshedAt, found, getErr := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, getErr)
	require.True(t, found)
	require.Len(t, channels, 1)
	require.Equal(t, "FRESH", channels[0].GetId())
	require.WithinDuration(t, time.Now(), refreshedAt, time.Minute)
}

// TestWriteChannelsIfGrantStillLiveWritesWhenGrantLive proves the ordinary
// case: a live grant lets the write through.
func TestWriteChannelsIfGrantStillLiveWritesWhenGrantLive(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(liveInternalGrant(), nil)

	cache := newTestSlackChannelCache(t)
	svc := &Service{ConfigDAO: cfgDAO, ChannelCache: cache}

	err := svc.writeChannelsIfGrantStillLive(context.Background(), internalTestOrg,
		[]*connectorpb.SlackChannel{{Id: "FRESH", Name: "fresh"}}, time.Time{})
	require.NoError(t, err)

	channels, _, found, getErr := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, getErr)
	require.True(t, found)
	require.Len(t, channels, 1)
}

// TestWriteChannelsIfGrantStillLiveSkipsWriteWhenGrantReconnected proves the
// credential-identity half of the liveness recheck: a grant that stays
// enabled=true throughout (a reconnect never disables the old grant first —
// saveSlackCommsConnection overwrites the same row's credentials in place)
// but whose CredentialsUpdatedAt no longer matches what the fetch started
// under must not have its result written — the completed fetch belongs to a
// session that no longer exists, so writing it would resurrect a stale or
// foreign workspace's channel list into the served cache.
func TestWriteChannelsIfGrantStillLiveSkipsWriteWhenGrantReconnected(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	reconnected := liveInternalGrant()
	reconnected.CredentialsUpdatedAt = time.Unix(2000, 0)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(reconnected, nil)

	cache := newTestSlackChannelCache(t)
	svc := &Service{ConfigDAO: cfgDAO, ChannelCache: cache}

	err := svc.writeChannelsIfGrantStillLive(context.Background(), internalTestOrg,
		[]*connectorpb.SlackChannel{{Id: "FRESH", Name: "fresh"}}, time.Unix(1000, 0))
	require.Error(t, err, "a grant whose credentials changed since the fetch started must skip the write")

	_, _, found, getErr := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, getErr)
	require.False(t, found, "channels from a stale/foreign session must not be written")
}

// pagedChannelLister is a SlackChannelLister test double keyed by the input
// cursor, letting refreshChannelsOnePage's resumability be driven
// deterministically across several direct (non-goroutine) calls, unlike the
// external test package's fakeChannelLister, which secret_broker_internal_test.go
// cannot import.
type pagedChannelLister struct {
	pages map[string]fakePage
}

type fakePage struct {
	channels   []*connectorpb.SlackChannel
	nextCursor string
	err        error
}

func (l *pagedChannelLister) ListChannels(_ context.Context, _, cursor string) ([]*connectorpb.SlackChannel, string, error) {
	page, ok := l.pages[cursor]
	if !ok {
		return nil, "", fmt.Errorf("pagedChannelLister: no page configured for cursor %q", cursor)
	}
	return page.channels, page.nextCursor, page.err
}

// TestRefreshChannelsOnePageResumesAcrossAttempts proves the actual
// multi-attempt resumability contract: a first attempt that runs out of
// budget partway through stages what it fetched plus a resume cursor
// (never discarding real progress, and never touching the served cache), and
// a second attempt started from that cursor merges in the rest, writes the
// full result to the served cache, and clears the resume state once Slack
// signals completion — exactly the sequence triggerBackgroundChannelRefresh's
// repeated triggers drive in real use, without needing a real goroutine or
// sleep to observe it.
func TestRefreshChannelsOnePageResumesAcrossAttempts(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(liveInternalGrant(), nil).AnyTimes()

	cache := newTestSlackChannelCache(t)
	lister := &pagedChannelLister{pages: map[string]fakePage{
		"": {channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "alpha"}}, nextCursor: "page-2"},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	require.NoError(t, svc.refreshChannelsOnePage(context.Background(), internalTestOrg))

	_, _, servedFound, err := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, err)
	require.False(t, servedFound, "the served cache must stay untouched while pagination is still incomplete")

	stagedChannels, cursor, _, resuming, err := cache.GetResumeState(context.Background(), internalTestOrg)
	require.NoError(t, err)
	require.True(t, resuming, "an incomplete first attempt must leave a resume state for the next one")
	require.Equal(t, "page-2", cursor)
	require.Len(t, stagedChannels, 1, "the first attempt's partial progress must be staged, not discarded")
	require.Equal(t, "C1", stagedChannels[0].GetId())

	lister.pages["page-2"] = fakePage{channels: []*connectorpb.SlackChannel{{Id: "C2", Name: "zebra"}}, nextCursor: ""}
	require.NoError(t, svc.refreshChannelsOnePage(context.Background(), internalTestOrg))

	channels, _, servedFound, err := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, err)
	require.True(t, servedFound, "the served cache must be written once the cycle completes")
	require.Len(t, channels, 2, "the second attempt must merge with the first attempt's staged channels, not replace them")
	require.Equal(t, "alpha", channels[0].GetName(), "merged result must still be sorted name-ascending")
	require.Equal(t, "zebra", channels[1].GetName())

	_, _, _, resuming, err = cache.GetResumeState(context.Background(), internalTestOrg)
	require.NoError(t, err)
	require.False(t, resuming, "a completed cycle must clear the resume state")
}

// TestRefreshChannelsOnePageRoutineRefreshDoesNotShrinkServedCache proves a
// brand new cycle (no resume state — the ordinary 15-minute soft-TTL
// trigger, not a resumption) does not shrink what the picker shows down to
// whatever its first, incomplete page range holds: the complete
// served cache must survive untouched until the new cycle itself fully
// completes.
func TestRefreshChannelsOnePageRoutineRefreshDoesNotShrinkServedCache(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(liveInternalGrant(), nil).AnyTimes()

	cache := newTestSlackChannelCache(t)
	require.NoError(t, cache.Set(context.Background(), internalTestOrg,
		[]*connectorpb.SlackChannel{{Id: "OLD1", Name: "old1"}, {Id: "OLD2", Name: "old2"}}, time.Now()))

	lister := &pagedChannelLister{pages: map[string]fakePage{
		"": {channels: []*connectorpb.SlackChannel{{Id: "NEW1", Name: "new1"}}, nextCursor: "page-2"},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	require.NoError(t, svc.refreshChannelsOnePage(context.Background(), internalTestOrg))

	channels, _, found, err := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, channels, 2, "the old, complete list must still be served while the new cycle is only one page in")
}

// TestRefreshChannelsOnePageFreshCycleErrorDoesNotDestroyServedCache proves
// that a fresh cycle
// (resumeCursor == "") whose very first page fails outright must not
// destroy a complete served cache. The fake page here matches
// the REAL production lister's error contract — it returns the unchanged
// input cursor (empty, for a fresh cycle) alongside the error — the exact
// shape that, if ever read as a completion signal, would look like
// "pagination finished with zero channels."
func TestRefreshChannelsOnePageFreshCycleErrorDoesNotDestroyServedCache(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(liveInternalGrant(), nil).AnyTimes()

	cache := newTestSlackChannelCache(t)
	require.NoError(t, cache.Set(context.Background(), internalTestOrg,
		[]*connectorpb.SlackChannel{{Id: "OLD1", Name: "old1"}}, time.Now()))

	lister := &pagedChannelLister{pages: map[string]fakePage{
		"": {channels: nil, nextCursor: "", err: errors.New("slack API error: internal_error")},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	err := svc.refreshChannelsOnePage(context.Background(), internalTestOrg)
	require.Error(t, err, "no progress was made at all, so the error must surface and the caller should back off")

	channels, _, found, getErr := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, getErr)
	require.True(t, found, "the complete served cache must survive an errored fresh-cycle attempt")
	require.Len(t, channels, 1)
	require.Equal(t, "OLD1", channels[0].GetId())

	_, _, _, resuming, resumeErr := cache.GetResumeState(context.Background(), internalTestOrg)
	require.NoError(t, resumeErr)
	require.False(t, resuming, "a fresh cycle that made zero progress before erroring has nothing to resume — "+
		"persisting resume state here would violate SetResumeState's own precondition and spuriously report "+
		"syncing:true for an org whose served cache is actually complete and fine")
}

// TestRefreshChannelsOnePageReturnsNilWhenProgressMadeDespiteError proves
// real progress made before an error (at least one channel gathered) is
// still staged and reported as a nil error, so
// triggerBackgroundChannelRefresh releases the lock and the next poll can
// retry promptly instead of waiting out the backoff meant for an org making
// no progress at all.
func TestRefreshChannelsOnePageReturnsNilWhenProgressMadeDespiteError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(liveInternalGrant(), nil).AnyTimes()

	cache := newTestSlackChannelCache(t)
	lister := &pagedChannelLister{pages: map[string]fakePage{
		"": {channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "alpha"}}, nextCursor: "page-2", err: errors.New("slack API error: internal_error")},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	err := svc.refreshChannelsOnePage(context.Background(), internalTestOrg)
	require.NoError(t, err, "progress was made and staged despite the error, so the caller should not back off")

	_, _, servedFound, getErr := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, getErr)
	require.False(t, servedFound, "an errored, incomplete attempt must never write the served cache")

	stagedChannels, cursor, _, resuming, getErr := cache.GetResumeState(context.Background(), internalTestOrg)
	require.NoError(t, getErr)
	require.True(t, resuming)
	require.Len(t, stagedChannels, 1)
	require.Equal(t, "page-2", cursor)
}

// TestRefreshChannelsOnePageSkipsResumeStateWriteWhenGrantGoneMidFetch proves
// persistResumeState's liveness recheck: a grant that was live when this
// attempt started (so the fetch itself proceeded and made real progress) but
// is gone by the time the attempt tries to stage that progress must not have
// its resume state written — otherwise a later reconnect (even to a
// different Slack workspace) within slackChannelResumeStateTTL would resume
// pagination from, and merge in, a disconnected/foreign session's staged
// channels and cursor.
func TestRefreshChannelsOnePageSkipsResumeStateWriteWhenGrantGoneMidFetch(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	getCalls := 0
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		DoAndReturn(func(context.Context, string, sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
			getCalls++
			if getCalls == 1 {
				// The initial grant/token check, before the fetch starts.
				return liveInternalGrant(), nil
			}
			// persistResumeState's own liveness recheck, after the fetch —
			// the grant was disconnected/purged in between.
			return nil, gorm.ErrRecordNotFound
		}).AnyTimes()

	cache := newTestSlackChannelCache(t)
	lister := &pagedChannelLister{pages: map[string]fakePage{
		"": {channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "alpha"}}, nextCursor: "page-2", err: errors.New("slack API error: internal_error")},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	err := svc.refreshChannelsOnePage(context.Background(), internalTestOrg)
	require.NoError(t, err, "progress was made despite the error, so the caller should not back off, even though the grant vanished before staging")

	_, _, _, resuming, resumeErr := cache.GetResumeState(context.Background(), internalTestOrg)
	require.NoError(t, resumeErr)
	require.False(t, resuming, "a grant that's gone by staging time must not have its resume state written — "+
		"a later reconnect must not resume from a disconnected session's cursor")
}

// TestRefreshChannelsOnePageSkipsServedCacheWriteWhenGrantReconnectedMidFetch
// proves the completion-path counterpart of the resume-state fence: when a
// fetch's LAST page finishes in this same call (nextCursor == "") but the org
// reconnected (even to a different Slack workspace) while that fetch was in
// flight, writeChannelsIfGrantStillLive must not write the just-fetched
// channels to the served cache — they were retrieved under a token resolved
// from a session that no longer exists by the time the write happens.
func TestRefreshChannelsOnePageSkipsServedCacheWriteWhenGrantReconnectedMidFetch(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	getCalls := 0
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		DoAndReturn(func(context.Context, string, sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
			getCalls++
			cfg := liveInternalGrant()
			if getCalls == 1 {
				cfg.CredentialsUpdatedAt = time.Unix(1000, 0)
			} else {
				// writeChannelsIfGrantStillLive's own recheck, after the
				// fetch completed — the org reconnected while it was in
				// flight; enabled stays true throughout.
				cfg.CredentialsUpdatedAt = time.Unix(2000, 0)
			}
			return cfg, nil
		}).AnyTimes()

	cache := newTestSlackChannelCache(t)
	require.NoError(t, cache.Set(context.Background(), internalTestOrg,
		[]*connectorpb.SlackChannel{{Id: "OLD1", Name: "old1"}}, time.Now()))
	lister := &pagedChannelLister{pages: map[string]fakePage{
		"": {channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "alpha"}}, nextCursor: ""},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	err := svc.refreshChannelsOnePage(context.Background(), internalTestOrg)
	require.Error(t, err, "the grant reconnected before the completed fetch could be written, so the write must fail rather than silently succeed with stale data")

	channels, _, found, getErr := cache.Get(context.Background(), internalTestOrg)
	require.NoError(t, getErr)
	require.True(t, found)
	require.Len(t, channels, 1, "the served cache must still be the pre-existing list, never overwritten by a completed fetch from a session that no longer exists")
	require.Equal(t, "OLD1", channels[0].GetId())
}

// TestRefreshChannelsOnePageSkipsResumeStateWriteWhenGrantReconnectedMidFetch
// proves the credential-identity half of persistResumeState's recheck: a
// reconnect never disables the old grant first (saveSlackCommsConnection
// overwrites the same row's credentials in place), so a grant that stays
// live=true and enabled=true throughout — but whose CredentialsUpdatedAt
// changed while this attempt's fetch was in flight — must be treated the
// same as a fully-absent grant: the fetch's staged progress belongs to a
// session that no longer exists, so it must not be persisted for a later
// resume to merge into the new session's channel list.
func TestRefreshChannelsOnePageSkipsResumeStateWriteWhenGrantReconnectedMidFetch(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	getCalls := 0
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		DoAndReturn(func(context.Context, string, sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
			getCalls++
			cfg := liveInternalGrant()
			if getCalls == 1 {
				cfg.CredentialsUpdatedAt = time.Unix(1000, 0)
			} else {
				// persistResumeState's own recheck, after the fetch — the org
				// reconnected (even to a different Slack workspace) while
				// this attempt's fetch was in flight; enabled stays true
				// throughout, only the credential identity changed.
				cfg.CredentialsUpdatedAt = time.Unix(2000, 0)
			}
			return cfg, nil
		}).AnyTimes()

	cache := newTestSlackChannelCache(t)
	lister := &pagedChannelLister{pages: map[string]fakePage{
		"": {channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "alpha"}}, nextCursor: "page-2", err: errors.New("slack API error: internal_error")},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	err := svc.refreshChannelsOnePage(context.Background(), internalTestOrg)
	require.NoError(t, err, "progress was made despite the error, so the caller should not back off, even though the grant was reconnected before staging")

	_, _, _, resuming, resumeErr := cache.GetResumeState(context.Background(), internalTestOrg)
	require.NoError(t, resumeErr)
	require.False(t, resuming, "a grant whose credentials changed by staging time must not have its resume state written — "+
		"a later resume must not merge a disconnected/foreign session's cursor into the new session's channel list")
}

// TestRefreshChannelsOnePagePreservesStateOnResumeStateReadError proves a
// transient failure reading the resume state (a Redis blip, or here a
// corrupted stored value) does not get treated as "nothing to resume" — that
// would let this attempt persist its own (much smaller) progress over
// whatever multi-page state the read merely failed to retrieve, silently
// discarding real accumulated work.
func TestRefreshChannelsOnePagePreservesStateOnResumeStateReadError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(liveInternalGrant(), nil).AnyTimes()

	cache := newTestSlackChannelCache(t)
	// Corrupt the resume-state value directly so GetResumeState's own JSON
	// unmarshal fails — a deterministic stand-in for a transient read error,
	// without needing to fail the underlying Redis connection itself.
	require.NoError(t, cache.redis.Set(context.Background(), slackChannelResumeStateKey(internalTestOrg), "{not json", 0).Err())

	lister := &pagedChannelLister{pages: map[string]fakePage{
		"": {channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "alpha"}}, nextCursor: "page-2"},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	err := svc.refreshChannelsOnePage(context.Background(), internalTestOrg)
	require.Error(t, err, "a resume-state read failure must surface, not be treated as a fresh cycle")

	raw, getErr := cache.redis.Get(context.Background(), slackChannelResumeStateKey(internalTestOrg)).Result()
	require.NoError(t, getErr)
	require.Equal(t, "{not json", raw, "the corrupted (but potentially recoverable) prior state must survive untouched, not be overwritten by this attempt's own smaller progress")
}

// TestRefreshChannelsOnePageBacksOffAfterConsecutiveErrors proves that
// partial progress alone doesn't let a degraded (not fully dead) Slack
// integration retry every few seconds forever: after
// slackChannelMaxConsecutiveErrorsBeforeBackoff attempts in a row that each
// made some progress but then errored, the next such attempt must surface
// the error (so the caller withholds the singleflight lock and backs off for
// its TTL) instead of returning nil.
func TestRefreshChannelsOnePageBacksOffAfterConsecutiveErrors(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(liveInternalGrant(), nil).AnyTimes()

	cache := newTestSlackChannelCache(t)
	lister := &pagedChannelLister{pages: map[string]fakePage{
		"":       {channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "alpha"}}, nextCursor: "page-2", err: errors.New("slack API error: internal_error")},
		"page-2": {channels: []*connectorpb.SlackChannel{{Id: "C1", Name: "alpha"}}, nextCursor: "page-2", err: errors.New("slack API error: internal_error")},
	}}
	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: lister, ChannelCache: cache}

	for i := 0; i < slackChannelMaxConsecutiveErrorsBeforeBackoff-1; i++ {
		require.NoError(t, svc.refreshChannelsOnePage(context.Background(), internalTestOrg),
			"attempt %d: progress was made despite the error, so this attempt alone must not back off yet", i+1)
	}

	err := svc.refreshChannelsOnePage(context.Background(), internalTestOrg)
	require.Error(t, err, "after enough consecutive errors in a row, the caller must back off even though this attempt also made progress")
}

// slowIncompleteLister always reports it has more to paginate, deliberately
// slow enough that a caller's short budget context reliably expires before
// this call returns — simulating a page that comes back just as the deadline
// hits.
type slowIncompleteLister struct{}

func (l *slowIncompleteLister) ListChannels(_ context.Context, _, _ string) ([]*connectorpb.SlackChannel, string, error) {
	time.Sleep(20 * time.Millisecond)
	return []*connectorpb.SlackChannel{{Id: "C1", Name: "partial"}}, "page-2", nil
}

// TestFetchChannelsFromSlackErrorsOnBudgetExhaustionBeforeCompletion proves
// fetchChannelsFromSlack (the nil-ChannelCache fallback, which has no resume
// state to continue from later) fails loudly rather than silently reporting
// a truncated channel list as a successful, complete fetch when its budget
// runs out mid-pagination — a fetch that looks successful but was cut off,
// which the real, cache-backed path never reports.
func TestFetchChannelsFromSlackErrorsOnBudgetExhaustionBeforeCompletion(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	cfgDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
	cfgDAO.EXPECT().Get(gomock.Any(), internalTestOrg, sdk.ConnectorID(slackCommsConnectorID)).
		Return(liveInternalGrant(), nil)

	svc := &Service{ConfigDAO: cfgDAO, SlackChannels: &slowIncompleteLister{}}

	_, err := svc.fetchChannelsFromSlack(context.Background(), internalTestOrg, time.Millisecond)
	require.Error(t, err, "running out of budget before pagination completes must be a real error, not a silently truncated success")
}
