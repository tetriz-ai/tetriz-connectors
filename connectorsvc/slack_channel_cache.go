// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// slackChannelCacheHardTTL is the outer bound on how long a cached channel
// list survives before it is treated as no-cache-at-all (cold start), even if
// nobody read it in the meantime — so an org that goes quiet for a long time
// doesn't serve indefinitely-stale data once someone finally reopens the
// picker. Freshness inside this window (the 15-minute soft window that
// triggers a background refresh vs. an instant fresh return) is the caller's
// own comparison against the stored RefreshedAt — this type only stores and
// retrieves, it does not decide what "fresh" means.
const slackChannelCacheHardTTL = 24 * time.Hour

// slackChannelRefreshLockTTL bounds how long the background-refresh
// singleflight lock is held. It is deliberately not released early on
// failure (see Service.triggerBackgroundChannelRefresh) — the TTL then also
// acts as a minimum backoff between refresh attempts for the same org.
// Should stay strictly greater than slackChannelBackgroundFetchBudget (the
// longest operation this lock guards) so a refresh normally finishes before
// its lock expires. release() is fencing-token guarded against lock-stealing;
// see releaseLockScript.
const slackChannelRefreshLockTTL = 3 * time.Minute

// cachedSlackChannel is the on-the-wire (Redis JSON) shape of one channel —
// deliberately not the generated connectorpb.SlackChannel struct, so this
// cache's storage format doesn't depend on protobuf-generated Go struct
// layout or JSON tag behavior.
type cachedSlackChannel struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IsPrivate bool   `json:"is_private"`
}

type cachedSlackChannelList struct {
	Channels    []cachedSlackChannel `json:"channels"`
	RefreshedAt time.Time            `json:"refreshed_at"`
}

// cachedResumeState is the on-the-wire shape of an in-progress background
// sync's OWN accumulated progress — deliberately separate from
// cachedSlackChannelList (the served cache callers actually read). Keeping
// the two apart means a multi-attempt resync never has to touch, and can
// never prematurely shrink, what ListCommsSlackChannels returns: the served
// cache only gets replaced once pagination fully completes and this staged
// state is discarded.
type cachedResumeState struct {
	Cursor            string               `json:"cursor"`
	Channels          []cachedSlackChannel `json:"channels"`
	ConsecutiveErrors int                  `json:"consecutive_errors,omitempty"`
}

// SlackChannelCache is a per-org, Redis-backed cache of the slack_comms
// workspace's channel list, so ListCommsSlackChannels doesn't hit
// Slack's conversations.list (Tier 2, ~20 req/min) on every picker open.
type SlackChannelCache struct {
	redis *redis.Client
}

// NewSlackChannelCache builds a cache over the given Redis client.
func NewSlackChannelCache(redisClient *redis.Client) *SlackChannelCache {
	return &SlackChannelCache{redis: redisClient}
}

// slackChannelResumeStateTTL bounds how long a saved resync's own progress
// (cursor + staged channels) survives between background-refresh attempts.
// Generous relative to how often attempts actually resume (each poll
// re-triggers one within seconds of the last finishing) — this is a backstop
// against a permanently-abandoned cycle (e.g. the org disconnects mid-sync
// and nothing ever resumes it), not a normal expiry path.
const slackChannelResumeStateTTL = 1 * time.Hour

func slackChannelCacheKey(orgID string) string {
	return slackCommsConnectorID + ":channels:" + orgID
}

func slackChannelRefreshLockKey(orgID string) string {
	return slackCommsConnectorID + ":refreshing:" + orgID
}

func slackChannelResumeStateKey(orgID string) string {
	return slackCommsConnectorID + ":resume_state:" + orgID
}

// Get returns the cached channel list for orgID, or found=false when there is
// no entry (cold start, or the hard TTL has expired the key).
func (c *SlackChannelCache) Get(ctx context.Context, orgID string) (channels []*connectorpb.SlackChannel, refreshedAt time.Time, found bool, err error) {
	raw, err := c.redis.Get(ctx, slackChannelCacheKey(orgID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, time.Time{}, false, nil
		}
		return nil, time.Time{}, false, fmt.Errorf("get slack channel cache: %w", err)
	}
	var stored cachedSlackChannelList
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, time.Time{}, false, fmt.Errorf("unmarshal slack channel cache: %w", err)
	}
	out := make([]*connectorpb.SlackChannel, 0, len(stored.Channels))
	for _, ch := range stored.Channels {
		out = append(out, &connectorpb.SlackChannel{Id: ch.ID, Name: ch.Name, IsPrivate: ch.IsPrivate})
	}
	return out, stored.RefreshedAt.UTC(), true, nil
}

// Set writes the channel list for orgID, stamped with refreshedAt. The Redis
// key TTL is the hard ceiling (slackChannelCacheHardTTL); freshness inside
// that window is the caller's own soft-window comparison against refreshedAt.
func (c *SlackChannelCache) Set(ctx context.Context, orgID string, channels []*connectorpb.SlackChannel, refreshedAt time.Time) error {
	stored := cachedSlackChannelList{
		Channels:    make([]cachedSlackChannel, 0, len(channels)),
		RefreshedAt: refreshedAt.UTC(),
	}
	for _, ch := range channels {
		stored.Channels = append(stored.Channels, cachedSlackChannel{ID: ch.GetId(), Name: ch.GetName(), IsPrivate: ch.GetIsPrivate()})
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("marshal slack channel cache: %w", err)
	}
	if err := c.redis.Set(ctx, slackChannelCacheKey(orgID), raw, slackChannelCacheHardTTL).Err(); err != nil {
		return fmt.Errorf("set slack channel cache: %w", err)
	}
	return nil
}

// releaseLockScript deletes the refresh lock only if it still holds the
// caller's own fencing token (a compare-and-delete, not a bare DEL). Without
// this, a caller whose work outran slackChannelRefreshLockTTL could delete a
// lock a second caller has since acquired for the same org, letting two
// refreshes run concurrently; the fencing token means each release() can only
// ever remove its own acquisition, however late it runs.
const releaseLockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`

// TryLockRefresh acquires the per-org background-refresh singleflight lock.
// ok is false when another refresh is already in flight (this process or
// another replica) — the caller should skip spawning a duplicate. Callers
// should call release() only on a successful refresh; on failure, let the
// lock expire naturally (slackChannelRefreshLockTTL then doubles as a minimum
// backoff before the next attempt). release() is fencing-token guarded
// against lock-stealing; see releaseLockScript.
func (c *SlackChannelCache) TryLockRefresh(ctx context.Context, orgID string) (release func(), ok bool, err error) {
	key := slackChannelRefreshLockKey(orgID)
	token := uuid.NewString()
	acquired, err := c.redis.SetNX(ctx, key, token, slackChannelRefreshLockTTL).Result()
	if err != nil {
		return func() {}, false, fmt.Errorf("acquire slack channel refresh lock: %w", err)
	}
	if !acquired {
		return func() {}, false, nil
	}
	// release() may run well after this call returns (the background-refresh
	// path invokes it from inside goroutine.RunWithCtx, after the caller's own
	// ctx could be canceled) — derive from ctx via WithoutCancel so the Lua
	// eval keeps trace/log fields without inheriting cancellation.
	releaseCtx := context.WithoutCancel(ctx)
	release = func() {
		if err := c.redis.Eval(releaseCtx, releaseLockScript, []string{key}, token).Err(); err != nil {
			logger.Error(releaseCtx, "TryLockRefresh: release failed", zap.String(logger.OrgID, orgID), zap.Error(err))
		}
	}
	return release, true, nil
}

// IsRefreshInFlight reports whether the per-org refresh lock is currently
// held, without acquiring or otherwise affecting it — a peek, not a claim.
// True does not always mean a fetch is actively running: the lock is also
// held, unreleased, for the rest of its TTL after a failed attempt (see
// TryLockRefresh), so this reports "won't retry yet" as well as "in flight".
func (c *SlackChannelCache) IsRefreshInFlight(ctx context.Context, orgID string) (bool, error) {
	n, err := c.redis.Exists(ctx, slackChannelRefreshLockKey(orgID)).Result()
	if err != nil {
		return false, fmt.Errorf("check slack channel refresh lock: %w", err)
	}
	return n > 0, nil
}

// Delete removes any cached channel list for orgID (e.g. when the org's
// Slack grant is disabled or purged, so a stale list isn't served for the
// remainder of the hard TTL) along with any in-progress resync state — a
// purged org's sync should not resume once the grant is reconnected.
func (c *SlackChannelCache) Delete(ctx context.Context, orgID string) error {
	if err := c.redis.Del(ctx, slackChannelCacheKey(orgID), slackChannelResumeStateKey(orgID)).Err(); err != nil {
		return fmt.Errorf("delete slack channel cache: %w", err)
	}
	return nil
}

// GetResumeState returns a prior, incomplete background-refresh attempt's own
// accumulated channels, the cursor to continue from, and how many consecutive
// attempts have ended in a fetch error (see SetResumeState) — or found=false
// if the last attempt for this org completed (or none has ever run), in
// which case there is nothing to resume and the served cache (Get) is the
// current full workspace. This is deliberately separate storage from the
// served cache: an in-progress resync's own progress is invisible to
// ListCommsSlackChannels' callers until it fully completes (see Set's doc).
func (c *SlackChannelCache) GetResumeState(ctx context.Context, orgID string) (channels []*connectorpb.SlackChannel, cursor string, consecutiveErrors int, found bool, err error) {
	raw, err := c.redis.Get(ctx, slackChannelResumeStateKey(orgID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, "", 0, false, nil
		}
		return nil, "", 0, false, fmt.Errorf("get slack channel resume state: %w", err)
	}
	var stored cachedResumeState
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, "", 0, false, fmt.Errorf("unmarshal slack channel resume state: %w", err)
	}
	out := make([]*connectorpb.SlackChannel, 0, len(stored.Channels))
	for _, ch := range stored.Channels {
		out = append(out, &connectorpb.SlackChannel{Id: ch.ID, Name: ch.Name, IsPrivate: ch.IsPrivate})
	}
	return out, stored.Cursor, stored.ConsecutiveErrors, true, nil
}

// SetResumeState persists an in-progress resync's own accumulated channels,
// the cursor to continue pagination from on the next attempt, and the
// consecutive-error count the caller is tracking (0 for a clean, error-free
// step; incremented by the caller across attempts that keep hitting fetch
// errors, which escalate from "retry promptly" to "back off" — see
// refreshChannelsOnePage). Callers must only call this while the sync is
// still incomplete (a non-empty cursor to resume from) — once pagination
// fully completes, write the result to the served cache (Set) and call
// ClearResumeState instead, never this.
func (c *SlackChannelCache) SetResumeState(ctx context.Context, orgID string, channels []*connectorpb.SlackChannel, cursor string, consecutiveErrors int) error {
	stored := cachedResumeState{
		Cursor:            cursor,
		Channels:          make([]cachedSlackChannel, 0, len(channels)),
		ConsecutiveErrors: consecutiveErrors,
	}
	for _, ch := range channels {
		stored.Channels = append(stored.Channels, cachedSlackChannel{ID: ch.GetId(), Name: ch.GetName(), IsPrivate: ch.GetIsPrivate()})
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("marshal slack channel resume state: %w", err)
	}
	if err := c.redis.Set(ctx, slackChannelResumeStateKey(orgID), raw, slackChannelResumeStateTTL).Err(); err != nil {
		return fmt.Errorf("set slack channel resume state: %w", err)
	}
	return nil
}

// ClearResumeState removes the in-progress resync marker once pagination
// completes and its result has been written to the served cache — the
// served cache is now the full workspace, not a partial result.
func (c *SlackChannelCache) ClearResumeState(ctx context.Context, orgID string) error {
	if err := c.redis.Del(ctx, slackChannelResumeStateKey(orgID)).Err(); err != nil {
		return fmt.Errorf("clear slack channel resume state: %w", err)
	}
	return nil
}
