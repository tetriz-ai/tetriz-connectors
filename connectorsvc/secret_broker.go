// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/async/goroutine"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The secret-broker surface: the connector owns the vendor token and the
// connection lifecycle, and lends the token to a host that sends messages
// with it; the host reports a dead grant back. A grant is identified by
// its connector_id (e.g. "slack_comms"); GetSecret audits every call and NEVER
// logs the token.

// slackCommsConnectorID is the notifications Slack grant's connector id.
const slackCommsConnectorID = "slack_comms"

// errNoLiveGrant signals the "absent grant" case — no row, a soft-deleted row, or
// a disabled grant — as distinct from a lookup failure. Callers errors.Is on it to
// map the absent case to their own status (NotFound / FailedPrecondition / not
// connected) rather than an Internal error.
var errNoLiveGrant = errors.New("connector: no live grant")

// errGrantRevoked signals an enabled grant whose connector is paused as
// auth_revoked: its credential is confirmed dead, so it is not live and its
// token is not lent. It wraps errNoLiveGrant, so a caller that only asks "is
// there a usable grant?" treats it as absent; a caller that reports a reconnect
// (GetConnectionStatus) or refuses distinctly (GetSecret) checks it first.
var errGrantRevoked = fmt.Errorf("%w: the grant's credential is revoked, reconnect required", errNoLiveGrant)

// liveGrant returns the org's enabled, non-deleted, healthy grant for a
// connector. It returns errNoLiveGrant when none exists (Get already filters
// soft-deleted rows; a disabled grant is treated as absent for the broker's
// purposes) and errGrantRevoked when the grant is paused as auth_revoked.
func (s *Service) liveGrant(ctx context.Context, orgID, connectorID string) (*sdk.ConnectorConfig, error) {
	cfg, err := s.ConfigDAO.Get(ctx, orgID, sdk.ConnectorID(connectorID))
	if store.IsNotFound(err) {
		return nil, errNoLiveGrant
	}
	if err != nil {
		return nil, fmt.Errorf("get connector config: %w", err)
	}
	// store.IsNotFound already mapped a real not-found error above; this guards the
	// (nil, nil) return the IConnectorConfigDAO interface doesn't rule out.
	if cfg == nil || !cfg.Enabled {
		return nil, errNoLiveGrant
	}
	if cfg.HealthState == sdk.HealthStateAuthRevoked {
		return nil, errGrantRevoked
	}
	return cfg, nil
}

// GetSecret returns the bot token for an org's grant for the given connector.
//
// The caller must first pass the service's SecretAuthorizer; without one,
// every call is denied with PermissionDenied (deny by default). The
// organization is canonicalised first, and one that is not a UUID is
// InvalidArgument, so every spelling of an organization meets the same
// policy. The check comes before the grant is read, so a denied GetSecret
// does not tell whether the grant exists; the service's other RPCs are not
// gated by it (see WithSecretAuthorizer).
// A grant paused as auth_revoked is refused with FailedPrecondition: its
// credential is confirmed dead, so a reconnect is needed, not a retry.
func (s *Service) GetSecret(ctx context.Context, req *connectorpb.GetSecretRequest) (*connectorpb.GetSecretResponse, error) {
	if req.GetOrganizationId() == "" || req.GetConnectorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id and connector_id are required")
	}
	u, parseErr := uuid.Parse(req.GetOrganizationId())
	if parseErr != nil {
		return nil, status.Error(codes.InvalidArgument, "organization_id is not a UUID")
	}
	orgID := u.String()
	if err := s.authorizeSecret(ctx, orgID, req.GetConnectorId()); err != nil {
		return nil, err
	}
	cfg, err := s.liveGrant(ctx, orgID, req.GetConnectorId())
	if errors.Is(err, errGrantRevoked) {
		return nil, status.Error(codes.FailedPrecondition, "grant's credential is revoked; reconnect required")
	}
	if errors.Is(err, errNoLiveGrant) {
		return nil, status.Error(codes.NotFound, "no live grant for that connector")
	}
	if err != nil {
		logger.Error(ctx, "GetSecret: resolve grant failed",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, req.GetConnectorId()), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to resolve grant")
	}
	token := extractToken(cfg.Credentials)
	if token == "" {
		return nil, status.Error(codes.FailedPrecondition, "grant has no token")
	}
	// Audit: record the access WITHOUT the token. The caller's identity is
	// whatever the SecretAuthorizer checked; this logs the grant lent.
	logger.Info(ctx, "connector GetSecret access",
		zap.String(logger.OrgID, orgID),
		zap.String(logger.ConnectorID, string(cfg.ConnectorID)))
	return &connectorpb.GetSecretResponse{Token: token, ConnectorId: string(cfg.ConnectorID)}, nil
}

// GetConnectionStatus reports whether an org has a live grant for the connector.
func (s *Service) GetConnectionStatus(ctx context.Context, req *connectorpb.GetConnectionStatusRequest) (*connectorpb.GetConnectionStatusResponse, error) {
	if req.GetOrganizationId() == "" || req.GetConnectorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id and connector_id are required")
	}
	_, err := s.liveGrant(ctx, orgid.Canonical(req.GetOrganizationId()), req.GetConnectorId())
	if errors.Is(err, errGrantRevoked) {
		// Enabled, but its credential is dead: connected and unusable at once.
		return &connectorpb.GetConnectionStatusResponse{Connected: true, NeedsReconnect: true, Usable: false}, nil
	}
	if errors.Is(err, errNoLiveGrant) {
		return &connectorpb.GetConnectionStatusResponse{Connected: false}, nil
	}
	if err != nil {
		logger.Error(ctx, "GetConnectionStatus: resolve grant failed",
			zap.String(logger.OrgID, orgid.Canonical(req.GetOrganizationId())), zap.String(logger.ConnectorID, req.GetConnectorId()), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to resolve grant")
	}
	// Enabled and able to authenticate are independent: HealthState gates
	// scheduled syncs on its own, so an enabled grant with a dead credential is
	// connected and useless at once. Connected keeps its enablement-only meaning
	// so existing readers are unchanged; Usable is the combined answer a caller
	// asking "can this sync?" wants.
	// liveGrant already returned errNoLiveGrant for a missing or disabled row
	// and errGrantRevoked for an auth_revoked one, so reaching here means an
	// enabled, healthy grant exists.
	return &connectorpb.GetConnectionStatusResponse{Connected: true, Usable: true}, nil
}

// slackChannelSoftTTL is the freshness window: a cache read younger than this
// is returned as-is. A read past it still serves the cached list immediately
// (stale-while-revalidate) but also kicks off a background refresh.
const slackChannelSoftTTL = 15 * time.Minute

// slackChannelBackgroundFetchBudget bounds a single background-refresh
// pagination step (see refreshChannelsOnePage), not the time to fetch an
// entire workspace — pagination resumes across as many of these short steps
// as a workspace needs, so this only needs to be "long enough to make real
// progress," never "long enough to finish." A real workspace can exceed any
// one-shot budget under Slack's ~20/min Tier 2 rate limit, because a fixed
// budget can never be enough for an unbounded channel count.
const slackChannelBackgroundFetchBudget = 90 * time.Second

// slackChannelSyncFetchBudget bounds the synchronous fallback used only when
// ChannelCache is nil (tests that don't exercise caching — production always
// wires a real cache via DI and never takes this branch). Kept short and
// distinct from slackChannelBackgroundFetchBudget so that if this path were
// ever reached, it fails no slower than the background budget.
const slackChannelSyncFetchBudget = 45 * time.Second

// ListCommsSlackChannels lists the org's slack_comms workspace channels for a
// host's channel picker. This RPC never makes
// a live Slack call itself — it reads the cache and returns immediately,
// always. A cache miss, a stale cache, or an explicit force_refresh all
// trigger the same singleflight-locked background refresh
// (triggerBackgroundChannelRefresh) and return whatever's currently cached
// (possibly empty, possibly stale) plus syncing=true, so the caller can poll
// until it settles, so a cold cache never blocks the picker. A nil ChannelCache (tests
// that don't exercise it) disables caching — every call fetches live and
// synchronously, matching this RPC's pre-cache behavior.
func (s *Service) ListCommsSlackChannels(ctx context.Context, req *connectorpb.ListCommsSlackChannelsRequest) (*connectorpb.ListCommsSlackChannelsResponse, error) {
	orgID := orgid.Canonical(req.GetOrganizationId())
	if orgID == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}
	if s.SlackChannels == nil {
		return nil, status.Error(codes.Unimplemented, "slack channel listing is not configured")
	}

	if s.ChannelCache == nil {
		channels, err := s.fetchChannelsFromSlack(ctx, orgID, slackChannelSyncFetchBudget)
		if err != nil {
			return nil, fmt.Errorf("synchronous fetch: %w", err)
		}
		return toListCommsSlackChannelsResponse(channels, time.Now(), false), nil
	}

	channels, refreshedAt, found, err := s.ChannelCache.Get(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "ListCommsSlackChannels: cache read failed", zap.String(logger.OrgID, orgID), zap.Error(err))
	}
	fresh := found && err == nil && time.Since(refreshedAt) < slackChannelSoftTTL
	// An in-progress resume state means the last background-refresh attempt
	// hadn't finished paginating the whole workspace — the SERVED cache
	// (channels/refreshedAt above) is still the last fully-completed sync, not
	// a partial result, but this read must keep triggering follow-up attempts
	// (and keep telling the caller syncing:true) until that resync actually
	// finishes; freshness alone can't tell "nothing to do" apart from
	// "resuming in the background right now."
	_, _, _, resuming, resumeErr := s.ChannelCache.GetResumeState(ctx, orgID)
	if resumeErr != nil {
		// Fail closed, consistent with the cache-read error above (which
		// already drops `fresh` to false on its own read error): a false
		// "incomplete" costs one extra trigger/lock-check, but a false
		// "nothing to resume" can silently stall a real in-progress resync
		// for up to slackChannelSoftTTL, since progression here is entirely
		// poll-driven — nothing else re-triggers refreshChannelsOnePage.
		logger.Error(ctx, "ListCommsSlackChannels: resume-state read failed, treating as still incomplete",
			zap.String(logger.OrgID, orgID), zap.Error(resumeErr))
	}
	incomplete := resuming || resumeErr != nil
	triggered := !fresh || req.GetForceRefresh() || incomplete
	syncing := triggered
	if triggered {
		// A refresh may already be running for this org — a large workspace's
		// background fetch can take up to slackChannelBackgroundFetchBudget, and
		// every poll in that window would otherwise re-trigger this same check.
		// Skip it: triggerBackgroundChannelRefresh would just no-op on the held
		// lock anyway, so this only saves a redundant Postgres round trip.
		inFlight, lockErr := s.ChannelCache.IsRefreshInFlight(ctx, orgID)
		if lockErr != nil {
			logger.Error(ctx, "ListCommsSlackChannels: refresh-lock check failed, proceeding as not-in-flight",
				zap.String(logger.OrgID, orgID), zap.Error(lockErr))
		}
		if lockErr != nil || !inFlight {
			// The grant+token check is a cheap, single-row Postgres lookup —
			// checked synchronously only on this path (a miss, a stale read, or
			// an explicit force_refresh) so "not connected" still returns
			// FailedPrecondition immediately, exactly as before this RPC gained
			// caching. A fresh, non-forced hit skips this check entirely:
			// invalidateSlackChannelCache already deletes the cache entry the
			// moment a grant is disabled/purged (see
			// ReportGrantInvalid/PurgeOrgGrants), so a fresh hit implies the grant
			// was live as of that write. Only the Slack HTTP call itself (inside
			// fetchChannelsFromSlack, invoked from triggerBackgroundChannelRefresh)
			// is deferred to the background; without this check, a never-connected
			// org (no grant, or a grant with no token) would silently loop forever
			// on syncing:true instead of getting a connect prompt, since the
			// background goroutine's own check has nowhere to surface to the caller.
			if _, _, checkErr := s.checkGrantAndToken(ctx, orgID); checkErr != nil {
				// A definitive "not usable" signal (no grant, or a grant with no
				// token) always fails the request. Any other error — a transient
				// Postgres blip, not a real disconnect — instead falls back to
				// serving the stale cache when there is one, the same way the
				// cache-read error above doesn't fail the request either: a
				// request with real data to show should not hard-fail just
				// because a health check hiccuped.
				if !found || status.Code(checkErr) == codes.FailedPrecondition {
					return nil, checkErr
				}
				logger.Error(ctx, "ListCommsSlackChannels: grant/token check failed, serving stale cache",
					zap.String(logger.OrgID, orgID), zap.Error(checkErr))
			} else {
				s.triggerBackgroundChannelRefresh(ctx, orgID)
			}
		}
	} else {
		// A plain (non-triggering) read can still land while an earlier
		// force_refresh's own background fetch hasn't written its result yet:
		// that read's freshness math compares against the OLD cached
		// timestamp and says "fresh", which would wrongly report
		// syncing=false and stop the caller's poll before the new data
		// arrives. Peek the singleflight lock (never acquiring it) so the
		// response reflects an in-flight refresh even when this particular
		// read didn't trigger one. Best-effort: a lock-check error just falls
		// back to the freshness-only answer rather than failing the request.
		if inFlight, lockErr := s.ChannelCache.IsRefreshInFlight(ctx, orgID); lockErr != nil {
			logger.Error(ctx, "ListCommsSlackChannels: refresh-lock check failed, falling back to freshness-only",
				zap.String(logger.OrgID, orgID), zap.Error(lockErr))
		} else {
			syncing = inFlight
		}
	}
	return toListCommsSlackChannelsResponse(channels, refreshedAt, syncing), nil
}

// resolveLiveGrantOrStatus wraps liveGrant with the errNoLiveGrant→
// FailedPrecondition / other-err→Internal mapping every caller in this file
// needs, so that mapping lives in exactly one place.
func (s *Service) resolveLiveGrantOrStatus(ctx context.Context, orgID string) (*sdk.ConnectorConfig, error) {
	cfg, err := s.liveGrant(ctx, orgID, slackCommsConnectorID)
	if errors.Is(err, errNoLiveGrant) {
		return nil, status.Error(codes.FailedPrecondition, "no live notifications Slack grant")
	}
	if err != nil {
		logger.Error(ctx, "resolveLiveGrantOrStatus: resolve grant failed", zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to resolve grant")
	}
	return cfg, nil
}

// requireToken extracts cfg's bot token, mapping an empty result to the
// FailedPrecondition checkGrantAndToken's callers need — a live, enabled
// grant can still carry malformed/missing credentials.
func requireToken(ctx context.Context, cfg *sdk.ConnectorConfig) (string, error) {
	token := extractToken(cfg.Credentials)
	if token == "" {
		logger.Error(ctx, "requireToken: grant present but missing bot token", zap.String(logger.OrgID, cfg.OrganizationID))
		return "", status.Error(codes.FailedPrecondition, "grant has no token")
	}
	return token, nil
}

// checkGrantAndToken resolves orgID's live grant and its bot token together —
// every caller in this file needs both checks back-to-back before trusting a
// grant is actually usable, so pairing them here keeps that one operation
// from being reimplemented (or drifting) at each call site.
func (s *Service) checkGrantAndToken(ctx context.Context, orgID string) (*sdk.ConnectorConfig, string, error) {
	cfg, err := s.resolveLiveGrantOrStatus(ctx, orgID)
	if err != nil {
		return nil, "", err
	}
	token, err := requireToken(ctx, cfg)
	if err != nil {
		return nil, "", err
	}
	return cfg, token, nil
}

// fetchChannelsFromSlack resolves the org's slack_comms grant and fully paginates
// its channel list from Slack in one call, bounded by budget. Used only by
// ListCommsSlackChannels' nil-ChannelCache fallback (tests that don't
// exercise caching — production always wires a real cache and never takes
// this branch): that path has nowhere to persist resume state between calls,
// so it loops to completion itself rather than resuming across separate
// calls the way the real, cache-backed path does (see
// fetchChannelsWithToken).
func (s *Service) fetchChannelsFromSlack(ctx context.Context, orgID string, budget time.Duration) ([]*connectorpb.SlackChannel, error) {
	_, token, err := s.checkGrantAndToken(ctx, orgID)
	if err != nil {
		return nil, err
	}

	fetchCtx, cancel := context.WithTimeout(reqctx.WithOrgID(ctx, orgID), budget)
	defer cancel()
	var all []*connectorpb.SlackChannel
	cursor := ""
	for {
		channels, nextCursor, err := s.SlackChannels.ListChannels(fetchCtx, token, cursor)
		if err != nil {
			logger.Error(ctx, "fetchChannelsFromSlack: list channels failed", zap.String(logger.OrgID, orgID), zap.Error(err))
			slackChannelRefreshTotal.WithLabelValues(slackChannelRefreshResultError).Inc()
			return nil, status.Error(codes.Internal, "failed to list Slack channels")
		}
		all = append(all, channels...)
		if nextCursor == "" {
			break
		}
		// This path has no resume state to continue from later (unlike
		// fetchChannelsWithToken) — it must fully paginate in this one call
		// or fail outright. Falling through to a success return here would
		// silently report a truncated channel list as the complete
		// workspace: a fetch that looks successful but was cut off.
		if ctxErr := fetchCtx.Err(); ctxErr != nil {
			logger.Error(ctx, "fetchChannelsFromSlack: ran out of budget before pagination completed",
				zap.String(logger.OrgID, orgID), zap.Error(ctxErr))
			slackChannelRefreshTotal.WithLabelValues(slackChannelRefreshResultError).Inc()
			return nil, status.Error(codes.Internal, "failed to list Slack channels: timed out before completion")
		}
		cursor = nextCursor
	}
	slackChannelRefreshTotal.WithLabelValues(slackChannelRefreshResultSuccess).Inc()
	sort.SliceStable(all, func(i, j int) bool { return all[i].GetName() < all[j].GetName() })
	return all, nil
}

// fetchChannelsWithToken fetches one bounded page-range of orgID's Slack
// channel list using an already-resolved bot token, resuming from cursor.
// Unlike fetchChannelsFromSlack, this is a single resumable step, not a loop
// to completion — see SlackChannelLister.ListChannels and
// refreshChannelsOnePage for why. This is the only place production code
// calls Slack's conversations.list. On error, nextCursor is whatever
// ListChannels returned alongside it — callers must NOT treat that as
// meaningful completion state; only a nil error's nextCursor=="" means
// pagination actually finished (see ListChannels' own doc).
func (s *Service) fetchChannelsWithToken(ctx context.Context, orgID, token, cursor string, budget time.Duration) ([]*connectorpb.SlackChannel, string, error) {
	fetchCtx, cancel := context.WithTimeout(reqctx.WithOrgID(ctx, orgID), budget)
	defer cancel()
	channels, nextCursor, err := s.SlackChannels.ListChannels(fetchCtx, token, cursor)
	if err != nil {
		logger.Error(ctx, "fetchChannelsWithToken: list channels failed", zap.String(logger.OrgID, orgID), zap.Error(err))
		slackChannelRefreshTotal.WithLabelValues(slackChannelRefreshResultError).Inc()
		return channels, nextCursor, status.Error(codes.Internal, "failed to list Slack channels")
	}
	if nextCursor == "" {
		slackChannelRefreshTotal.WithLabelValues(slackChannelRefreshResultSuccess).Inc()
	} else {
		slackChannelRefreshTotal.WithLabelValues(slackChannelRefreshResultPartial).Inc()
	}
	return channels, nextCursor, nil
}

// triggerBackgroundChannelRefresh kicks off a best-effort background refresh of
// orgID's channel cache — on a cache miss, a stale cache, an explicit
// force_refresh against an otherwise-fresh cache, or a resume cursor left by
// a prior incomplete attempt. It derives a detached context from the
// caller's (context.WithoutCancel — keeps trace/log fields, drops the
// inbound request's cancellation, which would otherwise fire once this
// handler returns) and uses the cache's singleflight lock so concurrent
// callers across replicas don't spawn duplicate refreshes. Only a call that
// made no progress at all withholds release() (the lock's TTL is then the
// backoff before retrying — see SlackChannelCache.TryLockRefresh); any call
// that persisted real forward progress — whether it fully completed, ran out
// of budget cleanly, or hit an error partway through a page range — releases
// immediately so the very next poll can continue without waiting out a
// backoff a workspace that's actively making progress doesn't need.
func (s *Service) triggerBackgroundChannelRefresh(ctx context.Context, orgID string) {
	detached := context.WithoutCancel(ctx)
	release, ok, err := s.ChannelCache.TryLockRefresh(detached, orgID)
	if err != nil {
		logger.Error(detached, "triggerBackgroundChannelRefresh: lock acquire failed", zap.String(logger.OrgID, orgID), zap.Error(err))
		return
	}
	if !ok {
		return // another refresh is already in flight for this org
	}
	// goroutine.RunWithCtx is the panic-safe launcher (a bare `go func` would
	// crash the whole process on an unrecovered panic).
	goroutine.RunWithCtx(detached, func(ctx context.Context) {
		if err := s.refreshChannelsOnePage(ctx, orgID); err != nil {
			logger.Error(ctx, "background Slack channel refresh failed", zap.String(logger.OrgID, orgID), zap.Error(err))
			return
		}
		release()
	})
}

// refreshChannelsOnePage fetches and persists one bounded page-range of
// orgID's Slack channel list, resuming from any cursor a prior attempt left
// behind and merging with that attempt's own staged progress (see
// SlackChannelCache.GetResumeState) — never with the served cache directly.
// It deliberately never tries to finish an entire workspace in one call: for
// a workspace large enough that full pagination takes many minutes, no fixed
// per-call budget is ever "enough" for every workspace size, so a single call
// would time out however generous the budget. Instead, each call makes bounded, cheap progress;
// completion is however many of these short calls it takes, chained
// automatically by the client's own poll-while-syncing loop re-triggering
// the next one within seconds of the last finishing (see
// ListCommsSlackChannels' resume-state check).
//
// The served cache — what ListCommsSlackChannels actually returns — is only
// ever touched once pagination FULLY completes (nextCursor == "" with no
// error): every attempt in between updates only the separate staging state,
// so a resync in progress never shrinks what callers see back down to
// whatever one page range happens to hold; they keep seeing the last
// complete list until the new one is ready to fully replace it. This also
// makes the error path unambiguous even though the real SlackChannelLister
// can return an empty nextCursor alongside an error (the unchanged input
// cursor, for a fresh cycle) — that value is only ever stored as this org's
// staged resume position, never read as a completion signal, because the
// served-cache write sits behind a separate "fetchErr == nil" branch this
// function has already returned out of by the time nextCursor's emptiness
// would otherwise matter.
//
// The grant/token check and the pre-write liveness recheck each run in their
// OWN short-lived tenantdb.RunInScope, deliberately never wrapping the Slack
// network call itself: a Postgres transaction held open across a slow call
// gets killed by Postgres's own idle-in-transaction timeout before it can
// commit or roll back, turning an ordinary slow fetch into a confusing
// rollback-failure error instead of the fetch's own, more informative one.

// slackChannelMaxConsecutiveErrorsBeforeBackoff bounds how many attempts in a
// row may make partial progress and still release the singleflight lock for
// an immediate retry (see refreshChannelsOnePage). A degraded — not fully
// dead — Slack integration can keep succeeding on at least one page per call
// before failing on a later one every time; without this cap, that pattern
// would retry every few seconds forever, hammering an already-struggling
// vendor instead of ever falling back to the lock TTL's backoff. Chosen
// conservatively.
const slackChannelMaxConsecutiveErrorsBeforeBackoff = 3

// persistResumeState stages an in-progress resync's progress, but only when
// doing so is safe and meaningful: it is a no-op when there is nothing worth
// resuming from (a fresh cycle's first page failed with zero progress —
// writing cursor=="" would violate SetResumeState's own "only call while
// still incomplete" precondition and spuriously flip GetResumeState's found
// to true for a sync that never actually started), and it rechecks the grant
// the same way the served-cache write already does — both that it is still
// live AND that it is still the SAME grant this fetch actually ran under
// (expectedCredentialsUpdatedAt, fenced the same way MarkAuthRevoked fences
// its own write). The identity check matters because a reconnect never
// disables the old grant first: saveSlackCommsConnection overwrites the same
// row's credentials in place, so a mid-fetch reconnect — even to a different
// Slack workspace — leaves liveGrant alone reporting "still enabled" the
// whole time; only comparing CredentialsUpdatedAt catches that the row now
// describes a different session than the one this fetch's cursor and staged
// channels belong to. Skipping in either case (nil return, same as a
// successful stage — the caller only needs to know staging didn't fail, not
// which of the two happened) stops a fetch that outlived a disconnect or a
// reconnect from resurrecting a stale/foreign session's staged channels and
// cursor into a later resync — for the ordinary case: a real gap remains
// between this check and the SetResumeState write below, since neither is
// serialized against invalidateSlackChannelCache's own Delete (a purge/
// disconnect landing in that exact window can still have its Delete
// immediately undone by this write). That narrow window isn't unique to this
// path — writeChannelsIfGrantStillLive has the identical check-then-act gap
// for the served-cache write — and closing it for both would need a Redis
// CAS or a double-delete, not attempted here.
func (s *Service) persistResumeState(ctx context.Context, orgID string, channels []*connectorpb.SlackChannel, cursor string, consecutiveErrors int, expectedCredentialsUpdatedAt time.Time) error {
	if cursor == "" && len(channels) == 0 {
		return nil
	}
	return tenantdb.RunInScope(ctx, orgID, tenantdb.ScopeEnforced, func(sCtx context.Context) error { //nolint:wrapcheck // tenantdb.RunInScope's own error, already carries its cause; wrapping here would double-wrap
		cfg, err := s.liveGrant(sCtx, orgID, slackCommsConnectorID)
		if err != nil {
			if errors.Is(err, errNoLiveGrant) {
				return nil
			}
			return fmt.Errorf("recheck grant liveness before staging resume state: %w", err)
		}
		if !cfg.CredentialsUpdatedAt.Equal(expectedCredentialsUpdatedAt) {
			return nil
		}
		return s.ChannelCache.SetResumeState(sCtx, orgID, channels, cursor, consecutiveErrors)
	})
}

func (s *Service) refreshChannelsOnePage(ctx context.Context, orgID string) error {
	var token, resumeCursor string
	var stagedChannels []*connectorpb.SlackChannel
	var consecutiveErrors int
	var credentialsUpdatedAt time.Time
	if err := tenantdb.RunInScope(ctx, orgID, tenantdb.ScopeEnforced, func(sCtx context.Context) error {
		cfg, t, err := s.checkGrantAndToken(sCtx, orgID)
		if err != nil {
			return err
		}
		token = t
		credentialsUpdatedAt = cfg.CredentialsUpdatedAt
		channels, cursor, errCount, found, err := s.ChannelCache.GetResumeState(sCtx, orgID)
		if err != nil {
			// A genuine read failure (not "nothing to resume") must not be
			// treated as a fresh cycle — doing so would proceed to persist
			// this attempt's own (much smaller) progress over whatever
			// multi-page state this read merely failed to retrieve,
			// silently discarding real accumulated work over a transient
			// blip. Surface it instead, so the caller backs off and the
			// next attempt gets a clean read.
			return fmt.Errorf("read resume state: %w", err)
		}
		if found {
			resumeCursor = cursor
			stagedChannels = channels
			consecutiveErrors = errCount
		}
		return nil
	}); err != nil {
		return fmt.Errorf("resolve grant: %w", err)
	}

	newChannels, nextCursor, fetchErr := s.fetchChannelsWithToken(ctx, orgID, token, resumeCursor, slackChannelBackgroundFetchBudget)
	merged := dedupeChannelsByID(stagedChannels, newChannels)
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].GetName() < merged[j].GetName() })

	if fetchErr != nil {
		consecutiveErrors++
		// nextCursor is safe to persist here even though it came back
		// alongside an error: the served cache is only ever written a few
		// lines below, behind an explicit "fetchErr == nil" gate this
		// function already returned out of — so an ambiguous or unchanged
		// nextCursor on this path can never be mistaken for completion by
		// this function, regardless of its value. It's also strictly better
		// than falling back to resumeCursor: SlackChannelLister.ListChannels
		// returns progress made on any pages that succeeded before the
		// failing one, so nextCursor may already be ahead of resumeCursor —
		// discarding that would mean needlessly re-fetching those pages next
		// attempt.
		if writeErr := s.persistResumeState(ctx, orgID, merged, nextCursor, consecutiveErrors, credentialsUpdatedAt); writeErr != nil {
			return fmt.Errorf("background fetch: %w; persisting partial progress also failed: %w", fetchErr, writeErr) //nolint:wrapcheck // combined-error terminal path; both causes must be visible in the log line
		}
		if len(newChannels) > 0 && consecutiveErrors < slackChannelMaxConsecutiveErrorsBeforeBackoff {
			// Real progress was made and safely staged despite the error
			// (already logged inside fetchChannelsWithToken), and this
			// hasn't happened too many times in a row — return nil so
			// triggerBackgroundChannelRefresh releases the lock and the next
			// poll can retry promptly, instead of waiting out the backoff
			// meant for an org making no progress at all. Once the
			// consecutive-error streak crosses the threshold, fall through
			// to the backoff path below even though progress was made — a
			// degraded vendor gets the same relief a fully dead one does.
			return nil
		}
		logger.Error(ctx, "refreshChannelsOnePage: backing off after consecutive errors",
			zap.String(logger.OrgID, orgID), zap.Int("consecutive_errors", consecutiveErrors))
		return fmt.Errorf("background fetch: %w", fetchErr)
	}

	if nextCursor == "" {
		// Pagination completed — the ONLY place the served cache gets
		// updated, and only from a fully-merged, error-free result.
		return tenantdb.RunInScope(ctx, orgID, tenantdb.ScopeEnforced, func(sCtx context.Context) error { //nolint:wrapcheck // tenantdb scope primitive: passes the closure error through transparently so status codes / caller context survive; wrapping would corrupt them
			if err := s.writeChannelsIfGrantStillLive(sCtx, orgID, merged, credentialsUpdatedAt); err != nil {
				return err //nolint:wrapcheck // tenantdb.RunInScope's own error, already carries its cause; wrapping here would double-wrap
			}
			// The served cache — the authoritative result — is already
			// written and correct at this point. A failure to clear the now-
			// stale resume-state key is a much smaller, self-healing defect
			// (one redundant resume cycle re-fetching data that's already
			// served) than treating it as a failed refresh: doing that would
			// withhold the singleflight lock release and keep reporting
			// syncing:true off a stale resume-state key despite the served
			// cache already being complete. So it's logged, not propagated.
			if err := s.ChannelCache.ClearResumeState(sCtx, orgID); err != nil {
				logger.Error(sCtx, "refreshChannelsOnePage: clear resume state failed after successful sync",
					zap.String(logger.OrgID, orgID), zap.Error(err))
			}
			return nil
		})
	}

	// A clean, error-free step (even if still incomplete) resets the
	// consecutive-error streak — only actual fetch errors, not ordinary
	// budget-bounded progress, count toward the backoff escalation above.
	return s.persistResumeState(ctx, orgID, merged, nextCursor, 0, credentialsUpdatedAt) //nolint:wrapcheck // persistResumeState's own error already carries its cause (RunInScope or the liveness recheck); wrapping here would double-wrap
}

// dedupeChannelsByID merges channel lists, keeping the LAST occurrence of
// each channel ID — a safety net against duplicate entries if a resumed
// cycle's staged-progress read and write ever raced or retried for the same
// page (the two are separate, non-atomic Redis calls).
func dedupeChannelsByID(lists ...[]*connectorpb.SlackChannel) []*connectorpb.SlackChannel {
	seen := make(map[string]*connectorpb.SlackChannel)
	order := make([]string, 0)
	for _, list := range lists {
		for _, ch := range list {
			id := ch.GetId()
			if _, ok := seen[id]; !ok {
				order = append(order, id)
			}
			seen[id] = ch
		}
	}
	out := make([]*connectorpb.SlackChannel, 0, len(order))
	for _, id := range order {
		out = append(out, seen[id])
	}
	return out
}

// writeChannelsIfGrantStillLive writes the fully-paginated channel list to
// the served cache, unless orgID's grant is now definitively gone, or is no
// longer the SAME grant this fetch actually ran under — closing the window
// where a fetch outlives a mid-flight ReportGrantInvalid/PurgeOrgGrants (a
// confirmed disconnect) or a mid-flight reconnect (saveSlackCommsConnection
// overwrites the same row's credentials in place, so a reconnect never shows
// up as errNoLiveGrant — only expectedCredentialsUpdatedAt, captured by the
// caller before the fetch started, catches it) and would otherwise resurrect
// a disconnected or foreign session's channel list into the served cache.
// Any OTHER liveness-check error is transient (e.g. a DB blip), not evidence
// the grant changed, so it must not discard a completed fetch. Callers must
// only call this once pagination has fully completed (see
// refreshChannelsOnePage) — an incomplete result belongs in
// SlackChannelCache's resume state, never here.
func (s *Service) writeChannelsIfGrantStillLive(ctx context.Context, orgID string, channels []*connectorpb.SlackChannel, expectedCredentialsUpdatedAt time.Time) error {
	cfg, err := s.liveGrant(ctx, orgID, slackCommsConnectorID)
	if errors.Is(err, errNoLiveGrant) {
		return fmt.Errorf("grant became invalid during refresh: %w", err)
	}
	if err == nil && !cfg.CredentialsUpdatedAt.Equal(expectedCredentialsUpdatedAt) {
		return errors.New("grant reconnected during refresh: stale session")
	}
	if err := s.ChannelCache.Set(ctx, orgID, channels, time.Now()); err != nil {
		return fmt.Errorf("write slack channel cache: %w", err)
	}
	return nil
}

func toListCommsSlackChannelsResponse(channels []*connectorpb.SlackChannel, refreshedAt time.Time, syncing bool) *connectorpb.ListCommsSlackChannelsResponse {
	resp := &connectorpb.ListCommsSlackChannelsResponse{Channels: channels, Syncing: syncing}
	if !refreshedAt.IsZero() {
		resp.RefreshedAt = timestamppb.New(refreshedAt)
	}
	return resp
}

// ReportGrantInvalid disables the named grant when a send proved its token dead.
// The reason is recorded; the token is never in the request or the log. A
// grant that does not exist answers NotFound; an already disabled one is a
// no-op.
func (s *Service) ReportGrantInvalid(ctx context.Context, req *connectorpb.ReportGrantInvalidRequest) (*connectorpb.ReportGrantInvalidResponse, error) {
	if req.GetOrganizationId() == "" || req.GetConnectorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id and connector_id are required")
	}
	cfg, err := s.ConfigDAO.Get(ctx, orgid.Canonical(req.GetOrganizationId()), sdk.ConnectorID(req.GetConnectorId()))
	if store.IsNotFound(err) || (err == nil && cfg == nil) {
		return nil, status.Error(codes.NotFound, "no grant for that connector")
	}
	if err != nil {
		logger.Error(ctx, "ReportGrantInvalid: load grant failed",
			zap.String(logger.OrgID, orgid.Canonical(req.GetOrganizationId())), zap.String(logger.ConnectorID, req.GetConnectorId()), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to load grant")
	}
	if !cfg.Enabled {
		// Already disabled — idempotent no-op.
		return &connectorpb.ReportGrantInvalidResponse{}, nil
	}
	// Only enabled is written, and only while the row still carries the
	// credentials read above: a reconnect that landed since stands.
	disabled, err := s.ConfigDAO.DisableIfCredentialsUnchanged(ctx, orgid.Canonical(req.GetOrganizationId()), sdk.ConnectorID(req.GetConnectorId()), cfg.CredentialsUpdatedAt)
	if err != nil {
		logger.Error(ctx, "ReportGrantInvalid: disable grant failed",
			zap.String(logger.OrgID, orgid.Canonical(req.GetOrganizationId())), zap.String(logger.ConnectorID, req.GetConnectorId()), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to disable grant")
	}
	if !disabled {
		logger.Info(ctx, "ReportGrantInvalid: the grant was rewritten since the failed send; leaving the new grant enabled",
			zap.String(logger.OrgID, orgid.Canonical(req.GetOrganizationId())), zap.String(logger.ConnectorID, req.GetConnectorId()))
		return &connectorpb.ReportGrantInvalidResponse{}, nil
	}
	logger.WarnWithCtx(ctx, "connector grant reported invalid; disabled, reconnect required",
		zap.String(logger.OrgID, orgid.Canonical(req.GetOrganizationId())),
		zap.String(logger.ConnectorID, req.GetConnectorId()),
		zap.String("reason", req.GetReason()))
	s.invalidateSlackChannelCache(ctx, orgid.Canonical(req.GetOrganizationId()), req.GetConnectorId())
	return &connectorpb.ReportGrantInvalidResponse{}, nil
}

// invalidateSlackChannelCache best-effort clears the org's cached Slack channel
// list when its slack_comms grant is disabled or purged, so a disconnected org
// isn't served a stale channel list for the remainder of the hard TTL. Cache
// invalidation is best-effort — a failure is logged, never surfaced, since the
// caller's primary action (disabling/deleting the grant) has already succeeded.
func (s *Service) invalidateSlackChannelCache(ctx context.Context, orgID, connectorID string) {
	if s.ChannelCache == nil || connectorID != slackCommsConnectorID {
		return
	}
	if err := s.ChannelCache.Delete(ctx, orgID); err != nil {
		logger.Error(ctx, "invalidateSlackChannelCache: cache delete failed", zap.String(logger.OrgID, orgID), zap.Error(err))
	}
}

// PurgeOrgGrants deletes an org's grant for a connector, so an organization
// whose data is removed leaves no live bot token. System-scoped. A grant that
// does not exist answers NotFound, so a caller can tell "deleted" from
// "nothing was there".
func (s *Service) PurgeOrgGrants(ctx context.Context, req *connectorpb.PurgeOrgGrantsRequest) (*connectorpb.PurgeOrgGrantsResponse, error) {
	if req.GetOrganizationId() == "" || req.GetConnectorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id and connector_id are required")
	}
	connID := sdk.ConnectorID(req.GetConnectorId())
	// Look up first so the response carries an honest deleted_count and the delete
	// stays an idempotent no-op when there's nothing to purge.
	cfg, err := s.ConfigDAO.Get(ctx, orgid.Canonical(req.GetOrganizationId()), connID)
	if store.IsNotFound(err) || (err == nil && cfg == nil) {
		return nil, status.Error(codes.NotFound, "no grant for that connector")
	}
	if err != nil {
		logger.Error(ctx, "PurgeOrgGrants: load grant failed",
			zap.String(logger.OrgID, orgid.Canonical(req.GetOrganizationId())), zap.String(logger.ConnectorID, req.GetConnectorId()), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to purge grants")
	}
	if err := s.ConfigDAO.Delete(ctx, orgid.Canonical(req.GetOrganizationId()), connID); err != nil {
		logger.Error(ctx, "PurgeOrgGrants: delete failed",
			zap.String(logger.OrgID, orgid.Canonical(req.GetOrganizationId())), zap.String(logger.ConnectorID, req.GetConnectorId()), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to purge grants")
	}
	logger.Info(ctx, "connector PurgeOrgGrants",
		zap.String(logger.OrgID, orgid.Canonical(req.GetOrganizationId())), zap.String(logger.ConnectorID, req.GetConnectorId()), zap.Int64("deleted_count", 1))
	s.invalidateSlackChannelCache(ctx, orgid.Canonical(req.GetOrganizationId()), req.GetConnectorId())
	return &connectorpb.PurgeOrgGrantsResponse{DeletedCount: 1}, nil
}

// extractToken pulls the bot token out of a grant's credentials JSON, accepting
// the common "bot_token" (Slack) or "token" key. Returns "" if neither is present.
func extractToken(creds json.RawMessage) string {
	if len(creds) == 0 {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(creds, &m); err != nil {
		// Malformed credentials blob is a data-integrity anomaly; surface it so a
		// resulting empty token isn't misread as "no token configured".
		logger.ErrorNoCtx("extractToken: unmarshal credentials failed", zap.Error(err))
		return ""
	}
	for _, k := range []string{"bot_token", "token", "access_token"} {
		if v, ok := m[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}
