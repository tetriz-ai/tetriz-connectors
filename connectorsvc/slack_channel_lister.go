// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	slackgateway "github.com/tetriz-ai/tetriz-connectors/internal/gateway/slack"
	vgconnectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// slackChannelJSON is the subset of a Slack conversations.list channel the
// notifications channel picker needs.
type slackChannelJSON struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IsPrivate bool   `json:"is_private"`
}

// SlackChannelLister lists a workspace's channels given a bot token, returning the
// connector SlackChannel protos, unsorted. token is the slack_comms grant's bot
// token, which the Service resolves; it never leaves the call.
type SlackChannelLister interface {
	// ListChannels pages conversations.list starting from cursor (empty
	// starts from the beginning), stopping either when Slack signals
	// completion or when ctx is done — whichever comes first. It returns the
	// channels fetched by THIS call only (the caller merges them with any
	// channels a prior, incomplete call already gathered) and the cursor to
	// resume from. Running out of ctx mid-fetch is not itself an error: the
	// channels gathered so far and a resumable cursor are still returned, so
	// no completed page is ever thrown away — a workspace too large to
	// finish in one call just continues across however many calls it takes,
	// instead of needing a single call's budget to cover the whole
	// workspace.
	//
	// nextCursor is meaningful ONLY when err == nil: an empty nextCursor then
	// means pagination genuinely completed. When err != nil, nextCursor is
	// whatever cursor a retry should use. NewSlackChannelLister's lister returns
	// the cursor it was given, which for a fresh cycle (cursor == "") is itself
	// empty. Callers must never read an empty
	// nextCursor as "completed" on an error return — err != nil always means
	// "still incomplete, resume from nextCursor," regardless of its value.
	ListChannels(ctx context.Context, token, cursor string) (channels []*connectorpb.SlackChannel, nextCursor string, err error)
}

// gatewaySlackChannelLister is the production SlackChannelLister: it builds
// a token-bound Slack gateway per call and pages conversations.list. The
// shared HTTP doer + rate-limit store are captured once; the per-org token is
// bound per call (it never persists the token).
type gatewaySlackChannelLister struct {
	doer    vendorapi.HTTPDoer
	rlStore ratelimiter.Store
}

// httpDoer adapts *http.Client to vendorapi.HTTPDoer, threading ctx onto the req.
type httpDoer struct{ c *http.Client }

func (d httpDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	//#nosec G704 -- thin HTTPDoer adapter; req URL is built upstream against configured vendor endpoints, not untrusted input
	resp, err := d.c.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("executing http request: %w", err)
	}
	return resp, nil
}

// NewSlackChannelLister returns a lister over a 30s HTTP client and the Redis
// rate-limit store.
func NewSlackChannelLister(rlStore ratelimiter.Store) SlackChannelLister {
	return &gatewaySlackChannelLister{
		doer:    httpDoer{c: &http.Client{Timeout: 30 * time.Second}},
		rlStore: rlStore,
	}
}

// maxChannelPages bounds a single ListChannels call purely as an
// infinite-loop guard, not as the real limit on total pagination — a
// workspace needing more pages than one call's ctx budget allows simply
// resumes across further calls (see the interface doc). Hitting this
// backstop before either the context expiring or Slack signalling completion
// (a non-empty next_cursor after the last page) is always a bug, never an
// expected outcome, so it returns an error rather than a silently truncated
// "success" list. A var, not a const, so tests can shrink it to exercise the backstop
// path cheaply.
var maxChannelPages = 2000

// canResumeFrom reports whether a resumable ("no error, resume from cursor")
// budget-exhaustion return is safe: only when there is a real cursor or real
// gathered channels for the caller to resume from. Without either, that
// return would be (nil, "", nil) — indistinguishable from "pagination
// genuinely completed, zero channels" per this interface's own contract.
func canResumeFrom(cursor string, out []*connectorpb.SlackChannel) bool {
	return cursor != "" || len(out) > 0
}

// isBudgetExhaustion reports whether err is (or wraps) ctx's own
// cancellation/deadline error — as opposed to a real vendor error that
// merely happens to land after ctx has separately expired. Testing the
// ambient ctx.Err() state alone can't tell those apart: a permanent Slack
// error (revoked token, malformed request) returned by fetchConversationsPageWithRetry's
// final attempt is exactly as likely to land after the budget has run out as
// a genuine timeout is, since the retry helper's own backoff/Retry-After
// waits consume a large share of that same budget. Misclassifying the
// former as benign would silently defeat slackChannelMaxConsecutiveErrorsBeforeBackoff's
// circuit breaker and suppress the error log a dead integration needs.
func isBudgetExhaustion(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// ListChannels pages conversations.list (public channels plus the private channels
// the bot is a member of) starting from cursor, returning the channel records
// (unsorted; the caller orders them) gathered before ctx ran out or Slack
// signalled completion. A Slack ok:false surfaces as an error so a caller
// reports a failure rather than a silently-empty picker — but even on error,
// whatever channels were gathered on already-succeeded pages are still
// returned alongside it, since that work is real and shouldn't be discarded.
func (l *gatewaySlackChannelLister) ListChannels(ctx context.Context, token, cursor string) ([]*connectorpb.SlackChannel, string, error) {
	gw, err := slackgateway.NewSlackVendorGateway(l.doer, token, l.rlStore)
	if err != nil {
		return nil, cursor, fmt.Errorf("build slack gateway: %w", err)
	}
	scoped := gw.ForGroup(constants.GroupSlackWeb)

	var out []*connectorpb.SlackChannel
	for pages := 0; pages < maxChannelPages; pages++ {
		// Out of budget for this call, not a Slack error — return what was
		// gathered plus the cursor to resume from on the next call, PROVIDED
		// there is actually a cursor or gathered channels to resume from
		// (canResume below). Not logged: running out of budget mid-page is
		// the normal, expected way a large workspace's fetch ends, not a
		// failure worth a log line on every resumable step.
		if ctx.Err() != nil && canResumeFrom(cursor, out) {
			return out, cursor, nil
		}
		resp, err := fetchConversationsPageWithRetry(ctx, scoped, &vgconnectorpb.SlackListConversationsRequest{
			// public_channel + private_channel: private ones are returned only when
			// the slack_comms bot is a member.
			Types:           "public_channel,private_channel",
			ExcludeArchived: true,
			Limit:           200,
			Cursor:          cursor,
		})
		if err != nil {
			if isBudgetExhaustion(err) && canResumeFrom(cursor, out) {
				// The budget ran out while this page was in flight or while
				// honoring a Slack Retry-After wait — not a Slack error, same
				// as the top-of-loop check above: return what was gathered
				// plus the cursor to resume from, rather than counting an
				// ordinary budget-bounded step toward the degraded-vendor
				// backoff. err itself is discarded deliberately: it is only
				// ctx's own deadline surfacing through the retry helper, not
				// new information beyond what ctx.Err() already says.
				//
				// canResumeFrom guards this case: on a fresh cycle (cursor=="")
				// whose very first page never completes even once (out is
				// still empty), returning (nil, "", nil) here would be
				// byte-for-byte the same signature this interface reserves
				// for "pagination genuinely completed, zero channels" — a
				// caller has no way to tell "the workspace is empty" apart
				// from "budget ran out before any page ever succeeded". With
				// nothing to resume from, this must fall through to a real
				// error instead, so the caller backs off rather than writing
				// an empty result as if it were a complete sync.
				return out, cursor, nil
			}
			return out, cursor, fmt.Errorf("fetch conversations page: %w", err)
		}
		if !resp.GetOk() {
			return out, cursor, fmt.Errorf("slack API error: %s", resp.GetError())
		}
		for _, raw := range resp.GetChannels() {
			var ch slackChannelJSON
			if err := json.Unmarshal(raw, &ch); err != nil {
				// Skip an unparseable channel rather than failing the whole list,
				// but log it so a systematic decode break isn't silently invisible.
				logger.WarnWithCtx(ctx, "ListChannels: skipping unparseable slack channel", zap.Error(err))
				continue
			}
			if ch.ID == "" {
				continue
			}
			out = append(out, &connectorpb.SlackChannel{Id: ch.ID, Name: ch.Name, IsPrivate: ch.IsPrivate})
		}
		cursor = resp.GetNextCursor()
		if cursor == "" {
			return out, "", nil
		}
	}

	slackChannelPaginationIncompleteTotal.WithLabelValues(reqctx.OrgIDFromContext(ctx)).Inc()
	logger.Error(ctx, "ListChannels: hit page-count safety backstop before Slack signalled completion",
		zap.Int("channels_so_far", len(out)), zap.Int("max_pages", maxChannelPages))
	return out, cursor, fmt.Errorf("list slack channels: exceeded %d pages without completion", maxChannelPages)
}

// fetchConversationsPageWithRetry fetches one conversations.list page,
// retrying on failure up to a fixed attempt cap. On a rate limit, it honors
// the vendor's own Retry-After hint — populated generically by
// vendorapi.ClassifyHTTPErrorWithHeaders onto sdk.SyncError.RetryAfterSeconds
// for every vendor gateway, Slack included — instead of guessing via blind
// exponential backoff; other errors fall back to a short exponential backoff.
// Bounded by ctx so a stuck page can't outlive the caller's deadline.
func fetchConversationsPageWithRetry(ctx context.Context, gw sdk.VendorGateway, req *vgconnectorpb.SlackListConversationsRequest) (*vgconnectorpb.SlackListConversationsResponse, error) {
	const maxAttempts = 4
	backoff := 500 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			wait := backoff
			var syncErr *sdk.SyncError
			if errors.As(lastErr, &syncErr) && syncErr.RetryAfterSeconds > 0 {
				wait = sdk.SecondsToDuration(syncErr.RetryAfterSeconds)
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, fmt.Errorf("conversations.list: %w", ctx.Err())
			}
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
		respMsg, err := gw.Execute(ctx, req)
		if err == nil {
			resp, ok := respMsg.(*vgconnectorpb.SlackListConversationsResponse)
			if !ok {
				return nil, fmt.Errorf("conversations.list: unexpected response type %T", respMsg)
			}
			return resp, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("conversations.list: %w", lastErr)
}
