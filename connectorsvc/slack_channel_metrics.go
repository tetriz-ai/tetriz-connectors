// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	slackChannelRefreshResultSuccess = "success"
	slackChannelRefreshResultError   = "error"
	// slackChannelRefreshResultPartial is a bounded resumable-pagination step
	// that made progress but hasn't finished the workspace yet (no error,
	// non-empty next cursor) — the common case for any workspace too large to
	// paginate within one call's budget. Without this label, that majority of
	// real Slack API call volume would be invisible on the success/error
	// counter, making a stalled or very slow resync look identical to no
	// traffic at all.
	slackChannelRefreshResultPartial = "partial"
)

// slackChannelRefreshTotal counts every attempt to populate the Slack
// notifications channel-list cache from Slack (cold start, forced refresh, or
// background refresh), by outcome. A sustained non-zero "error" rate means
// Slack access is broadly broken for the slack_comms connector's app
// (token/scope/API outage), which leaves a host's channel picker empty.
// A sustained "partial" rate with no
// matching rise in "success" for the same org means that org's resync isn't
// converging.
var slackChannelRefreshTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_slack_channel_refresh_total",
		Help: "Count of Slack notifications channel-list cache refresh attempts, by result (success/error).",
	},
	[]string{"result"},
)

// slackChannelPaginationIncompleteTotal counts a Slack conversations.list
// fetch that exhausted the page-count safety backstop before Slack signalled
// completion (a non-empty next_cursor after the last page). Expected to stay
// at zero; a non-zero count means a fetch stopped short instead of silently
// truncating the channel list.
var slackChannelPaginationIncompleteTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_slack_channel_pagination_incomplete_total",
		Help: "Count of Slack conversations.list fetches that hit the page-count safety backstop before completion. Should always be zero.",
	},
	[]string{"org_id"},
)

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import. A host that runs the broker registers them on its own registry;
// app.RegisterMetrics does not include them.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		slackChannelRefreshTotal,
		slackChannelPaginationIncompleteTotal,
	}
}
