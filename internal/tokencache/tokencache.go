// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package tokencache forgets an org's cached vendor access token. The OAuth
// gateways prefer a cached access token over the stored credentials, so every
// credential write must forget it, or the next request still carries the
// replaced account's token until the cache entry expires.
package tokencache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Key returns the cache key the connector's gateway reads for orgID, and false
// for a connector whose gateway caches no access token.
func Key(connectorID sdk.ConnectorID, orgID string) (string, bool) {
	switch connectorID {
	case "jira":
		return jiraconfig.AccessTokenKey(orgID), true
	case "gitlab":
		return glconfig.AccessTokenKey(orgID), true
	case "bitbucket":
		return bbconfig.AccessTokenKey(orgID), true
	case "linear":
		return linearconfig.AccessTokenKey(orgID), true
	default:
		return "", false
	}
}

// Forget deletes the connector's cached access token for orgID. It is a no-op
// for a connector with no token cache or a nil client.
func Forget(ctx context.Context, rdb redis.Cmdable, connectorID sdk.ConnectorID, orgID string) error {
	key, ok := Key(connectorID, orgID)
	if !ok || rdb == nil {
		return nil
	}
	if err := rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("forget %s access token: %w", connectorID, err)
	}
	return nil
}

// ForgetIn is Forget for a runner whose Redis keys carry namespace ns
// ("ns:<key>"; see TETRIZ_NAMESPACE). An empty ns is Forget.
func ForgetIn(ctx context.Context, rdb redis.Cmdable, ns string, connectorID sdk.ConnectorID, orgID string) error {
	key, ok := Key(connectorID, orgID)
	if !ok || rdb == nil {
		return nil
	}
	if ns != "" {
		key = ns + ":" + key
	}
	if err := rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("forget %s access token: %w", connectorID, err)
	}
	return nil
}

// ForgetFromCache is Forget for any auth.TokenCache, such as the in-process
// auth/memory.Store a runner with no Redis gives its gateways. It is a no-op
// for a connector with no token cache or a nil cache.
func ForgetFromCache(ctx context.Context, cache auth.TokenCache, connectorID sdk.ConnectorID, orgID string) error {
	key, ok := Key(connectorID, orgID)
	if !ok || cache == nil {
		return nil
	}
	if err := cache.Delete(ctx, key); err != nil {
		return fmt.Errorf("forget %s access token: %w", connectorID, err)
	}
	return nil
}
