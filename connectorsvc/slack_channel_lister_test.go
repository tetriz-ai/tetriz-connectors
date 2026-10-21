// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	rlstore "github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/store"
)

// NewSlackChannelLister's lister pages conversations.list against the
// configured Slack endpoint with the given bot token and returns the channels.
func TestNewSlackChannelListerListsChannels(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channels":[{"id":"C1","name":"general","is_private":false}],"response_metadata":{"next_cursor":""}}`))
	}))
	t.Cleanup(srv.Close)
	cfg := gateway.DefaultConfig()
	for i := range cfg.Slack {
		cfg.Slack[i].BaseURL = srv.URL
	}
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	l := NewSlackChannelLister(rlstore.NewSlidingWindowLogWithRedis(rc))
	channels, next, err := l.ListChannels(context.Background(), "xoxb-test-1", "")
	require.NoError(t, err)
	assert.Empty(t, next, "pagination completed")
	require.Len(t, channels, 1)
	assert.Equal(t, "C1", channels[0].GetId())
	assert.Equal(t, "Bearer xoxb-test-1", gotAuth)
}
