// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package tokencache_test

import (
	"context"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/memory"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
	"github.com/tetriz-ai/tetriz-connectors/internal/tokencache"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// This package had no tests, so it carries its own guard.
func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

// Forget removes exactly the one organization's cached token for every
// connector that caches one, leaves other organizations' tokens alone, and
// is a no-op for a connector that caches nothing or without Redis.
func TestEdge_ForgetIsScopedToOneOrganization(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	for _, id := range []sdk.ConnectorID{"jira", "gitlab", "bitbucket", "linear"} {
		keyA, ok := tokencache.Key(id, "org-a")
		require.True(t, ok, id)
		keyB, _ := tokencache.Key(id, "org-b")
		require.NotEqual(t, keyA, keyB, id)
		require.NoError(t, mr.Set(keyA, "a"))
		require.NoError(t, mr.Set(keyB, "b"))

		require.NoError(t, tokencache.Forget(ctx, rc, id, "org-a"))
		assert.False(t, mr.Exists(keyA), id)
		assert.True(t, mr.Exists(keyB), "%s: another organization's token was forgotten", id)
	}

	_, ok := tokencache.Key("github", "org-a")
	assert.False(t, ok)
	assert.NoError(t, tokencache.Forget(ctx, rc, "github", "org-a"))
	assert.NoError(t, tokencache.Forget(ctx, nil, "jira", "org-a"))

	// An org id that is a prefix of another names a different key.
	k1, _ := tokencache.Key("jira", "org")
	k2, _ := tokencache.Key("jira", "org-a")
	assert.NotEqual(t, k1, k2)
}

// ForgetFromCache does the same on any auth.TokenCache, here the in-process one.
func TestForgetFromCacheIsScopedToOneOrganization(t *testing.T) {
	ctx := context.Background()
	cache := memory.New()
	for _, id := range []sdk.ConnectorID{"jira", "gitlab", "bitbucket", "linear"} {
		keyA, _ := tokencache.Key(id, "org-a")
		keyB, _ := tokencache.Key(id, "org-b")
		require.NoError(t, cache.Set(ctx, keyA, "a", 0))
		require.NoError(t, cache.Set(ctx, keyB, "b", 0))

		require.NoError(t, tokencache.ForgetFromCache(ctx, cache, id, "org-a"))
		_, err := cache.Get(ctx, keyA)
		assert.ErrorIs(t, err, auth.ErrCacheMiss, id)
		v, err := cache.Get(ctx, keyB)
		require.NoError(t, err, id)
		assert.Equal(t, "b", v)
	}
	assert.NoError(t, tokencache.ForgetFromCache(ctx, cache, "github", "org-a"))
	assert.NoError(t, tokencache.ForgetFromCache(ctx, nil, "jira", "org-a"))
}
