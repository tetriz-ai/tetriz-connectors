// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package auth_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/memory"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

type reconnectOnGet struct {
	fence time.Time
	onGet func()
}

func (r *reconnectOnGet) Get(context.Context, string, sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	cfg := &sdk.ConnectorConfig{CredentialsUpdatedAt: r.fence}
	if f := r.onGet; f != nil {
		r.onGet = nil
		f()
	}
	return cfg, nil
}
func (r *reconnectOnGet) Save(context.Context, *sdk.ConnectorConfig) error { return nil }
func (r *reconnectOnGet) CompareAndSwapCredentials(context.Context, string, sdk.ConnectorID, time.Time, json.RawMessage) (bool, error) {
	return false, nil
}

// The cache entry is set before the fence re-read. A reconnect that writes
// and forgets the cache right after the re-read saw the old fence therefore
// forgets the entry, and the old account's token is not left cached.
func TestCacheIsSetBeforeTheFenceReRead(t *testing.T) {
	fence := time.Unix(100, 0)
	cache := &memory.Store{}
	cs := &reconnectOnGet{fence: fence}
	cs.onGet = func() { cs.fence = time.Unix(200, 0); _ = cache.Delete(context.Background(), "k") }
	_, _ = auth.CacheIfCredentialsUnchanged(context.Background(), store.NoopTxRunner{}, cs, cache, "o", "jira", fence, "k", "old-account", time.Hour)
	if v, err := cache.Get(context.Background(), "k"); err == nil {
		t.Fatalf("old account's token cached after the reconnect forgot it: %q", v)
	}
}
