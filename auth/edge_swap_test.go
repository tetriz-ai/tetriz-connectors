// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package auth_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/memory"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// swapStore records the context each call ran under and swaps when the
// expected fence matches.
type swapStore struct {
	fence    time.Time
	getErr   error
	swapCtx  error
	creds    json.RawMessage
	newFence time.Time
}

func (s *swapStore) Get(context.Context, string, sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &sdk.ConnectorConfig{CredentialsUpdatedAt: s.fence, Credentials: s.creds}, nil
}
func (s *swapStore) Save(context.Context, *sdk.ConnectorConfig) error { return nil }
func (s *swapStore) CompareAndSwapCredentials(ctx context.Context, _ string, _ sdk.ConnectorID, expected time.Time, c json.RawMessage) (bool, error) {
	s.swapCtx = ctx.Err()
	if !expected.Equal(s.fence) {
		return false, nil
	}
	s.creds, s.fence = c, s.newFence
	return true, nil
}

// The vendor has already rotated the refresh token when the swap runs, so a
// caller whose context is already cancelled must still persist it, and the
// returned fence is the one the swap wrote.
func TestEdge_SwapPersistsUnderACancelledCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st := &swapStore{fence: time.Unix(100, 0), newFence: time.Unix(200, 0)}
	swapped, fence, err := auth.SwapRefreshedCredentials(ctx, store.NoopTxRunner{}, st, "o", "jira", time.Unix(100, 0), json.RawMessage(`{"r":"rotated"}`))
	if err != nil || !swapped {
		t.Fatalf("swap under a cancelled caller context: swapped=%v err=%v", swapped, err)
	}
	if st.swapCtx != nil {
		t.Fatalf("swap ran on a cancelled context: %v", st.swapCtx)
	}
	if !fence.Equal(time.Unix(200, 0)) {
		t.Fatalf("fence = %v, want the swap's own write", fence)
	}
}

// A swap against a stale fence (a reconnect landed) reports false and leaves
// the stored credentials alone.
func TestEdge_SwapAgainstStaleFenceLeavesCredentials(t *testing.T) {
	st := &swapStore{fence: time.Unix(300, 0), creds: json.RawMessage(`{"r":"reconnected"}`)}
	swapped, _, err := auth.SwapRefreshedCredentials(context.Background(), store.NoopTxRunner{}, st, "o", "jira", time.Unix(100, 0), json.RawMessage(`{"r":"rotated"}`))
	if err != nil || swapped {
		t.Fatalf("stale fence: swapped=%v err=%v", swapped, err)
	}
	if string(st.creds) != `{"r":"reconnected"}` {
		t.Fatalf("stale swap overwrote credentials: %s", st.creds)
	}
}

// When the fence re-read fails the cached token is dropped and the error
// returned, so a token whose credentials cannot be confirmed is not served.
func TestEdge_CacheDroppedWhenFenceReReadFails(t *testing.T) {
	cache := &memory.Store{}
	st := &swapStore{getErr: errors.New("db down")}
	kept, err := auth.CacheIfCredentialsUnchanged(context.Background(), store.NoopTxRunner{}, st, cache, "o", "jira", time.Unix(1, 0), "k", "tok", time.Hour)
	if err == nil || kept {
		t.Fatalf("kept=%v err=%v, want dropped with an error", kept, err)
	}
	if _, gerr := cache.Get(context.Background(), "k"); !errors.Is(gerr, auth.ErrCacheMiss) {
		t.Fatalf("token left cached after a failed fence re-read: %v", gerr)
	}
}
