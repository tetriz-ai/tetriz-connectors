// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"
	"sync"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// credentialSwaps records, for one run, the credential fence each successful
// CompareAndSwapCredentials made with the run's context wrote.
type credentialSwaps struct {
	mu     sync.Mutex
	fences map[credentialSwapKey]time.Time
}

type credentialSwapKey struct {
	orgID       string
	connectorID sdk.ConnectorID
}

type credentialSwapsCtxKey struct{}

// WithCredentialSwaps returns a context that records the credential swaps
// made with it or a context derived from it: a token refresh inside a run
// rotates the credentials, and the run's later auth failure belongs to the
// rotated credentials, whose fence CredentialSwap then returns.
func WithCredentialSwaps(ctx context.Context) context.Context {
	return context.WithValue(ctx, credentialSwapsCtxKey{}, &credentialSwaps{fences: map[credentialSwapKey]time.Time{}})
}

// RecordCredentialSwap notes that a credential swap made with ctx wrote
// fence for the connector. The note lands when ctx's store scope commits, and
// not at all if it rolls back, so an uncommitted swap is never a fence.
// Without WithCredentialSwaps it does nothing.
func RecordCredentialSwap(ctx context.Context, orgID string, connectorID sdk.ConnectorID, fence time.Time) {
	s, ok := ctx.Value(credentialSwapsCtxKey{}).(*credentialSwaps)
	if !ok {
		return
	}
	tenantdb.AfterCommit(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.fences[credentialSwapKey{orgID, connectorID}] = fence
	})
}

// CredentialSwap returns the fence the last credential swap made with ctx
// wrote for the connector, and whether there was one.
func CredentialSwap(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (time.Time, bool) {
	s, ok := ctx.Value(credentialSwapsCtxKey{}).(*credentialSwaps)
	if !ok {
		return time.Time{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.fences[credentialSwapKey{orgID, connectorID}]
	return f, ok
}
