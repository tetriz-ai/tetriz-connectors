// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
)

// Scope says whether a transaction is bound to one tenant or spans all of
// them.
type Scope = tenantdb.Scope

const (
	// ScopeEnforced binds the transaction to one tenant.
	ScopeEnforced = tenantdb.ScopeEnforced
	// ScopeSystem spans every tenant.
	ScopeSystem = tenantdb.ScopeSystem
)

// TxRunner runs fn with a context that scopes every store call made with it
// to one tenant (or to all tenants for ScopeSystem). With the Postgres store
// those calls share one transaction that carries the tenant for row-level
// security, committed when fn returns nil and rolled back otherwise.
type TxRunner interface {
	RunInScope(ctx context.Context, orgID string, scope Scope, fn func(ctx context.Context) error) error
}

// PostgresTxRunner is the TxRunner for the Postgres store: it sets
// app.current_tenant with SET LOCAL on the first query of the scope, so the
// tables' tenant_isolation policies apply.
type PostgresTxRunner struct{}

// RunInScope implements TxRunner.
func (PostgresTxRunner) RunInScope(ctx context.Context, orgID string, scope Scope, fn func(ctx context.Context) error) error {
	return tenantdb.RunInScope(ctx, orgID, scope, fn) //nolint:wrapcheck // the callback's error is returned verbatim by contract
}

// NoopTxRunner calls fn directly. Use it with a single-tenant store or the
// in-memory store, which have no transactions and no row-level security.
type NoopTxRunner struct{}

// RunInScope implements TxRunner.
func (NoopTxRunner) RunInScope(ctx context.Context, _ string, _ Scope, fn func(ctx context.Context) error) error {
	return fn(ctx)
}
