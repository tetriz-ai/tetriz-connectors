// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package tenantdb provides tenant-scoped database access for PostgreSQL
// Row Level Security (RLS) enforcement.
//
// Every store query must run inside a transaction that has
// SET LOCAL app.current_tenant = '<org_id>' (or '__system__' for the system
// scope), so the tables' RLS policies filter rows by tenant. Nothing in this
// module adds a scope for you: a caller opens one with [RunInScope], or passes
// a store.TxRunner to the code that makes the store calls (the orchestrator,
// the schedule provider's source scope, and the integration and connector
// services all take one).
//
// # Two usage patterns
//
// 1. Context scope: [NewContext] or [RunInScope] puts tenant metadata in the
// context. DAO methods call [DB], which lazily opens a transaction with SET
// LOCAL on first use, and [CommitOrRollback] finishes it. Every DAO call made
// with that context shares the one transaction.
//
// 2. Explicit: [TenantTx] or [SystemTx] wrap individual DB operations in
// short-lived scoped transactions. See tx.go.
//
// # Thread safety — concurrent goroutines
//
// The lazy transaction ([txHolder]) caches a single *gorm.DB transaction.
// PostgreSQL connections are NOT safe for concurrent use — sharing one
// transaction across multiple goroutines causes "conn busy" panics.
//
// If code fans out to concurrent goroutines (errgroup, go func), each
// goroutine that makes DAO calls MUST use [RunInScope] to get its own
// transaction. RunInScope creates a child context with a new txHolder that
// shadows the parent's, so each goroutine gets an independent connection.
// [CommitOrRollback] is a no-op when a holder never opened a transaction.
//
// # Transaction lifecycle guarantees
//
// Every transaction opened by this package is finalized:
//
//   - [RunInScope]: deferred panic recovery rolls back and re-panics.
//   - [TenantTx]/[SystemTx]: deferred panic recovery in scopedTx rolls back
//     and re-panics.
//   - A context from [NewContext] is finished by the caller's
//     [CommitOrRollback].
//
// # Safe failure mode
//
// If no tenant context is set, [DB] falls back to the default gormctx
// behavior — no transaction and no tenant setting. PostgreSQL's
// current_setting then returns NULL, which fails the RLS policy check for a
// role the policies bind (the table owner too, since the tables force RLS),
// so the query returns zero rows and writes none.
package tenantdb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/gormctx"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

type contextKey string

const txHolderKey contextKey = "tenantdb_tx_holder"

// Scope represents the tenant isolation mode of a scoped context.
type Scope int

const (
	// ScopeEnforced scopes the context to a single tenant: the lazy
	// transaction will SET LOCAL app.current_tenant = '<org_id>'.
	ScopeEnforced Scope = 0

	// ScopeSystem spans every tenant.
	// The lazy transaction will SET LOCAL app.current_tenant = '__system__',
	// which the RLS policy recognises as a bypass sentinel. Used for cross-
	// tenant discovery queries like ListEnabledConfigs.
	ScopeSystem Scope = 1

	// ScopeCallerManaged marks code that creates no txHolder and calls
	// [TenantTx] or [SystemTx] around each DB operation itself, so no
	// transaction stays open across a slow outbound call.
	ScopeCallerManaged Scope = 2
)

// SystemTenantID is the sentinel value that bypasses RLS tenant filtering.
// RLS policies check: current_setting('app.current_tenant') = '__system__'
// and grant access to all rows when this matches.
const SystemTenantID = "__system__"

// canonicalTenantSetting validates a tenant identifier for interpolation into
// the SET LOCAL app.current_tenant statement and returns the exact string to
// interpolate. PostgreSQL's SET LOCAL rejects bind parameters, so the value is
// concatenated into SQL — it must be trusted. The org-scope value is
// attacker-influenceable (the sandbox HMAC-token path sets it from the token
// payload with no format check), so anything other than the SystemTenantID
// sentinel must be a strict UUID or this fails closed. The returned value is
// uuid.Parse's canonical .String() form, not the raw input: uuid.Parse accepts
// non-canonical encodings (URN, Microsoft braces) that would otherwise set a
// malformed current_tenant and silently match no rows under RLS.
func canonicalTenantSetting(tenantID string) (string, error) {
	if tenantID == SystemTenantID {
		return SystemTenantID, nil
	}
	parsed, err := uuid.Parse(tenantID)
	if err != nil {
		return "", fmt.Errorf("invalid tenant orgID %q: %w", tenantID, err)
	}
	return parsed.String(), nil
}

// txHolder is a mutable, thread-safe container placed in context by
// [NewContext]. It carries tenant metadata (orgID, scope) and
// lazily holds the *gorm.DB transaction once [DB] creates it. The mutex
// ensures only one goroutine creates the transaction; others reuse it.
type txHolder struct {
	mu     sync.Mutex
	orgID  string   // Tenant org ID, or SystemTenantID for cross-tenant.
	scope  Scope    // Determines what SET LOCAL value is used.
	tx     *gorm.DB // Lazily created on first DB() call; nil until then.
	dbUsed *gorm.DB // The *gorm.DB the transaction was opened on.
	// afterCommit runs, in order, once the scope's work has committed.
	afterCommit []func()
}

// AfterCommit runs fn once the scope ctx carries has committed, and never if it
// rolls back. Without a scope (an unscoped call autocommits) fn runs now.
func AfterCommit(ctx context.Context, fn func()) {
	holder := holderFromContext(ctx)
	if holder == nil {
		fn()
		return
	}
	holder.mu.Lock()
	defer holder.mu.Unlock()
	holder.afterCommit = append(holder.afterCommit, fn)
}

// runAfterCommit runs and clears the hooks; holder.mu must not be held.
func (h *txHolder) runAfterCommit() {
	h.mu.Lock()
	hooks := h.afterCommit
	h.afterCommit = nil
	h.mu.Unlock()
	for _, fn := range hooks {
		fn()
	}
}

// NewContext returns a context carrying tenant metadata for lazy transaction
// creation. The actual database transaction is NOT opened here — it is
// deferred to the first [DB] call, so the caller need not know which database
// the store calls use. The caller finishes it with [CommitOrRollback].
func NewContext(ctx context.Context, orgID string, scope Scope) context.Context {
	holder := &txHolder{orgID: orgID, scope: scope}
	return context.WithValue(ctx, txHolderKey, holder)
}

func holderFromContext(ctx context.Context) *txHolder {
	h, _ := ctx.Value(txHolderKey).(*txHolder)
	return h
}

// InScope reports whether ctx already carries a tenant scope from [NewContext]
// or [RunInScope], so a store call made with it runs in that scope's
// transaction.
func InScope(ctx context.Context) bool { return holderFromContext(ctx) != nil }

// AssertNoOpenTx guards a boundary that must run with NO tenant transaction open:
// immediately before a slow external call (a VendorGateway HTTP
// call, an S3 read). Holding a scope's transaction open across such a call leaves
// the session idle-in-transaction; PostgreSQL's idle_in_transaction_session_timeout
// (1 min) then terminates the connection, and every subsequent statement — the
// whole post-call tail of the handler — fails with SQLSTATE 25P03. The remedy is to
// do DB work in short scopes on either side of the call and keep the call itself
// outside any scope.
//
// Under `go test` a violation panics, so the trap is caught in CI rather than in
// production. In real use it degrades to a logged error (the idle timeout stays
// the final backstop) rather than crashing the batch.
func AssertNoOpenTx(ctx context.Context) {
	h := holderFromContext(ctx)
	if h == nil {
		return
	}
	h.mu.Lock()
	open := h.tx != nil
	h.mu.Unlock()
	if !open {
		return
	}
	if testing.Testing() {
		panic("tenantdb: a DB transaction is open across a boundary that must be transaction-free (external vendor/S3 call) — split DB work into scopes on either side and keep the call outside any scope; see the no-open-transaction rule")
	}
	// Code opened a transaction across an external call: fix the phasing so no
	// transaction is open across the call, not a per-record ops step. org_id makes the firing traceable.
	logger.Error(ctx, "tenantdb: open transaction across an external-call boundary", zap.String(logger.OrgID, h.orgID))
}

// DB returns a tenant-scoped *gorm.DB handle for use in DAO methods.
// It is a drop-in replacement for gormctx.FromContextOrDefault(ctx, r.db).
//
// Behavior depends on whether a txHolder exists in ctx (set by [NewContext]):
//
//   - No txHolder (any unscoped path): falls back to
//     gormctx.FromContextOrDefault, returning defaultDB bound to ctx. No
//     transaction and no tenant setting, so forced RLS returns no rows.
//
//   - txHolder exists, first call: opens a BEGIN transaction on defaultDB,
//     runs SET LOCAL app.current_tenant = '<orgID>' (or '__system__' for
//     ScopeSystem), caches the transaction in the holder, and returns it.
//     All subsequent queries through this handle run inside the scoped tx.
//
//   - txHolder exists, subsequent calls with the same defaultDB: returns the
//     cached transaction. This means multiple DAO calls within the same
//     scope share a single transaction — exactly one BEGIN per scope.
//
// The defaultDB parameter is the DAO's own *gorm.DB. The first DAO to call
// DB() in a scope "wins" the transaction.
//
// On error (BEGIN fails), the errored *gorm.DB is returned. GORM will
// propagate the error on the next operation, so callers don't need to
// check tx.Error explicitly.
func DB(ctx context.Context, defaultDB *gorm.DB) *gorm.DB {
	holder := holderFromContext(ctx)
	if holder == nil {
		return gormctx.FromContextOrDefault(ctx, defaultDB)
	}

	holder.mu.Lock()
	defer holder.mu.Unlock()

	if holder.tx != nil {
		return holder.tx
	}

	tx := defaultDB.WithContext(ctx).Begin()
	if tx.Error != nil {
		return tx
	}

	// PostgreSQL SET does not support parameterized values ($1), so we
	// interpolate directly; the value must be trusted. SystemTenantID is a
	// compile-time constant, selected here by scope (not by orgID value) — an
	// enforced-scope orgID of "__system__" is an injection attempt and must be
	// rejected, so this branch validates a strict UUID and fails closed. We
	// interpolate the canonical .String() form, not the raw input: uuid.Parse
	// accepts URN/brace encodings that would otherwise set a malformed
	// current_tenant and silently match no rows under RLS.
	var setting string
	if holder.scope == ScopeSystem {
		setting = SystemTenantID
	} else {
		canonical, err := uuid.Parse(holder.orgID)
		if err != nil {
			tx.Rollback()
			// Surface the rejection via tx.Error (nil here — Begin succeeded, no
			// SET ran), mirroring the failed-SET path below. Enforced scope must
			// not accept "__system__"; uuid.Parse rejects it as a non-UUID.
			tx.Error = fmt.Errorf("invalid tenant orgID %q: %w", holder.orgID, err)
			return tx
		}
		setting = canonical.String()
	}
	if setErr := tx.Exec("SET LOCAL app.current_tenant = '" + setting + "'").Error; setErr != nil {
		tx.Rollback()
		// Return the failed tx so callers see the error via tx.Error.
		return tx
	}
	holder.tx = tx
	holder.dbUsed = defaultDB
	return tx
}

// RunInScope runs fn inside a short-lived tenant-scoped context. It creates a
// new [txHolder], calls fn, and commits or rolls back based on the returned error.
// Unlike [TenantTx] (which passes a *gorm.DB), this works with existing DAO
// methods that call [DB](ctx) — the DAO code doesn't need to change.
//
// Use this for long-running handlers that need per-operation transaction
// boundaries instead of one wrapping transaction. Example:
//
//	// Each date gets its own transaction — partial failures don't roll back other dates.
//	for _, date := range dates {
//	    err := tenantdb.RunInScope(ctx, orgID, ScopeEnforced, func(scopedCtx context.Context) error {
//	        return handler.HandleDate(scopedCtx, date)
//	    })
//	    if err != nil { ... }
//	}
//
// A parent txHolder, if present, is shadowed:
// DAO calls inside fn always run on the new scope's transaction, and the parent's
// open transaction is untouched — it stays idle until its own commit, so it remains
// subject to the idle-in-transaction timeout while fn runs. Nesting inside
// another scope is how concurrent goroutines each get their own transaction.
//
// Do NOT make a slow external call (VendorGateway HTTP, S3) inside fn between DB
// accesses — the transaction idles across it and PostgreSQL kills it. Phase the
// DB work into scopes on either side, the call outside any scope. See [AssertNoOpenTx].
func RunInScope(ctx context.Context, orgID string, scope Scope, fn func(ctx context.Context) error) error {
	scopedCtx := NewContext(ctx, orgID, scope)
	defer func() {
		if r := recover(); r != nil {
			_ = CommitOrRollback(scopedCtx, fmt.Errorf("panic: %v", r)) //nolint:errcheck // best-effort rollback before re-panic; the panic is the real signal
			panic(r)                                                    // re-panic after rollback so caller's recovery still fires
		}
	}()
	err := fn(scopedCtx)
	return CommitOrRollback(scopedCtx, err) //nolint:wrapcheck // tenantdb scope primitive: passes the closure error through transparently so status codes / caller context survive; wrapping would corrupt them
}

// CommitOrRollback finalizes the lazy transaction if one was created. The
// caller that made the context with [NewContext] calls it when its work ends.
//
// Behavior:
//   - No txHolder in ctx, or txHolder exists but no transaction was created
//     (handler made zero DB calls): returns err unchanged. No-op.
//   - err != nil: rolls back the transaction. If rollback itself fails,
//     returns a combined error wrapping both the original and rollback errors.
//   - err == nil: commits the transaction. If commit fails, returns the
//     commit error.
//
// This is safe to call unconditionally — it gracefully handles the case where
// no transaction was ever opened (e.g. a handler that only made outbound gRPC
// calls and never touched the database).
func CommitOrRollback(ctx context.Context, err error) error {
	holder := holderFromContext(ctx)
	if holder == nil {
		return err
	}

	holder.mu.Lock()
	if holder.tx == nil {
		holder.mu.Unlock()
		if err == nil {
			holder.runAfterCommit()
		}
		return err
	}

	if err != nil {
		holder.afterCommit = nil
		rbErr := holder.tx.Rollback().Error
		holder.mu.Unlock()
		if rbErr != nil {
			return fmt.Errorf("rollback failed (%w) after handler error: %w", rbErr, err)
		}
		return err
	}

	cErr := holder.tx.Commit().Error
	if cErr != nil {
		holder.afterCommit = nil
		holder.mu.Unlock()
		return fmt.Errorf("commit failed: %w", cErr)
	}
	holder.mu.Unlock()
	holder.runAfterCommit()
	return nil
}
