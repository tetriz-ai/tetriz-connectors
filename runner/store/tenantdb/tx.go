// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package tenantdb

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// TenantTx runs fn inside a short-lived, self-contained transaction scoped to
// a single tenant. The transaction lifecycle is: BEGIN → SET LOCAL → fn → COMMIT
// (or ROLLBACK on error). The transaction is opened and closed within this call
// — it does not use the lazy txHolder from [NewContext].
//
// Use it where the caller controls transaction boundaries, such as a task
// handler, or for per-tenant writes after a SystemTx discovery. The caller
// supplies orgID explicitly, for example from a request or a task payload.
func TenantTx(ctx context.Context, db *gorm.DB, orgID string, fn func(tx *gorm.DB) error) error {
	if orgID == "" {
		return fmt.Errorf("tenantdb: org_id must not be empty")
	}
	// Reject the system sentinel on the tenant-scoped entry point: only SystemTx
	// may bypass RLS. Otherwise an attacker-influenceable orgID of "__system__"
	// would escalate a tenant call into a cross-tenant one. scopedTx's
	// canonicalTenantSetting allows the sentinel (for SystemTx); this guard
	// closes that door for TenantTx.
	if orgID == SystemTenantID {
		return fmt.Errorf("tenantdb: org_id must not be the system sentinel")
	}
	return scopedTx(ctx, db, orgID, fn) //nolint:wrapcheck // tenantdb scope primitive: passes the closure error through transparently so status codes / caller context survive; wrapping would corrupt them
}

// SystemTx runs fn inside a short-lived transaction with the __system__
// sentinel, bypassing RLS tenant filtering. Use this for cross-tenant
// operations like scanning all orgs for pending work, or batch cleanup tasks.
//
// Same lifecycle as [TenantTx]: BEGIN → SET LOCAL '__system__' → fn → COMMIT.
func SystemTx(ctx context.Context, db *gorm.DB, fn func(tx *gorm.DB) error) error {
	return scopedTx(ctx, db, SystemTenantID, fn) //nolint:wrapcheck // tenantdb scope primitive: passes the closure error through transparently so status codes / caller context survive; wrapping would corrupt them
}

func scopedTx(ctx context.Context, db *gorm.DB, tenantID string, fn func(tx *gorm.DB) error) error {
	tx := db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("tenantdb: begin tx: %w", tx.Error)
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r) // re-panic after rollback so caller's recovery still fires
		}
	}()

	// Same SQL-injection guard as DB(): the org-scope tenantID can come from a
	// request or a task payload (attacker-influenceable), and
	// SET LOCAL cannot take a bind parameter. Validate to the canonical UUID
	// form before interpolating; SystemTx's __system__ sentinel is allowed.
	// Fail closed before executing the SET.
	setting, err := canonicalTenantSetting(tenantID)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("tenantdb: %w", err)
	}

	if err := tx.Exec("SET LOCAL app.current_tenant = '" + setting + "'").Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("tenantdb: SET LOCAL: %w", err)
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err //nolint:wrapcheck // tenantdb scope primitive: passes the closure error through transparently so status codes / caller context survive; wrapping would corrupt them
	}

	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("tenantdb: commit: %w", err)
	}
	return nil
}
