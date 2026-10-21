// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package tenantdb_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
)

// testDB is a live, RLS-irrelevant Postgres handle leased once for the package.
// The orgID-validation path only needs a real connection so DB() can BEGIN;
// the SET LOCAL value, not the schema, is what these tests exercise.
var testDB *gorm.DB

func TestMain(m *testing.M) {
	if err := testdb.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "skipping tenantdb DB tests: %v\n", err)
		os.Exit(0)
	}
	db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tenantdb test setup: %v\n", err)
		os.Exit(1)
	}
	testDB = db
	code := testguard.Run(m)
	cleanup()
	os.Exit(code)
}

// TestDBOrgIDValidation guards against SQL injection via the org-scope
// app.current_tenant interpolation: a non-UUID orgID must fail closed (errored
// handle, no injected SQL executed), while a valid UUID still opens the scoped tx.
func TestDBOrgIDValidation(t *testing.T) {
	tests := []struct {
		name      string
		orgID     string
		wantError bool
	}{
		{
			name:      "valid uuid opens scoped tx",
			orgID:     uuid.NewString(),
			wantError: false,
		},
		{
			name:      "sql injection payload rejected",
			orgID:     "x'; DROP TABLE users; --",
			wantError: true,
		},
		{
			name:      "rls breakout payload rejected",
			orgID:     "__system__",
			wantError: true,
		},
		{
			name:      "empty orgID rejected",
			orgID:     "",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tenantdb.NewContext(context.Background(), tt.orgID, tenantdb.ScopeEnforced)
			tx := tenantdb.DB(ctx, testDB)
			defer func() { _ = tenantdb.CommitOrRollback(ctx, fmt.Errorf("test teardown")) }()

			if tt.wantError {
				if tx.Error == nil {
					t.Fatalf("orgID %q: expected errored handle, got nil error", tt.orgID)
				}
				return
			}
			if tx.Error != nil {
				t.Fatalf("orgID %q: expected no error, got %v", tt.orgID, tx.Error)
			}
		})
	}
}

// TestTenantTxOrgIDValidation guards the caller-managed scopedTx path (reached
// via TenantTx from CALLER_MANAGED RPCs / asynq handlers and via SystemTx for
// cross-tenant work). It mirrors TestDBOrgIDValidation: a non-UUID/injection
// orgID must fail closed before the SET LOCAL runs, a valid UUID commits, and
// the SystemTenantID sentinel (SystemTx) is allowed through.
func TestTenantTxOrgIDValidation(t *testing.T) {
	tests := []struct {
		name      string
		system    bool // exercise SystemTx (sentinel) instead of TenantTx(orgID)
		orgID     string
		wantError bool
	}{
		{
			name:      "valid uuid commits",
			orgID:     uuid.NewString(),
			wantError: false,
		},
		{
			name:      "sql injection payload rejected",
			orgID:     "x'; DROP TABLE users; --",
			wantError: true,
		},
		{
			name:      "rls breakout payload rejected via TenantTx",
			orgID:     "__system__",
			wantError: true,
		},
		{
			name:      "empty orgID rejected",
			orgID:     "",
			wantError: true,
		},
		{
			name:      "system sentinel allowed via SystemTx",
			system:    true,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ran := false
			noop := func(*gorm.DB) error { ran = true; return nil }

			var err error
			if tt.system {
				err = tenantdb.SystemTx(context.Background(), testDB, noop)
			} else {
				err = tenantdb.TenantTx(context.Background(), testDB, tt.orgID, noop)
			}

			if tt.wantError {
				if err == nil {
					t.Fatalf("orgID %q: expected error, got nil", tt.orgID)
				}
				if ran {
					t.Fatalf("orgID %q: fn ran despite invalid tenant — SET LOCAL was not guarded", tt.orgID)
				}
				return
			}
			if err != nil {
				t.Fatalf("orgID %q: expected no error, got %v", tt.orgID, err)
			}
			if !ran {
				t.Fatalf("orgID %q: fn did not run on the valid path", tt.orgID)
			}
		})
	}
}
