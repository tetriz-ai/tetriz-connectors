// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package tenantdb

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

// AssertNoOpenTx catches a tenant transaction left open across a long external call, which
// PostgreSQL's idle_in_transaction_session_timeout kills (SQLSTATE 25P03). The guard must
// fire (panic under test) exactly when a transaction is open, and stay silent otherwise.
func TestAssertNoOpenTx(t *testing.T) {
	tests := []struct {
		name        string
		ctx         func() context.Context
		expectPanic bool
	}{
		{
			name:        "no tenant holder in context is a no-op",
			ctx:         func() context.Context { return context.Background() },
			expectPanic: false,
		},
		{
			name:        "holder present but no transaction opened is a no-op",
			ctx:         func() context.Context { return NewContext(context.Background(), "org-1", ScopeEnforced) },
			expectPanic: false,
		},
		{
			name: "holder with an open transaction panics",
			ctx: func() context.Context {
				ctx := NewContext(context.Background(), "org-1", ScopeEnforced)
				h := holderFromContext(ctx)
				// A non-nil tx is exactly what "the transaction is open" means to
				// tenantdb; AssertNoOpenTx checks only for non-nil, never dereferences it.
				h.tx = &gorm.DB{}
				return ctx
			},
			expectPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx()
			defer func() {
				r := recover()
				switch {
				case tt.expectPanic && r == nil:
					t.Fatal("expected AssertNoOpenTx to panic on an open transaction, but it did not")
				case !tt.expectPanic && r != nil:
					t.Fatalf("expected no panic, got: %v", r)
				}
			}()
			AssertNoOpenTx(ctx)
		})
	}
}
