// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncError_Error(t *testing.T) {
	tests := []struct {
		name    string
		syncErr *SyncError
		wantSub string
	}{
		{
			name: "formats error with cause",
			syncErr: &SyncError{
				ConnectorID:    "github",
				OperationID:    "fetch_prs",
				OrganizationID: "org_123",
				Kind:           ErrKindAuth,
				Cause:          fmt.Errorf("token expired"),
			},
			wantSub: "sync error [auth] connector=github operation=fetch_prs org=org_123: token expired",
		},
		{
			name: "formats error without cause",
			syncErr: &SyncError{
				ConnectorID:    "slack",
				OperationID:    "fetch_messages",
				OrganizationID: "org_456",
				Kind:           ErrKindRateLimit,
			},
			wantSub: "sync error [rate_limit] connector=slack operation=fetch_messages org=org_456",
		},
		{
			name:    "leaves out the names a gateway does not have",
			syncErr: &SyncError{Kind: ErrKindInternal, Cause: fmt.Errorf("HTTP 400")},
			wantSub: "sync error [internal]: HTTP 400",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.syncErr.Error()
			assert.Equal(t, tt.wantSub, got)
		})
	}
}

func TestSyncError_Unwrap(t *testing.T) {
	cause := fmt.Errorf("root cause")
	syncErr := &SyncError{
		ConnectorID: "github",
		OperationID: "fetch_prs",
		Kind:        ErrKindVendorError,
		Cause:       cause,
	}

	assert.Equal(t, cause, syncErr.Unwrap())
	assert.ErrorIs(t, syncErr, cause)
}

func TestSyncError_As(t *testing.T) {
	syncErr := &SyncError{
		ConnectorID: "github",
		Kind:        ErrKindTimeout,
		Cause:       fmt.Errorf("context deadline exceeded"),
	}

	wrappedErr := fmt.Errorf("wrapped: %w", syncErr)

	var target *SyncError
	require.ErrorAs(t, wrappedErr, &target)
	assert.Equal(t, ErrKindTimeout, target.Kind)
}

func TestSyncErrorKind_String(t *testing.T) {
	tests := []struct {
		name string
		kind SyncErrorKind
		want string
	}{
		{name: "auth", kind: ErrKindAuth, want: "auth"},
		{name: "rate_limit", kind: ErrKindRateLimit, want: "rate_limit"},
		{name: "not_found", kind: ErrKindNotFound, want: "not_found"},
		{name: "timeout", kind: ErrKindTimeout, want: "timeout"},
		{name: "vendor_error", kind: ErrKindVendorError, want: "vendor_error"},
		{name: "config_invalid", kind: ErrKindConfigInvalid, want: "config_invalid"},
		{name: "sink_failure", kind: ErrKindSinkFailure, want: "sink_failure"},
		{name: "internal", kind: ErrKindInternal, want: "internal"},
		{name: "unknown", kind: SyncErrorKind(99), want: "unknown(99)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.kind.String()
			assert.Equal(t, tt.want, got)
		})
	}
}
