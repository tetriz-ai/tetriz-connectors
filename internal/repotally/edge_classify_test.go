// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package repotally

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The classifier must see through wrapping: a repo loop wraps the gateway's
// SyncError with the repo name, sometimes twice, before asking.
func TestEdge_StopsTheRunSeesThroughWrapping(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("repo a/b: %w", fmt.Errorf("page 2: %w", err)) }
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrapped 401", wrap(&sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: http.StatusUnauthorized}), true},
		{"wrapped per-repo 403", wrap(&sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: http.StatusForbidden}), false},
		{"auth with no status (refresh failed)", wrap(&sdk.SyncError{Kind: sdk.ErrKindAuth}), true},
		{"vendor error carrying a deadline", wrap(&sdk.SyncError{Kind: sdk.ErrKindVendorError, Cause: context.DeadlineExceeded}), true},
		{"joined rate limit", errors.Join(errors.New("x"), &sdk.SyncError{Kind: sdk.ErrKindRateLimit}), true},
		{"wrapped 404", wrap(&sdk.SyncError{Kind: sdk.ErrKindNotFound}), false},
		{"plain error", errors.New("parse"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { assert.Equal(t, c.want, StopsTheRun(c.err)) })
	}
}

// Every listed repo answered 403 (per-resource refusals, skipped one by one):
// the run fails as a credential failure and the returned error still carries the
// SyncError so the orchestrator can pause. A run that stopped before the last
// listed repo does not pause.
func TestEdge_SkipperAllForbiddenPausesOnlyWhenEveryRepoWasRead(t *testing.T) {
	forbidden := &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: http.StatusForbidden}

	var s Skipper
	for _, repo := range []string{"a", "b", "c"} {
		s.Attempt()
		require.True(t, s.Skip(repo, forbidden))
	}
	err := s.Err(3)
	require.Error(t, err)
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, sdk.ErrKindAuth, se.Kind)
	assert.Contains(t, err.Error(), "a: ")

	assert.NoError(t, s.Err(4), "a loop that stopped early has not shown the credential reaches nothing")
}

// AllFailed prefers a typed non-credential failure over an earlier untyped one,
// so its kind and retry hint survive, and returns the first credential failure
// when that is all there is.
func TestEdge_AllFailedPrefersTheTypedFailure(t *testing.T) {
	plain := errors.New("plain")
	typed := fmt.Errorf("team x: %w", &sdk.SyncError{Kind: sdk.ErrKindRateLimit, RetryAfterSeconds: 30})
	auth := &sdk.SyncError{Kind: sdk.ErrKindAuth}

	assert.Same(t, typed, AllFailed(0, []error{auth, plain, typed}))
	assert.Same(t, plain, AllFailed(0, []error{auth, plain}))
	assert.Same(t, auth, AllFailed(0, []error{auth, &sdk.SyncError{Kind: sdk.ErrKindAuth}}))
	assert.NoError(t, AllFailed(1, []error{plain}))
}
