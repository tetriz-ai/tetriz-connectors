// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package repotally

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestTally(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")

	var none Tally
	assert.NoError(t, none.Err(0), "nothing attempted is not a failure")

	var some Tally
	some.Attempt()
	some.Fail("acme/a", first)
	some.Attempt()
	assert.NoError(t, some.Err(2), "one of two failed: the run completes")

	var all Tally
	all.Attempt()
	all.Fail("acme/a", first)
	all.Attempt()
	all.Fail("acme/b", second)
	err := all.Err(2)
	require.Error(t, err)
	assert.ErrorIs(t, err, first, "the first failure is returned")
	assert.NotErrorIs(t, err, second)
	assert.Contains(t, err.Error(), "2 of 2")
}

func TestSkipper(t *testing.T) {
	forbidden := &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 403, Terminal: true}
	unauthorized := &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401, Terminal: true}
	rateLimited := &sdk.SyncError{Kind: sdk.ErrKindRateLimit, HTTPStatus: 403}

	notFound := &sdk.SyncError{Kind: sdk.ErrKindNotFound, HTTPStatus: 404}
	sinkFailed := &sdk.SyncError{Kind: sdk.ErrKindSinkFailure}

	var f Skipper
	f.Attempt()
	require.False(t, f.Skip("acme/a", unauthorized), "a 401 is the credential's, so the loop stops")
	require.False(t, f.Skip("acme/a", rateLimited), "a rate limit stops the loop")
	require.False(t, f.Skip("acme/a", sinkFailed), "a sink failure stops the loop")
	require.False(t, f.Skip("acme/a", context.Canceled), "the run's context ending stops the loop")
	require.True(t, f.Skip("acme/a", forbidden))
	f.Attempt()
	require.NoError(t, f.Err(2), "one of two forbidden is that repository's failure")

	var mixed Skipper
	mixed.Attempt()
	require.True(t, mixed.Skip("acme/a", forbidden))
	mixed.Attempt()
	require.True(t, mixed.Skip("acme/b", notFound), "a 404 is that repository's failure")
	var mse *sdk.SyncError
	require.ErrorAs(t, mixed.Err(2), &mse)
	require.Equal(t, sdk.ErrKindNotFound, mse.Kind, "a 403 next to a 404 fails as the 404, without pausing")

	var all Skipper
	all.Attempt()
	all.Skip("acme/a", forbidden)
	all.Attempt()
	all.Skip("acme/b", forbidden)
	var se *sdk.SyncError
	require.ErrorAs(t, all.Err(2), &se, "every repository forbidden keeps the first failure's kind")
	require.Equal(t, sdk.ErrKindAuth, se.Kind)
	require.True(t, se.Terminal)

	// The loop stopped (its deadline) after one 403, before the second repository.
	var cut Skipper
	cut.Attempt()
	cut.Skip("acme/a", forbidden)
	require.NoError(t, cut.Err(2), "a 403 before a deadline break does not pause")
}

// When every repository failed, the run pauses only if every failure was a
// credential failure; a 403 next to any other failure fails without pausing.
func TestTallyPausesOnlyWhenEveryFailureIsACredentialFailure(t *testing.T) {
	forbidden := &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 403, Terminal: true}
	unauthorized := &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: 401, Terminal: true}
	badGateway := &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 502, Retryable: true}
	notFound := &sdk.SyncError{Kind: sdk.ErrKindNotFound, HTTPStatus: 404}
	blob := errors.New("upload blob: s3 unavailable")
	pauses := func(err error) bool {
		var se *sdk.SyncError
		return errors.As(err, &se) && se.Kind == sdk.ErrKindAuth && se.Terminal
	}
	for _, tc := range []struct {
		name   string
		errs   []error
		pauses bool
	}{
		{"403 and 502", []error{forbidden, badGateway}, false},
		{"403 and 404", []error{forbidden, notFound}, false},
		{"403 and a blob failure", []error{forbidden, blob}, false},
		{"502 then 403", []error{badGateway, forbidden}, false},
		{"all 403", []error{forbidden, forbidden}, true},
		{"all 401", []error{unauthorized, unauthorized}, true},
		{"401 and 403", []error{unauthorized, forbidden}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tl Tally
			for i, e := range tc.errs {
				tl.Attempt()
				tl.Fail([]string{"acme/a", "acme/b"}[i], e)
			}
			err := tl.Err(len(tc.errs))
			require.Error(t, err, "every repository failed: the run fails")
			assert.Equal(t, tc.pauses, pauses(err), "err=%v", err)
		})
	}
}
