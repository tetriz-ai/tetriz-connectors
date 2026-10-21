// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package repotally decides whether a per-repository operation failed as a
// whole. One failing repository does not fail the run: the others still sync,
// and the failed one keeps its cursor state for the next run. When every
// repository the run attempted failed, the run fails. It fails as a credential
// failure, which pauses the connector, only when every failure was one;
// otherwise it fails with the first other failure, which does not pause.
package repotally

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Tally counts the repositories an operation attempted and the ones that
// failed. The zero value is ready to use.
type Tally struct {
	attempted, failed, credential int
	first, firstOther             error
}

// Attempt records that the operation started work on one repository.
func (t *Tally) Attempt() { t.attempted++ }

// Fail records that the repository the operation last attempted failed.
func (t *Tally) Fail(repo string, err error) {
	t.failed++
	wrapped := fmt.Errorf("%s: %w", repo, err)
	if t.first == nil {
		t.first = wrapped
	}
	if IsCredentialFailure(err) {
		t.credential++
	} else if t.firstOther == nil {
		t.firstOther = wrapped
	}
}

// Err returns nil unless every attempted repository failed (and nil when none
// was attempted). listed is how many repositories the loop had to read. When
// every failure was a credential failure and every listed repository was
// attempted it returns the first one, so a terminal auth error pauses the
// connector. A loop that stopped early (its deadline) has not shown that the
// credential reaches nothing, so it does not pause. Otherwise it returns the
// first failure that was not a credential failure, so the run fails without
// pausing: one repository's 403 next to another's 502 is not a dead
// credential.
func (t *Tally) Err(listed int) error {
	if t.attempted == 0 || t.failed < t.attempted {
		return nil
	}
	if t.credential == t.failed {
		if t.attempted < listed {
			return nil
		}
		return fmt.Errorf("every repository failed (%d of %d), the first: %w", t.failed, t.attempted, t.first)
	}
	return fmt.Errorf("every repository failed (%d of %d, %d with a credential failure), the first other: %w", t.failed, t.attempted, t.credential, t.firstOther)
}

// StopsTheRun reports whether a repository's failure is one the whole loop
// shares, so the loop stops rather than skips the repository: a rejected or
// unrefreshable credential (any auth failure but a per-resource 403), a rate
// limit (the next repository would hit it too, and the run's retry honours
// the vendor's hint), a sink failure, a terminal config_invalid (such as a
// token scope every repository needs), or the run's context ending.
func StopsTheRun(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var syncErr *sdk.SyncError
	if !errors.As(err, &syncErr) {
		return false
	}
	switch syncErr.Kind {
	case sdk.ErrKindAuth:
		return syncErr.HTTPStatus != http.StatusForbidden
	case sdk.ErrKindRateLimit, sdk.ErrKindSinkFailure, sdk.ErrKindCtxExhausted:
		return true
	case sdk.ErrKindConfigInvalid:
		return syncErr.Terminal
	default:
		return false
	}
}

// AllFailed applies Err's rule to a loop that keeps its own counts (Jira
// projects, Linear teams): it returns nil when any unit succeeded or none
// failed. When every attempted unit failed, it returns the first failure if
// every failure was a credential failure, and otherwise the first other
// failure, preferring one that carries a *sdk.SyncError so its kind and retry
// hint survive.
func AllFailed(succeeded int, failures []error) error {
	if succeeded > 0 || len(failures) == 0 {
		return nil
	}
	var other, otherTyped error
	for _, err := range failures {
		if IsCredentialFailure(err) {
			continue
		}
		if other == nil {
			other = err
		}
		var syncErr *sdk.SyncError
		if otherTyped == nil && errors.As(err, &syncErr) {
			otherTyped = err
		}
	}
	switch {
	case otherTyped != nil:
		return otherTyped
	case other != nil:
		return other
	default:
		return failures[0]
	}
}

// IsCredentialFailure reports whether err is an auth SyncError: the vendor
// rejected the credential (401) or refused the resource (403), or the token
// could not be refreshed. A 403 the gateway read as a rate limit is not one.
func IsCredentialFailure(err error) bool {
	var syncErr *sdk.SyncError
	return errors.As(err, &syncErr) && syncErr.Kind == sdk.ErrKindAuth
}

// Skipper is a Tally for a repository loop that would otherwise stop on the
// first repository's failure. Skip records a failure that is the repository's
// own (a 403, a 404, a 5xx, an unparseable response) so the loop moves on and
// the repository keeps its cursor; a failure the whole loop shares
// (StopsTheRun) is not skipped, and the loop returns it. Err is Tally's, so
// the run fails only when every attempted repository failed, and pauses only
// when every failure was a credential failure and none was left unread. The
// zero value is ready to use.
type Skipper struct{ Tally }

// Skip reports whether err is the repository's own failure and, if so,
// records it so the loop can move on to the next repository.
func (s *Skipper) Skip(repo string, err error) bool {
	if StopsTheRun(err) {
		return false
	}
	s.Fail(repo, err)
	return true
}
