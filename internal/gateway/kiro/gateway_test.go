// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	connectorkiro "github.com/tetriz-ai/tetriz-connectors/connectors/kiro"
)

// apiErr is the shape the AWS SDK returns for a service error: the code is what the
// classification turns on.
type apiErr struct{ code string }

func (e apiErr) Error() string                 { return e.code }
func (e apiErr) ErrorCode() string             { return e.code }
func (e apiErr) ErrorMessage() string          { return e.code }
func (e apiErr) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

// This mapping decides which of two very different things happens to a failure: a
// configuration fault, which tells an operator the stored settings are wrong, or an object failure that
// accumulates until the report is abandoned for good. Getting a code into the wrong side
// of it either abandons data that was only temporarily unreachable, or leaves a revoked
// grant sitting in the class a failure rate barely shows for a connector running once a day.
func TestTheGrantClassifierReadsEveryDenialAsTheGrantsOwn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want error // nil means it must stay unclassified
	}{
		{"a bucket that is not there", &types.NoSuchBucket{}, connectorkiro.ErrBucketUnreachable},
		{"the grant was revoked", apiErr{"AccessDenied"}, connectorkiro.ErrBucketUnreachable},
		{"forbidden", apiErr{"Forbidden"}, connectorkiro.ErrBucketUnreachable},
		{"the whole account is barred", apiErr{"AllAccessDisabled"}, connectorkiro.ErrBucketUnreachable},
		{"our key is not valid", apiErr{"InvalidAccessKeyId"}, connectorkiro.ErrBucketUnreachable},
		{"no such bucket by code", apiErr{"NoSuchBucket"}, connectorkiro.ErrBucketUnreachable},

		// Under grant scope these are not the grant's problem and must NOT be settled
		// here: a listing that failed on one of them says nothing about an object.
		{"an encryption key we may not use", apiErr{"KMSAccessDenied"}, nil},
		{"the key is disabled", apiErr{"KMS.DisabledException"}, nil},
		{"the key is in the wrong state", apiErr{"KMS.KMSInvalidStateException"}, nil},
		{"the key is gone", apiErr{"KMS.NotFoundException"}, nil},
		{"the key id does not resolve", apiErr{"InvalidKeyId.NotFound"}, nil},
		{"the object needs restoring first", apiErr{"InvalidObjectState"}, nil},

		// Anything unrecognised must stay unclassified. Classifying by default would put
		// a throttle or a network blip into a class that accumulates toward abandoning a
		// report, which is the one mistake here that loses data silently.
		{"a throttle", apiErr{"SlowDown"}, nil},
		{"an internal error", apiErr{"InternalError"}, nil},
		{"a request timeout", apiErr{"RequestTimeout"}, nil},
		{"not an aws error at all", errors.New("dial tcp: i/o timeout"), nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyGrant(tc.err)
			require.ErrorIs(t, got, tc.err, "the cause must survive classification")

			if tc.want == nil {
				require.NotErrorIs(t, got, connectorkiro.ErrBucketUnreachable)
				require.NotErrorIs(t, got, connectorkiro.ErrObjectUnreadable)
				return
			}
			require.ErrorIs(t, got, tc.want)

			// The two are mutually exclusive: one settles an object, the other never does.
			other := connectorkiro.ErrObjectUnreadable
			if errors.Is(tc.want, connectorkiro.ErrObjectUnreadable) {
				other = connectorkiro.ErrBucketUnreachable
			}
			require.NotErrorIs(t, got, other, "a failure must not land in both classes")
		})
	}
}

// The classification has to survive the wrapping the gateway's own callers add, because
// the operation reads it several frames above where it was decided.
func TestClassificationSurvivesWrapping(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("copying an object: %w",
		fmt.Errorf("fetching a key: %w", classifyObject(apiErr{"KMSAccessDenied"})))

	require.ErrorIs(t, wrapped, connectorkiro.ErrObjectUnreadable)
	require.NotErrorIs(t, wrapped, connectorkiro.ErrBucketUnreachable)
}

// The correction this pair exists for. One code, two meanings, decided by what has already
// been proven when it arrives — and the case that matters is AccessDenied, because S3
// answers a GetObject it cannot decrypt with a plain AccessDenied at least as often as with
// a KMS-prefixed one. Classified by code alone it landed in the grant class, so the object
// never accrued a failure, never settled, and one unreadable file pinned the organisation
// at its day for good: the exact outcome the settling mechanism was built to prevent.
func TestTheSameDenialMeansTheGrantOnAListAndTheObjectOnARead(t *testing.T) {
	t.Parallel()

	for _, code := range []string{"AccessDenied", "Forbidden"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()

			// On a listing: nothing has been proven, so it is the grant.
			onList := classifyGrant(apiErr{code})
			require.ErrorIs(t, onList, connectorkiro.ErrBucketUnreachable)
			require.NotErrorIs(t, onList, connectorkiro.ErrObjectUnreadable)

			// On a steady-state read: the day listed under this grant and connect proved
			// the read, so what we cannot reach is this object.
			onRead := classifyObject(apiErr{code})
			require.ErrorIs(t, onRead, connectorkiro.ErrObjectUnreadable,
				"an object-scoped denial must be able to settle, or the mechanism is dead")
			require.NotErrorIs(t, onRead, connectorkiro.ErrBucketUnreachable)
		})
	}
}

func TestObjectScopeStillRecognisesABucketThatWentAway(t *testing.T) {
	t.Parallel()

	// A bucket can be deleted between the listing and the fetch. That is not the object's
	// fault and must not accrue against it.
	got := classifyObject(&types.NoSuchBucket{})
	require.ErrorIs(t, got, connectorkiro.ErrBucketUnreachable)
	require.NotErrorIs(t, got, connectorkiro.ErrObjectUnreadable)
}

func TestObjectScopeCoversTheKMSCodesAWSActuallyReturns(t *testing.T) {
	t.Parallel()

	// The dotted form was missing while its three siblings were present — the shape of a
	// list written from memory rather than from the API's own naming.
	for _, code := range []string{
		"KMS.AccessDeniedException", "KMS.DisabledException",
		"KMS.KMSInvalidStateException", "KMS.NotFoundException",
		"KMSAccessDenied", "InvalidKeyId.NotFound",
		// Restorable rather than permanent, and settled anyway: unclassified it would
		// retry forever and one archived file would hold the mark for the whole
		// organisation. The page names the restore and the job that re-collects.
		"InvalidObjectState",
	} {
		require.ErrorIs(t, classifyObject(apiErr{code}), connectorkiro.ErrObjectUnreadable, code)
	}

	// A throttle or a blip on a read must stay unclassified, or a transient failure
	// accumulates toward abandoning a report.
	for _, code := range []string{"SlowDown", "InternalError", "RequestTimeout"} {
		got := classifyObject(apiErr{code})
		require.NotErrorIs(t, got, connectorkiro.ErrObjectUnreadable, code)
		require.NotErrorIs(t, got, connectorkiro.ErrBucketUnreachable, code)
	}
}
