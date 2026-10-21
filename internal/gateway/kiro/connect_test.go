// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A delimited listing returns whole prefixes, and the caller wants the one segment it did
// not already know. Getting that slice wrong is invisible: the walk would descend into a
// path built from a mangled segment, find nothing, and report the bucket as empty rather
// than as mis-parsed.
func TestASegmentIsTheNameBelowThePrefixItWasListedUnder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		parent string
		full   string
		want   string
	}{
		{
			name:   "a year under the report root",
			parent: "kiro/AWSLogs/123456789012/KiroLogs/user_report/us-east-1/",
			full:   "kiro/AWSLogs/123456789012/KiroLogs/user_report/us-east-1/2026/",
			want:   "2026",
		},
		{
			name:   "a day under a month",
			parent: "kiro/AWSLogs/123456789012/KiroLogs/user_report/us-east-1/2026/08/",
			full:   "kiro/AWSLogs/123456789012/KiroLogs/user_report/us-east-1/2026/08/30/",
			want:   "30",
		},
		{
			// The administrator's own prefix may contain the vendor's marker segment. The
			// slice is positional rather than a search, so it is unaffected — which is the
			// property worth pinning, because the same recovery done by searching for that
			// marker would find the administrator's copy instead of the vendor's.
			name:   "an administrator prefix that itself contains the marker",
			parent: "logs/AWSLogs/AWSLogs/1/KiroLogs/user_report/us-east-1/",
			full:   "logs/AWSLogs/AWSLogs/1/KiroLogs/user_report/us-east-1/2026/",
			want:   "2026",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, trimTrailingSlash(tc.full[len(tc.parent):]))
		})
	}
}

func TestTrailingSlashIsRemovedOnlyWhenPresent(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "2026", trimTrailingSlash("2026/"))
	assert.Equal(t, "2026", trimTrailingSlash("2026"))
	assert.Empty(t, trimTrailingSlash(""))
	assert.Empty(t, trimTrailingSlash("/"))
}

// The region lookup starts from us-east-1; the lookup itself reports the
// bucket's real region.
func TestBootstrapRegion(t *testing.T) {
	if bootstrapRegion != "us-east-1" {
		t.Fatalf("bootstrapRegion = %q, want us-east-1", bootstrapRegion)
	}
}
