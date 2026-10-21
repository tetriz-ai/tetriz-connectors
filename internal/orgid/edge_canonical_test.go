// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package orgid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const canonical = "aaaaaaaa-0000-4000-8000-0000000000f1"

// Every spelling uuid.Parse accepts maps to the one lower-case form the store
// scopes row-level security to, and canonicalising is idempotent.
func TestEdge_CanonicalSpellings(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		canonical,
		"AAAAAAAA-0000-4000-8000-0000000000F1",
		"urn:uuid:" + canonical,
		"URN:UUID:" + canonical,
		"{" + canonical + "}",
		"{AAAAAAAA-0000-4000-8000-0000000000F1}",
		"aaaaaaaa000040008000" + "0000000000f1",
	} {
		got := Canonical(in)
		assert.Equal(t, canonical, got, in)
		assert.Equal(t, got, Canonical(got), "idempotent for %q", in)
	}
}

// A value that is not a UUID is returned unchanged, so a caller's own
// validation (not this function) decides how to reject it; nothing is
// trimmed or lower-cased into a different organization's id.
func TestEdge_NonUUIDIsUnchanged(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"", "ORG-1", " " + canonical, canonical + " ", canonical + "0",
		"aaaaaaaa-0000-4000-8000-0000000000g1", "system",
	} {
		assert.Equal(t, in, Canonical(in), "%q", in)
	}
}
