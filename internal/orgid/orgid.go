// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package orgid canonicalizes organization ids. A UUID has several spellings
// (upper case, urn:uuid:, braces); the store scopes a transaction to the
// canonical lower-case form, so a row written under another spelling fails
// row-level security. Every entry point canonicalizes the id before it
// reaches the store.
package orgid

import "github.com/google/uuid"

// Canonical returns id's canonical UUID form, or id unchanged when it is not a
// UUID.
func Canonical(id string) string {
	u, err := uuid.Parse(id)
	if err != nil {
		return id
	}
	return u.String()
}
