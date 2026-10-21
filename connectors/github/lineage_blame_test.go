// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The provider-agnostic walk logic and its tests live in package lineage. This file tests
// only the GitHub Source specifics: mapping blame ranges + blob text onto owned lines, and the
// GraphQL query shapes.

// TestZipBlame maps blame ranges plus blob text onto owned lines with their author, leaves a line
// no range covers with an empty owner, and returns nothing for an absent path (empty text) so the
// walk fails closed rather than reading a phantom empty line.
func TestZipBlame(t *testing.T) {
	t.Parallel()
	var resp ghBlameResponse
	resp.Repository.Text.Text = "a\nb\nc"
	var r struct {
		StartingLine int `json:"startingLine"`
		EndingLine   int `json:"endingLine"`
		Commit       struct {
			OID           string    `json:"oid"`
			CommittedDate time.Time `json:"committedDate"`
			Author        struct {
				Name  string `json:"name"`
				Email string `json:"email"`
			} `json:"author"`
		} `json:"commit"`
	}
	r.StartingLine, r.EndingLine = 1, 2
	r.Commit.OID = "deadbeef"
	r.Commit.Author.Name = "dana"
	r.Commit.Author.Email = "dana@example.com"
	resp.Repository.Object.Blame.Ranges = append(resp.Repository.Object.Blame.Ranges, r)

	got := zipBlame(resp)
	require.Len(t, got, 3)
	assert.Equal(t, "deadbeef", got[0].OwnerSHA)
	assert.Equal(t, "dana", got[0].AuthorName)
	assert.Equal(t, "dana@example.com", got[0].AuthorEmail)
	assert.Equal(t, "deadbeef", got[1].OwnerSHA)
	assert.Empty(t, got[2].OwnerSHA, "an uncovered line has no owner")

	var empty ghBlameResponse
	assert.Empty(t, zipBlame(empty), "an absent path (empty text) yields no lines")
}

// TestBuildQueries pins the request shape, the layer a live GitHub call cannot be tested at
// locally: the blame query addresses the revision and path, asks for the blob text, and requests
// the commit's author (name and email); the parent query asks for the first parent.
func TestBuildQueries(t *testing.T) {
	t.Parallel()
	blame := buildBlameQuery("acme", "billing", "abc123", "pay/settle.go")
	assert.Contains(t, blame, `owner: "acme"`)
	assert.Contains(t, blame, `name: "billing"`)
	// Commits are addressed by object(expression: <sha>), not oid: — a bare SHA is a valid revspec
	// and oid: did not resolve the blame walk against the live API. The blob keeps the sha:path form.
	assert.Contains(t, blame, `object(expression: "abc123")`)
	assert.Contains(t, blame, `blame(path: "pay/settle.go")`)
	assert.Contains(t, blame, `object(expression: "abc123:pay/settle.go")`)
	assert.Contains(t, blame, `author { name email }`)

	parent := buildFirstParentQuery("acme", "billing", "abc123")
	assert.Contains(t, parent, `parents(first: 1)`)
	assert.Contains(t, parent, `object(expression: "abc123")`)
}
