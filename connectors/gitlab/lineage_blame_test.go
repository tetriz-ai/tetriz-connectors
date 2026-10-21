// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The provider-agnostic walk logic and its tests live in package lineage. This file tests
// only the GitLab Source specifics: flattening blame groups into owned lines, and the REST URL
// shapes.

// TestBlameEntriesToLines flattens GitLab's per-commit blame groups into one owned line per file
// line in order, carrying the owning commit and its author onto each line.
func TestBlameEntriesToLines(t *testing.T) {
	t.Parallel()
	var e1, e2 glBlameEntry
	e1.Commit.ID = "sha-a"
	e1.Commit.CommittedDate = time.Unix(1000, 0).UTC()
	e1.Commit.AuthorName = "dana"
	e1.Commit.AuthorEmail = "dana@example.com"
	e1.Lines = []string{"line one", "line two"}
	e2.Commit.ID = "sha-b"
	e2.Commit.AuthorName = "gale"
	e2.Lines = []string{"line three"}

	got := blameEntriesToLines([]glBlameEntry{e1, e2})
	require.Len(t, got, 3)
	assert.Equal(t, "line one", got[0].Text)
	assert.Equal(t, "sha-a", got[0].OwnerSHA)
	assert.Equal(t, "dana", got[0].AuthorName)
	assert.Equal(t, "dana@example.com", got[0].AuthorEmail)
	assert.False(t, got[0].CommittedAt.IsZero())
	assert.Equal(t, "line two", got[1].Text)
	assert.Equal(t, "sha-a", got[1].OwnerSHA, "both lines of an entry share its commit")
	assert.Equal(t, "line three", got[2].Text)
	assert.Equal(t, "sha-b", got[2].OwnerSHA)

	assert.Empty(t, blameEntriesToLines(nil), "no entries yields no lines")
}

// TestGitlabURLs pins the REST request shapes: the blame endpoint addresses the project, the
// slash-encoded file path, and the ref; the commit endpoint addresses the project and sha.
func TestGitlabURLs(t *testing.T) {
	t.Parallel()
	blame := gitlabBlameURL("org-1", "42", "pay/settle.go", "abc123")
	assert.Contains(t, blame, "/projects/42/repository/files/")
	assert.Contains(t, blame, "pay%2Fsettle.go", "the file path's slashes are encoded as %2F")
	assert.Contains(t, blame, "/blame?")
	assert.Contains(t, blame, "ref=abc123")

	commit := gitlabCommitURL("org-1", "42", "abc123")
	assert.Contains(t, commit, "/projects/42/repository/commits/abc123")
}
