// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestHasCategory_VCSConnector(t *testing.T) {
	reg := testRegistry()

	c, err := reg.Get("github")
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, sdk.HasCategory(c, sdk.CategoryVCS),
		"github connector must be identified as VCS")
}

func TestHasCategory_NonVCSConnector(t *testing.T) {
	reg := testRegistry()

	c, err := reg.Get("jira")
	if err != nil {
		t.Fatal(err)
	}
	assert.False(t, sdk.HasCategory(c, sdk.CategoryVCS),
		"jira connector must not be identified as VCS")
	assert.True(t, sdk.HasCategory(c, sdk.CategoryIssueTracking),
		"jira connector must be identified as IssueTracking")
}

func TestHasCategory_NilConnector(t *testing.T) {
	assert.False(t, sdk.HasCategory(nil, sdk.CategoryVCS),
		"nil connector must return false without panicking")
}
