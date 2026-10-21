// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"slices"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/auth/integrations"

	"github.com/tetriz-ai/tetriz-connectors/connectors/anthropic"
	"github.com/tetriz-ai/tetriz-connectors/connectors/codex"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/connectorsource"

	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestShippedConnectorsConform runs the declaration checks over every
// connector the runner registers.
func TestShippedConnectorsConform(t *testing.T) {
	reg := NewRegistry(nil)
	conns := reg.List()
	var ids []string
	for _, c := range conns {
		ids = append(ids, string(c.ID()))
	}
	slices.Sort(ids)
	want := []string{"bitbucket", "claude-code", "codex", "cursor", "github", "gitlab", "jira", "kiro", "linear"}
	if !slices.Equal(ids, want) {
		t.Fatalf("registry holds %v, want %v", ids, want)
	}
	for _, c := range conns {
		t.Run(string(c.ID()), func(t *testing.T) { testkit.RunConformance(t, c) })
	}
}

// The schedule source routes Codex and Claude Code to the individual-tenant
// lane by id. The connector packages declare their ids separately, so this
// keeps the two declarations equal.
func TestIndividualLaneIDsMatchConnectors(t *testing.T) {
	if got := codex.NewConnector().ID(); got != connectorsource.ConnectorIDCodex {
		t.Fatalf("codex id %q, schedule source expects %q", got, connectorsource.ConnectorIDCodex)
	}
	if got := anthropic.NewConnector().ID(); got != connectorsource.ConnectorIDClaudeCode {
		t.Fatalf("anthropic id %q, schedule source expects %q", got, connectorsource.ConnectorIDClaudeCode)
	}
}

// The integrations catalog lists exactly the connectors this module ships,
// plus github-issues, a setting of the GitHub connector.
func TestCatalogListsTheShippedConnectors(t *testing.T) {
	want := map[string]bool{"github-issues": true}
	for _, c := range NewRegistry(nil).List() {
		want[string(c.ID())] = true
	}
	got := map[string]bool{}
	for _, cat := range integrations.Catalog().GetCategories() {
		for _, it := range cat.GetIntegrations() {
			if got[it.GetId()] {
				t.Errorf("catalog lists %q twice", it.GetId())
			}
			got[it.GetId()] = true
		}
	}
	for id := range want {
		if !got[id] {
			t.Errorf("catalog is missing shipped connector %q", id)
		}
	}
	for id := range got {
		if !want[id] {
			t.Errorf("catalog lists %q, which this module does not ship", id)
		}
	}
}
