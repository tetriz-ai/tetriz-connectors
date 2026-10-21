// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// One vendor resource reached with access the runner holds or mints belongs to
// one organization; one reached with a credential each organization brings may
// be connected by several. Each row is one decision of the claims sweep.
func TestVendorResourceClaims(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("vendor claims as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	repo := store.NewConnectorConfigRepo(db, nil)
	tx := store.PostgresTxRunner{}
	for i, tc := range []struct {
		connector, settings string
		index               string // the claim index; empty when sharing is allowed
	}{
		{"github", `{"organization":"acme","installation_id":"777"}`, "uniq_github_installation_per_org"},
		{"kiro", `{"bucket":"reports-bucket","prefix":"p"}`, "uniq_kiro_bucket_per_org"},
		{"claude-code", `{"anthropic_organization_id":"org-anthropic-1"}`, "uniq_anthropic_organization_per_org"},
		{"codex", `{"workspace_id":"ws-1"}`, "uniq_codex_workspace_per_org"},
		{"cursor", `{}`, ""},
		{"jira", `{"cloud_id":"cloud-1","site_url":"https://example.atlassian.net"}`, ""},
		{"gitlab", `{"group_id":42}`, ""},
		{"bitbucket", `{"workspace":"acme"}`, ""},
		{"linear", `{"workspace_id":"lin-1"}`, ""},
	} {
		t.Run(tc.connector, func(t *testing.T) {
			save := func(org string) error {
				return tx.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
					return repo.Save(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: sdk.ConnectorID(tc.connector),
						Credentials: json.RawMessage(`{"token":"t"}`), Settings: json.RawMessage(tc.settings), Enabled: true})
				})
			}
			orgA := fmt.Sprintf("00000000-0000-4000-8000-0000000e%04d", 2*i)
			orgB := fmt.Sprintf("00000000-0000-4000-8000-0000000e%04d", 2*i+1)
			require.NoError(t, save(orgA))
			err := save(orgB)
			if tc.index == "" {
				assert.NoError(t, err, "each organization brings its own credential, so the resource can be shared")
				return
			}
			require.Error(t, err, "a second organization's claim is refused")
			assert.Contains(t, err.Error(), tc.index)
		})
	}
}
