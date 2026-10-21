// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

type memUpdater struct{ m *memstore.Store }

func (u memUpdater) UpdateSettings(ctx context.Context, org string, id sdk.ConnectorID, s json.RawMessage) error {
	cfg, err := u.m.Get(ctx, org, id)
	if err != nil {
		return err
	}
	cfg.Settings = s
	return u.m.Save(ctx, cfg)
}

func (u memUpdater) PatchSettings(ctx context.Context, org string, id sdk.ConnectorID, fn func(json.RawMessage) (json.RawMessage, error)) error {
	return u.m.PatchSettings(ctx, org, id, fn)
}

// Saving discovered projects patches only discovered_repos: an operator's
// settings change made during the run survives.
func TestSaveDiscoveredReposKeepsAConcurrentSettingsChange(t *testing.T) {
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-000000000002"
	m := memstore.New()
	require.NoError(t, m.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "gitlab",
		Settings: json.RawMessage(`{"group_id":1}`), Enabled: true}))
	snapshot, err := m.Get(ctx, org, "gitlab")
	require.NoError(t, err)
	require.NoError(t, m.PatchSettings(ctx, org, "gitlab", func(json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"group_id":2,"operator_key":"kept"}`), nil
	}))

	var snapSettings GitLabSettings
	require.NoError(t, json.Unmarshal(snapshot.Settings, &snapSettings))
	saveDiscoveredRepos(ctx, &sdk.ExecutionParams{Config: snapshot, ConfigUpdater: memUpdater{m}}, snapSettings,
		[]glProject{{ID: 7, PathWithNamespace: "g/p", DefaultBranch: "main"}})

	after, err := m.Get(ctx, org, "gitlab")
	require.NoError(t, err)
	var stored map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(after.Settings, &stored))
	assert.JSONEq(t, `2`, string(stored["group_id"]))
	assert.JSONEq(t, `"kept"`, string(stored["operator_key"]))
	assert.Contains(t, stored, "discovered_repos")
}
