// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	bitbucketpb "github.com/tetriz-ai/tetriz-connectors/gen/bitbucket/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"
)

// TestLoadCursorStates_OlderAndNewerShapes: a struct from a newer build carrying
// fields this build does not know loads with its instant intact (offset and
// microseconds preserved). An entry that is not an object, such as a bare
// timestamp, and a JSON null entry are dropped rather than read as a watermark.
func TestLoadCursorStates_OlderAndNewerShapes(t *testing.T) {
	recent := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	local := recent.In(time.FixedZone("UTC+2", 2*3600))
	parts := map[string]json.RawMessage{
		"ws/bare":   json.RawMessage(`"` + local.Format(time.RFC3339Nano) + `"`),
		"ws/future": json.RawMessage(`{"timestamp":"` + local.Format(time.RFC3339Nano) + `","pc":"3","from_v9":{"x":1}}`),
		"ws/null":   json.RawMessage(`null`),
	}
	states, dropped := loadCursorStates(context.Background(), sdk.NewCompositeCursor(parts))
	assert.False(t, dropped)
	require.Contains(t, states, "ws/future")
	assert.NotContains(t, states, "ws/bare")
	assert.NotContains(t, states, "ws/null")
	assert.True(t, states["ws/future"].Timestamp.Equal(recent))

	// Round trip through the writer keeps the instant.
	back, _ := loadCursorStates(context.Background(), buildStateCursor(context.Background(), states))
	assert.True(t, back["ws/future"].Timestamp.Equal(recent))
}

// TestParseConfig_UnknownSettingsAndEmptyRepositories: settings written by a newer
// build (unknown keys) still parse, and an explicit empty repositories list means
// "no manual override" exactly like an absent one, so the discovered cache is used
// and deleted repos stay excluded.
func TestParseConfig_UnknownSettingsAndEmptyRepositories(t *testing.T) {
	deleted, err := json.Marshal(bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED)
	require.NoError(t, err)
	raw := `{"workspace":"ws","repositories":[],"some_future_flag":true,` +
		`"discovered_repos":[{"full_name":"ws/a","status":"BITBUCKET_REPO_STATUS_ACTIVE","added_by_v9":1},` +
		`{"full_name":"ws/gone","status":` + string(deleted) + `}]}`
	_, settings, perr := parseConfig(&sdk.ConnectorConfig{
		Credentials: json.RawMessage(`{"oauth_access_token":"x"}`),
		Settings:    json.RawMessage(raw),
	})
	require.NoError(t, perr)
	require.NotNil(t, settings.Repositories)
	called := false
	gw := &mockVendorGateway{executeFunc: func(context.Context, proto.Message) (proto.Message, error) {
		called = true
		return bbReposResponse(nil, false), nil
	}}
	repos, err := fetchAllRepos(context.Background(), gw, settings, "fetch_prs", "org-test", &pacing.PacingBudget{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ws/a"}, repos)
	assert.False(t, called)
}
