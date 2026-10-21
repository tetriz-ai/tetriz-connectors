// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package settingskey

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const edgeOrg = "00000000-0000-4000-8000-0000000e5e01"

// updater is memstore's PatchSettings as an sdk.ConfigUpdater; Patch never
// calls UpdateSettings.
type updater struct{ *memstore.Store }

func (updater) UpdateSettings(context.Context, string, sdk.ConnectorID, json.RawMessage) error {
	return errors.New("Patch must not call UpdateSettings")
}

func storeWith(t *testing.T, settings string) *memstore.Store {
	t.Helper()
	mem := memstore.New()
	cfg := &sdk.ConnectorConfig{OrganizationID: edgeOrg, ConnectorID: "edge", Enabled: true}
	if settings != "" {
		cfg.Settings = json.RawMessage(settings)
	}
	require.NoError(t, mem.Save(context.Background(), cfg))
	return mem
}

func stored(t *testing.T, mem *memstore.Store) map[string]json.RawMessage {
	t.Helper()
	cfg, err := mem.Get(context.Background(), edgeOrg, "edge")
	require.NoError(t, err)
	out := map[string]json.RawMessage{}
	if len(cfg.Settings) > 0 && string(cfg.Settings) != "null" {
		require.NoError(t, json.Unmarshal(cfg.Settings, &out))
	}
	return out
}

func set(v any) func(json.RawMessage) (any, bool, error) {
	return func(json.RawMessage) (any, bool, error) { return v, true, nil }
}

// Patching one key leaves every other key, including ones no connector
// models and values whose precision a float round trip would lose, as stored.
func TestEdge_PatchKeepsOtherKeysByteForByte(t *testing.T) {
	t.Parallel()
	mem := storeWith(t, `{"unmodeled":{"deep":[1,2,{"x":null}]},"big":12345678901234567890,"cache":"old"}`)
	require.NoError(t, Patch(context.Background(), updater{mem}, edgeOrg, "edge", "cache", set("new")))
	got := stored(t, mem)
	assert.JSONEq(t, `{"deep":[1,2,{"x":null}]}`, string(got["unmodeled"]))
	assert.Equal(t, "12345678901234567890", string(got["big"]), "a large integer is not rounded through float64")
	assert.JSONEq(t, `"new"`, string(got["cache"]))
}

// Stored settings that are null or empty count as an empty object: the key
// is added and the absent value reaches build as nil.
func TestEdge_PatchOnNullOrEmptySettings(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"", "null", "{}"} {
		mem := storeWith(t, s)
		seen := json.RawMessage("sentinel")
		require.NoError(t, Patch(context.Background(), updater{mem}, edgeOrg, "edge", "k",
			func(cur json.RawMessage) (any, bool, error) { seen = cur; return 1, true, nil }), s)
		assert.Nil(t, seen, "absent key reaches build as nil for stored %q", s)
		assert.Equal(t, map[string]json.RawMessage{"k": json.RawMessage("1")}, stored(t, mem), s)
	}
}

// build reporting no change writes nothing; build's error is returned and
// writes nothing; stored settings that are not an object are an error, not
// overwritten.
func TestEdge_PatchNoChangeErrorsAndNonObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mem := storeWith(t, `{"k":"v"}`)
	before, err := mem.Get(ctx, edgeOrg, "edge")
	require.NoError(t, err)
	require.NoError(t, Patch(ctx, updater{mem}, edgeOrg, "edge", "k", func(cur json.RawMessage) (any, bool, error) {
		assert.JSONEq(t, `"v"`, string(cur))
		return "ignored", false, nil
	}))
	after, err := mem.Get(ctx, edgeOrg, "edge")
	require.NoError(t, err)
	assert.Equal(t, before.Settings, after.Settings)

	boom := errors.New("boom")
	err = Patch(ctx, updater{mem}, edgeOrg, "edge", "k", func(json.RawMessage) (any, bool, error) { return "x", true, boom })
	require.ErrorIs(t, err, boom)
	assert.JSONEq(t, `"v"`, string(stored(t, mem)["k"]))

	for _, s := range []string{`[1,2]`, `"str"`, `7`} {
		mem := storeWith(t, s)
		err := Patch(ctx, updater{mem}, edgeOrg, "edge", "k", set(1))
		require.Error(t, err, s)
		assert.Contains(t, err.Error(), "parse stored settings")
	}
}

// A value that cannot be marshalled is an error and leaves the settings alone.
func TestEdge_PatchUnmarshalableValue(t *testing.T) {
	t.Parallel()
	mem := storeWith(t, `{"k":"v"}`)
	err := Patch(context.Background(), updater{mem}, edgeOrg, "edge", "k", set(make(chan int)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal k")
	assert.JSONEq(t, `"v"`, string(stored(t, mem)["k"]))
}

// A connector with no stored config is the store's not-found error, not a
// silently created row.
func TestEdge_PatchWithoutAConfig(t *testing.T) {
	t.Parallel()
	mem := memstore.New()
	err := Patch(context.Background(), updater{mem}, edgeOrg, "edge", "k", set(1))
	require.Error(t, err)
	_, getErr := mem.Get(context.Background(), edgeOrg, "edge")
	assert.Error(t, getErr, "no row was created")
}
