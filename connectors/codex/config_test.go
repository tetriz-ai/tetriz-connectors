// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	codexconfig "github.com/tetriz-ai/tetriz-connectors/connectors/codex"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestParseSettingsRefusesAMissingWorkspace is the sync-time half of the invariant the
// connect path guards. The workspace is a path parameter, so a configuration without one
// cannot form a request — and returning an empty Settings would push that failure to the
// vendor, where a malformed path comes back 404 and reads as a wrong workspace rather
// than a missing one.
//
// It matters most for a row this package's own RPC did not write. A script or a repair
// writes bytes directly, passing no request-validation interceptor, so this is the only
// check standing between such a row and a live sync.
func TestParseSettingsRefusesAMissingWorkspace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *sdk.ConnectorConfig
		wantErr bool
	}{
		{name: "no settings at all", cfg: &sdk.ConnectorConfig{}, wantErr: true},
		{name: "settings without a workspace", cfg: &sdk.ConnectorConfig{Settings: []byte(`{"lookback_days":30}`)}, wantErr: true},
		{name: "a workspace carrying the record-key delimiter", cfg: &sdk.ConnectorConfig{Settings: []byte(`{"workspace_id":"ws|x"}`)}, wantErr: true},
		{name: "a workspace carrying a path separator", cfg: &sdk.ConnectorConfig{Settings: []byte(`{"workspace_id":"ws/../admin"}`)}, wantErr: true},
		{name: "a usable workspace", cfg: &sdk.ConnectorConfig{Settings: []byte(`{"workspace_id":"00000000-0000-4000-8000-00000000c0de"}`)}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := codexconfig.ParseSettings(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestValidWorkspaceIDIsTheVendorsUUID pins this function against what the endpoint actually
// addresses, and against the two jobs the identifier has to survive.
//
// The vendor parses the path segment as a UUID and answers a malformed one with 400
// `uuid_parsing` BEFORE it examines the key. So admitting a non-UUID here does not buy
// leniency, it just moves the refusal from the field the administrator typed into to a vendor
// error we then have to explain. The cases below are the shapes a paste or a typo plausibly
// produces — a slug, a name, an opaque handle.
//
// The record-key case is the late failure: a workspace carrying the delimiter would
// save an enabled connector and then fail every scheduled run as unkeyable. A UUID cannot
// contain it, so that is now excluded by construction — asserted anyway, because the guarantee
// is what the record identity depends on.
//
// The proto declares the same constraint and is enforced elsewhere (a request interceptor);
// this is enforced on every read of stored bytes. Nothing but an assertion keeps them the same,
// and if they drift the looser one becomes the real rule for any row the RPC did not write.
func TestValidWorkspaceIDIsTheVendorsUUID(t *testing.T) {
	t.Parallel()

	for _, ok := range []string{
		"00000000-0000-4000-8000-00000000c0de",
		"00000000-0000-4000-8000-00000000C0DE", // upper case is the same UUID
	} {
		assert.Truef(t, codexconfig.ValidWorkspaceID(ok), "%q is a UUID the endpoint can address", ok)
	}

	for _, bad := range []string{
		"",
		// The shapes someone reasonably pastes instead. Every one of these reached OpenAI
		// under the previous alphabet check and came back 400.
		"ws_abc123", "WS-Mixed.Case", "a", "ws.1-2_3", "my-workspace", "Acme Engineering",
		// Not-quite-UUIDs.
		"00000000-0000-4000-8000-00000000c0d",   // one char short
		"00000000-0000-4000-8000-00000000c0de1", // one char long
		"0000000000004000800000000000c0de",      // unhyphenated
		"00000000-0000-4000-8000-00000000c0zz",  // non-hex
		// Forms uuid.Parse accepts but the vendor path does not: these would be sent verbatim.
		"{00000000-0000-4000-8000-00000000c0de}",
		"urn:uuid:00000000-0000-4000-8000-00000000c0de",
		// The record-key delimiter, and the characters a hand-written denial list forgets.
		"00000000-0000-4000-8000-00000000c0de|x", "ws|x", "ws/x", "ws?x", "ws#x", "ws%x",
		"ws x", " ws", "ws ", "ws:x", "ws@x", "ws;x", `ws\x`, "ws+x", "ws=x", "ws\tx",
		"wsé", "ws\x00x",
	} {
		assert.Falsef(t, codexconfig.ValidWorkspaceID(bad), "%q is not a UUID and must be rejected here", bad)
	}
}

// TestLookbackSeparatesTheDefaultFromTheVendorCap. The two are equal today and are not the
// same decision: the default is how much history we choose to backfill, the cap is how far
// OpenAI will answer. Clamping rather than refusing keeps an over-large setting from
// reading as a broken sync — the request would come back empty either way.
func TestLookbackSeparatesTheDefaultFromTheVendorCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "unset falls back to the default", in: 0, want: codexconfig.DefaultLookbackDays},
		{name: "a negative value is not a window", in: -1, want: codexconfig.DefaultLookbackDays},
		{name: "a shorter window is honoured", in: 30, want: 30},
		{name: "past the vendor cap is clamped, not refused", in: 365, want: codexconfig.VendorLookbackCapDays},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &codexconfig.Settings{LookbackDays: tt.in}
			assert.Equal(t, tt.want, s.GetLookbackDays())
		})
	}
}

// TestParseCredentialsRefusesAnEmptyKey. A gateway built with an empty key sends no
// credentials at all, and the vendor's answer to that reads as a rejected key rather than
// as a missing one — which sends an administrator to re-paste a key that was never stored.
func TestParseCredentialsRefusesAnEmptyKey(t *testing.T) {
	t.Parallel()

	for _, cfg := range []*sdk.ConnectorConfig{
		{},
		{Credentials: []byte(`{}`)},
		{Credentials: []byte(`{"api_key":""}`)},
	} {
		_, err := codexconfig.ParseCredentials(cfg)
		require.Error(t, err)
	}
	creds, err := codexconfig.ParseCredentials(&sdk.ConnectorConfig{Credentials: []byte(`{"api_key":"sk-x"}`)})
	require.NoError(t, err)
	assert.Equal(t, "sk-x", creds.APIKey)
}
