// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestGetLookbackDays covers the fallback, which is the whole point of the accessor: a
// zero is "no override set", not "read nothing".
func TestGetLookbackDays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings *Settings
		want     int
	}{
		{"nil settings fall back", nil, DefaultLookbackDays},
		{"unset falls back", &Settings{}, DefaultLookbackDays},
		{"zero falls back rather than reading nothing", &Settings{LookbackDays: 0}, DefaultLookbackDays},
		{"a negative value cannot mean a window, so it falls back", &Settings{LookbackDays: -5}, DefaultLookbackDays},
		{"an override is honoured", &Settings{LookbackDays: 30}, 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.settings.GetLookbackDays())
		})
	}
}

// TestParseCredentials refuses the shapes that would otherwise build a gateway sending no
// credential at all, which reaches the vendor as a rejected key rather than as the
// configuration problem it is.
func TestParseCredentials(t *testing.T) {
	t.Parallel()

	t.Run("a stored key round-trips", func(t *testing.T) {
		t.Parallel()
		creds, err := ParseCredentials(&sdk.ConnectorConfig{Credentials: []byte(`{"api_key":"sk-ant-api01-x"}`)})
		require.NoError(t, err)
		assert.Equal(t, "sk-ant-api01-x", creds.APIKey)
	})

	for _, tt := range []struct {
		name string
		cfg  *sdk.ConnectorConfig
	}{
		{"nil config", nil},
		{"no credentials stored", &sdk.ConnectorConfig{}},
		{"unreadable blob", &sdk.ConnectorConfig{Credentials: []byte(`{`)}},
		{"present but empty key", &sdk.ConnectorConfig{Credentials: []byte(`{"api_key":""}`)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseCredentials(tt.cfg)
			require.Error(t, err)
		})
	}
}

// TestParseSettings pins that absent settings are not an error. Every field has a default,
// and a configuration saved before a field existed carries none of it — refusing that
// would break an organisation that connected earlier.
func TestParseSettings(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		cfg  *sdk.ConnectorConfig
	}{
		{"nil config", nil},
		{"no settings stored", &sdk.ConnectorConfig{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := ParseSettings(tt.cfg)
			require.NoError(t, err)
			assert.Equal(t, DefaultLookbackDays, s.GetLookbackDays())
		})
	}

	t.Run("a stored organisation and window round-trip", func(t *testing.T) {
		t.Parallel()
		s, err := ParseSettings(&sdk.ConnectorConfig{
			Settings: []byte(`{"anthropic_organization_id":"org_abc","lookback_days":30}`),
		})
		require.NoError(t, err)
		assert.Equal(t, "org_abc", s.AnthropicOrganizationID)
		assert.Equal(t, 30, s.GetLookbackDays())
	})

	t.Run("an unreadable blob is an error, never a silent default", func(t *testing.T) {
		t.Parallel()
		_, err := ParseSettings(&sdk.ConnectorConfig{Settings: []byte(`{`)})
		require.Error(t, err)
	})
}
