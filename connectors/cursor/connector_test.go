// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestConnector_Identity(t *testing.T) {
	c := NewConnector()

	assert.Equal(t, sdk.ConnectorID("cursor"), c.ID())
	assert.Equal(t, "Cursor", c.DisplayName())
}

// TestConnector_IsAnalyticsNotVCS is not a restatement of Categories(): the
// source-control guard keys on CategoryVCS, so a Cursor connector mis-classified
// as VCS would start refusing to connect whenever an organisation already had a
// source-control connector. Asserting the negative is the point.
func TestConnector_IsAnalyticsNotVCS(t *testing.T) {
	c := NewConnector()

	assert.Equal(t, []sdk.ConnectorCategory{sdk.CategoryAnalytics}, c.Categories())
	assert.True(t, sdk.HasCategory(c, sdk.CategoryAnalytics))
	assert.False(t, sdk.HasCategory(c, sdk.CategoryVCS),
		"classifying Cursor as VCS would make the single-source-control guard fire on it")
}

// TestConnector_UsageOperationDeclared pins the descriptor's load-bearing fields, each
// of which fails silently if wrong: an empty schedule emits nothing, a non-pull type is
// skipped by the schedule source, and the scheduler's default timeout cancels a
// ninety-day first sync partway.
func TestConnector_UsageOperationDeclared(t *testing.T) {
	ops := NewConnector().Operations()
	require.Len(t, ops, 1)
	op := ops[0]

	assert.Equal(t, sdk.OperationID("fetch_usage_events"), op.ID)
	assert.Equal(t, sdk.IngestionPull, op.IngestionType,
		"the schedule source only emits pull operations")
	assert.NotEmpty(t, op.DefaultSchedule,
		"an empty schedule emits no entry at all, silently")
	assert.Greater(t, op.ExecutionTimeout, 10*time.Minute,
		"a first sync is orders of magnitude larger than a daily one; the scheduler default cancels it")
	require.NotNil(t, op.Factory)
	assert.NotNil(t, op.Factory(), "a fresh instance per execution prevents state leaking between runs")

	// Cursor is org-wide analytics and branches on neither, so both must stay false —
	// RequiresMultiUser would drop the schedule for an individual tenant, and
	// RequiresTenantType fails the run before Execute when the type is unresolved.
	assert.False(t, op.RequiresMultiUser)
	assert.False(t, op.RequiresTenantType)
}

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name         string
		credentials  string
		settings     string
		wantErr      string
		wantKey      string
		wantLookback int
	}{
		{
			name:         "credentials and settings both present",
			credentials:  `{"api_key":"crsr_abc"}`,
			settings:     `{"lookback_days":30}`,
			wantKey:      "crsr_abc",
			wantLookback: 30,
		},
		{
			name:         "empty settings falls back to the default lookback",
			credentials:  `{"api_key":"crsr_abc"}`,
			settings:     `{}`,
			wantKey:      "crsr_abc",
			wantLookback: DefaultLookbackDays,
		},
		{
			name:        "malformed credentials",
			credentials: `{"api_key":`,
			settings:    `{}`,
			wantErr:     "parse credentials",
		},
		{
			name:        "malformed settings",
			credentials: `{"api_key":"crsr_abc"}`,
			settings:    `{"lookback_days":`,
			wantErr:     "parse settings",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds, settings, err := ParseConfig(&sdk.ConnectorConfig{
				Credentials: json.RawMessage(tt.credentials),
				Settings:    json.RawMessage(tt.settings),
			})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantKey, creds.APIKey)
			assert.Equal(t, tt.wantLookback, settings.GetLookbackDays())
		})
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name        string
		credentials string
		settings    string
		wantErr     string
	}{
		{
			name:        "valid",
			credentials: `{"api_key":"crsr_abc"}`,
			settings:    `{}`,
		},
		{
			name:        "missing api key",
			credentials: `{}`,
			settings:    `{}`,
			wantErr:     "credentials must include api_key",
		},
		{
			name:        "empty api key is the same as missing",
			credentials: `{"api_key":""}`,
			settings:    `{}`,
			wantErr:     "credentials must include api_key",
		},
		{
			name:        "negative lookback is rejected rather than inverting the window",
			credentials: `{"api_key":"crsr_abc"}`,
			settings:    `{"lookback_days":-7}`,
			wantErr:     "lookback_days must not be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewConnector().ValidateConfig(context.Background(), &sdk.ConnectorConfig{
				Credentials: json.RawMessage(tt.credentials),
				Settings:    json.RawMessage(tt.settings),
			})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Contains(t, err.Error(), "cursor:", "errors carry the connector prefix")
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestGetLookbackDays_ZeroMeansDefault pins that zero is legal and means the
// default, rather than meaning "read nothing" — an operation reading the raw
// field would produce an empty first sync.
func TestGetLookbackDays_ZeroMeansDefault(t *testing.T) {
	assert.Equal(t, DefaultLookbackDays, CursorSettings{}.GetLookbackDays())
	assert.Equal(t, 14, CursorSettings{LookbackDays: 14}.GetLookbackDays())
}

// interface satisfaction is asserted at compile time, so a signature drift on
// sdk.Connector fails the build rather than a test run.
var _ sdk.Connector = (*CursorConnector)(nil)

func TestConnector_RegistersAndResolves(t *testing.T) {
	reg := sdk.NewConnectorRegistry()
	reg.Register(NewConnector())

	got, err := reg.Get(connectorID)
	require.NoError(t, err)
	assert.Equal(t, connectorID, got.ID())
}
