// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Every one of these is a case the connect dialogue should already have prevented, which
// is exactly why they are asserted here: this is the second line, and a configuration that
// cannot address a bucket must fail as a configuration fault rather than be walked.
func TestValidateSettingsRefusesAConfigurationThatCannotAddressABucket(t *testing.T) {
	t.Parallel()

	whole := Settings{Bucket: "acme-kiro-reports", Prefix: "kiro", Region: "us-east-1", AccountID: "123456789012"}
	require.NoError(t, validateSettings(whole), "a whole configuration must pass")

	tests := []struct {
		name string
		edit func(s *Settings)
		want string
	}{
		{"no bucket", func(s *Settings) { s.Bucket = "" }, "bucket is required"},
		{"blank bucket", func(s *Settings) { s.Bucket = "   " }, "bucket is required"},
		// The vendor refuses to write reports to a bucket root, so an empty prefix
		// describes a layout it never produces.
		{"no prefix", func(s *Settings) { s.Prefix = "" }, "prefix is required"},
		{"no region", func(s *Settings) { s.Region = "" }, "region is required"},
		{"no account id", func(s *Settings) { s.AccountID = "" }, "account id is required"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := whole
			tc.edit(&s)
			err := validateSettings(s)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A stored role nothing reads is the failure that looks like a success: reads would go out
// under the ambient principal while an operator believed they were scoped by the role. The
// refusal is what makes that impossible rather than merely unlikely.
func TestValidateCredentialsRefusesARoleThisConnectorWouldNotUse(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateCredentials(Credentials{}), "the default grant carries no secret at all")

	for _, c := range []Credentials{
		{RoleARN: "arn:aws:iam::123456789012:role/KiroRead"},
		{ExternalID: "org-1234"},
		{RoleARN: "arn:aws:iam::123456789012:role/KiroRead", ExternalID: "org-1234"},
	} {
		err := validateCredentials(c)
		require.Error(t, err, "a configuration naming a role must be refused, not quietly ignored")
		assert.Contains(t, err.Error(), "assumes no role")
	}
}

// Zero means the default rather than zero, and the difference is not cosmetic: a zero
// trailing window would stop collecting corrections, and a zero first-run cap would walk
// no days at all.
func TestTheDayDialsFallBackToTheirDefaults(t *testing.T) {
	t.Parallel()

	var unset Settings
	assert.Equal(t, DefaultTrailingOverlapDays, unset.GetTrailingOverlapDays())
	assert.Equal(t, DefaultFirstRunWindowDays, unset.GetFirstRunWindowDays())

	set := Settings{TrailingOverlapDays: 7, FirstRunWindowDays: 30}
	assert.Equal(t, 7, set.GetTrailingOverlapDays(), "an override must be honoured")
	assert.Equal(t, 30, set.GetFirstRunWindowDays(), "an override must be honoured")

	// Negative is nonsense rather than a smaller window, and falling back is the only
	// reading that cannot produce an empty or inverted walk.
	negative := Settings{TrailingOverlapDays: -1, FirstRunWindowDays: -1}
	assert.Equal(t, DefaultTrailingOverlapDays, negative.GetTrailingOverlapDays())
	assert.Equal(t, DefaultFirstRunWindowDays, negative.GetFirstRunWindowDays())
}

func TestParseConfigReadsBothBlobsAndToleratesEmptyOnes(t *testing.T) {
	t.Parallel()

	settings, err := json.Marshal(Settings{
		Bucket: "acme-kiro-reports", Prefix: "kiro", Region: "us-east-1", AccountID: "123456789012",
	})
	require.NoError(t, err)

	creds, parsed, err := ParseConfig(&sdk.ConnectorConfig{Settings: settings})
	require.NoError(t, err)
	assert.Equal(t, "acme-kiro-reports", parsed.Bucket)
	assert.Equal(t, Credentials{}, creds, "an organisation on the default grant stores no credentials")

	_, _, err = ParseConfig(&sdk.ConnectorConfig{Settings: json.RawMessage(`{"bucket":`)})
	require.Error(t, err, "unparseable settings must not read as an empty configuration")
	assert.Contains(t, err.Error(), "parsing settings")
}

// ValidateConfig is what the orchestrator calls, so the composition has to fail on either
// half rather than only the one that happens to be checked first.
func TestValidateConfigFailsOnEitherHalf(t *testing.T) {
	t.Parallel()

	c := NewConnector()
	whole, err := json.Marshal(Settings{
		Bucket: "acme-kiro-reports", Prefix: "kiro", Region: "us-east-1", AccountID: "123456789012",
	})
	require.NoError(t, err)
	role, err := json.Marshal(Credentials{RoleARN: "arn:aws:iam::1:role/r", ExternalID: "x"})
	require.NoError(t, err)

	require.NoError(t, c.ValidateConfig(t.Context(), &sdk.ConnectorConfig{Settings: whole}))

	err = c.ValidateConfig(t.Context(), &sdk.ConnectorConfig{Settings: whole, Credentials: role})
	require.Error(t, err, "a role must fail validation even when the settings are whole")

	short, err := json.Marshal(Settings{Bucket: "acme-kiro-reports"})
	require.NoError(t, err)
	require.Error(t, c.ValidateConfig(t.Context(), &sdk.ConnectorConfig{Settings: short}),
		"incomplete settings must fail even with no credentials at all")
}
