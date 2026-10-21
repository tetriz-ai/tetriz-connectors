// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// edgeClearEnv blanks every variable FromEnv reads, so a developer's shell
// cannot change a case's outcome.
func edgeClearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"TETRIZ_POSTGRES_DSN", "TETRIZ_REDIS_ADDR", "TETRIZ_CREDENTIALS_KEY", "TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS",
		"TETRIZ_SINK", "TETRIZ_BLOB_DIR", "TETRIZ_HTTP_ADDR", "TETRIZ_METRICS_PREFIX", "TETRIZ_CONCURRENCY",
		"TETRIZ_DEFAULT_TENANT_KIND", "TETRIZ_TENANT_KINDS_FILE", "TETRIZ_GITHUB_APP_ID", "TETRIZ_GITHUB_APP_PRIVATE_KEY",
		"TETRIZ_REDIS_URL", "TETRIZ_REDIS_PASSWORD", "TETRIZ_NAMESPACE", "TETRIZ_SINK_POSTGRES_DSN",
	} {
		t.Setenv(k, "")
	}
	for _, v := range OAuthVendors {
		p := "TETRIZ_" + strings.ToUpper(strings.ReplaceAll(v, "-", "_")) + "_OAUTH_"
		t.Setenv(p+"CLIENT_ID", "")
		t.Setenv(p+"CLIENT_SECRET", "")
	}
}

func key32() string { return base64.StdEncoding.EncodeToString(make([]byte, 32)) }

// FromEnv rejects every credentials key the AES-256 encryptor cannot use,
// and a set key is validated even when plaintext credentials are allowed:
// a typo in the key must not quietly fall back to plaintext.
func TestEdge_FromEnvCredentialsKey(t *testing.T) {
	for _, tc := range []struct {
		name, key, plaintext string
		wantErr              string
	}{
		{"empty key without plaintext", "", "", "TETRIZ_CREDENTIALS_KEY is empty"},
		{"plaintext must be exactly 1", "", "true", "TETRIZ_CREDENTIALS_KEY is empty"},
		{"plaintext allowed", "", "1", ""},
		{"valid key", key32(), "", ""},
		{"valid key with a trailing newline", key32() + "\n", "", ""},
		{"the .env.example placeholder", "REPLACE_WITH_BASE64_32_BYTE_KEY", "", "not base64"},
		{"url-safe base64 alphabet", strings.ReplaceAll(strings.ReplaceAll(base64.StdEncoding.EncodeToString([]byte(strings.Repeat("\xfb\xff", 16))), "+", "-"), "/", "_"), "", "not base64"},
		{"16-byte key", base64.StdEncoding.EncodeToString(make([]byte, 16)), "", "decodes to 16 bytes, want 32"},
		{"33-byte key", base64.StdEncoding.EncodeToString(make([]byte, 33)), "", "decodes to 33 bytes, want 32"},
		{"bad key with plaintext allowed", "not-base64!", "1", "not base64"},
		{"short key with plaintext allowed", base64.StdEncoding.EncodeToString(make([]byte, 24)), "1", "want 32"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			edgeClearEnv(t)
			t.Setenv("TETRIZ_CREDENTIALS_KEY", tc.key)
			t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", tc.plaintext)
			_, err := FromEnv()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// Malformed numeric and enum variables are errors naming the variable, not
// silent defaults.
func TestEdge_FromEnvMalformedValuesNameTheVariable(t *testing.T) {
	for _, tc := range []struct{ name, k, v string }{
		{"concurrency not a number", "TETRIZ_CONCURRENCY", "ten"},
		{"concurrency float", "TETRIZ_CONCURRENCY", "2.5"},
		{"github app id not a number", "TETRIZ_GITHUB_APP_ID", "app-123"},
		{"github app id overflows int64", "TETRIZ_GITHUB_APP_ID", "99999999999999999999"},
		{"tenant kind upper case", "TETRIZ_DEFAULT_TENANT_KIND", "Individual"},
		{"tenant kind unknown", "TETRIZ_DEFAULT_TENANT_KIND", "team"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			edgeClearEnv(t)
			t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
			t.Setenv(tc.k, tc.v)
			_, err := FromEnv()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.k)
		})
	}
}

// With nothing set but plaintext, every field takes its documented default,
// and an OAuth client is read under the vendor's upper-snake name.
func TestEdge_FromEnvDefaultsAndOAuthNames(t *testing.T) {
	edgeClearEnv(t)
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	t.Setenv("TETRIZ_BITBUCKET_OAUTH_CLIENT_ID", "bb-id")
	t.Setenv("TETRIZ_LINEAR_OAUTH_CLIENT_SECRET", "lin-secret")
	c, err := FromEnv()
	require.NoError(t, err)
	assert.Equal(t, 10, c.Concurrency)
	assert.Equal(t, "log", c.SinkPath)
	assert.Equal(t, ":9090", c.HTTPAddr)
	assert.Equal(t, "tetriz_", c.MetricsPrefix)
	assert.Equal(t, sdk.TenantKindOrganization, c.DefaultTenantKind)
	assert.Equal(t, map[string]OAuthClient{
		"bitbucket": {ClientID: "bb-id"},
		"linear":    {ClientSecret: "lin-secret"},
	}, c.OAuth)
}

// A command that reads no credentials ignores a malformed key rather than
// refusing to start, but still reports a malformed unrelated variable.
func TestEdge_FromEnvWithoutCredentialsIgnoresTheKey(t *testing.T) {
	edgeClearEnv(t)
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "not-base64!")
	c, err := FromEnvWithoutCredentials()
	require.NoError(t, err)
	assert.Empty(t, c.CredentialsKey)

	t.Setenv("TETRIZ_CONCURRENCY", "x")
	_, err = FromEnvWithoutCredentials()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TETRIZ_CONCURRENCY")
}
