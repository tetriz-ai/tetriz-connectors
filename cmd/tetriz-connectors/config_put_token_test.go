// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
	jiragw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/jira"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
)

// tokenEndpoint answers a refresh exchange with an access token named after
// the refresh token it was given. No request leaves the process.
type tokenEndpoint struct{}

func (tokenEndpoint) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	form, _ := url.ParseQuery(string(body))
	out, _ := json.Marshal(map[string]any{"access_token": "access-for-" + form.Get("refresh_token"), "expires_in": 3600})
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(out))), Request: req}, nil
}

type bearerRecorder struct{ got string }

func (b *bearerRecorder) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	b.got = req.Header.Get("Authorization")
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

// After `config put --credentials`, the connector's next request carries a
// token minted from the new credentials, not the replaced account's cached one.
func TestConfigPutCredentialsForgetsTheCachedAccessToken(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("config put", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	mr := miniredis.RunT(t)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", mr.Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	orig := http.DefaultTransport
	http.DefaultTransport = tokenEndpoint{}
	t.Cleanup(func() { http.DefaultTransport = orig })
	const org = "00000000-0000-4000-8000-0000000c0f03"

	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	require.NoError(t, rc.Set(context.Background(), jiraconfig.AccessTokenKey(org), "access-token-old", 0).Err())

	code, _, errOut := runCLI(t, "config", "put", "--org", org, "--connector", "jira",
		"--settings", `{"cloud_id":"c1"}`, "--credentials", `{"oauth_refresh_token":"refresh-new"}`)
	require.Equal(t, 0, code, errOut)

	cfg, err := app.FromEnv()
	require.NoError(t, err)
	a, err := app.New(context.Background(), cfg)
	require.NoError(t, err)
	defer a.Close() //nolint:errcheck // test
	api := &bearerRecorder{}
	doer := jiragw.NewAutoRefreshDoer(api, auth.RefreshDeps{Cache: authredis.NewTokenCache(rc), Locker: authredis.NewLocker(rc), Creds: a.Configs, Tx: store.PostgresTxRunner{}}, "id", "secret", org)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://vendor.invalid/api", nil)
	require.NoError(t, err)
	resp, err := doer.Do(context.Background(), req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Bearer access-for-refresh-new", api.got)
}
