// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

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
	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	intjira "github.com/tetriz-ai/tetriz-connectors/auth/integrations/jira"
	intlinear "github.com/tetriz-ai/tetriz-connectors/auth/integrations/linear"
	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
	bitbucketgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/bitbucket"
	gitlabgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/gitlab"
	jiragw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/jira"
	lineargw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/linear"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
)

// fakeTokenEndpoint answers every refresh exchange with an access token named
// after the refresh token it was given, so a request's bearer shows which
// stored credentials minted it. No request leaves the process.
type fakeTokenEndpoint struct{}

func (fakeTokenEndpoint) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	form, _ := url.ParseQuery(string(body))
	out, _ := json.Marshal(map[string]any{
		"access_token":  "access-for-" + form.Get("refresh_token"),
		"refresh_token": form.Get("refresh_token"),
		"expires_in":    3600,
		"token_type":    "Bearer",
	})
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(string(out))), Request: req}, nil
}

// useFakeTokenEndpoint routes the refresh exchanges, which use a default
// http.Client, to fakeTokenEndpoint for one test.
func useFakeTokenEndpoint(t *testing.T) {
	t.Helper()
	orig := http.DefaultTransport
	http.DefaultTransport = fakeTokenEndpoint{}
	t.Cleanup(func() { http.DefaultTransport = orig })
}

// bearerRecorder is the vendor API: it records the Authorization header.
type bearerRecorder struct{ got string }

func (b *bearerRecorder) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	b.got = req.Header.Get("Authorization")
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

// After a connect flow writes new credentials, the vendor's next request
// carries a token minted from them, not the replaced account's cached token.
func TestConnectFlowsForgetTheCachedAccessToken(t *testing.T) {
	useFakeTokenEndpoint(t)
	enc, err := cryptutil.NewAESGCMEncryptor([]byte("0123456789abcdef0123456789abcdef"))
	require.NoError(t, err)

	type vendor struct {
		name     string
		cacheKey string
		cached   func(t *testing.T) string // the old account's cached value
		connect  func(t *testing.T, s *Service, rc *redis.Client)
		doer     func(inner vendorapi.HTTPDoer, deps auth.RefreshDeps) vendorapi.HTTPDoer
	}
	vendors := []vendor{
		{
			name: "jira", cacheKey: jiraconfig.AccessTokenKey(testOrgID),
			connect: func(t *testing.T, s *Service, _ *redis.Client) {
				require.NoError(t, s.saveJiraConfig(context.Background(), testOrgID, &intjira.CloudSite{ID: "c1", Name: "Site", URL: "https://example.atlassian.net"}, "refresh-new", "Sam"))
			},
			doer: func(inner vendorapi.HTTPDoer, deps auth.RefreshDeps) vendorapi.HTTPDoer {
				return jiragw.NewAutoRefreshDoer(inner, deps, "id", "secret", testOrgID)
			},
		},
		{
			name: "gitlab", cacheKey: glconfig.AccessTokenKey(testOrgID),
			connect: func(t *testing.T, s *Service, _ *redis.Client) {
				require.NoError(t, s.saveGitLabConfig(context.Background(), testOrgID, &GitLabGroup{ID: 7, Path: "acme", FullPath: "acme", Name: "Acme"}, "refresh-new", "access-new", &GitLabUser{ID: 1, Username: "sam"}))
			},
			doer: func(inner vendorapi.HTTPDoer, deps auth.RefreshDeps) vendorapi.HTTPDoer {
				return gitlabgw.NewAutoRefreshDoer(inner, deps, "id", "secret", testOrgID)
			},
		},
		{
			name: "bitbucket", cacheKey: bbconfig.AccessTokenKey(testOrgID),
			connect: func(t *testing.T, s *Service, _ *redis.Client) {
				require.NoError(t, s.saveBitbucketConfig(context.Background(), testOrgID, &BitbucketWorkspace{UUID: "{w}", Slug: "acme", Name: "Acme"}, "refresh-new", &BitbucketUser{UUID: "{u}", DisplayName: "Sam"}))
			},
			doer: func(inner vendorapi.HTTPDoer, deps auth.RefreshDeps) vendorapi.HTTPDoer {
				return bitbucketgw.NewAutoRefreshDoer(inner, deps, "id", "secret", testOrgID)
			},
		},
		{
			name: "linear", cacheKey: linearconfig.AccessTokenKey(testOrgID),
			cached: func(t *testing.T) string {
				ct, err := enc.Encrypt([]byte("access-token-old"))
				require.NoError(t, err)
				return string(ct)
			},
			connect: func(t *testing.T, s *Service, _ *redis.Client) {
				require.NoError(t, s.saveLinearConfig(context.Background(), testOrgID, "access-new", "refresh-new", &intlinear.WorkspaceInfo{ID: "w", Name: "Acme", URLKey: "acme"}, "Sam"))
			},
			doer: func(inner vendorapi.HTTPDoer, deps auth.RefreshDeps) vendorapi.HTTPDoer {
				return lineargw.NewAutoRefreshDoer(inner, deps, enc, "id", "secret", testOrgID)
			},
		},
	}
	for _, v := range vendors {
		t.Run(v.name, func(t *testing.T) {

			rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
			cached := "access-token-old"
			if v.cached != nil {
				cached = v.cached(t)
			}
			require.NoError(t, rc.Set(context.Background(), v.cacheKey, cached, 0).Err())

			configs := memstore.New()
			svc := NewService(Deps{Cfg: testLinearConfig(), ConnectorDAO: configs, Redis: rc, Registry: testRegistry()})
			v.connect(t, svc, rc)

			api := &bearerRecorder{}
			deps := auth.RefreshDeps{Cache: authredis.NewTokenCache(rc), Locker: authredis.NewLocker(rc), Creds: configs, Tx: store.NoopTxRunner{}}
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://vendor.invalid/api", nil)
			require.NoError(t, err)
			resp, err := v.doer(api, deps).Do(context.Background(), req)
			require.NoError(t, err)
			_ = resp.Body.Close()
			assert.Equal(t, "Bearer access-for-refresh-new", api.got, "the next request carries the new credentials")
		})
	}
}
