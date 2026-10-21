// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExchangeAuthCode_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/site/oauth2/access_token", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))

		// Verify Basic auth header present
		user, pass, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "test-client-id", user)
		assert.Equal(t, "test-client-secret", pass)

		// Verify form body
		assert.NoError(t, r.ParseForm())
		assert.Equal(t, "authorization_code", r.PostForm.Get("grant_type"))
		assert.Equal(t, "test-auth-code", r.PostForm.Get("code"))
		// redirect_uri must match the authorize request (RFC 6749 §4.1.3).
		assert.Equal(t, "http://localhost/callback", r.PostForm.Get("redirect_uri"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "access-token-123",
			"refresh_token": "refresh-token-456",
			"expires_in":    7200,
			"scopes":        "repository pullrequest",
		})
	}))
	defer server.Close()

	client := NewBitbucketOAuthClient("test-client-id", "test-client-secret", "http://localhost/callback",
		WithBitbucketBaseURL(server.URL))

	resp, err := client.ExchangeAuthCode(context.Background(), "test-auth-code")
	require.NoError(t, err)
	assert.Equal(t, "access-token-123", resp.AccessToken)
	assert.Equal(t, "refresh-token-456", resp.RefreshToken)
	assert.Equal(t, 7200, resp.ExpiresIn)
	assert.Equal(t, "repository pullrequest", resp.Scopes)
}

func TestExchangeAuthCode_InvalidCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_grant",
			"error_description": "Invalid authorization code",
		})
	}))
	defer server.Close()

	client := NewBitbucketOAuthClient("test-client-id", "test-client-secret", "http://localhost/callback",
		WithBitbucketBaseURL(server.URL))

	_, err := client.ExchangeAuthCode(context.Background(), "bad-code")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 400")
}

func TestFetchWorkspaces_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/2.0/user/workspaces", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "Bearer access-token-123", r.Header.Get("Authorization"))
		assert.Equal(t, "100", r.URL.Query().Get("pagelen"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"values": []map[string]any{
				{"workspace": map[string]any{
					"uuid": "{ws-uuid-1}", "slug": "acme",
					"links": map[string]any{"avatar": map[string]any{"href": "https://example/acme.png"}},
				}},
				{"workspace": map[string]any{
					"uuid": "{ws-uuid-2}", "slug": "acme",
				}},
			},
		})
	}))
	defer server.Close()

	client := NewBitbucketOAuthClient("test-client-id", "test-client-secret", "http://localhost/callback",
		WithBitbucketBaseURL(server.URL))

	workspaces, err := client.FetchWorkspaces(context.Background(), "access-token-123")
	require.NoError(t, err)
	require.Len(t, workspaces, 2)
	assert.Equal(t, "{ws-uuid-1}", workspaces[0].UUID)
	assert.Equal(t, "acme", workspaces[0].Slug)
	assert.Equal(t, "acme", workspaces[0].Name)
	assert.Equal(t, "https://example/acme.png", workspaces[0].AvatarURL)
	assert.Equal(t, "{ws-uuid-2}", workspaces[1].UUID)
	assert.Equal(t, "acme", workspaces[1].Slug)
	assert.Equal(t, "acme", workspaces[1].Name)
}

func TestFetchWorkspaces_SingleWorkspace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"values": []map[string]any{
				{"workspace": map[string]any{"uuid": "{ws-uuid-1}", "slug": "acme"}},
			},
		})
	}))
	defer server.Close()

	client := NewBitbucketOAuthClient("test-client-id", "test-client-secret", "http://localhost/callback",
		WithBitbucketBaseURL(server.URL))

	workspaces, err := client.FetchWorkspaces(context.Background(), "token")
	require.NoError(t, err)
	require.Len(t, workspaces, 1)
	assert.Equal(t, "acme", workspaces[0].Slug)
}

func TestFetchWorkspaces_MultipleWorkspaces(t *testing.T) {
	callCount := 0
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")

		if callCount == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"values": []map[string]any{
					{"workspace": map[string]any{"uuid": "{ws-1}", "slug": "ws1"}},
				},
				"next": serverURL + "/2.0/user/workspaces?pagelen=100&page=2",
			})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"values": []map[string]any{
					{"workspace": map[string]any{"uuid": "{ws-2}", "slug": "ws2"}},
				},
			})
		}
	}))
	defer server.Close()
	serverURL = server.URL

	client := NewBitbucketOAuthClient("test-client-id", "test-client-secret", "http://localhost/callback",
		WithBitbucketBaseURL(server.URL))

	workspaces, err := client.FetchWorkspaces(context.Background(), "token")
	require.NoError(t, err)
	require.Len(t, workspaces, 2)
	assert.Equal(t, "ws1", workspaces[0].Slug)
	assert.Equal(t, "ws2", workspaces[1].Slug)
	assert.Equal(t, 2, callCount)
}

// Pin both default hosts so a well-meaning consolidation onto one base URL is
// caught here, not at a customer's first connect.
func TestNewBitbucketOAuthClient_DefaultHosts(t *testing.T) {
	client := NewBitbucketOAuthClient("id", "secret", "http://localhost/callback")

	impl, ok := client.(*bitbucketOAuthClientImpl)
	require.True(t, ok)
	assert.Equal(t, "https://bitbucket.org", impl.oauthBaseURL,
		"authorize/token endpoints live on bitbucket.org")
	assert.Equal(t, "https://api.bitbucket.org", impl.apiBaseURL,
		"token-authenticated REST must go to api.bitbucket.org")
}

// Pinning the two default fields proves what the client is configured with, not
// what it dials. Bind each host to its own server so a method silently reverting
// to the OAuth host fails here rather than at a customer's first connect.
func TestBitbucketClient_RoutesEachCallToItsHost(t *testing.T) {
	var oauthHits, apiHits []string

	oauthSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oauthHits = append(oauthHits, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok", "scopes": "account"})
	}))
	defer oauthSrv.Close()

	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiHits = append(apiHits, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/2.0/user" {
			_ = json.NewEncoder(w).Encode(map[string]any{"uuid": "{u}", "display_name": "U"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"values": []map[string]any{}})
	}))
	defer apiSrv.Close()

	c := &bitbucketOAuthClientImpl{
		clientID:     "id",
		clientSecret: "secret",
		callbackURL:  "http://localhost/callback",
		oauthBaseURL: oauthSrv.URL,
		apiBaseURL:   apiSrv.URL,
		httpClient:   oauthSrv.Client(),
	}

	_, err := c.ExchangeAuthCode(context.Background(), "code")
	require.NoError(t, err)
	_, err = c.FetchCurrentUser(context.Background(), "tok")
	require.NoError(t, err)
	_, err = c.FetchWorkspaces(context.Background(), "tok")
	require.NoError(t, err)

	assert.Equal(t, []string{"/site/oauth2/access_token"}, oauthHits,
		"only the token exchange may use the OAuth host")
	assert.Equal(t, []string{"/2.0/user", "/2.0/user/workspaces"}, apiHits,
		"every token-authenticated REST call must use the API host")
}

// A rejected token is terminal; a 5xx, a throttle or a transport error is not.
// Collapsing the two would tell an admin to stop retrying during an outage.
func TestVendorRefusedCredentials(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"Unauthorized", &bitbucketAPIError{Op: "user", StatusCode: 401}, true},
		{"Forbidden", &bitbucketAPIError{Op: "user", StatusCode: 403}, true},
		{"ServerError", &bitbucketAPIError{Op: "user", StatusCode: 500}, false},
		{"Throttled", &bitbucketAPIError{Op: "user", StatusCode: 429}, false},
		{"Transport", errors.New("dial tcp: i/o timeout"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, vendorRefusedCredentials(tt.err))
		})
	}
}
