// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slack

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSlackAppClient(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *SlackAppConfig
		wantNil bool
	}{
		{
			name:    "nil config returns nil",
			cfg:     nil,
			wantNil: true,
		},
		{
			name:    "empty secret returns nil",
			cfg:     &SlackAppConfig{ClientID: "id", ClientSecret: ""},
			wantNil: true,
		},
		{
			name: "valid config returns client",
			cfg:  &SlackAppConfig{ClientID: "id", ClientSecret: "secret"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewSlackAppClient(tt.cfg)
			if tt.wantNil {
				assert.Nil(t, client)
				return
			}
			assert.NotNil(t, client)
		})
	}
}

// newTestClient creates a slackAppClient that talks to the given test server.
// URLs are snapshotted from the package vars (which each test overrides before
// calling this), mirroring how NewSlackAppClient captures them.
func newTestClient(cfg *SlackAppConfig) *slackAppClient {
	return &slackAppClient{
		cfg:            cfg,
		httpClient:     &http.Client{},
		oauthAccessURL: slackOAuthAccessURL,
		authRevokeURL:  slackAuthRevokeURL,
		usersInfoURL:   slackUsersInfoURL,
		teamInfoURL:    slackTeamInfoURL,
	}
}

func TestExchangeOAuthCode_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))

		// Verify parameters are in the POST body, not the URL query string.
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		form, err := url.ParseQuery(string(body))
		assert.NoError(t, err)
		assert.Equal(t, "test-code", form.Get("code"))
		assert.Equal(t, "client-id", form.Get("client_id"))
		assert.Equal(t, "client-secret", form.Get("client_secret"))
		assert.Empty(t, r.URL.RawQuery, "credentials must not appear in URL query string")

		resp := oauthV2AccessResponse{
			OK: true,
		}
		resp.Team.ID = "T12345"
		resp.Team.Name = "Test Team"
		resp.AuthedUser.ID = "U12345"
		resp.AuthedUser.AccessToken = "xoxp-test-token"
		resp.AuthedUser.Scope = "channels:history,channels:read"
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Override the package-level URL for testing.
	origURL := slackOAuthAccessURL
	defer func() { slackOAuthAccessURL = origURL }()
	slackOAuthAccessURL = server.URL

	client := newTestClient(&SlackAppConfig{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		RedirectURI:  "http://localhost/callback",
	})

	resp, err := client.ExchangeOAuthCode(context.Background(), "test-code")
	require.NoError(t, err)
	assert.Equal(t, "xoxp-test-token", resp.UserToken)
	assert.Equal(t, "T12345", resp.TeamID)
	assert.Equal(t, "Test Team", resp.TeamName)
	assert.Equal(t, "U12345", resp.UserID)
	assert.Equal(t, "channels:history,channels:read", resp.Scope)
}

func TestExchangeOAuthCode_SlackError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := oauthV2AccessResponse{OK: false, Error: "invalid_code"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	origURL := slackOAuthAccessURL
	defer func() { slackOAuthAccessURL = origURL }()
	slackOAuthAccessURL = server.URL

	client := newTestClient(&SlackAppConfig{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	})

	_, err := client.ExchangeOAuthCode(context.Background(), "bad-code")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_code")
}

func TestExchangeOAuthCode_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer server.Close()

	origURL := slackOAuthAccessURL
	defer func() { slackOAuthAccessURL = origURL }()
	slackOAuthAccessURL = server.URL

	client := newTestClient(&SlackAppConfig{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	})

	_, err := client.ExchangeOAuthCode(context.Background(), "test-code")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestRevokeToken_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "Bearer xoxb-test-token", r.Header.Get("Authorization"))

		resp := authRevokeResponse{OK: true, Revoked: true}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	origURL := slackAuthRevokeURL
	defer func() { slackAuthRevokeURL = origURL }()
	slackAuthRevokeURL = server.URL

	client := newTestClient(&SlackAppConfig{})

	err := client.RevokeToken(context.Background(), "xoxb-test-token")
	require.NoError(t, err)
}

func TestRevokeToken_SlackError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := authRevokeResponse{OK: false, Error: "token_revoked"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	origURL := slackAuthRevokeURL
	defer func() { slackAuthRevokeURL = origURL }()
	slackAuthRevokeURL = server.URL

	client := newTestClient(&SlackAppConfig{})

	err := client.RevokeToken(context.Background(), "xoxb-bad-token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token_revoked")
}

func TestGetUserInfo(t *testing.T) {
	tests := []struct {
		name        string
		token       string
		userID      string
		handler     func(t *testing.T, w http.ResponseWriter, r *http.Request)
		wantErr     bool
		errContains string
		wantIsAdmin bool
	}{
		{
			name:   "workspace admin",
			token:  "xoxp-test-token",
			userID: "U12345",
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "Bearer xoxp-test-token", r.Header.Get("Authorization"))
				assert.Equal(t, "U12345", r.URL.Query().Get("user"))
				resp := usersInfoResponse{OK: true}
				resp.User.IsAdmin = true
				_ = json.NewEncoder(w).Encode(resp)
			},
			wantIsAdmin: true,
		},
		{
			name:   "non-admin user",
			token:  "xoxp-test-token",
			userID: "U99999",
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				resp := usersInfoResponse{OK: true}
				resp.User.IsAdmin = false
				_ = json.NewEncoder(w).Encode(resp)
			},
			wantIsAdmin: false,
		},
		{
			name:   "slack api error",
			token:  "xoxp-bad-token",
			userID: "U12345",
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(usersInfoResponse{OK: false, Error: "user_not_found"})
			},
			wantErr:     true,
			errContains: "user_not_found",
		},
		{
			name:   "http error",
			token:  "xoxp-test-token",
			userID: "U12345",
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantErr:     true,
			errContains: "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				tt.handler(t, w, r)
			}))
			defer server.Close()

			origURL := slackUsersInfoURL
			defer func() { slackUsersInfoURL = origURL }()
			slackUsersInfoURL = server.URL

			client := newTestClient(&SlackAppConfig{})
			info, err := client.GetUserInfo(context.Background(), tt.token, tt.userID)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantIsAdmin, info.IsAdmin)
		})
	}
}
