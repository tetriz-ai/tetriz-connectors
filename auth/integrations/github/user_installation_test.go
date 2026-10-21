// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyUserInstallation(t *testing.T) {
	_, pem := generateTestPEM(t)
	for _, mode := range []string{"visible", "second page", "not visible", "page cap", "exchange denied", "exchange malformed", "API denied", "API malformed", "oversized", "redirect", "visible collaborator", "inactive admin", "wrong organization", "malformed membership", "permission missing", "personal owner", "personal other user", "account mismatch"} {
		t.Run(mode, func(t *testing.T) {
			pages, exchanges := 0, 0
			expected := &InstallationInfo{ID: 777, AccountID: 42, AccountLogin: "acme", AccountType: "Organization"}
			if strings.HasPrefix(mode, "personal") {
				expected.AccountType = "User"
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/login/oauth/access_token" {
					exchanges++
					require.NoError(t, r.ParseForm())
					assert.Equal(t, "client", r.Form.Get("client_id"))
					assert.Equal(t, "secret", r.Form.Get("client_secret"))
					assert.Equal(t, "callback-code", r.Form.Get("code"))
					assert.Equal(t, http.MethodPost, r.Method)
					switch mode {
					case "exchange denied":
						w.WriteHeader(http.StatusUnauthorized)
						_, _ = w.Write([]byte(`{"access_token":"SECRET-ERROR"}`))
						return
					case "exchange malformed":
						_, _ = w.Write([]byte(`{"error":"SECRET-ERROR"}`))
						return
					case "redirect":
						w.Header().Set("Location", "https://unexpected.invalid/SECRET-ERROR")
						w.WriteHeader(http.StatusTemporaryRedirect)
						return
					}
					_, _ = w.Write([]byte(`{"access_token":"user-token","token_type":"bearer"}`))
					return
				}
				if r.URL.Path == "/user/memberships/orgs/acme" {
					assert.Equal(t, "Bearer user-token", r.Header.Get("Authorization"))
					if mode == "permission missing" {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					if mode == "malformed membership" {
						_, _ = w.Write([]byte(`{}`))
						return
					}
					state := "active"
					if mode == "inactive admin" {
						state = "pending"
					}
					orgID := 42
					if mode == "wrong organization" {
						orgID = 43
					}
					role := "admin"
					if mode == "visible collaborator" {
						role = "member"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"state": state, "role": role, "organization": map[string]any{"id": orgID, "login": "acme"}})
					return
				}
				if r.URL.Path == "/user" {
					id := 42
					if mode == "personal other user" {
						id = 43
					}
					_ = json.NewEncoder(w).Encode(map[string]int{"id": id})
					return
				}

				pages++
				assert.Equal(t, "/user/installations", r.URL.Path)
				assert.Equal(t, "Bearer user-token", r.Header.Get("Authorization"))
				assert.Equal(t, "100", r.URL.Query().Get("per_page"))
				assert.Equal(t, fmt.Sprint(pages), r.URL.Query().Get("page"))
				switch mode {
				case "API denied":
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte("SECRET-ERROR"))
					return
				case "API malformed":
					_, _ = w.Write([]byte("SECRET-ERROR"))
					return
				case "oversized":
					_, _ = w.Write([]byte(strings.Repeat("x", (1<<20)+1)))
					return
				case "visible", "visible collaborator", "inactive admin", "wrong organization", "malformed membership", "permission missing", "personal owner", "personal other user", "account mismatch":
					id := 42
					if mode == "account mismatch" {
						id = 43
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"installations": []any{map[string]any{"id": 777, "account": map[string]any{"id": id, "login": "acme", "type": expected.AccountType}}}})
					return
				case "second page":
					if pages == 2 {
						_, _ = w.Write([]byte(`{"installations":[{"id":777,"account":{"id":42,"login":"acme","type":"Organization"}}]}`))
						return
					}
				case "not visible":
					_, _ = w.Write([]byte(`{"installations":[{"id":778}]}`))
					return
				}
				entries := make([]map[string]int, 100)
				for i := range entries {
					entries[i] = map[string]int{"id": i + 1}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"installations": entries})
			}))
			t.Cleanup(srv.Close)
			c, err := NewGitHubAppClient(&GitHubAppConfig{ClientID: "client", ClientSecret: "secret", PrivateKeyPEM: pem}, WithGitHubTransport(&rewriteTransport{base: srv.Client().Transport, baseURL: srv.URL}))
			require.NoError(t, err)
			err = c.VerifyUserInstallation(context.Background(), "callback-code", "777", expected)
			if mode == "visible" || mode == "second page" || mode == "personal owner" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrUserInstallationDenied)
				assert.NotContains(t, err.Error(), "SECRET-ERROR")
				assert.NotContains(t, err.Error(), "user-token")
			}
			assert.Equal(t, 1, exchanges)
			if mode == "page cap" {
				assert.Equal(t, 10, pages)
			}
			if mode == "redirect" {
				assert.Zero(t, pages)
			}
		})
	}
}

func TestVerifyUserInstallationFailsClosed(t *testing.T) {
	_, pem := generateTestPEM(t)
	for _, tt := range []struct{ name, secret, code, id string }{
		{"missing secret", "", "code", "777"}, {"missing code", "secret", "", "777"}, {"bad id", "secret", "code", "../777"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewGitHubAppClient(&GitHubAppConfig{ClientID: "client", ClientSecret: tt.secret, PrivateKeyPEM: pem})
			require.NoError(t, err)
			require.ErrorIs(t, c.VerifyUserInstallation(context.Background(), tt.code, tt.id, &InstallationInfo{ID: 777, AccountID: 42, AccountLogin: "acme", AccountType: "Organization"}), ErrUserInstallationDenied)
		})
	}
}
