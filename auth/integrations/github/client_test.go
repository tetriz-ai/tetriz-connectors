// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestPEM generates an RSA key pair and returns the key and PEM string.
func generateTestPEM(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	return key, string(pemBytes)
}

func TestNewGitHubAppClient_EmptyPEM(t *testing.T) {
	client, err := NewGitHubAppClient(&GitHubAppConfig{
		ClientID:      "test-id",
		PrivateKeyPEM: "",
	})
	require.NoError(t, err)
	assert.Nil(t, client)
}

func TestNewGitHubAppClient_NilConfig(t *testing.T) {
	client, err := NewGitHubAppClient(nil)
	require.NoError(t, err)
	assert.Nil(t, client)
}

func TestNewGitHubAppClient_InvalidPEM(t *testing.T) {
	_, err := NewGitHubAppClient(&GitHubAppConfig{
		ClientID:      "test-id",
		PrivateKeyPEM: "not a real PEM",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse private key")
}

func TestGenerateJWT(t *testing.T) {
	key, pemStr := generateTestPEM(t)

	client, err := NewGitHubAppClient(&GitHubAppConfig{
		ClientID:      "test-client-id",
		PrivateKeyPEM: pemStr,
	})
	require.NoError(t, err)

	concrete := client.(*githubAppClient)
	tokenStr, err := concrete.generateJWT()
	require.NoError(t, err)

	parsed, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return &key.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)

	claims, ok := parsed.Claims.(jwt.MapClaims)
	require.True(t, ok)
	assert.Equal(t, "test-client-id", claims["iss"])
}

func TestGetInstallation_Success(t *testing.T) {
	_, pemStr := generateTestPEM(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/app/installations/12345", r.URL.Path)
		assert.Contains(t, r.Header.Get("Authorization"), "Bearer ")
		assert.Equal(t, "application/vnd.github+json", r.Header.Get("Accept"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": 12345,
			"account": map[string]any{
				"login": "test-org",
				"id":    67890,
				"type":  "Organization",
			},
			"repository_selection": "all",
		})
	}))
	defer server.Close()

	client, err := NewGitHubAppClient(&GitHubAppConfig{
		ClientID:      "test-client-id",
		PrivateKeyPEM: pemStr,
	})
	require.NoError(t, err)

	concrete := client.(*githubAppClient)
	concrete.httpClient = server.Client()
	concrete.httpClient.Transport = &rewriteTransport{
		base:    server.Client().Transport,
		baseURL: server.URL,
	}

	info, err := concrete.GetInstallation(context.Background(), "12345")
	require.NoError(t, err)
	assert.Equal(t, int64(12345), info.ID)
	assert.Equal(t, "test-org", info.AccountLogin)
	assert.Equal(t, int64(67890), info.AccountID)
	assert.Equal(t, "Organization", info.AccountType)
	assert.Equal(t, "all", info.RepositorySelection)
}

func TestGetInstallation_NotFound(t *testing.T) {
	_, pemStr := generateTestPEM(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewGitHubAppClient(&GitHubAppConfig{
		ClientID:      "test-client-id",
		PrivateKeyPEM: pemStr,
	})
	require.NoError(t, err)

	concrete := client.(*githubAppClient)
	concrete.httpClient = server.Client()
	concrete.httpClient.Transport = &rewriteTransport{
		base:    server.Client().Transport,
		baseURL: server.URL,
	}

	_, err = concrete.GetInstallation(context.Background(), "99999")
	require.ErrorIs(t, err, ErrInstallationNotFound)
}

func TestGetInstallation_ServerError(t *testing.T) {
	_, pemStr := generateTestPEM(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"internal error"}`))
	}))
	defer server.Close()

	client, err := NewGitHubAppClient(&GitHubAppConfig{
		ClientID:      "test-client-id",
		PrivateKeyPEM: pemStr,
	})
	require.NoError(t, err)

	concrete := client.(*githubAppClient)
	concrete.httpClient = server.Client()
	concrete.httpClient.Transport = &rewriteTransport{
		base:    server.Client().Transport,
		baseURL: server.URL,
	}

	_, err = concrete.GetInstallation(context.Background(), "12345")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

// rewriteTransport rewrites all request URLs to point at the test server,
// preserving the original path and query string.
type rewriteTransport struct {
	base    http.RoundTripper
	baseURL string
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = t.baseURL[len("http://"):]
	if t.base != nil {
		return t.base.RoundTrip(req)
	}
	return http.DefaultTransport.RoundTrip(req)
}
