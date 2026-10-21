// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// The two error bodies a vendor may answer with: JSON carrying its message
// beside a field that must never reach an error or a log, and an HTML page.
const (
	jsonErrorBody = `{"message":"Bad credentials","debug":"SECRET-TOKEN-xyz"}`
	htmlErrorBody = `<html><body>SECRET-HTML-abc internal stack trace</body></html>`
)

// errorBodyServer answers every request with status and body, typed as JSON
// or HTML.
func errorBodyServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if strings.HasPrefix(body, "{") {
			w.Header().Set("Content-Type", "application/json")
		} else {
			w.Header().Set("Content-Type", "text/html")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// capturedLogs returns a context whose log entries go to the returned buffer.
func capturedLogs() (context.Context, *bytes.Buffer) {
	var buf bytes.Buffer
	return logger.NewContext(context.Background(), slog.New(slog.NewTextHandler(&buf, nil))), &buf
}

// assertNoBodyLeak checks an error and the logs: a JSON body yields only its
// message field, and an HTML body only its length and content type.
func assertNoBodyLeak(t *testing.T, body string, err error, logs string) {
	t.Helper()
	require.Error(t, err)
	for _, s := range []string{err.Error(), logs} {
		assert.NotContains(t, s, "SECRET-TOKEN-xyz")
		assert.NotContains(t, s, "SECRET-HTML-abc")
		assert.NotContains(t, s, "<html>")
	}
	if strings.HasPrefix(body, "{") {
		assert.Contains(t, err.Error(), `message="Bad credentials"`)
	} else {
		assert.Contains(t, err.Error(), fmt.Sprintf("%d-byte text/html body", len(body)))
	}
}

// githubTestClient is a GitHub App client whose requests go to srv.
func githubTestClient(t *testing.T, srv *httptest.Server) *githubAppClient {
	t.Helper()
	_, pemStr := generateTestPEM(t)
	client, err := NewGitHubAppClient(&GitHubAppConfig{ClientID: "id", PrivateKeyPEM: pemStr})
	require.NoError(t, err)
	c := client.(*githubAppClient)
	c.httpClient = srv.Client()
	c.httpClient.Transport = &rewriteTransport{base: srv.Client().Transport, baseURL: srv.URL}
	return c
}

// A GitHub error answer reaches the error and the log only as its status and
// message field.
func TestGitHubClientErrorBodies(t *testing.T) {
	for _, body := range []string{jsonErrorBody, htmlErrorBody} {
		srv := errorBodyServer(t, http.StatusBadGateway, body)
		ctx, logs := capturedLogs()
		c := githubTestClient(t, srv)
		_, err := c.GetInstallation(ctx, "12345")
		assertNoBodyLeak(t, body, err, logs.String())
		err = c.DeleteInstallation(ctx, "12345")
		assertNoBodyLeak(t, body, err, logs.String())
	}
}
