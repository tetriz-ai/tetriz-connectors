// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

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

// A Linear error answer reaches the error only as its status and message
// field.
func TestLinearClientErrorBodies(t *testing.T) {
	for _, body := range []string{jsonErrorBody, htmlErrorBody} {
		srv := errorBodyServer(t, http.StatusBadGateway, body)
		ctx, logs := capturedLogs()
		client := NewOAuthClient("id", "secret", "cb", WithAPIBaseURL(srv.URL))
		_, err := client.FetchWorkspaceInfo(ctx, "token")
		assertNoBodyLeak(t, body, err, logs.String())
		_, err = client.FetchCurrentUser(ctx, "token")
		assertNoBodyLeak(t, body, err, logs.String())
		err = client.RevokeToken(ctx, "token")
		assertNoBodyLeak(t, body, err, logs.String())
	}
}

// The token exchange still reads Linear's OAuth error fields: both the code
// and its description reach the error.
func TestLinearTokenExchangeStillReadsTheOAuthError(t *testing.T) {
	srv := errorBodyServer(t, http.StatusBadRequest, `{"error":"invalid_grant","error_description":"The code has expired","debug":"SECRET-TOKEN-xyz"}`)
	client := NewOAuthClient("id", "secret", "cb", WithAPIBaseURL(srv.URL))
	_, err := client.ExchangeAuthCode(context.Background(), "code")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_grant")
	assert.Contains(t, err.Error(), "The code has expired")
	assert.NotContains(t, err.Error(), "SECRET-TOKEN-xyz")
}
