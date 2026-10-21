// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slack

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

// A Slack error answer reaches neither the error nor the log as raw text:
// only its status, message field, length and content type.
func TestSlackClientErrorBodies(t *testing.T) {
	for _, body := range []string{jsonErrorBody, htmlErrorBody} {
		srv := errorBodyServer(t, http.StatusBadGateway, body)
		ctx, logs := capturedLogs()
		client := NewSlackAppClient(&SlackAppConfig{ClientID: "id", ClientSecret: "secret", APIBaseURL: srv.URL})
		require.NotNil(t, client)
		_, err := client.ExchangeOAuthCode(ctx, "code")
		assertNoLogLeak(t, err, logs.String())
		_, err = client.GetUserInfo(ctx, "token", "U1")
		assertNoLogLeak(t, err, logs.String())
		_, err = client.GetTeamInfo(ctx, "token")
		assertNoLogLeak(t, err, logs.String())
		if strings.HasPrefix(body, "{") {
			assert.Contains(t, logs.String(), "Bad credentials", "the log keeps the message field")
		} else {
			assert.Contains(t, logs.String(), fmt.Sprintf("%d-byte text/html body", len(body)))
		}
	}
}

// assertNoLogLeak: the Slack calls' errors name only the status, and their
// logs carry no body text.
func assertNoLogLeak(t *testing.T, err error, logs string) {
	t.Helper()
	require.Error(t, err)
	for _, s := range []string{err.Error(), logs} {
		assert.NotContains(t, s, "SECRET-TOKEN-xyz")
		assert.NotContains(t, s, "SECRET-HTML-abc")
	}
}
