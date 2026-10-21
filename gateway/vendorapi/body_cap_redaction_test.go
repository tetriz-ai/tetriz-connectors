// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// sizedResponse is a 200 response whose body is n bytes of a JSON string, so
// a body under the cap still decodes.
func sizedResponse(n int) *http.Response {
	body := make([]byte, n)
	for i := range body {
		body[i] = 'a'
	}
	body[0], body[n-1] = '"', '"'
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}
}

// A shared-path response body of MaxResponseBytes+1 fails its request as a
// terminal vendor_error naming the cap; one of exactly MaxResponseBytes is
// read.
func TestResponseBodyOverTheCapFails(t *testing.T) {
	for _, path := range []string{"Handle", "DoJSON"} {
		t.Run(path, func(t *testing.T) {
			call := func(n int) error {
				doer := &stubHTTPDoer{resp: sizedResponse(n)}
				if path == "Handle" {
					_, err := Handle(context.Background(), doer, &stubRequest{method: "GET", url: "https://api.example.com/x", resp: &stubResponse{msg: &emptypb.Empty{}}})
					return err
				}
				var out string
				return (&BaseVendorGateway{Doer: doer}).DoJSON(context.Background(), "GET", "https://api.example.com/x", nil, &out)
			}
			err := call(MaxResponseBytes + 1)
			require.Error(t, err)
			var se *sdk.SyncError
			require.True(t, errors.As(err, &se), "%v", err)
			assert.Equal(t, sdk.ErrKindVendorError, se.Kind)
			assert.True(t, se.Terminal)
			assert.Contains(t, err.Error(), "33554432-byte (32 MiB) cap")
			require.NoError(t, call(MaxResponseBytes), "a body of exactly the cap is read")
		})
	}
}

// A failed request's error and log line carry its URL without the query
// string, which can hold search terms or emails.
func TestRequestURLIsLoggedWithoutItsQuery(t *testing.T) {
	const raw = "https://api.example.com/search/issues?q=user@example.com#frag"
	var logs bytes.Buffer
	ctx := logger.NewContext(context.Background(), slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	doer := &stubHTTPDoer{err: &url.Error{Op: "Get", URL: raw, Err: errors.New("connection refused")}}
	_, err := Handle(ctx, doer, &stubRequest{method: "GET", url: raw, resp: &stubResponse{msg: &emptypb.Empty{}}})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "user@example.com")
	assert.Contains(t, err.Error(), "https://api.example.com/search/issues")
	assert.NotContains(t, logs.String(), "user@example.com")
	assert.Contains(t, logs.String(), "https://api.example.com/search/issues")

	var out struct{}
	err = (&BaseVendorGateway{Doer: doer}).DoJSON(ctx, "GET", raw, nil, &out)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "user@example.com")
	_, err = (&BaseVendorGateway{Doer: doer}).Do(ctx, &http.Request{URL: &url.URL{Scheme: "https", Host: "api.example.com", Path: "/x", RawQuery: "q=user@example.com"}})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "user@example.com")
}

// An HTTP error keeps only the vendor's error code and message fields of a
// JSON body, and none of the text of any other body; the status and the
// vendor's request id stay.
func TestHTTPErrorKeepsOnlyTheVendorsErrorFields(t *testing.T) {
	h := http.Header{}
	h.Set("X-GitHub-Request-Id", "ABCD:1234")
	h.Set("Content-Type", "application/json")
	e := ClassifyHTTPErrorWithHeaders(422, []byte(`{"message":"Validation Failed","documentation_url":"https://docs","secret_echo":"user@example.com"}`), h)
	assert.Equal(t, `HTTP 422 (request id ABCD:1234): message="Validation Failed"`, e.Cause.Error())

	h.Set("Content-Type", "text/html")
	html := []byte(`<html><body>internal page for user@example.com</body></html>`)
	e = ClassifyHTTPErrorWithHeaders(502, html, h)
	assert.NotContains(t, e.Error(), "user@example.com")
	assert.NotContains(t, e.Error(), "html><body")
	assert.Equal(t, "HTTP 502 (request id ABCD:1234): 60-byte text/html body", e.Cause.Error())

	long := `{"message":"` + strings.Repeat("x", 300) + `"}`
	e = ClassifyHTTPErrorWithHeaders(400, []byte(long), nil)
	assert.LessOrEqual(t, len(e.Cause.Error()), len("HTTP 400: message=")+2+200+len("…"), "each field is cut to 200 bytes")
}

// A token-refresh response over the cap fails the refresh as a terminal
// vendor_error; one of exactly the cap is returned.
func TestTokenRefreshBodyOverTheCapFails(t *testing.T) {
	var size int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("a"), size))
	}))
	defer srv.Close()
	c := NewOAuthRefreshClassifier(sdk.ConnectorID("test-vendor-bodycap"))
	build := func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader(""))
	}

	size = MaxResponseBytes + 1
	_, _, err := c.Fetch(context.Background(), "org-cap", build)
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, sdk.ErrKindVendorError, se.Kind)
	assert.True(t, se.Terminal)
	assert.Contains(t, err.Error(), "(32 MiB) cap")

	size = MaxResponseBytes
	body, _, err := c.Fetch(context.Background(), "org-cap", build)
	require.NoError(t, err)
	assert.Len(t, body, MaxResponseBytes)
}
