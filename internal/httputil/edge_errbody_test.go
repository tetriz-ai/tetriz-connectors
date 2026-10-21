// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package httputil_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tetriz-ai/tetriz-connectors/internal/httputil"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

// This package had no tests, so it carries its own guard.
func TestMain(m *testing.M) {
	logger.Init("test", nil)
	os.Exit(testguard.Run(m))
}

type failingReader struct{ n int }

func (f *failingReader) Read(p []byte) (int, error) {
	if f.n == 0 {
		return 0, errors.New("connection reset")
	}
	k := copy(p, bytes.Repeat([]byte("x"), f.n))
	f.n = 0
	return k, nil
}

func errResp(body io.Reader) *http.Response {
	return &http.Response{StatusCode: 502, Header: http.Header{"Content-Type": {"text/plain"}}, Body: io.NopCloser(body)}
}

// The error body is read up to 1 KiB however large the response, an empty
// body is empty, a body that fails mid-read contributes nothing (not a
// partial prefix that looks complete), and an endless body does not hang.
// The message never carries the body's text, only its length and type.
func TestEdge_ReadErrorResponseBoundsAndFailures(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, "HTTP 502: 1024-byte text/plain body", httputil.ReadErrorResponse(ctx, errResp(bytes.NewReader(bytes.Repeat([]byte("a"), 10<<20)))).String())
	assert.Equal(t, "HTTP 502", httputil.ReadErrorResponse(ctx, errResp(bytes.NewReader(nil))).String())
	assert.Equal(t, "HTTP 502", httputil.ReadErrorResponse(ctx, errResp(&failingReader{n: 10})).String())
	assert.Equal(t, "HTTP 502: 1024-byte text/plain body", httputil.ReadErrorResponse(ctx, errResp(io.MultiReader(infinite{}))).String())
}

type infinite struct{}

func (infinite) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'z'
	}
	return len(p), nil
}
