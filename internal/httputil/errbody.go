// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package httputil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// errBodyLimit caps how much of an error response body is read: enough for
// the vendor's error fields without pulling a large HTML page into memory.
const errBodyLimit = 1024

// requestIDHeaders are the headers vendors and identity providers put a
// request id in, kept in an error message so a failure can be traced with them.
var requestIDHeaders = []string{"X-GitHub-Request-Id", "X-Request-Id", "X-Arequestid", "X-Slack-Req-Id", "X-Goog-Request-Id", "Request-Id"}

// RequestID returns the first request id h carries, or "".
func RequestID(h http.Header) string {
	for _, name := range requestIDHeaders {
		if id := h.Get(name); id != "" {
			return id
		}
	}
	return ""
}

// ErrorMessage is what an HTTP error's message carries: the status, the
// request id when a header names one, and SummarizeErrorBody of the body
// (only the error code and message fields of a JSON body; only the length and
// content type of any other body). It never carries raw body text.
func ErrorMessage(status int, body []byte, h http.Header) string {
	msg := fmt.Sprintf("HTTP %d", status)
	contentType := ""
	if h != nil {
		if id := RequestID(h); id != "" {
			msg += fmt.Sprintf(" (request id %s)", id)
		}
		contentType = h.Get("Content-Type")
	}
	if summary := SummarizeErrorBody(body, contentType); summary != "" {
		msg += ": " + summary
	}
	return msg
}

// ErrorResponse is an HTTP error response reduced to what may reach an error
// or a log. String gives ErrorMessage; Decode lets a caller that decides on
// a field of the body (an OAuth error code, a Google reason) read it, without
// the raw text ever being printed.
type ErrorResponse struct {
	Status int
	body   []byte
	header http.Header
}

// ReadErrorResponse reads a bounded prefix (errBodyLimit bytes) of resp's
// error body. A read failure is logged and leaves the body empty.
func ReadErrorResponse(ctx context.Context, resp *http.Response) ErrorResponse {
	b, err := io.ReadAll(io.LimitReader(resp.Body, errBodyLimit))
	if err != nil {
		logger.WarnWithCtx(ctx, "httputil: read error response body", zap.Error(err))
		b = nil
	}
	return ErrorResponse{Status: resp.StatusCode, body: b, header: resp.Header}
}

// String is ErrorMessage of the response.
func (e ErrorResponse) String() string { return ErrorMessage(e.Status, e.body, e.header) }

// Decode unmarshals the body's JSON into v.
func (e ErrorResponse) Decode(v any) error {
	if err := json.Unmarshal(e.body, v); err != nil {
		return fmt.Errorf("decode error body: %w", err)
	}
	return nil
}
