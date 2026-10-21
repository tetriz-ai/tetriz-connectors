// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// structFakeGW is a VendorGateway whose Do is a test callback; the other methods are unused here.
type structFakeGW struct {
	do func(*http.Request) (*http.Response, error)
}

func (f structFakeGW) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	return f.do(req)
}

func (f structFakeGW) DoJSON(context.Context, string, string, any, any) error {
	panic("unused")
}
func (f structFakeGW) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return f }
func (f structFakeGW) ForGroup(string) sdk.VendorGateway                      { return f }
func (f structFakeGW) Execute(context.Context, proto.Message) (proto.Message, error) {
	panic("unused")
}

func resp(status int, body string, header http.Header) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: header}
}

func TestFileAtRevision_Normal(t *testing.T) {
	t.Parallel()
	gw := structFakeGW{do: func(*http.Request) (*http.Response, error) {
		return resp(http.StatusOK, "package p\n", http.Header{"X-Gitlab-Blob-Id": {"abc123"}}), nil
	}}
	content, sha, err := FileAtRevision(context.Background(), gw, "org-1", "42", "deadbeef", "p/x.go")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(content) != "package p\n" || sha != "abc123" {
		t.Fatalf("got content=%q sha=%q, want the file + blob id", content, sha)
	}
}

func TestFileAtRevision_AbsentIs404Sentinel(t *testing.T) {
	t.Parallel()
	gw := structFakeGW{do: func(*http.Request) (*http.Response, error) {
		return resp(http.StatusNotFound, "not found", nil), nil
	}}
	content, sha, err := FileAtRevision(context.Background(), gw, "org-1", "42", "deadbeef", "p/gone.go")
	if err != nil || content != nil || sha != "" {
		t.Fatalf("got content=%v sha=%q err=%v, want the absent sentinel (nil, \"\", nil)", content, sha, err)
	}
}

func TestFileAtRevision_EmptyBlobIdFailsLoud(t *testing.T) {
	t.Parallel()
	// A 200 with no X-Gitlab-Blob-Id would return an empty SHA that collides with the absent sentinel;
	// it must fail loud instead.
	gw := structFakeGW{do: func(*http.Request) (*http.Response, error) {
		return resp(http.StatusOK, "package p\n", nil), nil
	}}
	_, _, err := FileAtRevision(context.Background(), gw, "org-1", "42", "deadbeef", "p/x.go")
	require.Error(t, err, "a 200 with no X-Gitlab-Blob-Id must fail loud")
}

func TestFileAtRevision_TooLargeDegrades(t *testing.T) {
	t.Parallel()
	// A body past the parse limit degrades to coverage: nil content with the blob SHA (so the caller
	// knows the file exists but was not parsed), no error.
	big := strings.Repeat("a", structuralFileBodyLimit+1)
	gw := structFakeGW{do: func(*http.Request) (*http.Response, error) {
		return resp(http.StatusOK, big, http.Header{"X-Gitlab-Blob-Id": {"bigsha"}}), nil
	}}
	content, sha, err := FileAtRevision(context.Background(), gw, "org-1", "42", "deadbeef", "p/big.go")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if content != nil || sha != "bigsha" {
		t.Fatalf("got content len=%d sha=%q, want nil content + the blob SHA (degrade)", len(content), sha)
	}
}

func TestFileAtRevision_UnexpectedStatusErrors(t *testing.T) {
	t.Parallel()
	gw := structFakeGW{do: func(*http.Request) (*http.Response, error) {
		return resp(http.StatusInternalServerError, "boom", nil), nil
	}}
	_, _, err := FileAtRevision(context.Background(), gw, "org-1", "42", "deadbeef", "p/x.go")
	require.Error(t, err, "a 500 status must error")
}
