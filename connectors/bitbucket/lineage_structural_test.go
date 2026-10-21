// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

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

func structResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}

// Bitbucket has no blob SHA, so the key is the commit+path composite and is non-empty when the file
// exists — distinguishing a present file from the absent sentinel.
func TestFileAtRevision_NormalUsesCommitPathKey(t *testing.T) {
	t.Parallel()
	gw := structFakeGW{do: func(*http.Request) (*http.Response, error) {
		return structResp(http.StatusOK, "package p\n"), nil
	}}
	content, key, err := FileAtRevision(context.Background(), gw, "org-1", "ws/repo", "deadbeef", "p/x.go")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(content) != "package p\n" {
		t.Fatalf("content = %q, want the file", content)
	}
	if key != "deadbeef:p/x.go" {
		t.Fatalf("key = %q, want the commit+path composite", key)
	}
}

func TestFileAtRevision_AbsentIs404Sentinel(t *testing.T) {
	t.Parallel()
	gw := structFakeGW{do: func(*http.Request) (*http.Response, error) {
		return structResp(http.StatusNotFound, "no such path"), nil
	}}
	content, key, err := FileAtRevision(context.Background(), gw, "org-1", "ws/repo", "deadbeef", "p/gone.go")
	if err != nil || content != nil || key != "" {
		t.Fatalf("got content=%v key=%q err=%v, want the absent sentinel", content, key, err)
	}
}

func TestFileAtRevision_BadRepoFullName(t *testing.T) {
	t.Parallel()
	gw := structFakeGW{do: func(*http.Request) (*http.Response, error) {
		t.Fatal("Do should not be called for a bad repo full name")
		return nil, nil
	}}
	_, _, err := FileAtRevision(context.Background(), gw, "org-1", "norepo", "deadbeef", "p/x.go")
	require.Error(t, err, "a repo full name with no slash must error")
}
