// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// commitPullsGateway is a minimal VendorGateway that returns one canned response for the
// commit->pulls REST lookup, capturing the requested URL for assertions.
type commitPullsGateway struct {
	status     int
	body       string
	doErr      error
	calledURL  string
	sawAccept  string
	sawVersion string
}

func (g *commitPullsGateway) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	g.calledURL = req.URL.String()
	g.sawAccept = req.Header.Get("Accept")
	g.sawVersion = req.Header.Get("X-GitHub-Api-Version")
	if g.doErr != nil {
		return nil, g.doErr
	}
	return &http.Response{
		StatusCode: g.status,
		Body:       io.NopCloser(bytes.NewBufferString(g.body)),
		Header:     http.Header{},
	}, nil
}

func (g *commitPullsGateway) DoJSON(_ context.Context, _, _ string, _, _ any) error {
	return fmt.Errorf("DoJSON not used")
}

func (g *commitPullsGateway) Execute(_ context.Context, _ proto.Message) (proto.Message, error) {
	return nil, fmt.Errorf("Execute not used")
}

func (g *commitPullsGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *commitPullsGateway) ForGroup(_ string) sdk.VendorGateway { return g }

func TestResolveGitHubCommitPR(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		doErr   error
		wantNum int32
		wantOK  bool
	}{
		{
			name:    "prefers the merged PR over an open one",
			status:  http.StatusOK,
			body:    `[{"number": 10, "merged_at": null}, {"number": 42, "merged_at": "2026-01-02T03:04:05Z"}]`,
			wantNum: 42,
			wantOK:  true,
		},
		{
			name:    "falls back to the first when none merged",
			status:  http.StatusOK,
			body:    `[{"number": 7, "merged_at": null}]`,
			wantNum: 7,
			wantOK:  true,
		},
		{
			name:    "empty list resolves nothing",
			status:  http.StatusOK,
			body:    `[]`,
			wantNum: 0,
			wantOK:  false,
		},
		{
			name:    "non-200 resolves nothing",
			status:  http.StatusForbidden,
			body:    `{"message":"nope"}`,
			wantNum: 0,
			wantOK:  false,
		},
		{
			name:    "malformed body resolves nothing",
			status:  http.StatusOK,
			body:    `not json`,
			wantNum: 0,
			wantOK:  false,
		},
		{
			name:    "transport error resolves nothing",
			status:  0,
			doErr:   fmt.Errorf("boom"),
			wantNum: 0,
			wantOK:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &commitPullsGateway{status: tt.status, body: tt.body, doErr: tt.doErr}
			num, ok := resolveGitHubCommitPR(context.Background(), gw, "org-1", "octo/repo", "deadbeef")
			assert.Equal(t, tt.wantNum, num)
			assert.Equal(t, tt.wantOK, ok)
			if tt.doErr == nil {
				assert.Contains(t, gw.calledURL, "/repos/octo/repo/commits/deadbeef/pulls")
				assert.Equal(t, "application/vnd.github+json", gw.sawAccept)
				assert.Equal(t, "2022-11-28", gw.sawVersion)
			}
		})
	}
}
