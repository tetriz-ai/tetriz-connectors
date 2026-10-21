// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ghgateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubHTTPDoer wraps http.Client.Do for use in tokenDoer tests.
type stubHTTPDoer struct{ inner *http.Client }

func (d *stubHTTPDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	return d.inner.Do(req)
}

// TestTokenDoer_InjectsAuthorizationHeader verifies the Wave-6 raw Do() path
// (used by fetch_pr_diffs) carries the GitHub installation token. The Execute
// path already covered by authSetter inside requests.go.
func TestTokenDoer_InjectsAuthorizationHeader(t *testing.T) {
	tests := []struct {
		name           string
		token          string
		preset         string
		wantAuthHeader string
	}{
		{
			name:           "token injected when header empty",
			token:          "ghs_secret",
			preset:         "",
			wantAuthHeader: "Bearer ghs_secret",
		},
		{
			name:           "preset header preserved",
			token:          "ghs_secret",
			preset:         "Bearer caller-set",
			wantAuthHeader: "Bearer caller-set",
		},
		{
			name:           "empty token leaves header empty",
			token:          "",
			preset:         "",
			wantAuthHeader: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Authorization")
			}))
			t.Cleanup(srv.Close)

			d := &tokenDoer{inner: &stubHTTPDoer{inner: srv.Client()}, token: tt.token}
			req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
			require.NoError(t, err)
			if tt.preset != "" {
				req.Header.Set("Authorization", tt.preset)
			}
			resp, err := d.Do(context.Background(), req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, tt.wantAuthHeader, got)
		})
	}
}
