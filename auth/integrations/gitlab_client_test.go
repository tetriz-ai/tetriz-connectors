// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FetchGroups follows the rel="next" URL from the GitLab Link header and re-sends
// the OAuth Bearer token on each hop. A compromised/malicious GitLab instance can
// point that next-URL at an arbitrary host (e.g. the cloud metadata endpoint),
// turning pagination into an SSRF that also exfiltrates the token. These cases lock
// in that only a same-origin next-URL is followed; any other host stops pagination
// with an error and is never contacted with the token.
func TestFetchGroups_PaginationLinkHostValidation(t *testing.T) {
	const accessToken = "gl-access-token"

	tests := []struct {
		name string
		// nextHostFn builds the rel="next" URL the first page returns, given the
		// trusted server's own base URL. Empty result means "no Link header".
		nextHostFn func(baseURL string) string
		wantErr    bool
		wantPages  int // GETs the trusted server should receive
	}{
		{
			name:       "same-origin next link is followed",
			nextHostFn: func(baseURL string) string { return baseURL + "/api/v4/groups?page=2" },
			wantErr:    false,
			wantPages:  2,
		},
		{
			name:       "no next link stops after first page",
			nextHostFn: func(string) string { return "" },
			wantErr:    false,
			wantPages:  1,
		},
		{
			name:       "cross-host next link (metadata endpoint) is refused",
			nextHostFn: func(string) string { return "http://169.254.169.254/latest/meta-data/" },
			wantErr:    true,
			wantPages:  1,
		},
		{
			name:       "different host next link is refused",
			nextHostFn: func(string) string { return "http://evil.example.com/api/v4/groups?page=2" },
			wantErr:    true,
			wantPages:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var trustedGETs int32
			var attackerHit int32

			// Attacker-controlled host: if the client ever follows a cross-host link,
			// this records the leaked-token request and fails the test.
			attacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&attackerHit, 1)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[]`))
			}))
			defer attacker.Close()

			var trusted *httptest.Server
			trusted = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := atomic.AddInt32(&trustedGETs, 1)
				assert.Equal(t, "Bearer "+accessToken, r.Header.Get("Authorization"))
				if n == 1 {
					next := tt.nextHostFn(trusted.URL)
					// For the cross-host cases, point at the live attacker server so a
					// regression would actually reach it (not just any unrouteable host).
					if next != "" && tt.wantErr {
						next = attacker.URL + "/api/v4/groups?page=2"
					}
					if next != "" {
						w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="next"`, next))
					}
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"id":1,"path":"g","full_path":"g","name":"G"}]`))
			}))
			defer trusted.Close()

			client := NewGitLabOAuthClient("cid", "secret", "cb", WithGitLabBaseURL(trusted.URL))
			_, err := client.FetchGroups(context.Background(), accessToken)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantPages, int(atomic.LoadInt32(&trustedGETs)), "trusted server page count")
			assert.Zero(t, atomic.LoadInt32(&attackerHit), "token must never be sent to the attacker host")
		})
	}
}
