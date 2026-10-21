// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// bodyRecordingDoer answers each call from statuses in order and records the
// request body each call actually carried.
type bodyRecordingDoer struct {
	statuses []int
	bodies   []string
	auths    []string
}

func (d *bodyRecordingDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	var b []byte
	if req.Body != nil {
		b, _ = io.ReadAll(req.Body)
	}
	d.bodies = append(d.bodies, string(b))
	d.auths = append(d.auths, req.Header.Get("Authorization"))
	st := d.statuses[len(d.bodies)-1]
	return &http.Response{StatusCode: st, Body: io.NopCloser(bytes.NewReader(nil)), Header: http.Header{}}, nil
}

func jiraTokenServer(t *testing.T, payload map[string]any) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(payload)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func jiraCreds(t *testing.T) *mockConfigStore {
	t.Helper()
	credsJSON, err := json.Marshal(jiraconfig.JiraCredentials{OAuthRefreshToken: "rt"})
	require.NoError(t, err)
	return &mockConfigStore{getCfg: &sdk.ConnectorConfig{ConnectorID: "jira", OrganizationID: testOrgID, Credentials: credsJSON}}
}

// Jira's issue search is a POST. When the first attempt is answered 401 and
// the doer refreshes and retries, the retry must carry the same JQL body:
// the first attempt drained req.Body, so the retry must rewind it from
// GetBody rather than resend an empty body.
func TestEdge_401RetryResendsThePOSTBody(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	_ = mr.Set(jiraconfig.AccessTokenKey(testOrgID), "stale")

	srv := jiraTokenServer(t, map[string]any{"access_token": "fresh", "expires_in": 3600})
	inner := &bodyRecordingDoer{statuses: []int{http.StatusUnauthorized, http.StatusOK}}
	d := newTestAutoRefreshDoer(inner, rc, jiraCreds(t), srv.URL)

	const jql = `{"jql":"project = ABC","maxResults":50}`
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"https://api.atlassian.com/ex/jira/x/rest/api/3/search/jql", bytes.NewReader([]byte(jql)))
	require.NoError(t, err)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	_ = resp.Body.Close()

	require.Len(t, inner.bodies, 2)
	assert.Equal(t, jql, inner.bodies[0])
	assert.Equal(t, "Bearer fresh", inner.auths[1])
	assert.Equal(t, jql, inner.bodies[1], "the retried POST after a 401 lost its body")
}

// A token endpoint that answers 200 without an access_token has not minted a
// usable token. The refresh must fail rather than send "Bearer " (an empty
// credential) to the vendor.
func TestEdge_RefreshWithoutAccessTokenFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	srv := jiraTokenServer(t, map[string]any{"refresh_token": "rotated"})
	inner := &bodyRecordingDoer{statuses: []int{http.StatusOK}}
	d := newTestAutoRefreshDoer(inner, rc, jiraCreds(t), srv.URL)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.atlassian.com/ex/jira/x/rest/api/3/myself", nil)
	require.NoError(t, err)
	resp, err := d.Do(context.Background(), req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	assert.Error(t, err, "a refresh that minted no access token must fail")
	for _, a := range inner.auths {
		assert.NotEqual(t, "Bearer ", a, "a request went out with an empty bearer token")
	}
}
