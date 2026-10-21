// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func pausesConnector(err error) bool {
	var se *sdk.SyncError
	return errors.As(err, &se) && se.Kind == sdk.ErrKindAuth && se.Terminal
}

// When every repository fails, the run pauses the connector only if every
// failure was a credential failure. One repository's 403 next to another
// repository's 502, 404 or blob failure fails the run without pausing.
func TestOne403PlusAnotherFailureDoesNotPauseFetchPRs(t *testing.T) {
	badGateway := vendorapi.ClassifyHTTPErrorWithHeaders(502, []byte(`bad gateway`), nil)
	notFound := vendorapi.ClassifyHTTPErrorWithHeaders(404, []byte(`{"message":"Not Found"}`), nil)
	unauthorized := vendorapi.ClassifyHTTPErrorWithHeaders(401, []byte(`{"message":"Bad credentials"}`), nil)
	blob := errors.New("upload blob: s3 unavailable")
	for _, tc := range []struct {
		name         string
		failA, failB error
		pauses       bool
	}{
		{"403 and 502", forbidden(), badGateway, false},
		{"403 and 404", forbidden(), notFound, false},
		{"403 and a blob failure", forbidden(), blob, false},
		{"all 403", forbidden(), forbidden(), true},
		{"all 401", unauthorized, unauthorized, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runRepoOp(t, &FetchPRsOp{}, tc.failA, tc.failB)
			require.Error(t, err, "every repository failed: the run fails")
			assert.Equal(t, tc.pauses, pausesConnector(err), "err=%v", err)
		})
	}
}

// A repository loop that skips 403s and then stops at its deadline has not
// reached every repository, so a 403 on the first one does not pause.
func TestForbiddenBeforeADeadlineBreakDoesNotPause(t *testing.T) {
	readB := false
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		if r, ok := req.(*connectorpb.GitHubGraphQLRequest); ok && strings.Contains(r.GetQuery(), `name: "b"`) {
			readB = true
		}
		if r, ok := req.(*connectorpb.GitHubGraphQLRequest); ok && strings.Contains(r.GetQuery(), `name: "a"`) {
			time.Sleep(300 * time.Millisecond) // the deadline's reserve is used up here
			return nil, forbidden()
		}
		return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{Issues: &ghGQLConnection[ghGQLIssueNode]{}}}), nil
	}}
	params := testParams(testkit.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/a", "acme/b"}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour+200*time.Millisecond)
	defer cancel()
	_, err := (&FetchIssuesOp{checkpointReserve: time.Hour}).Execute(ctx, params)
	require.False(t, readB, "the loop stopped at its deadline before acme/b")
	assert.False(t, pausesConnector(err), "err=%v", err)
}

// A repository loop that stops at its deadline after every repository it
// reached answered 403 does not pause: the rest were never read.
func TestTallyDeadlineBreakAfterA403DoesNotPause(t *testing.T) {
	readB := false
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		if r, ok := req.(*connectorpb.GitHubGraphQLRequest); ok && strings.Contains(r.GetQuery(), `name: "b"`) {
			readB = true
		}
		if r, ok := req.(*connectorpb.GitHubGraphQLRequest); ok && strings.Contains(r.GetQuery(), `name: "a"`) {
			time.Sleep(300 * time.Millisecond) // the deadline's reserve is used up here
			return nil, forbidden()
		}
		return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{Issues: &ghGQLConnection[ghGQLIssueNode]{}}}), nil
	}}
	params := testParams(testkit.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/a", "acme/b"}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour+200*time.Millisecond)
	defer cancel()
	_, err := (&FetchPRsOp{checkpointReserve: time.Hour}).Execute(ctx, params)
	require.False(t, readB, "the loop stopped at its deadline before acme/b")
	assert.False(t, pausesConnector(err), "err=%v", err)
}
