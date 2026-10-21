// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A failed enrichment inside a run logs a WARN and does not fail it:
// fetch_org_members emits its members without team memberships when the teams
// query answers 400, which is recorded as vendor_error, not internal.
func TestFailedTeamEnrichmentDoesNotFailFetchOrgMembers(t *testing.T) {
	badRequest := vendorapi.ClassifyHTTPErrorWithHeaders(400, []byte(`{"message":"Bad request"}`), nil)
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		r, ok := req.(*connectorpb.GitHubGraphQLRequest)
		if !ok {
			return nil, errors.New("unexpected request")
		}
		switch q := r.GetQuery(); {
		case strings.Contains(q, "membersWithRole"):
			return &connectorpb.GitHubGraphQLResponse{Data: []byte(`{"organization":{"membersWithRole":{"nodes":[{"login":"octo","databaseId":7}],"pageInfo":{"hasNextPage":false}}}}`)}, nil
		case strings.Contains(q, "teams("):
			return nil, badRequest
		default:
			return nil, errors.New("SAML is not configured here")
		}
	}}
	sink := testkit.NewChannelSink()
	_, err := (&FetchOrgMembersOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err, "the enrichment failure does not fail the run")
	require.Len(t, sink.Records(), 1)
	assert.Equal(t, sdk.ErrKindVendorError, badRequest.Kind)
}
