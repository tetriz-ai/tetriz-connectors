// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// fetch_prs resumes from the last pull request's updated_at, inclusive, so
// every incremental run emits the boundary pull request's records again,
// with the same external ids, even when nothing changed.
func TestLimitation_IncrementalFetchPRsReEmitsTheBoundary(t *testing.T) {
	updated := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	gw := &mockVendorGateway{
		executeFunc: func(context.Context, proto.Message) (proto.Message, error) {
			return graphQLDataResponse(graphQLPRsData(
				[]ghGQLPRNode{{Number: 42, UpdatedAt: updated, State: "OPEN", CreatedAt: updated.Add(-time.Hour)}},
				false, "",
			)), nil
		},
	}
	first := testkit.NewChannelSink()
	cursor, err := (&FetchPRsOp{}).Execute(context.Background(), testParams(first, gw, nil))
	require.NoError(t, err)
	require.Len(t, first.Records(), 2)

	second := testkit.NewChannelSink()
	_, err = (&FetchPRsOp{}).Execute(context.Background(), testParams(second, gw, cursor))
	require.NoError(t, err)
	require.Len(t, second.Records(), 2, "the unchanged boundary pull request comes again")
	assert.Equal(t, first.Records()[0].ExternalID, second.Records()[0].ExternalID)
	assert.Equal(t, "acme/widgets#PR-42#opened", second.Records()[0].ExternalID)
}
