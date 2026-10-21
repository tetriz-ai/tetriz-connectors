// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestEdge_ConcurrentRunsShareOnePaginationConfig: the connector hands one
// GitHubPaginationConfig to every operation instance it builds (connector.go), so two
// fetch_prs runs for different organizations execute against the same entry at once.
// An entry with zero-valued fields must be defaulted without writing to that shared
// entry — run under -race, a write is a data race — and the operator's config must
// read back as configured.
func TestEdge_ConcurrentRunsShareOnePaginationConfig(t *testing.T) {
	shared := &config.GitHubPaginationConfig{Operations: map[string]*config.OperationPaginationConfig{
		opFetchPRs: {PageSize: 10}, // MinPageSize and BackoffDivisor left zero
	}}
	now := time.Now().UTC().Truncate(time.Second)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gw := &mockVendorGateway{executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
				return graphQLDataResponse(graphQLPRsData(
					[]ghGQLPRNode{{Number: 1, UpdatedAt: now, CreatedAt: now, State: "OPEN"}}, false, "")), nil
			}}
			_, err := (&FetchPRsOp{paginationCfg: shared}).Execute(context.Background(),
				testParams(testkit.NewChannelSink(), gw, nil))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	got := opPageConfig(shared, opFetchPRs)
	assert.Equal(t, 10, got.PageSize)
	assert.Equal(t, 1, got.MinPageSize)
	assert.Equal(t, 2, got.BackoffDivisor)
	assert.Zero(t, shared.Operations[opFetchPRs].BackoffDivisor, "the shared entry is left as configured")
}
