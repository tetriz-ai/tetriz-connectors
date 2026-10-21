// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A commit whose diffstat fetch fails for a reason a later attempt can fix is
// never emitted with defaulted line counts: the cursor would move past it and the
// stats would never arrive. Our own limiter's rejection is waited out in the run;
// any other retryable failure stops the walk at that commit so the next run
// resumes there. Only a failure no retry can fix (a 404 for one commit) keeps the
// best-effort emit without stats.
func TestFetchCommitsOp_NeverEmitsACommitWithoutStatsOnARetryableDiffstatFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	localLimit := &sdk.SyncError{
		Kind: sdk.ErrKindRateLimit, Retryable: true,
		Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
		RetryAfterSeconds: 30, LocalWait: 10 * time.Millisecond,
	}
	vendor429 := &sdk.SyncError{Kind: sdk.ErrKindRateLimit, Retryable: true, HTTPStatus: 429, Cause: errors.New("HTTP 429")}
	vendor503 := &sdk.SyncError{Kind: sdk.ErrKindVendorError, Retryable: true, HTTPStatus: 503, Cause: errors.New("HTTP 503")}
	notFound := &sdk.SyncError{Kind: sdk.ErrKindNotFound, Terminal: true, HTTPStatus: 404, Cause: errors.New("HTTP 404")}

	for _, tc := range []struct {
		name string
		// sha-2's diffstat answers with errs in turn, then succeeds.
		errs        []error
		wantErr     bool
		wantEmitted []string
		// wantStats: commits emitted with line counts; the rest are emitted without.
		wantStats []string
	}{
		{"our limiter is waited out", []error{localLimit, localLimit}, false, []string{"sha-1", "sha-2", "sha-3"}, []string{"sha-1", "sha-2", "sha-3"}},
		{"a vendor 429 stops the walk", []error{vendor429}, true, []string{"sha-1"}, []string{"sha-1"}},
		{"a 5xx stops the walk", []error{vendor503}, true, []string{"sha-1"}, []string{"sha-1"}},
		{"a 404 for one commit stays best-effort", []error{notFound}, false, []string{"sha-1", "sha-2", "sha-3"}, []string{"sha-1", "sha-3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sha2Calls := 0
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch r := req.(type) {
					case *connectorpb.BitbucketListReposRequest:
						return &connectorpb.BitbucketListReposResponse{
							Repos: [][]byte{mustMarshalJSON(t, bbRepository{FullName: "ws/repo", Slug: "repo"})},
						}, nil
					case *connectorpb.BitbucketListCommitsRequest:
						return &connectorpb.BitbucketListCommitsResponse{Commits: [][]byte{
							mustMarshalJSON(t, bbCommit{Hash: "sha-1", Date: now}),
							mustMarshalJSON(t, bbCommit{Hash: "sha-2", Date: now.Add(-time.Minute)}),
							mustMarshalJSON(t, bbCommit{Hash: "sha-3", Date: now.Add(-2 * time.Minute)}),
						}}, nil
					case *connectorpb.BitbucketGetCommitDiffstatRequest:
						if r.GetCommitSha() == "sha-2" {
							sha2Calls++
							if sha2Calls <= len(tc.errs) {
								return nil, tc.errs[sha2Calls-1]
							}
						}
						return bbCommitDiffstatResponse([]bbDiffstatEntry{
							{LinesAdded: 4, LinesRemoved: 1, New: &bbDiffstatFile{Path: "a.go"}},
						}, ""), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}
			sink := testkit.NewChannelSink()
			cfg := validConfig()
			cfg.Settings = mustJSON(BitbucketSettings{Workspace: "ws"})
			params := &sdk.ExecutionParams{Config: cfg, Sink: sink, VendorGateway: gw, FetchCache: newFakeFetchCache()}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			_, err := (&FetchCommitsOp{}).Execute(ctx, params)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			var emitted, withStats []string
			for _, rec := range sink.Records() {
				sha := rec.ExternalID[len("ws/repo#"):]
				emitted = append(emitted, sha)
				if _, ok := rec.Metadata["additions"]; ok {
					withStats = append(withStats, sha)
				}
			}
			assert.Equal(t, tc.wantEmitted, emitted)
			assert.Equal(t, tc.wantStats, withStats)
		})
	}
}
