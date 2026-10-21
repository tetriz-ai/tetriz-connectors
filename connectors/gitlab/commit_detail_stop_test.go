// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A commit whose detail fetch (the call that carries its line stats) fails for a
// reason a later attempt can fix is never emitted with the stats-less list
// payload: the cursor would move past it and the stats would never arrive. The
// walk stops at that commit instead. Only a failure no retry can fix (a 404 for
// one commit) keeps the best-effort emit.
func TestFetchCommitsOp_NeverEmitsACommitWithoutStatsOnARetryableDetailFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	project := glProject{ID: 7, Path: "myrepo", PathWithNamespace: "mygroup/myrepo", DefaultBranch: "main"}
	localLimit := &sdk.SyncError{Kind: sdk.ErrKindRateLimit, Retryable: true, Cause: errors.New("rate limit exceeded for group gitlab-rest")}
	vendor503 := &sdk.SyncError{Kind: sdk.ErrKindVendorError, Retryable: true, HTTPStatus: 503, Cause: errors.New("HTTP 503")}
	notFound := &sdk.SyncError{Kind: sdk.ErrKindNotFound, Terminal: true, HTTPStatus: 404, Cause: errors.New("HTTP 404")}

	for _, tc := range []struct {
		name        string
		c2Err       error
		wantErr     bool
		wantEmitted []string
		wantStats   []string
	}{
		{"a rate limit stops the walk", localLimit, true, []string{"c1"}, []string{"c1"}},
		{"a 5xx stops the walk", vendor503, true, []string{"c1"}, []string{"c1"}},
		{"a 404 for one commit stays best-effort", notFound, false, []string{"c1", "c2", "c3"}, []string{"c1", "c3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var list [][]byte
			for i, id := range []string{"c1", "c2", "c3"} {
				b, err := json.Marshal(glCommit{ID: id, CommittedDate: now.Add(-time.Duration(i) * time.Minute)})
				require.NoError(t, err)
				list = append(list, b)
			}
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch r := req.(type) {
					case *connectorpb.GitLabListProjectsRequest:
						return glProjectsResponse([]glProject{project}), nil
					case *connectorpb.GitLabListCommitsRequest:
						return &connectorpb.GitLabListCommitsResponse{Commits: list}, nil
					case *connectorpb.GitLabGetCommitDetailRequest:
						if r.GetSha() == "c2" {
							return nil, tc.c2Err
						}
						return &connectorpb.GitLabGetCommitDetailResponse{Commit: []byte(fmt.Sprintf(
							`{"id":%q,"committed_date":%q,"stats":{"additions":1,"deletions":1,"total":2}}`, r.GetSha(), now.Format(time.RFC3339)))}, nil
					default:
						return nil, fmt.Errorf("unexpected request %T", req)
					}
				},
			}
			sink := testkit.NewChannelSink()
			_, err := (&FetchCommitsOp{}).Execute(context.Background(), testParams(sink, gw, nil))
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			var emitted, withStats []string
			for _, rec := range sink.Records() {
				id := rec.ExternalID[strings.LastIndex(rec.ExternalID, "#")+1:]
				emitted = append(emitted, id)
				if strings.Contains(string(rec.Payload), `"stats"`) {
					withStats = append(withStats, id)
				}
			}
			assert.Equal(t, tc.wantEmitted, emitted)
			assert.Equal(t, tc.wantStats, withStats)
		})
	}
}
