// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

// Tests of how project loops treat one failing project.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// One failing project (a 502) is that project's failure in GitLab fetch_prs
// and fetch_commits: the projects after it are still read, and the run fails
// only because every project failed here.
func TestOneFailingProjectIsSkipped(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   sdk.APIFetchOperation
	}{
		{"fetch_prs", &FetchPRsOp{}},
		{"fetch_commits", &FetchCommitsOp{}},
		{"fetch_reviews", &FetchReviewsOp{}},
		{"fetch_pr_comments", &FetchPRCommentsOp{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var read []int64
			gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				switch r := req.(type) {
				case *connectorpb.GitLabListProjectsRequest:
					return glProjectsResponse([]glProject{testGLProject(1, "mygroup/a"), testGLProject(2, "mygroup/b")}), nil
				case *connectorpb.GitLabListProjectMergeRequestsRequest:
					read = append(read, r.GetProjectId())
				case *connectorpb.GitLabListCommitsRequest:
					read = append(read, r.GetProjectId())
				default:
					return nil, errors.New("unexpected request")
				}
				return nil, errors.New("502 Bad Gateway")
			}}
			_, err := tc.op.Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, nil))
			require.Error(t, err, "every project failed")
			assert.Contains(t, read, int64(1))
			assert.Contains(t, read, int64(2), "the project after the failing one is read")
		})
	}
}
