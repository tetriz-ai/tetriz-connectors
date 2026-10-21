// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A project loop whose context ends after the first project saves that
// project's watermark through a checkpoint and does not read the next one.
func TestProjectLoopKeepsProgressAtTheDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var read []int64
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch r := req.(type) {
		case *connectorpb.GitLabListProjectsRequest:
			return glProjectsResponse([]glProject{testGLProject(1, "mygroup/a"), testGLProject(2, "mygroup/b")}), nil
		case *connectorpb.GitLabListCommitsRequest:
			read = append(read, r.GetProjectId())
			cancel() // the deadline lands once the first project is read
			return &connectorpb.GitLabListCommitsResponse{Commits: [][]byte{
				[]byte(`{"id":"c1","committed_date":"2026-06-01T10:00:00Z","authored_date":"2026-06-01T10:00:00Z"}`),
			}}, nil
		}
		return nil, errors.New("unexpected request")
	}}
	var saved []*sdk.Cursor
	params := testParams(testkit.NewChannelSink(), gw, nil)
	params.Checkpoint = func(_ context.Context, c *sdk.Cursor) error { saved = append(saved, c); return nil }
	_, err := (&FetchCommitsOp{}).Execute(ctx, params)
	require.Error(t, err, "the context ended")
	assert.Equal(t, []int64{1}, read, "the second project is not read")
	require.NotEmpty(t, saved, "the progress is checkpointed")
	assert.True(t, strings.Contains(string(saved[len(saved)-1].Data), `"1"`), "the first project's watermark is saved: %s", saved[len(saved)-1].Data)
}
