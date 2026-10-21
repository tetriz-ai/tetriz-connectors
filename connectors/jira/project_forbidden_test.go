// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"errors"
	"fmt"
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

func runJiraIssues(t *testing.T, forbidden func(project string) bool) (*sdk.Cursor, error) {
	t.Helper()
	base := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch r := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "1", Key: "PROJA"}, {ID: "2", Key: "PROJB"}, {ID: "3", Key: "PROJC"}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			for _, p := range []string{"PROJA", "PROJB", "PROJC"} {
				if strings.Contains(r.GetJql(), p) && forbidden(p) {
					return nil, vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"errorMessages":["forbidden"]}`), nil)
				}
			}
			return searchIssuesResponse([]jiraIssue{{ID: "1", Key: "X-1", Fields: jiraIssueFields{Created: jiraTime{base}, Updated: jiraTime{base}}}}, ""), nil
		}
		return nil, fmt.Errorf("unexpected %T", req)
	}}
	return (&FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}).Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, nil))
}

// One project that answers 403 while the others sync is that project's
// failure: it is skipped and the run completes. Every project answering
// 403 is the credential's, so the run fails as a terminal auth error.
func TestJiraOneForbiddenProjectIsSkipped(t *testing.T) {
	cursor, err := runJiraIssues(t, func(p string) bool { return p == "PROJB" })
	require.NoError(t, err, "one forbidden project of three does not fail or pause the run")
	parts, err := sdk.CompositeFromCursor(cursor)
	require.NoError(t, err)
	assert.Contains(t, parts, "PROJA")
	assert.Contains(t, parts, "PROJC")
	assert.NotContains(t, parts, "PROJB", "the forbidden project keeps its (empty) cursor")

	_, err = runJiraIssues(t, func(string) bool { return true })
	var se *sdk.SyncError
	require.True(t, errors.As(err, &se), "err=%v", err)
	assert.Equal(t, sdk.ErrKindAuth, se.Kind)
	assert.True(t, se.Terminal, "every project forbidden pauses the connector")
}
