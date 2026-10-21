// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// glBranchGateway answers Execute (fetchAllProjects) and Do (branch listing).
// Do returns a page body keyed by the `page` query param and sets X-Next-Page
// for every page below lastPage.
type glBranchGateway struct {
	projects   [][]byte
	pages      map[int]string
	lastPage   int
	calledURLs []string
}

func (g *glBranchGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	if _, ok := req.(*connectorpb.GitLabListProjectsRequest); ok {
		return &connectorpb.GitLabListProjectsResponse{Projects: g.projects}, nil
	}
	return nil, fmt.Errorf("unexpected Execute call: %T", req)
}

func (g *glBranchGateway) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	g.calledURLs = append(g.calledURLs, req.URL.String())
	page := 1
	if p := req.URL.Query().Get("page"); p != "" {
		_, _ = fmt.Sscanf(p, "%d", &page)
	}
	header := http.Header{}
	if page < g.lastPage {
		header.Set("X-Next-Page", fmt.Sprintf("%d", page+1))
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(g.pages[page])),
		Header:     header,
	}, nil
}

func (g *glBranchGateway) DoJSON(_ context.Context, _, _ string, _, _ any) error {
	return fmt.Errorf("DoJSON not used in fetch_branches tests")
}

func (g *glBranchGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *glBranchGateway) ForGroup(_ string) sdk.VendorGateway { return g }

func newGLBranchGW(projects []glProject, pages map[int]string, lastPage int) *glBranchGateway {
	raw := make([][]byte, len(projects))
	for i, p := range projects {
		raw[i], _ = json.Marshal(p)
	}
	return &glBranchGateway{projects: raw, pages: pages, lastPage: lastPage}
}

func branchParamsGL(sink sdk.DataSink, gw sdk.VendorGateway) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: "org-branches-gl-test",
			Credentials:    mustJSON(GitLabCredentials{OAuthRefreshToken: "tok"}),
			Settings:       mustJSON(GitLabSettings{GroupID: 42, GroupName: "My Group"}),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}
}

func TestFetchBranchesOp_GL_OperationID(t *testing.T) {
	assert.Equal(t, sdk.OperationID("fetch_branches"), (&FetchBranchesOp{}).OperationID())
}

// TestFetchBranchesOp_GL_EmitsOnePerBranch verifies unmarshaling, record keys,
// operation_id, data dates, and the metadata (head_sha, protected, is_default).
func TestFetchBranchesOp_GL_EmitsOnePerBranch(t *testing.T) {
	body := `[
		{"name":"main","commit":{"id":"sha-main"},"default":true,"protected":true},
		{"name":"dev","commit":{"id":"sha-dev"},"default":false,"protected":false}
	]`
	gw := newGLBranchGW([]glProject{testGLProject(101, "mygroup/myrepo")}, map[int]string{1: body}, 1)
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsGL(sink, gw))
	require.NoError(t, err)

	require.NotEmpty(t, gw.calledURLs)
	assert.Contains(t, gw.calledURLs[0], "/projects/101/repository/branches")
	assert.Contains(t, gw.calledURLs[0], "per_page=100")

	records := sink.Records()
	require.Len(t, records, 2)

	byKey := map[string]*sdk.RawRecord{}
	for _, r := range records {
		byKey[r.ExternalID] = r
		assert.Equal(t, "fetch_branches", r.OperationID)
		assert.True(t, r.DataDate.IsZero(), "a branch is current state: no DataDate")
	}

	main := byKey["101:main"]
	require.NotNil(t, main, "record key must be {project_id}:{branch}")
	assert.Equal(t, "sha-main", main.Metadata["head_sha"])
	assert.Equal(t, "true", main.Metadata["protected"])
	assert.Equal(t, "true", main.Metadata["is_default"])
	// The project's full branch count, so a consumer can tell a complete
	// fetch from a torn one.
	assert.Equal(t, "2", main.Metadata["branch_total"])

	dev := byKey["101:dev"]
	require.NotNil(t, dev)
	assert.Equal(t, "false", dev.Metadata["is_default"])
	assert.Equal(t, "false", dev.Metadata["protected"])
	assert.Equal(t, "2", dev.Metadata["branch_total"])
}

// TestFetchBranchesOp_GL_DetectionPriors verifies committed_date is stamped as
// vendor_updated_at when present and omitted when absent, and the merged flag is
// always carried.
func TestFetchBranchesOp_GL_DetectionPriors(t *testing.T) {
	body := `[
		{"name":"main","commit":{"id":"sha-main","committed_date":"2026-05-05T10:00:00Z"},"default":true,"protected":true,"merged":false},
		{"name":"stale","commit":{"id":"sha-stale"},"default":false,"merged":true}
	]`
	gw := newGLBranchGW([]glProject{testGLProject(707, "mygroup/pri")}, map[int]string{1: body}, 1)
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsGL(sink, gw))
	require.NoError(t, err)

	byKey := map[string]*sdk.RawRecord{}
	for _, r := range sink.Records() {
		byKey[r.ExternalID] = r
	}

	main := byKey["707:main"]
	require.NotNil(t, main)
	assert.Equal(t, "2026-05-05T10:00:00Z", main.Metadata["vendor_updated_at"])
	assert.Equal(t, "false", main.Metadata["merged"])

	stale := byKey["707:stale"]
	require.NotNil(t, stale)
	_, hasDate := stale.Metadata["vendor_updated_at"]
	assert.False(t, hasDate, "an absent committed_date must omit vendor_updated_at, not stamp epoch")
	assert.Equal(t, "true", stale.Metadata["merged"])
}

// TestFetchBranchesOp_GL_Pagination verifies X-Next-Page is followed across pages.
func TestFetchBranchesOp_GL_Pagination(t *testing.T) {
	page1 := `[{"name":"b1","commit":{"id":"s1"},"default":true}]`
	page2 := `[{"name":"b2","commit":{"id":"s2"},"default":false}]`
	gw := newGLBranchGW([]glProject{testGLProject(202, "mygroup/big")}, map[int]string{1: page1, 2: page2}, 2)
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsGL(sink, gw))
	require.NoError(t, err)

	require.Len(t, gw.calledURLs, 2)
	assert.Contains(t, gw.calledURLs[1], "page=2")

	records := sink.Records()
	require.Len(t, records, 2)
	assert.ElementsMatch(t, []string{"202:b1", "202:b2"},
		[]string{records[0].ExternalID, records[1].ExternalID})
}

// TestFetchBranchesOp_GL_PayloadIsVendorJSON verifies the payload is verbatim vendor JSON.
func TestFetchBranchesOp_GL_PayloadIsVendorJSON(t *testing.T) {
	body := `[{"name":"main","commit":{"id":"abc"},"default":true,"protected":true}]`
	gw := newGLBranchGW([]glProject{testGLProject(303, "mygroup/r")}, map[int]string{1: body}, 1)
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsGL(sink, gw))
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1)
	assert.Contains(t, string(records[0].Payload), `"default":true`)
	assert.Contains(t, string(records[0].Payload), `"id":"abc"`)
}

// TestFetchBranchesOp_GL_ListErrorSkipsProject verifies a listing failure is
// logged and skipped rather than failing the run.
func TestFetchBranchesOp_GL_ListErrorSkipsProject(t *testing.T) {
	gw := &glErrBranchGateway{glBranchGateway: *newGLBranchGW([]glProject{testGLProject(404, "mygroup/r")}, nil, 0)}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsGL(sink, gw))
	require.Error(t, err, "the only repository failed, so the run fails")
	assert.Empty(t, sink.Records())
}

type glErrBranchGateway struct{ glBranchGateway }

func (g *glErrBranchGateway) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("simulated network error")
}
func (g *glErrBranchGateway) ForGroup(_ string) sdk.VendorGateway { return g }

// glPartialBranchGateway fails the branch listing of project 404 only.
type glPartialBranchGateway struct{ glBranchGateway }

func (g *glPartialBranchGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if strings.Contains(req.URL.Path, "/projects/404/") {
		return nil, fmt.Errorf("simulated network error")
	}
	return g.glBranchGateway.Do(ctx, req)
}
func (g *glPartialBranchGateway) ForGroup(_ string) sdk.VendorGateway { return g }

// One failing project of two is skipped: the run completes with the other
// project's branches.
func TestFetchBranchesOp_GL_OneFailingProjectOfTwoIsSkipped(t *testing.T) {
	body := `[{"name":"main","commit":{"id":"sha-main"},"default":true,"protected":true}]`
	gw := &glPartialBranchGateway{glBranchGateway: *newGLBranchGW(
		[]glProject{testGLProject(404, "mygroup/bad"), testGLProject(101, "mygroup/good")}, map[int]string{1: body}, 1)}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsGL(sink, gw))
	require.NoError(t, err, "one project of two failed, so the run completes")
	require.Len(t, sink.Records(), 1)
	assert.Contains(t, sink.Records()[0].ExternalID, "101")
}
