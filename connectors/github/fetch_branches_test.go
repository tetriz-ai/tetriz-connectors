// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// branchDoGateway is a test VendorGateway that answers gw.Do calls from a
// page→response map keyed by the `page` query parameter. Execute is unused
// because these tests configure an explicit repo list (no auto-discovery).
type branchDoGateway struct {
	// pages maps a page number to the JSON body returned for that page.
	pages map[int]string
	// lastPage is the highest page number; a Link rel="next" header is emitted
	// for every page below it.
	lastPage int
	// calledURLs records each requested URL for header/URL assertions.
	calledURLs []string
	// authHeaders records the Accept header seen on the first call.
	sawAccept string
}

func (g *branchDoGateway) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	g.calledURLs = append(g.calledURLs, req.URL.String())
	if g.sawAccept == "" {
		g.sawAccept = req.Header.Get("Accept")
	}
	page := 1
	if p := req.URL.Query().Get("page"); p != "" {
		_, _ = fmt.Sscanf(p, "%d", &page)
	}
	body := g.pages[page]
	header := http.Header{}
	if page < g.lastPage {
		next := fmt.Sprintf("<https://api.github.com/repos/o/r/branches?per_page=100&page=%d>; rel=\"next\"", page+1)
		header.Set("Link", next)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     header,
	}, nil
}

func (g *branchDoGateway) DoJSON(_ context.Context, _, _ string, _, _ any) error {
	return fmt.Errorf("DoJSON not used in fetch_branches tests")
}

func (g *branchDoGateway) Execute(_ context.Context, _ proto.Message) (proto.Message, error) {
	return nil, fmt.Errorf("Execute not used in fetch_branches tests")
}

func (g *branchDoGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *branchDoGateway) ForGroup(_ string) sdk.VendorGateway { return g }

// branchTestParams builds ExecutionParams with an explicit repo list so the op
// resolves DatabaseID + DefaultBranch from DiscoveredRepos (no Execute needed).
func branchTestParams(sink sdk.DataSink, gw sdk.VendorGateway) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-branches-test",
			Credentials:    mustJSON(GitHubCredentials{InstallationID: "99999"}),
			Settings: mustJSON(GitHubSettings{
				Organization: "testorg",
				Repositories: []string{"testorg/myrepo"},
				DiscoveredRepos: []GitHubDiscoveredRepo{
					{FullName: "testorg/myrepo", DatabaseID: 55, DefaultBranch: "main"},
				},
			}),
			Enabled: true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}
}

func TestFetchBranchesOp_OperationID(t *testing.T) {
	assert.Equal(t, sdk.OperationID("fetch_branches"), (&FetchBranchesOp{}).OperationID())
}

// TestFetchBranchesOp_EmitsOnePerBranch verifies unmarshaling, record keys,
// operation_id, data dates, and the computed metadata (head_sha, protected,
// is_default) for a single page of branches.
func TestFetchBranchesOp_EmitsOnePerBranch(t *testing.T) {
	body := `[
		{"name":"main","commit":{"sha":"sha-main"},"protected":true},
		{"name":"feature/x","commit":{"sha":"sha-feat"},"protected":false}
	]`
	gw := &branchDoGateway{pages: map[int]string{1: body}, lastPage: 1}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchTestParams(sink, gw))
	require.NoError(t, err)

	// Auth/URL construction assertions.
	require.NotEmpty(t, gw.calledURLs)
	assert.Contains(t, gw.calledURLs[0], "/repos/testorg/myrepo/branches")
	assert.Contains(t, gw.calledURLs[0], "per_page=100")
	assert.Equal(t, "application/vnd.github+json", gw.sawAccept)

	records := sink.Records()
	require.Len(t, records, 2)

	byKey := map[string]*sdk.RawRecord{}
	for _, r := range records {
		byKey[r.ExternalID] = r
		assert.Equal(t, "fetch_branches", r.OperationID)
		// A branch is current state: no DataDate, so a date-keyed sink keeps
		// one row per branch across runs.
		assert.True(t, r.DataDate.IsZero(), "DataDate must be empty, got %v", r.DataDate)
	}

	main := byKey["55:main"]
	require.NotNil(t, main, "record key must be {platform_repo_id}:{branch}")
	assert.Equal(t, "sha-main", main.Metadata["head_sha"])
	assert.Equal(t, "true", main.Metadata["protected"])
	assert.Equal(t, "true", main.Metadata["is_default"])
	assert.Equal(t, "main", main.Metadata["branch_name"])
	// Both records of this repo carry the repo's full branch count so the
	// consumer can tell a complete fetch from a torn one.
	assert.Equal(t, "2", main.Metadata["branch_total"])

	feat := byKey["55:feature/x"]
	require.NotNil(t, feat)
	assert.Equal(t, "sha-feat", feat.Metadata["head_sha"])
	assert.Equal(t, "false", feat.Metadata["protected"])
	assert.Equal(t, "false", feat.Metadata["is_default"], "non-default branch")
	assert.Equal(t, "2", feat.Metadata["branch_total"])
}

// TestFetchBranchesOp_Pagination verifies the Link rel="next" header is followed
// across two pages and all branches are emitted.
func TestFetchBranchesOp_Pagination(t *testing.T) {
	page1 := `[{"name":"b1","commit":{"sha":"s1"},"protected":false}]`
	page2 := `[{"name":"b2","commit":{"sha":"s2"},"protected":false}]`
	gw := &branchDoGateway{pages: map[int]string{1: page1, 2: page2}, lastPage: 2}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchTestParams(sink, gw))
	require.NoError(t, err)

	require.Len(t, gw.calledURLs, 2, "expected two page fetches")
	assert.Contains(t, gw.calledURLs[1], "page=2")

	records := sink.Records()
	require.Len(t, records, 2)
	keys := []string{records[0].ExternalID, records[1].ExternalID}
	assert.ElementsMatch(t, []string{"55:b1", "55:b2"}, keys)
}

// TestFetchBranchesOp_PayloadIsVendorJSON verifies the emitted payload is the
// verbatim vendor branch object (so a consumer parses vendor fields directly).
func TestFetchBranchesOp_PayloadIsVendorJSON(t *testing.T) {
	body := `[{"name":"main","commit":{"sha":"abc"},"protected":true}]`
	gw := &branchDoGateway{pages: map[int]string{1: body}, lastPage: 1}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchTestParams(sink, gw))
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1)
	assert.Contains(t, string(records[0].Payload), `"protected":true`)
	assert.Contains(t, string(records[0].Payload), `"sha":"abc"`)
}

// TestFetchBranchesOp_ListErrorSkipsRepo verifies a per-repo listing failure is
// logged and skipped rather than failing the run.
func TestFetchBranchesOp_ListErrorSkipsRepo(t *testing.T) {
	gw := &errBranchGateway{}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchTestParams(sink, gw))
	require.Error(t, err, "the only repository failed, so the run fails")
	assert.Empty(t, sink.Records())
}

// errBranchGateway fails every Do call.
type errBranchGateway struct{ branchDoGateway }

func (g *errBranchGateway) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("simulated network error")
}
