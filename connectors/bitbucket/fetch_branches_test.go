// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
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

// bbBranchGateway answers Execute (BitbucketGetRepo, for fetchAllReposInfo on
// the explicit-repo path) and DoJSON (refs/branches listing). DoJSON returns a
// response body keyed by whether the URL carries a `page` marker so a two-page
// `next` chain can be simulated.
type bbBranchGateway struct {
	repoUUID        string
	mainbranch      string
	branchBodyByURL func(url string) string
	// branchingModelBody is the JSON returned for the .../branching-model call. When
	// "", that call returns an error so the connector fails open (no companion
	// record) — the default the branch-listing tests rely on.
	branchingModelBody string
	calledURLs         []string
}

func (g *bbBranchGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	if _, ok := req.(*connectorpb.BitbucketGetRepoRequest); ok {
		repo := bbRepository{UUID: g.repoUUID, Mainbranch: &bbBranchRef{Name: g.mainbranch}}
		raw, _ := json.Marshal(repo)
		return &connectorpb.BitbucketGetRepoResponse{Repo: raw}, nil
	}
	return nil, fmt.Errorf("unexpected Execute call: %T", req)
}

func (g *bbBranchGateway) DoJSON(_ context.Context, _, urlStr string, _, respBody any) error {
	g.calledURLs = append(g.calledURLs, urlStr)
	if strings.Contains(urlStr, "/branching-model") {
		if g.branchingModelBody == "" {
			return fmt.Errorf("simulated 404: no branching model configured")
		}
		return json.Unmarshal([]byte(g.branchingModelBody), respBody)
	}
	body := g.branchBodyByURL(urlStr)
	return json.Unmarshal([]byte(body), respBody)
}

// branchListingURLs filters calledURLs to the refs/branches paging calls, dropping
// the per-repo branching-model call so a page-count assertion is unaffected by it.
func (g *bbBranchGateway) branchListingURLs() []string {
	var out []string
	for _, u := range g.calledURLs {
		if strings.Contains(u, "/refs/branches") {
			out = append(out, u)
		}
	}
	return out
}

func (g *bbBranchGateway) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not used in fetch_branches tests")
}

func (g *bbBranchGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *bbBranchGateway) ForGroup(_ string) sdk.VendorGateway { return g }

func branchParamsBB(sink sdk.DataSink, gw sdk.VendorGateway) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-branches-bb-test",
			Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "test-token"}),
			Settings: mustJSON(BitbucketSettings{
				Workspace:    "myws",
				Repositories: []string{"myws/myrepo"},
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

// TestFetchBranchesOp_EmitsOnePerBranch verifies URL/auth construction,
// unmarshaling, record keys, operation_id, data dates, and metadata including
// is_default (computed from the repo's mainbranch).
func TestFetchBranchesOp_EmitsOnePerBranch(t *testing.T) {
	body := `{"values":[
		{"name":"main","target":{"hash":"h-main"}},
		{"name":"feature","target":{"hash":"h-feat"}}
	],"next":""}`
	gw := &bbBranchGateway{
		repoUUID:        "{uuid-1}",
		mainbranch:      "main",
		branchBodyByURL: func(_ string) string { return body },
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsBB(sink, gw))
	require.NoError(t, err)

	require.NotEmpty(t, gw.calledURLs)
	assert.Contains(t, gw.calledURLs[0], "/repositories/myws/myrepo/refs/branches")
	assert.Contains(t, gw.calledURLs[0], "pagelen=100")

	records := sink.Records()
	require.Len(t, records, 2)

	byKey := map[string]*sdk.RawRecord{}
	for _, r := range records {
		byKey[r.ExternalID] = r
		assert.Equal(t, "fetch_branches", r.OperationID)
		assert.True(t, r.DataDate.IsZero(), "a branch is current state: no DataDate, or every run is a new row")
	}

	main := byKey["{uuid-1}:main"]
	require.NotNil(t, main, "record key must be {platform_repo_id}:{branch}")
	assert.Equal(t, "h-main", main.Metadata["head_sha"])
	assert.Equal(t, "true", main.Metadata["is_default"], "branch matching mainbranch is default")
	// The repo's full branch count, so a consumer can tell a complete fetch
	// from a torn one.
	assert.Equal(t, "2", main.Metadata["branch_total"])

	feat := byKey["{uuid-1}:feature"]
	require.NotNil(t, feat)
	assert.Equal(t, "h-feat", feat.Metadata["head_sha"])
	assert.Equal(t, "false", feat.Metadata["is_default"])
	assert.Equal(t, "2", feat.Metadata["branch_total"])
}

// TestFetchBranchesOp_Pagination verifies the `next` URL in the body is
// followed across two pages and its host is accepted (api.bitbucket.org).
func TestFetchBranchesOp_Pagination(t *testing.T) {
	page1 := `{"values":[{"name":"b1","target":{"hash":"s1"}}],` +
		`"next":"https://api.bitbucket.org/2.0/repositories/myws/myrepo/refs/branches?pagelen=100&page=2"}`
	page2 := `{"values":[{"name":"b2","target":{"hash":"s2"}}],"next":""}`
	gw := &bbBranchGateway{
		repoUUID:   "{uuid-2}",
		mainbranch: "b1",
		branchBodyByURL: func(url string) string {
			if strings.Contains(url, "page=2") {
				return page2
			}
			return page1
		},
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsBB(sink, gw))
	require.NoError(t, err)

	branchURLs := gw.branchListingURLs()
	require.Len(t, branchURLs, 2, "expected two page fetches following next")
	assert.Contains(t, branchURLs[1], "page=2")

	records := sink.Records()
	require.Len(t, records, 2)
	assert.ElementsMatch(t, []string{"{uuid-2}:b1", "{uuid-2}:b2"},
		[]string{records[0].ExternalID, records[1].ExternalID})
}

// TestFetchBranchesOp_PayloadIsVendorJSON verifies the payload is verbatim vendor JSON.
func TestFetchBranchesOp_PayloadIsVendorJSON(t *testing.T) {
	body := `{"values":[{"name":"main","target":{"hash":"abc"}}],"next":""}`
	gw := &bbBranchGateway{
		repoUUID:        "{uuid-3}",
		mainbranch:      "main",
		branchBodyByURL: func(_ string) string { return body },
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsBB(sink, gw))
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1)
	assert.Contains(t, string(records[0].Payload), `"hash":"abc"`)
	assert.Contains(t, string(records[0].Payload), `"name":"main"`)
}

// TestFetchBranchesOp_DetectionPriors verifies the head-commit date is stamped
// as vendor_updated_at when present and omitted when absent (never epoch).
func TestFetchBranchesOp_DetectionPriors(t *testing.T) {
	body := `{"values":[
		{"name":"main","target":{"hash":"h-main","date":"2026-05-05T10:00:00+00:00"}},
		{"name":"nodate","target":{"hash":"h-nd"}}
	],"next":""}`
	gw := &bbBranchGateway{
		repoUUID:        "{uuid-pri}",
		mainbranch:      "main",
		branchBodyByURL: func(_ string) string { return body },
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsBB(sink, gw))
	require.NoError(t, err)

	byKey := map[string]*sdk.RawRecord{}
	for _, r := range sink.Records() {
		byKey[r.ExternalID] = r
	}
	main := byKey["{uuid-pri}:main"]
	require.NotNil(t, main)
	assert.Equal(t, "2026-05-05T10:00:00Z", main.Metadata["vendor_updated_at"])

	nodate := byKey["{uuid-pri}:nodate"]
	require.NotNil(t, nodate)
	_, hasDate := nodate.Metadata["vendor_updated_at"]
	assert.False(t, hasDate, "an absent head-commit date must omit vendor_updated_at, not stamp epoch")
}

// TestFetchBranchesOp_BranchingModelCompanion verifies the declared branching
// model is fetched (one call) and emitted as the per-repo companion record with
// its dedicated operation_id, the __branching_model__ record key, and dev/prod names.
func TestFetchBranchesOp_BranchingModelCompanion(t *testing.T) {
	branchBody := `{"values":[{"name":"main","target":{"hash":"h"}}],"next":""}`
	gw := &bbBranchGateway{
		repoUUID:           "{uuid-bm}",
		mainbranch:         "main",
		branchBodyByURL:    func(_ string) string { return branchBody },
		branchingModelBody: `{"development":{"name":"develop"},"production":{"name":"main"},"branch_types":[{"kind":"feature","prefix":"feature/"}]}`,
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsBB(sink, gw))
	require.NoError(t, err)

	// Exactly one branching-model call was made for the repo.
	var modelCalls int
	for _, u := range gw.calledURLs {
		if strings.Contains(u, "/repositories/myws/myrepo/branching-model") {
			modelCalls++
		}
	}
	assert.Equal(t, 1, modelCalls, "one branching-model call per repo")

	var companion *sdk.RawRecord
	for _, r := range sink.Records() {
		if r.ExternalID == "{uuid-bm}:__branching_model__" {
			companion = r
		}
	}
	require.NotNil(t, companion, "expected a branching-model companion record")
	assert.Equal(t, "fetch_branching_model", companion.OperationID)
	assert.Equal(t, "develop", companion.Metadata["development"])
	assert.Equal(t, "main", companion.Metadata["production"])
	assert.Equal(t, "{uuid-bm}", companion.Metadata["platform_repo_id"])
	assert.True(t, companion.DataDate.IsZero(), "the branching model is current state: no DataDate")
	// Payload is the verbatim vendor branching-model JSON.
	assert.Contains(t, string(companion.Payload), `"branch_types"`)
}

// TestFetchBranchesOp_BranchingModel404 verifies a 404/absent branching model
// fails open: the branch records still emit, and no companion record is produced.
func TestFetchBranchesOp_BranchingModel404(t *testing.T) {
	branchBody := `{"values":[{"name":"main","target":{"hash":"h"}}],"next":""}`
	gw := &bbBranchGateway{
		repoUUID:        "{uuid-404}",
		mainbranch:      "main",
		branchBodyByURL: func(_ string) string { return branchBody },
		// branchingModelBody unset → the branching-model call errors (simulated 404).
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsBB(sink, gw))
	require.NoError(t, err, "a missing branching model must not fail the run")

	for _, r := range sink.Records() {
		assert.NotContains(t, r.ExternalID, "__branching_model__", "no companion record on a 404")
		assert.Equal(t, "fetch_branches", r.OperationID)
	}
	require.Len(t, sink.Records(), 1, "only the branch record, no companion")
}

// TestFetchBranchesOp_RejectsForeignNextHost verifies an off-host `next` URL
// is rejected (SSRF guard) so the repo is skipped, not followed.
func TestFetchBranchesOp_RejectsForeignNextHost(t *testing.T) {
	body := `{"values":[{"name":"b1","target":{"hash":"s1"}}],` +
		`"next":"https://evil.example.com/steal"}`
	gw := &bbBranchGateway{
		repoUUID:        "{uuid-4}",
		mainbranch:      "b1",
		branchBodyByURL: func(_ string) string { return body },
	}
	sink := testkit.NewChannelSink()

	_, err := (&FetchBranchesOp{}).Execute(context.Background(), branchParamsBB(sink, gw))
	require.Error(t, err, "an off-host next URL fails the repo listing; the only repo failed, so the run fails")
	// The single first-page listing failed the SSRF check → no records.
	assert.Empty(t, sink.Records())
}
