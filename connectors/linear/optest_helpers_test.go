// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// recordedCall captures one GraphQL request made through the fake gateway.
type recordedCall struct {
	Query string
	Vars  map[string]any
}

// fakeGateway is a hand-rolled sdk.VendorGateway for op tests. It routes
// each DoJSON call to the supplied respond func (which unmarshals a canned
// payload into dest) and records every call for assertion. ForGroup /
// WithRateLimits return the same instance; Do / Execute are unused by the
// GraphQL ops and panic if reached so a wrong code path is loud, not silent.
type fakeGateway struct {
	respond func(query string, vars map[string]any, dest any) error
	calls   []recordedCall
}

func (g *fakeGateway) DoJSON(_ context.Context, _, _ string, reqBody, respBody any) error {
	gr, ok := reqBody.(graphqlRequest)
	if !ok {
		return fmt.Errorf("fakeGateway: unexpected request body type %T", reqBody)
	}
	g.calls = append(g.calls, recordedCall{Query: gr.Query, Vars: gr.Variables})
	return g.respond(gr.Query, gr.Variables, respBody)
}

func (g *fakeGateway) ForGroup(string) sdk.VendorGateway                      { return g }
func (g *fakeGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return g }

func (g *fakeGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	panic("fakeGateway.Do: not expected in GraphQL op tests")
}

func (g *fakeGateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	panic("fakeGateway.Execute: not expected in GraphQL op tests")
}

// callsFor returns the recorded calls whose query contains the given marker
// (e.g. "FetchIssues", "FetchCycles", "DiscoverTeams").
func (g *fakeGateway) callsFor(marker string) []recordedCall {
	var out []recordedCall
	for _, c := range g.calls {
		if strings.Contains(c.Query, marker) {
			out = append(out, c)
		}
	}
	return out
}

// testConfig builds a minimal valid ConnectorConfig (ParseConfig only needs the
// JSON to unmarshal cleanly).
func testConfig(orgID string) *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID:    connectorID,
		OrganizationID: orgID,
		Credentials:    json.RawMessage(`{"access_token":"tok"}`),
		Settings:       json.RawMessage(`{"workspace_id":"ws-1"}`),
	}
}

// unmarshalInto writes a canned JSON payload into the op's response dest.
func unmarshalInto(dest any, payload string) error {
	return json.Unmarshal([]byte(payload), dest)
}
