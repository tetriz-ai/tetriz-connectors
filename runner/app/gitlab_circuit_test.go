// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/inprocess"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	gatewaypb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The factory's GitLab gateway carries the GitLab circuit breaker from
// GatewayDeps.GitLabCircuit: after three 429s it refuses without calling
// GitLab, as the Bitbucket gateway does with BitbucketCircuit.
func TestGatewayFactoryGivesGitLabItsCircuitBreaker(t *testing.T) {
	const org = "00000000-0000-4000-8000-00000000f00f"
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	cfg := gateway.DefaultConfig()
	cfg.SimulatorOrgIDs = []string{org}
	cfg.SimulatorBaseURL = srv.URL
	gateway.SetConfig(cfg)
	defer gateway.SetConfig(gateway.DefaultConfig())

	stores := inprocess.New()
	deps := InProcessGatewayDeps(Config{}, stores)
	require.NotNil(t, deps.GitLabCircuit, "the in-process stores carry a GitLab breaker")
	deps.HTTP = httpDoer{client: srv.Client()}
	factory, err := NewGatewayFactory(deps)
	require.NoError(t, err)
	gw, err := factory(context.Background(), &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "gitlab", Settings: json.RawMessage(`{}`)})
	require.NoError(t, err)

	ctx := reqctx.WithOrgID(context.Background(), org)
	rest := gw.ForGroup(constants.GroupGitLabREST)
	for i := 0; i < gateway.CircuitFailureThreshold; i++ {
		_, err := rest.Execute(ctx, &gatewaypb.GitLabListProjectsRequest{GroupId: 1})
		require.Error(t, err)
	}
	_, err = rest.Execute(ctx, &gatewaypb.GitLabListProjectsRequest{GroupId: 1})
	require.ErrorIs(t, err, gateway.ErrCircuitOpen)
	assert.Equal(t, int32(gateway.CircuitFailureThreshold), calls.Load(), "the open breaker refused before GitLab was called")
}
