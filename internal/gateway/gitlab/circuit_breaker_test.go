// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/inprocess"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

// statusDoer answers every request with its current status and counts the
// requests that reached it.
type statusDoer struct {
	status atomic.Int32
	calls  atomic.Int32
}

func (d *statusDoer) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return &http.Response{StatusCode: int(d.status.Load()), Body: io.NopCloser(strings.NewReader(`[]`)), Header: http.Header{}}, nil
}

// The GitLab gateway carries the shared 429 circuit breaker (ADR-0004 §5),
// with the Bitbucket gateway's semantics: three 429s within the failure
// window open it, and while it is open requests fail fast without reaching
// GitLab. After the cooldown it half-opens, one probe goes through, and a
// probe answered below 500 closes it for every caller.
func TestGatewayCircuitBreakerTripsAndRecovers(t *testing.T) {
	clk := clockwork.NewFakeClock()
	stores := inprocess.NewWithClock(clk)
	doer := &statusDoer{}
	doer.status.Store(http.StatusTooManyRequests)
	gw, err := NewGitLabVendorGateway(doer, stores.RateLimits, stores.GitLabCircuit, nil)
	require.NoError(t, err)
	rest := gw.ForGroup(constants.GroupGitLabREST)
	ctx := reqctx.WithOrgID(context.Background(), testOrgID)
	call := func() error {
		_, err := rest.Execute(ctx, &connectorpb.GitLabListProjectsRequest{GroupId: 1})
		clk.Advance(5 * time.Second) // keep the local limiter's window clear
		return err
	}

	for i := 0; i < gateway.CircuitFailureThreshold; i++ {
		require.Error(t, call(), "GitLab answers 429")
	}
	require.Equal(t, int32(gateway.CircuitFailureThreshold), doer.calls.Load())

	err = call()
	require.ErrorIs(t, err, gateway.ErrCircuitOpen, "the breaker is open")
	assert.Equal(t, int32(gateway.CircuitFailureThreshold), doer.calls.Load(), "an open breaker refuses before GitLab is called")

	clk.Advance(gateway.CircuitCooldown)
	doer.status.Store(http.StatusOK)
	require.NoError(t, call(), "the half-open probe goes through")
	require.NoError(t, call(), "the probe closed the circuit for every caller")
	assert.Equal(t, int32(gateway.CircuitFailureThreshold+2), doer.calls.Load())
}

// As for Bitbucket, the breaker gates on 429s only: repeated 5xx responses do
// not open it, because a failing vendor is not a rate-limit punishment window.
func TestGatewayCircuitBreakerIgnoresServerErrors(t *testing.T) {
	clk := clockwork.NewFakeClock()
	stores := inprocess.NewWithClock(clk)
	doer := &statusDoer{}
	doer.status.Store(http.StatusBadGateway)
	gw, err := NewGitLabVendorGateway(doer, stores.RateLimits, stores.GitLabCircuit, nil)
	require.NoError(t, err)
	rest := gw.ForGroup(constants.GroupGitLabREST)
	ctx := reqctx.WithOrgID(context.Background(), testOrgID)
	for i := 0; i < gateway.CircuitFailureThreshold+2; i++ {
		_, err := rest.Execute(ctx, &connectorpb.GitLabListProjectsRequest{GroupId: 1})
		require.Error(t, err)
		require.NotErrorIs(t, err, gateway.ErrCircuitOpen)
		clk.Advance(5 * time.Second)
	}
	assert.Equal(t, int32(gateway.CircuitFailureThreshold+2), doer.calls.Load(), "every request reached GitLab")
}
