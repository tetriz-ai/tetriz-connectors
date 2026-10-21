// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
)

// barrierOnGet holds every GET of one key until n callers have read it, which
// reproduces two OAuth callbacks that read the same state before either
// deletes it.
type barrierOnGet struct {
	key     string
	arrived sync.WaitGroup
}

func (b *barrierOnGet) DialHook(next redis.DialHook) redis.DialHook { return next }
func (b *barrierOnGet) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (b *barrierOnGet) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		args := cmd.Args()
		if cmd.Name() == "get" && len(args) == 2 && args[1] == b.key {
			b.arrived.Done()
			done := make(chan struct{})
			go func() { b.arrived.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
		}
		return err
	}
}

// A CSRF state token is single use. Two callbacks that race on the same state
// must not both get past the state check: only one may go on to the code
// exchange. (GitHub's install state uses GETDEL for exactly this; the Jira
// callback reads with GET and deletes afterwards.)
func TestEdge_JiraOAuthStateCannotBeConsumedTwiceConcurrently(t *testing.T) {
	mr := miniredis.RunT(t)
	const state = "race-state"
	key := jiraconfig.OAuthStateKey(state)
	require.NoError(t, mr.Set(key, `{"org_id":"`+testOrgID+`"}`))

	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	hook := &barrierOnGet{key: key}
	hook.arrived.Add(2)
	rc.AddHook(hook)

	ctrl := gomock.NewController(t)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	// No Jira OAuth client: a callback that passes the state check stops at
	// FailedPrecondition, one that fails it stops at InvalidArgument.
	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

	var passed atomic.Int32
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.ValidateAndSaveJiraConnection(context.Background(), &connintv1.ValidateAndSaveJiraConnectionRequest{
				OrgId: testOrgID, AuthCode: "code", State: state,
			})
			if status.Code(err) == codes.FailedPrecondition {
				passed.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), passed.Load(), "one state token admitted two concurrent callbacks past the CSRF check")
	assert.False(t, mr.Exists(key))
}
