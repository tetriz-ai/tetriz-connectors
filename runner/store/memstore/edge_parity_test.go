// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package memstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The sync-status aggregation agrees across the stores on its edge cases: a
// VCS connector's discovery runs never count, a since-retried failure no
// longer fails the connector, a failure after a success keeps the success
// time, and a run still in progress after a failure is not a failure.
func TestEdge_ContractSyncStatusEdges(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
		at := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
		add := func(conn sdk.ConnectorID, op sdk.OperationID, status model.SyncRunStatus, started int) {
			r := &model.SyncRun{ID: uuid.NewString(), OrganizationID: o, ConnectorID: conn, OperationID: op, Status: status, StartedAt: at(started), Trigger: "test"}
			if status != model.SyncRunStatusRunning {
				c := at(started + 1)
				r.CompletedAt = &c
			}
			in(func(ctx context.Context) error { return s.runs.Create(ctx, r) })
		}
		// github: only a discovery op ran, so it is absent.
		add("github", "fetch_repos", model.SyncRunStatusCompleted, 0)
		// gitlab: failed, then a retry completed — synced.
		add("gitlab", "fetch_prs", model.SyncRunStatusFailed, 0)
		add("gitlab", "fetch_prs", model.SyncRunStatusCompleted, 5)
		// bitbucket: completed, then the latest attempt failed — failed, with
		// the success time kept.
		add("bitbucket", "fetch_commits", model.SyncRunStatusCompleted, 0)
		add("bitbucket", "fetch_commits", model.SyncRunStatusFailed, 5)
		// jira (not VCS): failed, then a run still in progress — still syncing.
		add("jira", "fetch_issues", model.SyncRunStatusFailed, 0)
		add("jira", "fetch_issues", model.SyncRunStatusRunning, 5)

		in(func(ctx context.Context) error {
			got, err := s.runs.GetConnectorSyncStatusByOrg(ctx, o)
			require.NoError(t, err)
			_, hasGitHub := got["github"]
			assert.False(t, hasGitHub, "a VCS discovery op does not count")

			assert.Equal(t, store.ConnectorSyncStateSynced, got["gitlab"].State)
			require.NotNil(t, got["gitlab"].LastSuccessfulAt)
			assert.True(t, at(6).Equal(*got["gitlab"].LastSuccessfulAt))

			assert.Equal(t, store.ConnectorSyncStateFailed, got["bitbucket"].State)
			require.NotNil(t, got["bitbucket"].LastSuccessfulAt)
			assert.True(t, at(1).Equal(*got["bitbucket"].LastSuccessfulAt))

			assert.Equal(t, store.ConnectorSyncStateStillSyncing, got["jira"].State)
			assert.Nil(t, got["jira"].LastSuccessfulAt)
			return nil
		})
	})
}

// ListByOperation's limit means the same thing in every store: 0 returns
// none, a negative limit returns all, and a limit past the count returns all,
// newest first.
func TestEdge_ContractListLimitBoundaries(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		base := time.Now().UTC().Truncate(time.Second)
		in(func(ctx context.Context) error {
			for i := range 3 {
				if err := s.runs.Create(ctx, &model.SyncRun{ID: uuid.NewString(), OrganizationID: o, ConnectorID: "c", OperationID: "op", Status: model.SyncRunStatusRunning, StartedAt: base.Add(time.Duration(i) * time.Second), Trigger: "test"}); err != nil {
					return err
				}
			}
			return nil
		})
		in(func(ctx context.Context) error {
			for limit, want := range map[int]int{0: 0, -1: 3, 2: 2, 10: 3} {
				got, err := s.runs.ListByOperation(ctx, o, "c", "op", limit)
				require.NoError(t, err)
				assert.Len(t, got, want, "limit %d", limit)
				for i := 1; i < len(got); i++ {
					assert.True(t, got[i-1].StartedAt.After(got[i].StartedAt), "newest first")
				}
			}
			return nil
		})
	})
}

// A cursor saved for one operation does not leak into a sibling operation
// or another connector, and deleting one connector's cursors leaves the
// other's.
func TestEdge_ContractCursorKeysAreIndependent(t *testing.T) {
	both(t, func(t *testing.T, s impl, o string, in func(func(ctx context.Context) error)) {
		in(func(ctx context.Context) error {
			if err := s.states.SaveCursor(ctx, o, "github", "fetch_prs", &sdk.Cursor{Data: []byte(`{"a":1}`)}); err != nil {
				return err
			}
			return s.states.SaveCursor(ctx, o, "gitlab", "fetch_prs", &sdk.Cursor{Data: []byte(`{"b":1}`)})
		})
		in(func(ctx context.Context) error {
			_, err := s.states.GetCursor(ctx, o, "github", "fetch_commits")
			assert.ErrorIs(t, err, store.ErrCursorNotFound)
			require.NoError(t, s.states.DeleteByConnector(ctx, o, "github"))
			c, err := s.states.GetCursor(ctx, o, "gitlab", "fetch_prs")
			require.NoError(t, err)
			assert.JSONEq(t, `{"b":1}`, string(c.Data))
			return nil
		})
	})
}
