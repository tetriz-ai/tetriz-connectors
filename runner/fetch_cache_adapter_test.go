// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/mock/gomock"

	mocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// oversizedSkipped reads the current oversized-skipped counter for an org, using
// the same metrictenant-collapsed label the adapter emits.
func oversizedSkipped(org string) float64 {
	return testutil.ToFloat64(obs.FetchCacheOversizedSkippedTotal.WithLabelValues(
		string(fcaConnector), fcaType, tenantlabel.OrgID(org)))
}

const (
	fcaConnector = sdk.ConnectorID("bitbucket")
	fcaType      = "pr_diffstat"
)

// The adapter's contract is best-effort: a store error degrades (GetMany →
// miss, PutMany → drop) and an oversized value is skipped + counted — none of
// which may surface as an error to the connector.
func newAdapter(dao *mocks.MockIFetchCacheDAO, orgID string, maxBytes int) *fetchCacheAdapter {
	return &fetchCacheAdapter{dao: dao, orgID: orgID, connectorID: fcaConnector, maxValueBytes: maxBytes}
}

func TestFetchCacheAdapter_GetMany(t *testing.T) {
	t.Run("hit returns the store result", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDAO := mocks.NewMockIFetchCacheDAO(ctrl)
		want := map[string]json.RawMessage{"k1": json.RawMessage(`{"files":1}`)}
		mockDAO.EXPECT().GetMany(gomock.Any(), "org-a", fcaConnector, fcaType, []string{"k1"}).Return(want, nil)

		got, err := newAdapter(mockDAO, "org-a", 512).GetMany(context.Background(), fcaType, []string{"k1"})
		if err != nil {
			t.Fatalf("GetMany: unexpected error %v", err)
		}
		if string(got["k1"]) != `{"files":1}` {
			t.Errorf("got %v, want the store result", got)
		}
	})

	t.Run("store error degrades to an empty miss, never an error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDAO := mocks.NewMockIFetchCacheDAO(ctrl)
		mockDAO.EXPECT().GetMany(gomock.Any(), "org-b", fcaConnector, fcaType, gomock.Any()).
			Return(nil, errors.New("db down"))

		got, err := newAdapter(mockDAO, "org-b", 512).GetMany(context.Background(), fcaType, []string{"k1"})
		if err != nil {
			t.Fatalf("a cache read failure must not fail the op, got error %v", err)
		}
		if len(got) != 0 {
			t.Errorf("degrade must return an empty miss, got %v", got)
		}
	})
}

func TestFetchCacheAdapter_PutMany(t *testing.T) {
	t.Run("store error is dropped, never fails the op", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDAO := mocks.NewMockIFetchCacheDAO(ctrl)
		mockDAO.EXPECT().PutMany(gomock.Any(), "org-c", fcaConnector, fcaType, gomock.Any()).
			Return(int64(0), errors.New("db down"))

		err := newAdapter(mockDAO, "org-c", 512).PutMany(context.Background(), fcaType,
			map[string]json.RawMessage{"k1": json.RawMessage(`{"files":1}`)})
		if err != nil {
			t.Fatalf("a cache write failure must not fail the op, got error %v", err)
		}
	})

	t.Run("oversized value is skipped + counted; kept entries stored, oversized key tombstoned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDAO := mocks.NewMockIFetchCacheDAO(ctrl)
		const org = "org-oversize"
		big := json.RawMessage(`{"files":"` + string(make([]byte, 200)) + `"}`) // > cap
		small := json.RawMessage(`{"files":1}`)

		// Only the small entry is handed to the store — the big one is filtered out.
		mockDAO.EXPECT().PutMany(gomock.Any(), org, fcaConnector, fcaType, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, _ string, entries map[string]json.RawMessage) (int64, error) {
				if len(entries) != 1 {
					t.Errorf("store received %d entries, want 1 (oversized filtered)", len(entries))
				}
				if _, ok := entries["small"]; !ok {
					t.Error("the small entry must reach the store")
				}
				if _, ok := entries["big"]; ok {
					t.Error("the oversized entry must NOT reach the store")
				}
				return 1, nil
			})
		// The oversized key is tombstoned so a stale prior row can't serve as a hit.
		mockDAO.EXPECT().DeleteMany(gomock.Any(), org, fcaConnector, fcaType, []string{"big"}).Return(nil)
		// The write cadence refreshes the table-growth gauge.
		mockDAO.EXPECT().CountByType(gomock.Any(), org, fcaConnector, fcaType).Return(int64(1), nil)

		before := oversizedSkipped(org)
		if err := newAdapter(mockDAO, org, 50).PutMany(context.Background(), fcaType,
			map[string]json.RawMessage{"small": small, "big": big}); err != nil {
			t.Fatalf("PutMany: %v", err)
		}
		if oversizedSkipped(org)-before != 1 {
			t.Errorf("oversized-skipped counter moved by %v, want 1", oversizedSkipped(org)-before)
		}
	})

	t.Run("all entries oversized: store is never written, oversized keys tombstoned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDAO := mocks.NewMockIFetchCacheDAO(ctrl)
		const org = "org-all-oversize"
		big := json.RawMessage(`{"x":"` + string(make([]byte, 200)) + `"}`)

		// No PutMany expectation — nothing is kept; DeleteMany still tombstones the key.
		mockDAO.EXPECT().DeleteMany(gomock.Any(), org, fcaConnector, fcaType, []string{"big"}).Return(nil)
		// The tombstone-only cycle still commits, so the gauge is refreshed.
		mockDAO.EXPECT().CountByType(gomock.Any(), org, fcaConnector, fcaType).Return(int64(0), nil)

		before := oversizedSkipped(org)
		if err := newAdapter(mockDAO, org, 50).PutMany(context.Background(), fcaType,
			map[string]json.RawMessage{"big": big}); err != nil {
			t.Fatalf("PutMany: %v", err)
		}
		if oversizedSkipped(org)-before != 1 {
			t.Errorf("oversized-skipped counter moved by %v, want 1", oversizedSkipped(org)-before)
		}
	})

	t.Run("cap of 0 fails closed to the fallback, not open", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDAO := mocks.NewMockIFetchCacheDAO(ctrl)
		const org = "org-failclosed"
		// A value over the fallback backstop (512 KB) but there is NO configured cap
		// (maxValueBytes = 0). It must STILL be skipped — a 0 cap must not disable
		// the guard (the silent-disable footgun this fail-closed branch prevents).
		huge := json.RawMessage(`{"x":"` + string(make([]byte, 600*1024)) + `"}`)
		small := json.RawMessage(`{"files":1}`)
		mockDAO.EXPECT().PutMany(gomock.Any(), org, fcaConnector, fcaType, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, _ string, entries map[string]json.RawMessage) (int64, error) {
				if _, ok := entries["huge"]; ok {
					t.Error("a value over the fallback cap must be skipped even when maxValueBytes==0")
				}
				return 1, nil
			})
		mockDAO.EXPECT().DeleteMany(gomock.Any(), org, fcaConnector, fcaType, []string{"huge"}).Return(nil)
		mockDAO.EXPECT().CountByType(gomock.Any(), org, fcaConnector, fcaType).Return(int64(1), nil)

		before := oversizedSkipped(org)
		if err := newAdapter(mockDAO, org, 0).PutMany(context.Background(), fcaType,
			map[string]json.RawMessage{"small": small, "huge": huge}); err != nil {
			t.Fatalf("PutMany: %v", err)
		}
		if oversizedSkipped(org)-before != 1 {
			t.Errorf("fallback guard skipped %v entries, want 1", oversizedSkipped(org)-before)
		}
	})

	t.Run("empty batch is a no-op", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDAO := mocks.NewMockIFetchCacheDAO(ctrl)
		if err := newAdapter(mockDAO, "org-e", 512).PutMany(context.Background(), fcaType, nil); err != nil {
			t.Errorf("empty PutMany should be a no-op, got %v", err)
		}
	})
}
