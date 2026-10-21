// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fakevendor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// secondSaveFails fails the second cursor write it sees, as a run killed
// after writing a page's records but before saving that page's checkpoint.
type secondSaveFails struct {
	store.ISyncStateDAO
	saves int
}

func (s *secondSaveFails) SaveCursor(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, c *sdk.Cursor) error {
	s.saves++
	if s.saves == 2 {
		return errors.New("killed")
	}
	return s.ISyncStateDAO.SaveCursor(ctx, orgID, connectorID, opID, c) //nolint:wrapcheck // test wrapper
}

// A run that fails after writing records but before saving their checkpoint
// leaves the cursor at the previous checkpoint, so the next run emits those
// records again: delivery is at least once.
func TestLimitation_FailedRunReEmitsSinceLastCheckpoint(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(5, t0)...)
		defer srv.Close()
		failing := s
		failing.states = &secondSaveFails{ISyncStateDAO: s.states}
		first := newHarness(t, failing, srv)
		require.Error(t, first.run(context.Background()))
		assert.Equal(t, []string{"item-01", "item-02", "item-03", "item-04"}, first.keys(), "page 2 was written before its checkpoint failed")

		second := newHarness(t, s, srv)
		second.org = first.org
		require.NoError(t, second.run(context.Background()))
		assert.Equal(t, []string{"item-03", "item-04", "item-05"}, second.keys(), "page 2 comes again")
	})
}
