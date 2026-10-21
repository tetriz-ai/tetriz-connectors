// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fakevendor_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// An item updated between two pages of one listing moves to the end of the
// listing; every item is still emitted, none is skipped.
func TestEngine_ItemUpdatedMidListingIsNotLost(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(4, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		srv.FailCall(2, fakevendor.Fault{Before: func() {
			moved := items(4, t0)
			moved[0].UpdatedAt = t0.Add(10 * time.Minute)
			srv.SetItems(moved...)
		}})
		ctx := context.Background()
		for i := 0; i < 3; i++ {
			require.NoError(t, h.run(ctx))
		}
		keys := h.keys()
		for _, id := range []string{"item-01", "item-02", "item-03", "item-04"} {
			assert.Contains(t, keys, id)
		}
	})
}

// A listing starts after the last item already emitted, so a second run over
// unchanged items emits nothing.
func TestEngine_UnchangedItemsAreNotReEmitted(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(4, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		ctx := context.Background()
		require.NoError(t, h.run(ctx))
		require.Len(t, h.keys(), 4)
		require.NoError(t, h.run(ctx))
		assert.Len(t, h.keys(), 4, "the second run emits nothing")
		assert.Equal(t, int64(0), h.latest().RecordsEmitted)
	})
}
