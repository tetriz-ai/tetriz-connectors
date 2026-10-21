// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fakevendor_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// TestConnectorConforms is the conformance check the README shows: the
// fake vendor's connector against the suite, with the vendor it talks to.
func TestConnectorConforms(t *testing.T) {
	items := []fakevendor.Item{
		{ID: "a", Title: "first", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "b", Title: "second", UpdatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
	}
	vendor := fakevendor.New(items...)
	defer vendor.Close()
	// One item a page, so the listing checkpoints between pages.
	settings, _ := json.Marshal(fakevendor.Settings{BaseURL: vendor.URL, PageSize: 1})
	testkit.RunConformance(t, fakevendor.Connector{}, testkit.ConformanceRun{
		Operation: fakevendor.OpFetchItems,
		Config:    sdk.ConnectorConfig{Settings: settings},
		Gateway:   fakevendor.GatewayFactory(nil),
		// The second run's first request must resume from the stored cursor.
		Resume: func(stored *sdk.Cursor, first testkit.VendorCall) error {
			return fakevendor.ResumesFrom(stored, first.Request)
		},
		WantExternalIDs: []string{"a", "b"},
		// A record added after the first run must reach the second.
		AddBetweenRuns: func() []string {
			vendor.SetItems(append(items, fakevendor.Item{ID: "c", Title: "third", UpdatedAt: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)})...)
			return []string{"c"}
		},
		// The vendor fails on demand: each failure must fail the run with
		// the right error kind, and a run that fails after a checkpoint
		// must be resumed from it.
		Faults:      vendor,
		Checkpoints: true,
	})
}
