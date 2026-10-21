// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package testkit_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// RunConformance checks a connector from inside a test: it needs the test's
// *testing.T, so this example has no output to compare. conforms is the body
// of a test, here run against the fake vendor's connector:
//
//	func TestConnectorConforms(t *testing.T) { conforms(t) }
func ExampleRunConformance() {
	conforms := func(t *testing.T) {
		vendor := fakevendor.New(fakevendor.Item{ID: "a", Title: "first", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
		defer vendor.Close()
		settings, _ := json.Marshal(fakevendor.Settings{BaseURL: vendor.URL, PageSize: 1})
		testkit.RunConformance(t, fakevendor.Connector{}, testkit.ConformanceRun{
			Operation:       fakevendor.OpFetchItems,
			Config:          sdk.ConnectorConfig{Settings: settings},
			Gateway:         fakevendor.GatewayFactory(nil),
			WantExternalIDs: []string{"a"},
			Faults:          vendor,
		})
	}
	_ = conforms
}
