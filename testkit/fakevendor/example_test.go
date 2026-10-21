// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fakevendor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// Run one polling sync end to end: a connector, a vendor, an in-memory store
// and a sink, with the orchestrator in between.
func Example() {
	ctx := context.Background()
	vendor := fakevendor.New(
		fakevendor.Item{ID: "a", Title: "first", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		fakevendor.Item{ID: "b", Title: "second", UpdatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
	)
	defer vendor.Close()

	store := memstore.New()
	settings, _ := json.Marshal(fakevendor.Settings{BaseURL: vendor.URL})
	_ = store.Save(ctx, &sdk.ConnectorConfig{
		OrganizationID: "00000000-0000-4000-8000-000000000001", ConnectorID: fakevendor.ConnectorID,
		Settings: settings, Credentials: json.RawMessage(`{"token":"example"}`), Enabled: true,
	})

	registry := sdk.NewConnectorRegistry()
	registry.Register(fakevendor.Connector{})
	records := sink.NewChannelSink()
	orch := runner.NewOrchestrator(registry, store, store, store, records, fakevendor.GatewayFactory(nil))

	err := orch.RunOperation(ctx, "00000000-0000-4000-8000-000000000001", fakevendor.ConnectorID, fakevendor.OpFetchItems, sdk.TenantKindOrganization, "example")
	fmt.Println("error:", err)
	for _, r := range records.Records() {
		fmt.Println(r.ExternalID)
	}
	// Output:
	// error: <nil>
	// a
	// b
}
