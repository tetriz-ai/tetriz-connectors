// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// Run one sync and read back the cursor it stored: a connector, a fake
// vendor, the in-memory store and a channel sink, with the orchestrator in
// between.
func ExampleOrchestrator_RunOperation() {
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-000000000001"
	vendor := fakevendor.New(
		fakevendor.Item{ID: "a", Title: "first", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		fakevendor.Item{ID: "b", Title: "second", UpdatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
	)
	defer vendor.Close()

	mem := memstore.New()
	settings, _ := json.Marshal(fakevendor.Settings{BaseURL: vendor.URL})
	_ = mem.Save(ctx, &sdk.ConnectorConfig{
		OrganizationID: org, ConnectorID: fakevendor.ConnectorID,
		Settings: settings, Credentials: json.RawMessage(`{"token":"example"}`), Enabled: true,
	})
	registry := sdk.NewConnectorRegistry()
	registry.Register(fakevendor.Connector{})
	records := sink.NewChannelSink()
	orch := runner.NewOrchestrator(registry, mem, mem, mem, records, fakevendor.GatewayFactory(nil)).
		WithTxRunner(store.NoopTxRunner{})

	err := orch.RunOperation(ctx, org, fakevendor.ConnectorID, fakevendor.OpFetchItems, sdk.TenantKindOrganization, "example")
	fmt.Println("error:", err)
	fmt.Println("records:", len(records.Records()))
	run, _ := mem.GetLatest(ctx, org, fakevendor.ConnectorID, fakevendor.OpFetchItems)
	fmt.Println("run:", run.Status, run.RecordsEmitted)
	// Output:
	// error: <nil>
	// records: 2
	// run: completed 2
}
