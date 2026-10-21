// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package postgres_test

import (
	"context"
	"log"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	"github.com/tetriz-ai/tetriz-connectors/sink/postgres"
)

// Write records to connector_records. Open applies the table's migrations;
// sink.NewConnectorSink turns the publisher into the sdk.DataSink that
// runner.NewOrchestrator takes.
func ExampleOpen() {
	ctx := context.Background()
	publisher, err := postgres.Open(ctx, "host=localhost port=55432 user=tetriz dbname=tetriz sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()

	records := sink.NewConnectorSink(publisher)
	if err := records.Emit(ctx, &sdk.RawRecord{
		ConnectorID:    "github",
		OperationID:    "fetch_prs",
		OrganizationID: "org-1",
		ExternalID:     "acme/api#PR-42#opened",
		Payload:        []byte(`{"number":42,"title":"Fix the build"}`),
		FetchedAt:      time.Now(),
	}); err != nil {
		log.Fatal(err)
	}
}
