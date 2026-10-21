// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sink_test

import (
	"context"
	"os"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// A JSONL publisher writes one envelope per line; ConnectorSink puts it
// behind the sdk.DataSink a connector emits to.
func ExampleNewJSONLPublisher() {
	p := sink.NewJSONLPublisher(os.Stdout)
	_, _ = p.Publish(context.Background(), &connectorpb.ConnectorDataEnvelope{OrganizationId: "org-1"})
	// Output: {"organizationId":"org-1"}
}
