// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations_test

import (
	"fmt"
	"sort"

	"google.golang.org/grpc"

	"github.com/tetriz-ai/tetriz-connectors/auth/integrations"
	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/connectorsvc"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	integrationsv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// Register the connect flows and the connector queries on your own gRPC
// server. A real host passes the Postgres stores, Redis and the vendor OAuth
// clients, and puts its own authentication in front of the server.
// store.PostgresTxRunner scopes every store call to the organization. On the
// memory store it does nothing.
func Example_hostTheConnectFlows() {
	stores := memstore.New()
	registry := app.NewRegistry(nil)
	srv := grpc.NewServer()
	integrationsv1.RegisterIntegrationServiceServer(srv, integrations.NewService(integrations.Deps{
		Tx:           store.PostgresTxRunner{},
		Cfg:          &config.Config{},
		ConnectorDAO: stores,
		SyncStateDAO: stores,
		SyncRunDAO:   stores,
		Registry:     registry,
	}))
	// GetSecret lends only what the authorizer allows, and nothing without one.
	connectorpb.RegisterConnectorServiceServer(srv, connectorsvc.NewService(store.PostgresTxRunner{}, stores, stores, stores, registry, sink.NewChannelSink(), nil, nil,
		connectorsvc.WithSecretAuthorizer(connectorsvc.AllowConnectors("slack_comms"))))

	var names []string
	for name := range srv.GetServiceInfo() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Println(name)
	}
	// Output:
	// tetriz.connectors.connector.v1.ConnectorService
	// tetriz.connectors.integrations.v1.IntegrationService
}
