// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package gatewaytest installs a gateway config for tests: the built-in
// vendor defaults plus one organization routed to a simulator.
package gatewaytest

import "github.com/tetriz-ai/tetriz-connectors/gateway"

const (
	// SimulatorOrgID is the organization the test config routes to the
	// simulator.
	SimulatorOrgID = "5e1f0000-0000-4000-8000-000000000001"
	// SimulatorBaseURL is the simulator root in the test config.
	SimulatorBaseURL = "http://localhost:8080"
)

// Install sets the process-wide gateway config to the test config.
func Install() {
	c := gateway.DefaultConfig()
	c.SimulatorOrgIDs = []string{SimulatorOrgID}
	c.SimulatorBaseURL = SimulatorBaseURL
	gateway.SetConfig(c)
}
