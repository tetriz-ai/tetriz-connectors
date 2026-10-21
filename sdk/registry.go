// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"fmt"
	"sync"
)

// ConnectorRegistry is the central registry of all available connectors.
// Register every connector before building the Orchestrator.
type ConnectorRegistry struct {
	connectors map[ConnectorID]Connector
	mu         sync.RWMutex
}

// NewConnectorRegistry creates an empty registry.
func NewConnectorRegistry() *ConnectorRegistry {
	return &ConnectorRegistry{
		connectors: make(map[ConnectorID]Connector),
	}
}

// Register adds a connector. It panics (fail-fast at startup) on a duplicate
// ID, and on an operation whose IngestionType nothing hosts: only pull
// operations run, and webhook operations are received by the host. A stream
// (SSE, WebSocket, GraphQL subscription) or bulk-import operation would be
// accepted here and then fail every run, so it is refused instead.
func (r *ConnectorRegistry) Register(c Connector) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := c.ID()
	for _, op := range c.Operations() {
		if op.IngestionType != IngestionPull && op.IngestionType != IngestionWebhook {
			panic(fmt.Sprintf("connector %s: operation %s declares ingestion type %s, which nothing runs; only pull and webhook operations are supported",
				id, op.ID, op.IngestionType))
		}
	}
	if _, exists := r.connectors[id]; exists {
		panic(fmt.Sprintf("connector already registered: %s", id))
	}
	r.connectors[id] = c
}

// Get returns a connector by ID.
func (r *ConnectorRegistry) Get(id ConnectorID) (Connector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	c, ok := r.connectors[id]
	if !ok {
		return nil, fmt.Errorf("connector not found: %s", id)
	}
	return c, nil
}

// List returns all registered connectors.
func (r *ConnectorRegistry) List() []Connector {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]Connector, 0, len(r.connectors))
	for _, c := range r.connectors {
		result = append(result, c)
	}
	return result
}

// GetOperation returns a specific operation descriptor from a connector.
func (r *ConnectorRegistry) GetOperation(connectorID ConnectorID, opID OperationID) (OperationDescriptor, error) {
	c, err := r.Get(connectorID)
	if err != nil {
		return OperationDescriptor{}, fmt.Errorf("getting: %w", err)
	}

	for _, op := range c.Operations() {
		if op.ID == opID {
			return op, nil
		}
	}
	return OperationDescriptor{}, fmt.Errorf("operation %s not found in connector %s", opID, connectorID)
}
