// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	// ConnectorID is the todo connector's id. It must not be a built-in
	// connector's: app.New refuses a collision.
	ConnectorID sdk.ConnectorID = "todo"
	// OpFetchTodos pulls every todo.
	OpFetchTodos sdk.OperationID = "fetch_todos"
)

// Settings is the todo connector's `config put --settings`.
type Settings struct {
	// BaseURL is the todo API's root.
	BaseURL string `json:"base_url"`
}

// Credentials is the todo connector's `config put --credentials`.
type Credentials struct {
	Token string `json:"token"`
}

// Todo is one item of the todo API, and the payload of one record.
type Todo struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	Done  bool   `json:"done"`
}

// Connector is the todo connector.
type Connector struct{}

var _ sdk.Connector = Connector{}

// ID implements sdk.Connector.
func (Connector) ID() sdk.ConnectorID { return ConnectorID }

// DisplayName implements sdk.Connector.
func (Connector) DisplayName() string { return "Todo" }

// Categories implements sdk.Connector.
func (Connector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryIssueTracking}
}

// ValidateConfig implements sdk.Connector.
func (Connector) ValidateConfig(_ context.Context, cfg *sdk.ConnectorConfig) error {
	var s Settings
	if err := json.Unmarshal(cfg.Settings, &s); err != nil {
		return fmt.Errorf("todo: settings: %w", err)
	}
	if s.BaseURL == "" {
		return errors.New("todo: settings.base_url is empty")
	}
	return nil
}

// Operations implements sdk.Connector. fetch_todos is a pull operation with
// a default schedule, so `serve` schedules it.
func (Connector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{{
		ID:              OpFetchTodos,
		DisplayName:     "Fetch todos",
		Description:     "Pulls every todo.",
		DefaultSchedule: "*/15 * * * *",
		IngestionType:   sdk.IngestionPull,
		Factory:         func() sdk.Operation { return fetchTodos{} },
	}}
}

// NewGateway is the todo connector's app.GatewayBuilder: a plain HTTP
// gateway that sends the stored token as a bearer token.
func NewGateway(_ context.Context, cc *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
	var c Credentials
	if len(cc.Credentials) > 0 {
		if err := json.Unmarshal(cc.Credentials, &c); err != nil {
			return nil, fmt.Errorf("todo gateway: credentials: %w", err)
		}
	}
	doer := bearerDoer{client: &http.Client{Timeout: 30 * time.Second}, token: c.Token}
	return &gateway{BaseVendorGateway: vendorapi.BaseVendorGateway{Doer: doer}}, nil
}

// gateway has no rate-limit groups: ForGroup and WithRateLimits return it.
type gateway struct {
	vendorapi.BaseVendorGateway
}

func (g *gateway) ForGroup(string) sdk.VendorGateway { return g }

func (g *gateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return g }

type bearerDoer struct {
	client *http.Client
	token  string
}

func (d bearerDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if d.token != "" {
		req.Header.Set("Authorization", "Bearer "+d.token)
	}
	resp, err := d.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("todo: %w", err)
	}
	return resp, nil
}

// fetchTodos emits one record per todo, keyed by its id.
type fetchTodos struct{}

// OperationID implements sdk.Operation.
func (fetchTodos) OperationID() sdk.OperationID { return OpFetchTodos }

// Execute implements sdk.APIFetchOperation.
func (fetchTodos) Execute(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
	var s Settings
	if err := json.Unmarshal(p.Config.Settings, &s); err != nil {
		return nil, &sdk.SyncError{Kind: sdk.ErrKindConfigInvalid, Terminal: true, Cause: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.BaseURL+"/todos", nil)
	if err != nil {
		return nil, &sdk.SyncError{Kind: sdk.ErrKindConfigInvalid, Terminal: true, Cause: err}
	}
	resp, err := p.VendorGateway.Do(ctx, req)
	if err != nil {
		return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, Retryable: true, Cause: err}
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, Retryable: true, Cause: err}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, body, resp.Header)
	}
	var todos []Todo
	if err := json.Unmarshal(body, &todos); err != nil {
		return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, Cause: fmt.Errorf("decode todos: %w", err)}
	}
	now := time.Now().UTC()
	for _, t := range todos {
		raw, err := json.Marshal(t)
		if err != nil {
			return nil, fmt.Errorf("encode todo: %w", err)
		}
		if err := p.Sink.Emit(ctx, &sdk.RawRecord{
			ConnectorID:    string(ConnectorID),
			OperationID:    string(OpFetchTodos),
			OrganizationID: p.Config.OrganizationID,
			ExternalID:     strconv.Itoa(t.ID),
			Payload:        raw,
			FetchedAt:      now,
		}); err != nil {
			return nil, &sdk.SyncError{Kind: sdk.ErrKindSinkFailure, Cause: err}
		}
	}
	cur, err := json.Marshal(map[string]any{"synced_at": now, "count": len(todos)})
	if err != nil {
		return nil, fmt.Errorf("encode cursor: %w", err)
	}
	return &sdk.Cursor{Data: cur}, nil
}
