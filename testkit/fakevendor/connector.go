// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fakevendor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	// ConnectorID is the fake connector's id.
	ConnectorID sdk.ConnectorID = "fakevendor"
	// OpFetchItems pulls the vendor's items.
	OpFetchItems sdk.OperationID = "fetch_items"
)

// Settings is the connector config's Settings JSON.
type Settings struct {
	// BaseURL is the fake vendor's root, Server.URL in tests.
	BaseURL string `json:"base_url"`
	// PageSize is the limit sent per request. Zero means 2.
	PageSize int `json:"page_size,omitempty"`
}

// Credentials is the connector config's Credentials JSON.
type Credentials struct {
	Token string `json:"token"`
}

// cursorState is the operation's cursor: the (updated_at, id) of the last item
// emitted in listing order, and, mid-listing, the page to resume from.
type cursorState struct {
	Since       time.Time `json:"since"`
	PageToken   string    `json:"page_token,omitempty"`
	HighWater   time.Time `json:"high_water"`
	HighWaterID string    `json:"high_water_id,omitempty"`
}

// Connector is the fake vendor's connector.
type Connector struct{}

var _ sdk.Connector = Connector{}

// ID implements sdk.Connector.
func (Connector) ID() sdk.ConnectorID { return ConnectorID }

// DisplayName implements sdk.Connector.
func (Connector) DisplayName() string { return "Fake vendor" }

// Categories implements sdk.Connector.
func (Connector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryIssueTracking}
}

// ValidateConfig implements sdk.Connector.
func (Connector) ValidateConfig(_ context.Context, cfg *sdk.ConnectorConfig) error {
	var s Settings
	if err := json.Unmarshal(cfg.Settings, &s); err != nil {
		return fmt.Errorf("fakevendor: settings: %w", err)
	}
	if s.BaseURL == "" {
		return errors.New("fakevendor: settings.base_url is empty")
	}
	return nil
}

// Operations implements sdk.Connector.
func (Connector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{{
		ID:              OpFetchItems,
		DisplayName:     "Fetch items",
		Description:     "Pulls every item updated since the last run, a page at a time.",
		DefaultSchedule: "*/15 * * * *",
		IngestionType:   sdk.IngestionPull,
		Factory:         func() sdk.Operation { return &FetchItemsOp{} },
	}}
}

// GatewayFactory builds the gateway the orchestrator hands the operation: a
// plain HTTP gateway that sends the config's token as a bearer token.
func GatewayFactory(client *http.Client) sdk.VendorGatewayFactory {
	if client == nil {
		client = http.DefaultClient
	}
	return func(_ context.Context, cfg *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		var c Credentials
		if len(cfg.Credentials) > 0 {
			if err := json.Unmarshal(cfg.Credentials, &c); err != nil {
				return nil, fmt.Errorf("fakevendor: credentials: %w", err)
			}
		}
		return &gateway{BaseVendorGateway: vendorapi.BaseVendorGateway{Doer: bearerDoer{client: client, token: c.Token}}}, nil
	}
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
		return nil, fmt.Errorf("fakevendor: %w", err)
	}
	return resp, nil
}

// FetchItemsOp pulls items updated since the cursor, emitting one record per
// item keyed by the item id, and checkpointing after every page.
type FetchItemsOp struct{}

// OperationID implements sdk.Operation.
func (*FetchItemsOp) OperationID() sdk.OperationID { return OpFetchItems }

// Execute implements sdk.APIFetchOperation.
func (op *FetchItemsOp) Execute(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
	var s Settings
	if err := json.Unmarshal(p.Config.Settings, &s); err != nil {
		return nil, &sdk.SyncError{Kind: sdk.ErrKindConfigInvalid, Terminal: true, Cause: err}
	}
	if s.PageSize <= 0 {
		s.PageSize = 2
	}
	var st cursorState
	if p.Cursor != nil && len(p.Cursor.Data) > 0 {
		if err := json.Unmarshal(p.Cursor.Data, &st); err != nil {
			return nil, &sdk.SyncError{Kind: sdk.ErrKindInvalidInput, Cause: fmt.Errorf("cursor: %w", err)}
		}
	}
	if st.PageToken == "" {
		// A fresh listing starts after the last item emitted, in the same
		// (updated_at, id) keyset the page token uses, so an unchanged item is
		// not sent again. A cursor without the id falls back to the inclusive
		// updated_since bound.
		st.Since = st.HighWater
		if st.HighWaterID != "" {
			st.PageToken = st.HighWater.Format(time.RFC3339Nano) + "|" + st.HighWaterID
		}
	}
	for {
		items, next, err := op.page(ctx, p.VendorGateway, s, st)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			raw, _ := json.Marshal(it)
			rec := &sdk.RawRecord{
				ConnectorID:    string(ConnectorID),
				OperationID:    string(OpFetchItems),
				OrganizationID: p.Config.OrganizationID,
				ExternalID:     it.ID,
				Payload:        raw,
				FetchedAt:      time.Now().UTC(),
				DataDate:       it.UpdatedAt,
			}
			if err := p.Sink.Emit(ctx, rec); err != nil {
				return nil, &sdk.SyncError{Kind: sdk.ErrKindSinkFailure, Cause: err}
			}
			if it.UpdatedAt.After(st.HighWater) || (it.UpdatedAt.Equal(st.HighWater) && it.ID > st.HighWaterID) {
				st.HighWater, st.HighWaterID = it.UpdatedAt, it.ID
			}
		}
		if next != "" && next == st.PageToken {
			return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, Cause: fmt.Errorf("page token %q did not advance", next)}
		}
		st.PageToken = next
		if next == "" {
			break
		}
		if p.Checkpoint != nil {
			if err := p.Checkpoint(ctx, mustCursor(st)); err != nil {
				return nil, fmt.Errorf("checkpoint: %w", err)
			}
		}
	}
	return mustCursor(st), nil
}

// ResumesFrom reports whether req, the first request of a run, resumes from
// the stored cursor: a fresh listing starts after the cursor's last item, and
// a checkpointed one continues from its page token. RunConformance's Resume
// check uses it.
func ResumesFrom(stored *sdk.Cursor, req *http.Request) error {
	if req == nil {
		return errors.New("the first vendor call is not an HTTP request")
	}
	var st cursorState
	if stored == nil || json.Unmarshal(stored.Data, &st) != nil {
		return errors.New("the stored cursor does not decode")
	}
	want := st.PageToken
	if want == "" && st.HighWaterID != "" {
		want = st.HighWater.Format(time.RFC3339Nano) + "|" + st.HighWaterID
	}
	if got := req.URL.Query().Get("page_token"); got != want {
		return fmt.Errorf("page_token %q, want %q", got, want)
	}
	return nil
}

func mustCursor(st cursorState) *sdk.Cursor {
	b, err := json.Marshal(st)
	if err != nil {
		panic(fmt.Sprintf("fakevendor: marshal cursor: %v", err))
	}
	return &sdk.Cursor{Data: b}
}

func (op *FetchItemsOp) page(ctx context.Context, gw sdk.VendorGateway, s Settings, st cursorState) ([]Item, string, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(s.PageSize))
	if !st.Since.IsZero() {
		q.Set("updated_since", st.Since.Format(time.RFC3339Nano))
	}
	if st.PageToken != "" {
		q.Set("page_token", st.PageToken)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.BaseURL+"/items?"+q.Encode(), nil)
	if err != nil {
		return nil, "", &sdk.SyncError{Kind: sdk.ErrKindConfigInvalid, Terminal: true, Cause: err}
	}
	resp, err := gw.Do(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", &sdk.SyncError{Kind: sdk.ErrKindTimeout, Cause: err}
		}
		return nil, "", &sdk.SyncError{Kind: sdk.ErrKindVendorError, Retryable: true, Cause: err}
	}
	defer resp.Body.Close() //nolint:errcheck // read-only response
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, "", &sdk.SyncError{Kind: sdk.ErrKindVendorError, Retryable: true, Cause: err}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, body, resp.Header)
	}
	var pg struct {
		Items []struct {
			ID        *string   `json:"id"`
			Title     string    `json:"title"`
			UpdatedAt time.Time `json:"updated_at"`
		} `json:"items"`
		NextPageToken string `json:"next_page_token"`
	}
	if err := json.Unmarshal(body, &pg); err != nil {
		return nil, "", &sdk.SyncError{Kind: sdk.ErrKindVendorError, Cause: fmt.Errorf("decode page: %w", err)}
	}
	items := make([]Item, 0, len(pg.Items))
	for i, it := range pg.Items {
		// An item without its identity cannot be keyed; fail the run rather
		// than advance the cursor past records nobody can deduplicate.
		if it.ID == nil || *it.ID == "" {
			return nil, "", &sdk.SyncError{Kind: sdk.ErrKindVendorError, Cause: fmt.Errorf("item %d on the page has no id", i)}
		}
		items = append(items, Item{ID: *it.ID, Title: it.Title, UpdatedAt: it.UpdatedAt})
	}
	return items, pg.NextPageToken, nil
}
