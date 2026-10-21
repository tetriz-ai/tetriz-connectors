// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

//go:generate go tool mockgen -source=vendor_gateway.go -destination=../internal/mocks/sdk/mock_vendor_gateway.go -package=mocks

import (
	"context"
	"net/http"

	"google.golang.org/protobuf/proto"
)

// RateLimitConfig defines rate limiting parameters for an API group.
// Different orgs may have different API tiers, so rate limits are configurable
// per-org via ConnectorConfig.Settings — not hardcoded.
type RateLimitConfig struct {
	// Group is a logical name for this rate limit pool (e.g., "admin-sdk", "drive-api").
	Group string

	// RequestsPerSecond is the sustained rate limit.
	RequestsPerSecond float64

	// BurstSize is the maximum burst allowed above the sustained rate.
	BurstSize int

	// OperationShares reserves a fraction of this group's budget for named
	// operations so one cannot starve the rest. Empty keeps the single shared
	// bucket. Fractions must sum to less than 1 — operations without a named share
	// draw from the remainder, which is what makes the shares a partition of the
	// budget rather than an addition to it. Mirrors the static
	// gateway.APIEndpointConfig field of the same name.
	OperationShares map[string]float64
}

// VendorGateway is the interface for making outbound calls to vendor APIs.
// It supports two levels of abstraction:
//   - Do / DoJSON: raw HTTP and JSON convenience methods
//   - Execute: proto-level request/response via a SyncRequestFactory
//
// The implementations in gateway/ add rate limiting per group, error
// classification into SyncError, OAuth token refresh and request metrics.
//
// VendorGateway applies no global rate limit. A connector reads its limits from
// the organization's config and scopes a gateway with WithRateLimits.
type VendorGateway interface {
	// Do executes an HTTP request with all middleware applied.
	Do(ctx context.Context, req *http.Request) (*http.Response, error)

	// DoJSON is a convenience method: marshals request body, unmarshals response.
	DoJSON(ctx context.Context, method, url string, reqBody, respBody any) error

	// WithRateLimits returns a VendorGateway scoped to the given rate limit configs.
	// Each RateLimitConfig defines a named group with its own rate/burst settings.
	// Calls through this gateway are rate-limited per-group.
	WithRateLimits(configs []RateLimitConfig) VendorGateway

	// ForGroup returns a VendorGateway that applies rate limiting for the named group.
	// The group must already be configured via WithRateLimits().
	ForGroup(group string) VendorGateway

	// Execute sends a proto request through the vendor API pipeline and returns a proto response.
	// It uses a SyncRequestFactory to convert the proto message into an HTTP request,
	// executes it, and unmarshals the response back into a proto message.
	Execute(ctx context.Context, req proto.Message) (proto.Message, error)
}

// VendorGatewayFactory creates a VendorGateway scoped to a specific connector config.
// Called once per operation invocation with the loaded ConnectorConfig (which contains
// org-specific credentials). The factory constructs an authenticated gateway.
//
// Used by the orchestrator for scheduled operations.
// docs/adr/0002 records the design.
type VendorGatewayFactory func(ctx context.Context, cfg *ConnectorConfig) (VendorGateway, error)
