// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// TestBuildPRsQuery_IndividualScope pins that the individual variant drops the
// org-scoped requestedReviewer (a Team node needs Organization:Members, which a
// repo-scoped credential lacks) while keeping reviewRequests.totalCount; the org
// variant keeps the full reviewer set.
func TestBuildPRsQuery_IndividualScope(t *testing.T) {
	t.Parallel()
	since := time.Now().Add(-90 * 24 * time.Hour)

	tests := []struct {
		name            string
		individualScope bool
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:            "individual drops org-scoped reviewer identity",
			individualScope: true,
			wantContains:    []string{"reviewRequests(first: 10) { totalCount }"},
			wantNotContains: []string{"requestedReviewer", "on Team"},
		},
		{
			name:            "org keeps full reviewer set",
			individualScope: false,
			wantContains:    []string{"requestedReviewer", "... on Team { slug }", "totalCount"},
			wantNotContains: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q := buildPRsQuery("acme", "widgets", since, "", 50, nil, tt.individualScope)
			for _, s := range tt.wantContains {
				assert.Contains(t, q, s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, q, s)
			}
		})
	}
}

// TestDoGraphQLWithPartial covers the partial-success predicate. Tolerance is
// opt-in: only with tolerateForbiddenPartial=true AND real data AND every error a
// permanent permission error is the partial page ingested. Strict mode, any
// transient error, or a null/empty/truncated payload all fail so the caller
// leaves its cursor unadvanced and retries.
func TestDoGraphQLWithPartial(t *testing.T) {
	t.Parallel()
	forbidden := []*connectorpb.GitHubGraphQLError{{Message: "Resource not accessible by personal access token", Type: "FORBIDDEN"}}

	tests := []struct {
		name     string
		tolerant bool
		data     string
		errs     []*connectorpb.GitHubGraphQLError
		wantErr  bool
		wantData bool
	}{
		{
			name: "clean response returns data", tolerant: false,
			data: `{"repository":{"pullRequests":{"nodes":[{"number":1}]}}}`, errs: nil,
			wantErr: false, wantData: true,
		},
		{
			name: "tolerant: forbidden + populated data is ingested", tolerant: true,
			data: `{"repository":{"issues":{"nodes":[{"number":1}]}}}`, errs: forbidden,
			wantErr: false, wantData: true,
		},
		{
			name: "strict: forbidden + populated data still fails", tolerant: false,
			data: `{"repository":{"issues":{"nodes":[{"number":1}]}}}`, errs: forbidden,
			wantErr: true,
		},
		{
			name: "tolerant: forbidden + null data fails", tolerant: true,
			data: `null`, errs: forbidden, wantErr: true,
		},
		{
			name: "tolerant: forbidden + null repository fails", tolerant: true,
			data: `{"repository":null}`, errs: forbidden, wantErr: true,
		},
		{
			name: "tolerant: forbidden + empty repository object fails", tolerant: true,
			data: `{"repository":{}}`, errs: forbidden, wantErr: true,
		},
		{
			name: "tolerant: forbidden + empty data object fails", tolerant: true,
			data: `{}`, errs: forbidden, wantErr: true,
		},
		{
			name: "tolerant: transient error with data fails (retry-recoverable)", tolerant: true,
			data:    `{"repository":{"issues":{"nodes":[]}}}`,
			errs:    []*connectorpb.GitHubGraphQLError{{Message: "timeout", Type: "SERVICE_UNAVAILABLE"}},
			wantErr: true,
		},
		{
			name: "tolerant: mixed permission + transient fails", tolerant: true,
			data:    `{"repository":{"issues":{"nodes":[]}}}`,
			errs:    []*connectorpb.GitHubGraphQLError{{Type: "FORBIDDEN"}, {Type: "RATE_LIMITED"}},
			wantErr: true,
		},
		{
			name: "tolerant: unknown/empty error type fails", tolerant: true,
			data:    `{"repository":{"issues":{"nodes":[]}}}`,
			errs:    []*connectorpb.GitHubGraphQLError{{Message: "boom"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := &mockVendorGateway{
				executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
					return &connectorpb.GitHubGraphQLResponse{
						Data:   json.RawMessage(tt.data),
						Errors: tt.errs,
					}, nil
				},
			}
			data, err := doGraphQL(context.Background(), gw, "{ query }", tt.tolerant)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, data)
				return
			}
			require.NoError(t, err)
			if tt.wantData {
				assert.NotEmpty(t, data)
			}
		})
	}
}
