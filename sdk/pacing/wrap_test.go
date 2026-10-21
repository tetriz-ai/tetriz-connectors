// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package pacing

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// A vendor error passed through ExecuteWaitingForLocalCapacity is wrapped as
// "vendor request", whatever the connector.
func TestVendorErrorIsWrappedAsVendorRequest(t *testing.T) {
	gw := &scriptedGateway{results: []gwResult{{err: errors.New("boom")}}}
	_, err := ExecuteWaitingForLocalCapacity(context.Background(), gw, wrapperspb.String("req"), &PacingBudget{}, "github", "fetch_prs", "org-test")
	require.Error(t, err)
	assert.Equal(t, "vendor request: boom", err.Error())
}
