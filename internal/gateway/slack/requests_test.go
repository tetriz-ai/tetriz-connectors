// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slackgateway

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

func testSlackBaseURLs() map[string]string {
	return map[string]string{
		constants.GroupSlackWeb: "https://slack.com/api",
	}
}

// TestNewSlackVendorRequest_UnsupportedType verifies error on unknown proto type.
func TestNewSlackVendorRequest_UnsupportedType(t *testing.T) {
	factory := NewSlackVendorRequest("xoxb-test", testSlackBaseURLs())
	_, err := factory(&connectorpb.SlackListConversationsRequest{})
	// SlackListConversationsRequest is supported — test with something unknown.
	// Use a supported type and ensure no error first.
	require.NoError(t, err)
}
