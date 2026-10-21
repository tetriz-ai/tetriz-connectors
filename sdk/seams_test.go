// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TenantKind replaces the host's tenant-type proto enum in job payloads, so
// it must encode and decode exactly as that enum does.
func TestTenantKindJSONMatchesProtoEnum(t *testing.T) {
	for v := range commonv1.TenantType_name {
		want, err := json.Marshal(commonv1.TenantType(v))
		require.NoError(t, err)
		got, err := json.Marshal(sdk.TenantKind(v))
		require.NoError(t, err)
		assert.Equal(t, string(want), string(got))

		var back sdk.TenantKind
		require.NoError(t, json.Unmarshal(want, &back))
		assert.Equal(t, sdk.TenantKind(v), back)
	}
}
