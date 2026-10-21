// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	lineagemocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk/lineage"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Missing returns the required services that are nil, in the order asked,
// and Provided the ones that are set.
func TestHostServices_MissingAndProvided(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := sdk.HostServices{WorkQueue: sdkmocks.NewMockWorkQueue(ctrl), BlameSink: lineagemocks.NewMockBlameSink(ctrl)}

	assert.Empty(t, h.Missing(nil))
	assert.Empty(t, h.Missing([]sdk.HostService{sdk.HostWorkQueue, sdk.HostBlameSink}))
	assert.Equal(t, []sdk.HostService{sdk.HostDefectScorer, sdk.HostLineageWalker},
		h.Missing([]sdk.HostService{sdk.HostDefectScorer, sdk.HostBlameSink, sdk.HostLineageWalker}))
	assert.Equal(t, []sdk.HostService{"unknown"}, h.Missing([]sdk.HostService{"unknown"}), "an unknown service is never provided")
	assert.Equal(t, []sdk.HostService{sdk.HostWorkQueue, sdk.HostBlameSink}, h.Provided())
	assert.Empty(t, sdk.HostServices{}.Provided())
}

// The error is terminal config_invalid and names the operation and services.
func TestMissingHostServicesError(t *testing.T) {
	err := sdk.MissingHostServicesError("github", "drain_defect_lineage", "org-1", []sdk.HostService{sdk.HostBlameSink, sdk.HostLineageWalker})
	assert.Equal(t, sdk.ErrKindConfigInvalid, err.Kind)
	assert.True(t, err.Terminal)
	assert.Contains(t, err.Error(), "operation github/drain_defect_lineage needs host services: blame_sink, lineage_walker")
}
