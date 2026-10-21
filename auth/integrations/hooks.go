// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

//go:generate go tool mockgen -source=hooks.go -destination=../../internal/mocks/auth/integrations/hooks/mock_hooks.go -package=mocks

package integrations

import (
	"context"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
)

// BrandingSink receives the organization logo a connect flow reads from the
// vendor (a workspace, group or team avatar). Best effort: a failure is
// logged and the connect still succeeds.
type BrandingSink interface {
	UpdateOrgLogo(ctx context.Context, orgID, logoURL string) error
}

// SettingsChangeHook is told when an administrator changes a VCS setting
// that the host's derived data depends on: the tracked branches (pull
// request counts are re-derived) or the file-exclude patterns (diffs are
// re-analyzed).
type SettingsChangeHook interface {
	ReprocessConnectorPRCounts(ctx context.Context, req *workqueuev1.ReprocessConnectorPRCountsRequest) (*workqueuev1.ReprocessConnectorPRCountsResponse, error)
	ResetPRDiffAnalysisStatus(ctx context.Context, req *workqueuev1.ResetPRDiffAnalysisStatusRequest) (*workqueuev1.ResetPRDiffAnalysisStatusResponse, error)
}
