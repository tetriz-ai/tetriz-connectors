// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ConnectorSyncPayload is the JSON payload enqueued by the scheduler
// and consumed by ConnectorSyncHandler.
type ConnectorSyncPayload struct {
	OrgID       string          `json:"org_id"`
	ConnectorID sdk.ConnectorID `json:"connector_id"`
	OperationID sdk.OperationID `json:"operation_id"`

	// TenantType is the org's tenant intent (our tenant_type, not what the vendor
	// would serve), resolved at schedule-build and threaded to execution so an op
	// that switches behaviour on intent (repo discovery for an INDIVIDUAL vs
	// ORGANIZATION account) reads it without a tenant lookup at run time.
	// TENANT_TYPE_UNSPECIFIED means intent was not resolved at build time; an op
	// that requires intent treats that as fail-loud. A producer with a statically
	// tenant-agnostic op may leave it unset — UNSPECIFIED is the correct default.
	TenantType sdk.TenantKind `json:"tenant_type"`
}
