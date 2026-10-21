// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// orgScopeForbidden fails an operation on an organization-scoped endpoint the
// token may lawfully lack (Copilot usage, organization members, projects)
// without pausing the connector. GitHub answers 403 when a working token has
// no access to that endpoint, such as a read-only token without the
// organization's administration or Copilot permission, and the gateway
// classifies every non-throttle 403 as a terminal auth error, which pauses the
// whole connector. Here such a 403 becomes a terminal config_invalid error:
// this operation fails until the token is granted the access, and the
// connector's repository operations keep running. A 401, or any other error,
// is returned unchanged, so a dead token still pauses the connector.
func orgScopeForbidden(err error, opID sdk.OperationID, orgID, what string) error {
	var syncErr *sdk.SyncError
	if err == nil || !errors.As(err, &syncErr) || syncErr.Kind != sdk.ErrKindAuth || syncErr.HTTPStatus != http.StatusForbidden {
		return err
	}
	return &sdk.SyncError{
		ConnectorID:    connectorID,
		OperationID:    opID,
		OrganizationID: orgID,
		Kind:           sdk.ErrKindConfigInvalid,
		HTTPStatus:     http.StatusForbidden,
		Terminal:       true,
		Cause: fmt.Errorf("the token may not read %s (HTTP 403); grant it that access to sync this operation: %s",
			what, syncErr.Error()),
	}
}
