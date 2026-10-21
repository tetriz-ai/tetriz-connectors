// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A 403 whose GitHub scope headers show the token has none of the scopes the
// endpoint accepts is a terminal config_invalid naming them; a 403 without
// those headers, or with a granted accepted scope, stays a terminal auth error.
func TestForbiddenWithAMissingOAuthScopeIsConfigInvalid(t *testing.T) {
	h := http.Header{}
	h.Set("X-Accepted-OAuth-Scopes", "read:project, project")
	h.Set("X-OAuth-Scopes", "repo, read:org")
	e := ClassifyHTTPErrorWithHeaders(403, []byte(`{"message":"Forbidden"}`), h)
	assert.Equal(t, sdk.ErrKindConfigInvalid, e.Kind)
	assert.True(t, e.Terminal)
	assert.Contains(t, e.Error(), "read:project, project")

	h.Set("X-OAuth-Scopes", "repo, read:project")
	assert.Equal(t, sdk.ErrKindAuth, ClassifyHTTPErrorWithHeaders(403, nil, h).Kind, "the token has an accepted scope")
	assert.Equal(t, sdk.ErrKindAuth, ClassifyHTTPErrorWithHeaders(403, nil, nil).Kind, "no scope headers")
	h.Del("X-OAuth-Scopes")
	assert.Equal(t, sdk.ErrKindAuth, ClassifyHTTPErrorWithHeaders(403, nil, h).Kind, "no granted-scopes header")
}
