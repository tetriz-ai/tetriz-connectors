// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// sizedDoer answers every request with a 200 whose body is n bytes of a
// JSON array, so a body under the cap still decodes.
type sizedDoer struct{ n int }

func (d sizedDoer) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	body := bytes.Repeat([]byte(" "), d.n)
	body[0], body[d.n-1] = '[', ']'
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, nil
}

// The GitLab gateway reads at most vendorapi.MaxResponseBytes of a response:
// one byte more fails the request as a terminal vendor_error naming the cap,
// and a body of exactly the cap is read.
func TestExecute_ResponseBodyOverTheCapFails(t *testing.T) {
	gw := func(n int) *GitLabVendorGateway {
		return &GitLabVendorGateway{BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    sizedDoer{n: n},
			Factory: NewGitLabVendorRequest(map[string]string{constants.GroupGitLabREST: "https://gitlab.com/api/v4"}),
		}}
	}
	_, err := gw(vendorapi.MaxResponseBytes+1).Execute(context.Background(), &connectorpb.GitLabListProjectsRequest{GroupId: 1})
	require.Error(t, err)
	var se *sdk.SyncError
	require.True(t, errors.As(err, &se), "%v", err)
	assert.Equal(t, sdk.ErrKindVendorError, se.Kind)
	assert.True(t, se.Terminal)
	assert.Contains(t, err.Error(), "(32 MiB) cap")

	_, err = gw(vendorapi.MaxResponseBytes).Execute(context.Background(), &connectorpb.GitLabListProjectsRequest{GroupId: 1})
	require.NoError(t, err)
}
