// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	anthropicconfig "github.com/tetriz-ai/tetriz-connectors/connectors/anthropic"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vganthropicpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestClassifyAnthropicProbeFailure covers the three outcomes, and the middle one is the
// reason this classifier is not a copy of Cursor's.
//
// Anthropic separates a rejected credential from a key whose scopes are too narrow, and
// that separation changes the ADVICE rather than the wording: a key's scopes are fixed
// when it is minted, so a 403 cannot be resolved by granting permission. An administrator
// told "insufficient permission" would go looking for a setting that does not exist.
// TestScopeNamesMatchTheVendor pins the two strings against literals rather than against
// themselves. Every other assertion reads them back through the same constant the code
// writes, so a typo would ship silently — and these are copy an administrator has to match
// against Anthropic's own key-creation checkboxes, where a near-miss is useless.
func TestScopeNamesMatchTheVendor(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "read:members", scopeReadMembers)
	assert.Equal(t, "read:analytics", scopeReadAnalytics)
}

func TestClassifyAnthropicProbeFailure(t *testing.T) {
	t.Parallel()

	authErr := func(httpStatus int) error {
		return &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: httpStatus, Cause: errors.New("vendor said no")}
	}

	tests := []struct {
		name         string
		err          error
		missingScope string
		wantCode     codes.Code
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:         "a 403 on the roster names read:members and says to recreate",
			err:          authErr(http.StatusForbidden),
			missingScope: scopeReadMembers,
			wantCode:     codes.InvalidArgument,
			wantContains: []string{scopeReadMembers, "create a new key", scopeReadAnalytics},
		},
		{
			name:         "a 403 on the cost report names read:analytics",
			err:          authErr(http.StatusForbidden),
			missingScope: scopeReadAnalytics,
			wantCode:     codes.InvalidArgument,
			wantContains: []string{scopeReadAnalytics, "create a new key"},
		},
		{
			name:         "a 401 is a rejected credential, not a scope problem",
			err:          authErr(http.StatusUnauthorized),
			missingScope: scopeReadAnalytics,
			wantCode:     codes.InvalidArgument,
			wantContains: []string{"rejected by Anthropic"},
			// Telling someone to recreate the key with more scopes is wrong advice when
			// the key itself was refused.
			wantAbsent: []string{"create a new key", "scope"},
		},
		{
			name:         "a rate limit says to wait rather than that the key is bad",
			err:          &sdk.SyncError{Kind: sdk.ErrKindRateLimit},
			missingScope: scopeReadAnalytics,
			wantCode:     codes.Unavailable,
			wantContains: []string{"too many validation attempts", "wait a minute"},
			wantAbsent:   []string{"rejected", "scope"},
		},
		{
			name:         "a vendor outage is unavailable, not invalid",
			err:          &sdk.SyncError{Kind: sdk.ErrKindInternal},
			missingScope: scopeReadMembers,
			wantCode:     codes.Unavailable,
			wantContains: []string{"could not reach Anthropic"},
			wantAbsent:   []string{"rejected", "scope"},
		},
		{
			name:         "a plain error carries no vendor verdict, so it cannot condemn the key",
			err:          errors.New("context deadline exceeded"),
			missingScope: scopeReadMembers,
			wantCode:     codes.Unavailable,
			wantContains: []string{"could not reach Anthropic"},
			wantAbsent:   []string{"rejected", "scope"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			code, msg := classifyAnthropicProbeFailure(context.Background(), "org-1", "probe", tt.err, tt.missingScope)
			assert.Equal(t, tt.wantCode, code)
			for _, want := range tt.wantContains {
				assert.Containsf(t, msg, want, "message should name %q", want)
			}
			for _, absent := range tt.wantAbsent {
				assert.NotContainsf(t, strings.ToLower(msg), strings.ToLower(absent),
					"message must not say %q for this failure", absent)
			}
		})
	}
}

// TestAnthropicRefusalNamesTheScopeThatFailed is the roster-401 carry-forward, and it is
// about which probe failed rather than which code came back.
//
// The roster is the one endpoint outside the analytics family. If a key reaches the four
// analytics paths but not the roster, the administrator must be told read:members is
// missing — not handed a message about analytics, and not left reading it as a dead
// credential for the whole connector.
func TestAnthropicRefusalNamesTheScopeThatFailed(t *testing.T) {
	t.Parallel()

	forbidden := &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: http.StatusForbidden}

	_, rosterMsg := classifyAnthropicProbeFailure(context.Background(), "org-1",
		"list organization members", forbidden, scopeReadMembers)
	_, analyticsMsg := classifyAnthropicProbeFailure(context.Background(), "org-1",
		"read cost report", forbidden, scopeReadAnalytics)

	require.NotEqual(t, rosterMsg, analyticsMsg,
		"the two probes must not produce the same refusal, or the administrator cannot tell which scope is missing")
	assert.Contains(t, rosterMsg, "missing the "+scopeReadMembers)
	assert.Contains(t, analyticsMsg, "missing the "+scopeReadAnalytics)
}

// --- probe wiring, through a mocked gateway ---

const anthropicMembersJSON = `[
  {"id":"user_A","email":"dev@example.com","name":"Dev One","role":"admin"},
  {"id":"user_B","email":"","name":"Service Account","role":"user"}
]`

func anthropicMembersResponse(t *testing.T) *vganthropicpb.AnthropicOrganizationMembersResponse {
	t.Helper()
	var raw []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(anthropicMembersJSON), &raw))
	resp := &vganthropicpb.AnthropicOrganizationMembersResponse{}
	for _, m := range raw {
		resp.Data = append(resp.Data, m)
	}
	return resp
}

func newAnthropicDelegate(
	t *testing.T, gw sdk.VendorGateway, enabled bool,
) (*Service, *daomocks.MockIConnectorConfigDAO) {
	t.Helper()
	ctrl := gomock.NewController(t)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	cfg := testConfig()
	cfg.EnabledConnectors[string(connectorIDClaudeCode)] = enabled
	factory := sdk.VendorGatewayFactory(func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		return gw, nil
	})
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Registry: testRegistry(), GatewayFactory: factory})
	return svc, dao
}

// expectAnthropicProbes sets the roster call then the cost call, in that order, because
// the sequence is the design: one call per scope, and the cost report is also what yields
// the organisation identity.
//
// Both resolve against the same rate-limit group, which is the opposite of Cursor's case
// and is asserted rather than accepted as any — Anthropic meters per organisation across
// the family, so a second group would be a second budget the vendor never granted.
// orgCtx matches a context carrying the given organisation. The limiter reads the
// organisation off the REQUEST context inside the doer, not off the one the factory was
// built with, so this is the only matcher that can see the property being claimed.
type orgCtx string

func (o orgCtx) Matches(x any) bool {
	ctx, ok := x.(context.Context)
	return ok && reqctx.OrgIDFromContext(ctx) == string(o)
}

func (o orgCtx) String() string { return "context carrying org " + string(o) }

func expectAnthropicProbes(t *testing.T, gw *fwmocks.MockVendorGateway, membersErr, costErr error, orgID string) {
	t.Helper()

	// The context matcher is the point: every probe must carry the organisation, because
	// that is what buckets the rate limiter. Accepting any context here would let a
	// change that only sets the organisation on the factory's context pass, while in
	// production every organisation's probe would share one bucket.
	ctxWithOrg := orgCtx(probeOrgID)

	gw.EXPECT().ForGroup(constants.GroupAnthropicAnalytics).Return(gw).AnyTimes()
	members := gw.EXPECT().
		Execute(ctxWithOrg, gomock.AssignableToTypeOf(&vganthropicpb.AnthropicOrganizationMembersRequest{})).
		Return(proto.Message(anthropicMembersResponse(t)), membersErr)
	if membersErr != nil {
		return
	}
	gw.EXPECT().
		Execute(ctxWithOrg, gomock.AssignableToTypeOf(&vganthropicpb.AnthropicAnalyticsUserCostReportRequest{})).
		Return(proto.Message(&vganthropicpb.AnthropicAnalyticsUserCostReportResponse{OrganizationId: orgID}), costErr).
		After(members)
}

// probeOrgID is the organisation every probe test resolves for.
const probeOrgID = "org-1"

func anthropicAuthErr(httpStatus int) error {
	return &sdk.SyncError{Kind: sdk.ErrKindAuth, HTTPStatus: httpStatus, Cause: errors.New("vendor said no")}
}

// TestValidateAnthropicAnalyticsKey resolves both identities without persisting.
func TestValidateAnthropicAnalyticsKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectAnthropicProbes(t, gw, nil, nil, "org_abc")
	svc, _ := newAnthropicDelegate(t, gw, true)

	resp, err := svc.ValidateAnthropicAnalyticsKey(context.Background(),
		&connintv1.ValidateAnthropicAnalyticsKeyRequest{OrgId: "org-1", ApiKey: "sk-ant-api01-x"})
	require.NoError(t, err)

	assert.Equal(t, "org_abc", resp.GetAnthropicOrganizationId())
	assert.Equal(t, []string{"dev@example.com"}, resp.GetMemberEmails())
	// The member with no address is counted but not listed: dropping it from the count
	// would understate the organisation an administrator is asked to recognise.
	assert.Equal(t, int32(2), resp.GetMemberCount())
}

// TestAnthropicProbeFailuresNameTheScopeTheyProve is the wiring test the classifier's own
// table cannot be: it calls classifyAnthropicProbeFailure directly, so swapping the two
// scope arguments at the probe call sites would leave it green. Going through the probes
// is what binds each one to the scope it actually proves.
func TestAnthropicProbeFailuresNameTheScopeTheyProve(t *testing.T) {
	t.Parallel()

	t.Run("the roster probe names read:members", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		expectAnthropicProbes(t, gw, anthropicAuthErr(http.StatusForbidden), nil, "")
		svc, _ := newAnthropicDelegate(t, gw, true)

		_, err := svc.ValidateAnthropicAnalyticsKey(context.Background(),
			&connintv1.ValidateAnthropicAnalyticsKeyRequest{OrgId: "org-1", ApiKey: "k"})
		require.Error(t, err)
		assert.Contains(t, status.Convert(err).Message(), "missing the "+scopeReadMembers)
		assert.NotContains(t, status.Convert(err).Message(), "missing the "+scopeReadAnalytics)
	})

	t.Run("the cost probe names read:analytics", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		expectAnthropicProbes(t, gw, nil, anthropicAuthErr(http.StatusForbidden), "")
		svc, _ := newAnthropicDelegate(t, gw, true)

		_, err := svc.ValidateAnthropicAnalyticsKey(context.Background(),
			&connintv1.ValidateAnthropicAnalyticsKeyRequest{OrgId: "org-1", ApiKey: "k"})
		require.Error(t, err)
		assert.Contains(t, status.Convert(err).Message(), "missing the "+scopeReadAnalytics)
		assert.NotContains(t, status.Convert(err).Message(), "missing the "+scopeReadMembers)
	})
}

// TestValidateAnthropicAnalyticsKey_NoOrganisationIDRefuses pins that the identity is
// required rather than incidental. Storing an empty organisation id would disable the
// reconnect check forever, silently — a later key pointing at a different organisation
// would compare equal to nothing and pass.
func TestValidateAnthropicAnalyticsKey_NoOrganisationIDRefuses(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectAnthropicProbes(t, gw, nil, nil, "")
	svc, _ := newAnthropicDelegate(t, gw, true)

	_, err := svc.ValidateAnthropicAnalyticsKey(context.Background(),
		&connintv1.ValidateAnthropicAnalyticsKeyRequest{OrgId: "org-1", ApiKey: "k"})
	require.Error(t, err)
	// InvalidArgument, not FailedPrecondition: a retry cannot change this outcome, so it
	// must not be styled or worded like a transient one.
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.NotContains(t, status.Convert(err).Message(), "try again")
}

// TestSubmitAnthropicAnalyticsKey_PersistsEnabled covers the save: the connector is
// enabled, the resolved organisation is stored in settings, and the credential is the
// blob that was validated.
func TestSubmitAnthropicAnalyticsKey_PersistsEnabled(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectAnthropicProbes(t, gw, nil, nil, "org_abc")
	svc, dao := newAnthropicDelegate(t, gw, true)

	// The organisation id is patched separately, because the save preserves settings on
	// conflict — see the call site for why that is right for the windows and wrong here.
	dao.EXPECT().PatchSettings(gomock.Any(), "org-1", connectorIDClaudeCode, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, mutate func(json.RawMessage) (json.RawMessage, error)) error {
			// Exercise the mutator against an existing blob carrying a tuned window and a
			// stale organisation: the window must survive and the organisation must move.
			out, err := mutate(json.RawMessage(`{"anthropic_organization_id":"org_stale","lookback_days":30}`))
			require.NoError(t, err)
			var got anthropicconfig.Settings
			require.NoError(t, json.Unmarshal(out, &got))
			assert.Equal(t, "org_abc", got.AnthropicOrganizationID, "the organisation must follow the key")
			assert.Equal(t, 30, got.LookbackDays, "a tuned window must survive a key rotation")
			return nil
		})

	// Re-pasting a key after the previous one was revoked must clear the health state,
	// or the connector keeps presenting as broken even though it now works.
	dao.EXPECT().ResetHealthState(gomock.Any(), "org-1", connectorIDClaudeCode).
		Return(true, nil)
	dao.EXPECT().SaveCredentialsEnablePreservingSettings(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, cfg *sdk.ConnectorConfig) error {
			assert.Equal(t, connectorIDClaudeCode, cfg.ConnectorID)
			assert.Equal(t, "org-1", cfg.OrganizationID)

			var creds anthropicconfig.Credentials
			require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
			assert.Equal(t, "sk-ant-api01-x", creds.APIKey)

			// The organisation id is established at exactly this moment and by nothing
			// else, so it rides along with the first insert.
			var settings anthropicconfig.Settings
			require.NoError(t, json.Unmarshal(cfg.Settings, &settings))
			assert.Equal(t, "org_abc", settings.AnthropicOrganizationID)
			return nil
		})

	_, err := svc.SubmitAnthropicAnalyticsKey(context.Background(),
		&connintv1.SubmitAnthropicAnalyticsKeyRequest{
			OrgId: "org-1", ApiKey: "sk-ant-api01-x", ExpectedAnthropicOrganizationId: "org_abc",
		})
	require.NoError(t, err)
}

// TestSubmitAnthropicAnalyticsKey_TornOrganisationRefused is the guard that matters most
// on this path. A key swapped between confirm and save resolves to a different
// organisation, and saving it would ingest another organisation's spend under this one
// with nothing able to notice afterwards.
//
// The DAO expectation is deliberately absent: a save would fail the test, which is how
// "refused before persisting" is asserted rather than described.
func TestSubmitAnthropicAnalyticsKey_TornOrganisationRefused(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectAnthropicProbes(t, gw, nil, nil, "org_now_different")
	svc, _ := newAnthropicDelegate(t, gw, true)

	_, err := svc.SubmitAnthropicAnalyticsKey(context.Background(),
		&connintv1.SubmitAnthropicAnalyticsKeyRequest{
			OrgId: "org-1", ApiKey: "k", ExpectedAnthropicOrganizationId: "org_confirmed",
		})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "validate again")
}

// TestSubmitAnthropicAnalyticsKey_AbsentExpectationRefuses pins that there is no request
// shape that saves unconfirmed. An empty expectation must not match an empty resolved
// value either — the resolve refuses that case first, but this asserts the save path does
// not quietly accept a request that carries no confirmation at all.
func TestSubmitAnthropicAnalyticsKey_AbsentExpectationRefuses(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectAnthropicProbes(t, gw, nil, nil, "org_abc")
	svc, _ := newAnthropicDelegate(t, gw, true)

	_, err := svc.SubmitAnthropicAnalyticsKey(context.Background(),
		&connintv1.SubmitAnthropicAnalyticsKeyRequest{OrgId: "org-1", ApiKey: "k"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestAnthropicSetup_OpsDisabledRefusesBeforeAnyVendorCall pins the env-wide off switch.
// The gateway mock has no expectations, so a vendor call would fail the test.
func TestAnthropicSetup_OpsDisabledRefusesBeforeAnyVendorCall(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	svc, _ := newAnthropicDelegate(t, gw, false)

	_, err := svc.ValidateAnthropicAnalyticsKey(context.Background(),
		&connintv1.ValidateAnthropicAnalyticsKeyRequest{OrgId: "org-1", ApiKey: "k"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestAnthropicSetup_GatewaySeesTheOrganisation pins that the factory is handed the
// organisation too.
//
// The property that matters is asserted where the limiter actually reads it — on the
// context of each Execute call, via the matcher in expectAnthropicProbes. This covers the
// factory's own context, which a gateway builder could plausibly read; without both, a
// change that set the organisation on only one of the two would pass.
func TestAnthropicSetup_GatewaySeesTheOrganisation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectAnthropicProbes(t, gw, nil, nil, "org_abc")

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	cfg := testConfig()
	cfg.EnabledConnectors[string(connectorIDClaudeCode)] = true

	var sawOrgID string
	factory := sdk.VendorGatewayFactory(func(ctx context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		sawOrgID = reqctx.OrgIDFromContext(ctx)
		return gw, nil
	})
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Registry: testRegistry(), GatewayFactory: factory})

	_, err := svc.ValidateAnthropicAnalyticsKey(context.Background(),
		&connintv1.ValidateAnthropicAnalyticsKeyRequest{OrgId: "org-1", ApiKey: "k"})
	require.NoError(t, err)

	assert.Equal(t, "org-1", sawOrgID,
		"the gateway must see the organisation, or every organisation's validation shares one rate-limit bucket")
}
