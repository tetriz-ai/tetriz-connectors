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

	codexconfig "github.com/tetriz-ai/tetriz-connectors/connectors/codex"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgcodexpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestCodexProbeScopeMatchesTheVendor pins the scope against a literal rather than against
// itself. Every other assertion reads it back through the same constant the code writes,
// so a typo would ship silently — and this is copy an administrator has to match against
// OpenAI's own key-creation screen, where a near-miss is useless.
func TestCodexProbeScopeMatchesTheVendor(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "codex.enterprise.analytics.read", codexProbeScope)
}

// TestClassifyCodexProbeFailure is the slice's central assertion: three inputs can be
// wrong and each must be refused distinguishably.
//
// The workspace case is the one that separates this classifier from both siblings'. Their
// credential implies the tenant, so every failure is about the key. Here the workspace is
// typed by hand, so a 404 must point at the workspace field — an administrator sent to
// re-check the key re-pastes a perfectly good credential and gets the same failure.
func TestClassifyCodexProbeFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		wantCode     codes.Code
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:         "a 404 names the workspace field and clears the key",
			err:          codexVendorErr(http.StatusNotFound),
			wantCode:     codes.InvalidArgument,
			wantContains: []string{"workspace", "the key itself was accepted"},
			// Sending them to re-check the key is the wrong instruction when the key
			// worked and the workspace did not.
			wantAbsent: []string{"copied in full", "scope"},
		},
		{
			name:         "a 403 names the scope and says to recreate rather than to grant",
			err:          codexVendorErr(http.StatusForbidden),
			wantCode:     codes.InvalidArgument,
			wantContains: []string{codexProbeScope, "create a new key"},
			// A key's scopes are fixed at creation, so "grant permission" sends them
			// looking for a setting that does not exist.
			wantAbsent: []string{"grant", "permission"},
		},
		{
			name:         "a 401 is a rejected credential, not a scope or workspace problem",
			err:          codexVendorErr(http.StatusUnauthorized),
			wantCode:     codes.InvalidArgument,
			wantContains: []string{"rejected by OpenAI", "copied in full"},
			wantAbsent:   []string{"create a new key", "scope", "workspace was not found"},
		},
		{
			name: "a rate limit is ResourceExhausted, not Unavailable",
			// The code is the whole point: Unavailable is also an unreachable vendor and a
			// timeout, so a browser cannot tell a throttle from either and must fall back to
			// generic copy. ResourceExhausted means throttled and nothing else, which is what
			// lets the wait interval reach the person who has to wait it out.
			err:          &sdk.SyncError{Kind: sdk.ErrKindRateLimit},
			wantCode:     codes.ResourceExhausted,
			wantContains: []string{"too many validation attempts", "wait a minute"},
			wantAbsent:   []string{"rejected"},
		},
		{
			// The vendor types its path segment as a UUID and answers a malformed one with 400
			// BEFORE it authenticates, so this message must name the workspace's shape and must
			// NOT claim the key was accepted — on this path the key is never read. That
			// ordering is what separates this case from the 404 above, which may say it.
			name: "a 400 carrying the vendor's uuid_parsing marker names the workspace shape",
			// The real body, trimmed. The marker is what attributes the refusal to the
			// workspace; see the unmarked case below for why the status alone is not enough.
			err: codexVendorErrWithBody(http.StatusBadRequest,
				`{"error":{"message":"[{'type': 'uuid_parsing', 'loc': ('path', 'workspace_id'), 'msg': 'Input should be a valid UUID'}]"}}`),
			wantCode:     codes.InvalidArgument,
			wantContains: []string{"workspace", "uuid"},
			wantAbsent: []string{
				"the key itself was accepted", "rejected by OpenAI", "copied in full", "scope",
				// "could not reach" would be false here: the request reached OpenAI and OpenAI
				// answered with a classified refusal.
				"could not reach",
			},
		},
		{
			// The case that keeps the branch above honest. ValidWorkspaceID has already accepted
			// this workspace as a canonical UUID, and every OTHER field in the probe request —
			// the window bounds, the page limit — is our own constant. So a 400 without the
			// vendor's workspace marker is far more likely OUR bug than the administrator's
			// input, and telling them to re-copy a workspace ID that was already valid sends
			// them to fix something that is not broken.
			name:     "a 400 without the marker names no field, and does not offer a retry",
			err:      codexVendorErrWithBody(http.StatusBadRequest, `{"error":{"message":"limit must be <= 100"}}`),
			wantCode: codes.InvalidArgument,
			// It must not send anyone to retry: a 400 is deterministic, so the transient copy
			// would promise an outcome that never arrives.
			wantContains: []string{"did not say which value", "retrying will not"},
			wantAbsent: []string{
				"is a UUID", "copied in full", "scope", "could not reach",
				"try again in a moment",
			},
		},
		{
			// The classifier keeps the vendor's error message field of a JSON body (cut to 200
			// bytes) whatever the body's size, and drops every other field, so a marked refusal
			// with a large body still carries its marker and still names the workspace shape.
			name: "a marked refusal with a large body still names the workspace shape",
			err: codexVendorErrWithBody(http.StatusBadRequest,
				`{"error":{"message":"[{'type': 'uuid_parsing', 'loc': ('path', 'workspace_id')}]","pad":"`+
					strings.Repeat("x", 2100)+`"}}`),
			wantCode:     codes.InvalidArgument,
			wantContains: []string{"workspace", "uuid"},
			wantAbsent:   []string{"try again in a moment", "could not reach"},
		},
		{
			name:     "an unreachable vendor must not be reported as a bad key",
			err:      errors.New("dial tcp: i/o timeout"),
			wantCode: codes.Unavailable,
			// Deliberately NOT "could not reach OpenAI". This branch is every unclassified
			// failure, and most of them DID reach OpenAI — an unmapped status lands here too.
			// Asserting a transport failure points the reader at the network when the cause is
			// a response we did not expect.
			wantContains: []string{"did not give an answer we could act on"},
			wantAbsent:   []string{"rejected", "scope", "workspace was not found", "could not reach"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			code, msg := classifyCodexProbeFailure(context.Background(), probeOrgID, tt.err)
			assert.Equal(t, tt.wantCode, code)
			for _, want := range tt.wantContains {
				assert.Containsf(t, strings.ToLower(msg), strings.ToLower(want), "message must mention %q", want)
			}
			for _, absent := range tt.wantAbsent {
				assert.NotContainsf(t, strings.ToLower(msg), strings.ToLower(absent),
					"message must not say %q for this failure", absent)
			}
		})
	}
}

// TestCodexRefusalsAreMutuallyDistinguishable is the property the three messages exist for,
// asserted directly rather than inferred from three separate substring checks. Two failures
// that happened to render the same string would satisfy every test above and still leave an
// administrator unable to tell which of their three inputs was wrong.
func TestCodexRefusalsAreMutuallyDistinguishable(t *testing.T) {
	t.Parallel()

	seen := map[string]int{}
	for _, httpStatus := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusUnauthorized} {
		_, msg := classifyCodexProbeFailure(context.Background(), probeOrgID, codexVendorErr(httpStatus))
		seen[msg] = httpStatus
	}
	assert.Len(t, seen, 3, "the key, the scope and the workspace must each get their own message")
}

// --- probe wiring, through a mocked gateway ---

const codexWorkspaceID = "00000000-0000-4000-8000-00000000c0de"

// codexUsageJSON is a probe page whose rows exercise every case the roster fold has to get
// right. The third and fourth rows are the discriminating pair: the SAME developer, absent
// address first and present later. A fixture that repeated one address on both of a
// developer's days would pass whether or not the fold short-circuits after the first
// sighting — and short-circuiting is exactly how a colleague gets counted and never named.
const codexUsageJSON = `[
  {"user_id":"u_1","actor":{"email":"dev@example.com"},"bucket_start_time":1780272000},
  {"user_id":"u_1","actor":{"email":"dev@example.com"},"bucket_start_time":1780358400},
  {"user_id":"u_2","actor":{"email":null},"bucket_start_time":1780272000},
  {"user_id":"u_2","actor":{"email":"late@example.com"},"bucket_start_time":1780358400},
  {"user_id":"u_3","actor":{"email":null},"bucket_start_time":1780272000},
  {"bucket_start_time":1780272000}
]`

func codexUsageResponse(t *testing.T, hasMore bool) *vgcodexpb.CodexUsageResponse {
	t.Helper()
	var raw []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(codexUsageJSON), &raw))
	resp := &vgcodexpb.CodexUsageResponse{HasMore: hasMore}
	for _, r := range raw {
		resp.UsageRows = append(resp.UsageRows, r)
	}
	return resp
}

// codexVendorErr builds the SyncError the framework's own classifier produces for a
// status, rather than one shaped to suit the code under test.
//
// This is the difference between a test and a tautology. A fixture that stamped
// ErrKindAuth on every status would keep passing if the 404 branch were moved below the
// non-auth bail — while in real use every wrong workspace would degrade to "could not reach
// OpenAI", the exact misroute this classifier exists to prevent.
func codexVendorErr(httpStatus int) error {
	return vendorapi.ClassifyHTTPErrorWithHeaders(httpStatus, []byte("vendor said no"), nil)
}

// codexVendorErrWithBody is the same production classifier over a chosen response body, for the
// one branch that reads the body rather than the status: a 400 is attributed to the workspace
// only when the vendor's own uuid_parsing marker is in it.
func codexVendorErrWithBody(httpStatus int, body string) error {
	return vendorapi.ClassifyHTTPErrorWithHeaders(httpStatus, []byte(body), nil)
}

func newCodexDelegate(
	t *testing.T, gw sdk.VendorGateway, enabled bool,
) (*Service, *daomocks.MockIConnectorConfigDAO) {
	t.Helper()
	ctrl := gomock.NewController(t)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	cfg := testConfig()
	cfg.EnabledConnectors[string(connectorIDCodex)] = enabled
	factory := sdk.VendorGatewayFactory(func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		return gw, nil
	})
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Registry: testRegistry(), GatewayFactory: factory})
	return svc, dao
}

// expectCodexProbe sets the single usage call. The context matcher is the point: the probe
// must carry the organisation, because that is what buckets the rate limiter. Accepting any
// context would let a change that only set the organisation on the factory's context pass,
// while in real use every organisation's connect would share one bucket.
func expectCodexProbe(t *testing.T, gw *fwmocks.MockVendorGateway, resp *vgcodexpb.CodexUsageResponse, err error) {
	t.Helper()
	gw.EXPECT().ForGroup(constants.GroupCodexAnalytics).Return(gw).AnyTimes()
	gw.EXPECT().
		Execute(orgCtx(probeOrgID), gomock.AssignableToTypeOf(&vgcodexpb.CodexUsageRequest{})).
		Return(proto.Message(resp), err).AnyTimes()
}

// TestValidateCodexAnalyticsKey resolves the roster without persisting, and folds the
// window's rows into distinct developers.
//
// Three developers, two addresses. Asserting the pair together is what catches a fold that
// counted rows (six) or counted addresses (two) — and the second address is there only to
// catch a fold that stops looking at a developer after their first row, which would drop a
// colleague whose address the vendor filled in later in the window.
func TestValidateCodexAnalyticsKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectCodexProbe(t, gw, codexUsageResponse(t, false), nil)
	svc, _ := newCodexDelegate(t, gw, true)

	resp, err := svc.ValidateCodexAnalyticsKey(context.Background(), &connintv1.ValidateCodexAnalyticsKeyRequest{
		OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: codexWorkspaceID,
	})
	require.NoError(t, err)
	assert.Equal(t, codexWorkspaceID, resp.GetWorkspaceId())
	assert.Equal(t, []string{"dev@example.com", "late@example.com"}, resp.GetDeveloperEmails(),
		"an address the vendor filled in on a later day must still reach the confirmation step")
	assert.EqualValues(t, 3, resp.GetDeveloperCount(),
		"three distinct developers: a repeated day must not count twice, a developer the vendor never named must still count, and an unattributable row must count for nobody")
	assert.GreaterOrEqual(t, int(resp.GetDeveloperCount()), len(resp.GetDeveloperEmails()),
		"the count must never be smaller than the list, or the confirmation shows more colleagues than it claims developers")
	assert.False(t, resp.GetDevelopersTruncated())
}

// TestCodexValidateMergesADevelopersTwoIdentities. Both row fields are optional, so one
// person can appear on Monday with only an address and on Friday with an identifier too.
// Counting in one pass files Monday under the address and Friday under the identifier — one
// colleague reported as two, on a perfectly valid vendor response, overstating the workspace
// on the one screen an administrator is asked to trust.
func TestCodexValidateMergesADevelopersTwoIdentities(t *testing.T) {
	t.Parallel()

	// Deliberately ordered so the address-only row comes FIRST: a single pass would already
	// have committed it as its own identity by the time the identifier arrives.
	page := `[
	  {"actor":{"email":"same@example.com"},"bucket_start_time":1780272000},
	  {"user_id":"u_1","actor":{"email":"same@example.com"},"bucket_start_time":1780358400}
	]`
	var raw []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(page), &raw))
	resp := &vgcodexpb.CodexUsageResponse{}
	for _, r := range raw {
		resp.UsageRows = append(resp.UsageRows, r)
	}

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectCodexProbe(t, gw, resp, nil)
	svc, _ := newCodexDelegate(t, gw, true)

	got, err := svc.ValidateCodexAnalyticsKey(context.Background(), &connintv1.ValidateCodexAnalyticsKeyRequest{
		OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: codexWorkspaceID,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetDeveloperCount(),
		"one person seen with and without an identifier is one developer, not two")
	assert.Equal(t, []string{"same@example.com"}, got.GetDeveloperEmails())
}

// TestCodexProbeHonoursTheVendorsRetryAfter. A throttle measured in hours told as "wait a
// minute" sends an administrator straight back into it, and each retry renews the window.
func TestCodexProbeHonoursTheVendorsRetryAfter(t *testing.T) {
	t.Parallel()

	codeWithHint, withHint := classifyCodexProbeFailure(context.Background(), probeOrgID,
		&sdk.SyncError{Kind: sdk.ErrKindRateLimit, RetryAfterSeconds: 3600})
	assert.Equal(t, codes.ResourceExhausted, codeWithHint,
		"a throttle must be distinguishable from an unreachable vendor, or the interval cannot be shown")
	assert.Contains(t, withHint, "1h0m0s")
	assert.NotContains(t, withHint, "wait a minute")

	_, noHint := classifyCodexProbeFailure(context.Background(), probeOrgID,
		&sdk.SyncError{Kind: sdk.ErrKindRateLimit})
	assert.Contains(t, noHint, "wait a minute", "with no vendor guidance the fallback still applies")
}

// TestCodexValidateDegradesToNoActivity is the branch the confirmation copy depends on. A
// quiet window and a wrong workspace both produce an empty roster, so the response must
// come back successful and empty rather than as an error — a host's UI tells those two
// apart, and it cannot if this path refuses.
func TestCodexValidateDegradesToNoActivity(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectCodexProbe(t, gw, &vgcodexpb.CodexUsageResponse{}, nil)
	svc, _ := newCodexDelegate(t, gw, true)

	resp, err := svc.ValidateCodexAnalyticsKey(context.Background(), &connintv1.ValidateCodexAnalyticsKeyRequest{
		OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: codexWorkspaceID,
	})
	require.NoError(t, err, "an idle workspace is a valid answer, not a refusal")
	assert.Empty(t, resp.GetDeveloperEmails())
	assert.EqualValues(t, 0, resp.GetDeveloperCount())
}

// TestCodexValidateFlagsATruncatedRoster covers both ways the probe learns it saw a sample.
// Reading has_more alone would call a full page complete whenever the vendor happens not to
// set it, and the confirmation would render a count that reads as the workspace's size.
func TestCodexValidateFlagsATruncatedRoster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		resp    *vgcodexpb.CodexUsageResponse
		wantTrn bool
	}{
		{name: "the vendor said there is more", resp: &vgcodexpb.CodexUsageResponse{HasMore: true}, wantTrn: true},
		{name: "the page filled even though the vendor said nothing", resp: codexFullPage(), wantTrn: true},
		{name: "a short page with no more is the whole window", resp: &vgcodexpb.CodexUsageResponse{}, wantTrn: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			gw := fwmocks.NewMockVendorGateway(ctrl)
			expectCodexProbe(t, gw, tt.resp, nil)
			svc, _ := newCodexDelegate(t, gw, true)

			resp, err := svc.ValidateCodexAnalyticsKey(context.Background(), &connintv1.ValidateCodexAnalyticsKeyRequest{
				OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: codexWorkspaceID,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantTrn, resp.GetDevelopersTruncated())
		})
	}
}

func codexFullPage() *vgcodexpb.CodexUsageResponse {
	resp := &vgcodexpb.CodexUsageResponse{}
	for i := 0; i < codexProbePageSize; i++ {
		resp.UsageRows = append(resp.UsageRows, []byte(`{"user_id":"u_x"}`))
	}
	return resp
}

// TestCodexRejectsAnUnkeyableWorkspace pins WHERE a bad workspace is refused, not which
// character makes it bad.
//
// The record-key invariant holds by construction: the identifier
// must be a UUID, and a UUID cannot contain the key delimiter. So none of the values below
// reaches the delimiter check — each one fails as "not a UUID" first, and a
// value that is both a valid UUID and unkeyable cannot be written down.
//
// What still needs asserting is the timing. The refusal has to land BEFORE any vendor call
// and before anything is stored: a workspace rejected after a probe costs a request and reads
// to the administrator as a vendor failure, and one rejected after the write leaves an
// enabled connector that dies on every scheduled run — connected, never syncing. The
// Times(0) expectation below is the real assertion here.
func TestCodexRejectsAnUnkeyableWorkspace(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{
		"ws_a" + codexconfig.RecordKeyDelimiter + "b",
		"ws_a/../admin",
		" ws_a",
		"ws_a?limit=1",
		"",
	} {
		t.Run(bad, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			gw := fwmocks.NewMockVendorGateway(ctrl)
			// No probe is expected: the refusal must land before any vendor call, or a
			// bad workspace still costs a request and still reads as a vendor failure.
			gw.EXPECT().ForGroup(gomock.Any()).Times(0)
			svc, dao := newCodexDelegate(t, gw, true)
			dao.EXPECT().SaveCredentialsEnablePreservingSettings(gomock.Any(), gomock.Any()).Times(0)

			_, err := svc.SubmitCodexAnalyticsKey(context.Background(), &connintv1.SubmitCodexAnalyticsKeyRequest{
				OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: bad,
			})
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			// The code alone is not the claim: a rejected key and an unknown workspace
			// both return InvalidArgument. What an administrator needs is to be sent to
			// the right field.
			assert.Contains(t, strings.ToLower(status.Convert(err).Message()), "workspace")
		})
	}
}

// TestCodexSubmitWritesCredentialAndWorkspaceTogether is the save-path proof.
//
// One row-locked write, not two. Composed writes leave a window two overlapping submits can
// interleave through — A patches its workspace, B patches its workspace, B saves its key,
// A saves its key preserving B's workspace — ending with A's credential against B's
// workspace while both administrators are told they succeeded. Since the scheduler uses the
// stored workspace as the request path, that pair quietly ingests another workspace's usage
// under this tenant.
//
// It also asserts the mutator merges rather than replaces: a tuned lookback survives, and so
// does a settings key this package does not own.
func TestCodexSubmitWritesCredentialAndWorkspaceTogether(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectCodexProbe(t, gw, codexUsageResponse(t, false), nil)
	svc, dao := newCodexDelegate(t, gw, true)

	// A previous connect: a tuned lookback that must survive, a stale workspace that must
	// not, and a key owned by something else entirely that must be left alone.
	prior := json.RawMessage(`{"workspace_id":"5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a81","lookback_days":30,"observed_rate_limit":7}`)
	var (
		saved   *sdk.ConnectorConfig
		patched map[string]any
	)
	// Any composed alternative would show up here: these two must NOT be called.
	dao.EXPECT().SaveCredentialsEnablePreservingSettings(gomock.Any(), gomock.Any()).Times(0)
	dao.EXPECT().PatchSettings(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, cfg *sdk.ConnectorConfig,
			mutate func(json.RawMessage) (json.RawMessage, error),
		) error {
			saved = cfg
			out, err := mutate(prior)
			if err != nil {
				return err
			}
			return json.Unmarshal(out, &patched)
		})
	dao.EXPECT().ResetHealthState(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	_, err := svc.SubmitCodexAnalyticsKey(context.Background(), &connintv1.SubmitCodexAnalyticsKeyRequest{
		OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: codexWorkspaceID,
	})
	require.NoError(t, err)

	require.NotNil(t, saved)
	assert.Equal(t, connectorIDCodex, saved.ConnectorID)
	creds, err := codexconfig.ParseCredentials(saved)
	require.NoError(t, err)
	assert.Equal(t, "sk-codex", creds.APIKey)

	assert.Equal(t, codexWorkspaceID, patched["workspace_id"],
		"the reconnect must re-point the connector, not keep the stored workspace")
	assert.EqualValues(t, 30, patched["lookback_days"],
		"the mutator must merge into stored settings — the tuned lookback is what preserving exists for")
	assert.EqualValues(t, 7, patched["observed_rate_limit"],
		"a settings key this package does not know about must survive the round trip, not be narrowed away")
}

// TestCodexSubmitFirstConnectBuildsSettingsFromScratch. On a first connect the DAO hands the
// mutator nil, and it must produce the workspace rather than inherit anything.
func TestCodexSubmitFirstConnectBuildsSettingsFromScratch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectCodexProbe(t, gw, codexUsageResponse(t, false), nil)
	svc, dao := newCodexDelegate(t, gw, true)

	var patched map[string]any
	dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *sdk.ConnectorConfig,
			mutate func(json.RawMessage) (json.RawMessage, error),
		) error {
			out, err := mutate(nil)
			if err != nil {
				return err
			}
			return json.Unmarshal(out, &patched)
		})
	dao.EXPECT().ResetHealthState(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	_, err := svc.SubmitCodexAnalyticsKey(context.Background(), &connintv1.SubmitCodexAnalyticsKeyRequest{
		OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: codexWorkspaceID,
	})
	require.NoError(t, err)
	assert.Equal(t, codexWorkspaceID, patched["workspace_id"])
}

// TestCodexSubmitRefusesSettingsItCannotRead covers the two shapes a stored settings column
// can take that this package must not write over.
//
// The JSON-null row is the sharp one: the column is `jsonb NOT NULL`, which admits the
// literal `null`, and unmarshalling that into a map SUCCEEDS while leaving the map nil.
// Assigning into it panics instead of returning an error — so this asserts an error, and it
// would have caught a panic reaching the RPC.
func TestCodexSubmitRefusesSettingsItCannotRead(t *testing.T) {
	t.Parallel()

	for name, stored := range map[string]string{
		"a JSON null settings column":     `null`,
		"settings that are not an object": `"a string"`,
		"settings that do not parse":      `{`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			gw := fwmocks.NewMockVendorGateway(ctrl)
			expectCodexProbe(t, gw, codexUsageResponse(t, false), nil)
			svc, dao := newCodexDelegate(t, gw, true)

			dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ *sdk.ConnectorConfig,
					mutate func(json.RawMessage) (json.RawMessage, error),
				) error {
					_, err := mutate(json.RawMessage(stored))
					return err
				})
			dao.EXPECT().ResetHealthState(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			_, err := svc.SubmitCodexAnalyticsKey(context.Background(), &connintv1.SubmitCodexAnalyticsKeyRequest{
				OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: codexWorkspaceID,
			})
			require.Error(t, err, "settings we cannot read must fail cleanly, never panic and never be written over")
			assert.Equal(t, codes.Internal, status.Code(err))
		})
	}
}

// TestCodexSubmitRefusesWhenOpsDisabled proves the env-wide switch is checked in this
// service rather than trusted from the caller, and that it lands before anything is stored.
func TestCodexSubmitRefusesWhenOpsDisabled(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Times(0)
	svc, dao := newCodexDelegate(t, gw, false)
	dao.EXPECT().SaveCredentialsEnablePreservingSettings(gomock.Any(), gomock.Any()).Times(0)

	_, err := svc.SubmitCodexAnalyticsKey(context.Background(), &connintv1.SubmitCodexAnalyticsKeyRequest{
		OrgId: probeOrgID, ApiKey: "sk-codex", WorkspaceId: codexWorkspaceID,
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}
