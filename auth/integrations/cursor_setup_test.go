// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	cursorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/cursor"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// membersJSON is the vendor's own shape, captured live: an entry may carry an empty
// name, and the role vocabulary includes values the docs do not list.
const membersJSON = `[
  {"name":"Dev One","email":"dev@example.com","id":"user_A","role":"owner"},
  {"name":"","email":"svc@example.com","id":"user_B","role":"member"}
]`

func cursorMembersResponse(t *testing.T) *vgpb.CursorListTeamMembersResponse {
	t.Helper()
	var raw []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(membersJSON), &raw))
	resp := &vgpb.CursorListTeamMembersResponse{}
	for _, m := range raw {
		resp.TeamMembers = append(resp.TeamMembers, m)
	}
	return resp
}

// newCursorDelegate wires a Service with a DAO mock and a gateway factory returning
// the supplied gateway. No mgmt mock: the Cursor path resolves no tenant type, which
// is itself worth pinning — an unexpected tenant directory call would fail the test.
func newCursorDelegate(
	t *testing.T, gw sdk.VendorGateway, gwErr error, enabled bool,
) (*Service, *daomocks.MockIConnectorConfigDAO) {
	t.Helper()
	ctrl := gomock.NewController(t)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	cfg := testConfig()
	cfg.EnabledConnectors["cursor"] = enabled
	factory := sdk.VendorGatewayFactory(func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		return gw, gwErr
	})
	svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Registry: testRegistry(), GatewayFactory: factory})
	return svc, dao
}

// expectBothProbes sets the members call then the usage call, in that order, because
// the sequence is the design: the cheap members call establishes the credential
// authenticates before the usage call tests the privilege that matters.
//
// Each probe's rate-limit group is asserted rather than accepted as any: the two
// endpoints carry different published caps (20/min on the Admin API against 60/min on
// usage events), and both point at the same host, so routing one through the other's
// group would not break a URL — it would silently spend the wrong budget.
func expectBothProbes(t *testing.T, gw *fwmocks.MockVendorGateway, membersErr, usageErr error) {
	t.Helper()

	gw.EXPECT().ForGroup(constants.GroupCursorAdmin).Return(gw)
	members := gw.EXPECT().
		Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorListTeamMembersRequest{})).
		Return(proto.Message(cursorMembersResponse(t)), membersErr)
	if membersErr != nil {
		return
	}
	gw.EXPECT().ForGroup(constants.GroupCursorUsage).Return(gw).After(members)
	gw.EXPECT().
		Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorFilteredUsageEventsRequest{})).
		Return(proto.Message(&vgpb.CursorFilteredUsageEventsResponse{}), usageErr).
		After(members)
}

func authErr() error {
	return &sdk.SyncError{Kind: sdk.ErrKindAuth, Cause: errors.New("401 unauthorized")}
}

func TestValidateCursorTeamKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		orgID      string
		teamKey    string
		opsEnabled bool
		membersErr error
		usageErr   error
		wantErr    bool
		wantCode   codes.Code
	}{
		{name: "valid key resolves its members", orgID: testOrgID, teamKey: "crsr_ok", opsEnabled: true},
		{name: "missing org", orgID: "", teamKey: "crsr_ok", opsEnabled: true, wantErr: true, wantCode: codes.InvalidArgument},
		{name: "missing key", orgID: testOrgID, teamKey: "", opsEnabled: true, wantErr: true, wantCode: codes.InvalidArgument},
		{
			// The env-wide off switch, checked in the connector service rather than
			// trusted from the caller, so a direct gRPC caller is rejected too.
			name:  "ops switch off refuses before any vendor call",
			orgID: testOrgID, teamKey: "crsr_ok", opsEnabled: false,
			wantErr: true, wantCode: codes.FailedPrecondition,
		},
		{
			name:  "key Cursor rejects is invalid-argument",
			orgID: testOrgID, teamKey: "crsr_bad", opsEnabled: true, membersErr: authErr(),
			wantErr: true, wantCode: codes.InvalidArgument,
		},
		{
			// The assertion the second probe exists for: members succeed, usage does
			// not. Refused at entry rather than hours later on first sync.
			name:  "key that lists members but cannot read usage events is refused",
			orgID: testOrgID, teamKey: "crsr_weak", opsEnabled: true, usageErr: authErr(),
			wantErr: true, wantCode: codes.InvalidArgument,
		},
		{
			// A rate limit or outage must not tell an administrator their valid key is
			// bad — the difference between InvalidArgument and Unavailable is the
			// difference between "fix your key" and "try again".
			name:  "transient vendor failure is unavailable, not invalid",
			orgID: testOrgID, teamKey: "crsr_ok", opsEnabled: true,
			membersErr: errors.New("429 too many requests"),
			wantErr:    true, wantCode: codes.Unavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			gw := fwmocks.NewMockVendorGateway(ctrl)

			// Expectations are set only when the call is actually reachable, so
			// gomock's strict counting asserts the guards short-circuit: a missing
			// expectation for an unreached call is the point.
			fieldsOK := tt.orgID != "" && tt.teamKey != ""
			if fieldsOK && tt.opsEnabled {
				expectBothProbes(t, gw, tt.membersErr, tt.usageErr)
			}

			// No Save expectation anywhere in this table — validate must persist
			// nothing, and a persist would fail the test.
			svc, _ := newCursorDelegate(t, gw, nil, tt.opsEnabled)

			resp, err := svc.ValidateCursorTeamKey(context.Background(), &connintv1.ValidateCursorTeamKeyRequest{
				OrgId: tt.orgID, TeamKey: tt.teamKey,
			})
			if tt.wantErr {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, tt.wantCode, st.Code())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []string{"dev@example.com", "svc@example.com"}, resp.GetMemberEmails())
			assert.Equal(t, int32(2), resp.GetMemberCount())
		})
	}
}

// TestValidateCursorTeamKey_MemberWithoutEmailIsCountedNotListed pins the divergence
// between the list and the count. Deriving the count from the list would understate
// the team an administrator is asked to recognise, which is the one check available.
func TestValidateCursorTeamKey_MemberWithoutEmailIsCountedNotListed(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(constants.GroupCursorAdmin).Return(gw)
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorListTeamMembersRequest{})).
		Return(proto.Message(&vgpb.CursorListTeamMembersResponse{TeamMembers: [][]byte{
			[]byte(`{"email":"dev@example.com"}`),
			[]byte(`{"id":"user_no_email"}`),
		}}), nil)
	gw.EXPECT().ForGroup(constants.GroupCursorUsage).Return(gw)
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorFilteredUsageEventsRequest{})).
		Return(proto.Message(&vgpb.CursorFilteredUsageEventsResponse{}), nil)

	svc, _ := newCursorDelegate(t, gw, nil, true)
	resp, err := svc.ValidateCursorTeamKey(context.Background(), &connintv1.ValidateCursorTeamKeyRequest{
		OrgId: testOrgID, TeamKey: "crsr_ok",
	})
	require.NoError(t, err)
	assert.Len(t, resp.GetMemberEmails(), 1, "only the member with an address is listed")
	assert.Equal(t, int32(2), resp.GetMemberCount(), "both members are counted")
}

// fixtureFingerprint is the fingerprint of the two-member fixture roster, derived the
// same way the handler derives it. Computed rather than hardcoded so the test asserts
// the *binding* — save accepts only the roster validate returned — and not a
// particular digest, which would fail on any change to the hash and teach nothing.
func fixtureFingerprint(t *testing.T) string {
	t.Helper()
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectBothProbes(t, gw, nil, nil)
	svc, _ := newCursorDelegate(t, gw, nil, true)
	resp, err := svc.ValidateCursorTeamKey(context.Background(), &connintv1.ValidateCursorTeamKeyRequest{
		OrgId: testOrgID, TeamKey: "crsr_ok",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetTeamFingerprint())
	return resp.GetTeamFingerprint()
}

// TestSubmitCursorTeamKey_PersistsEnabled asserts the whole save contract by
// inspecting the persisted struct, including the credential blob's contents.
func TestSubmitCursorTeamKey_PersistsEnabled(t *testing.T) {
	t.Parallel()
	fp := fixtureFingerprint(t)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectBothProbes(t, gw, nil, nil)

	svc, dao := newCursorDelegate(t, gw, nil, true)
	// SaveCredentialsEnablePreservingSettings, not Save: it omits settings and operation_overrides on
	// conflict, so a key rotation cannot lose a value this path does not own. No Get
	// expectation either — the DAO preserves by omission, so there is no read to race.
	dao.EXPECT().SaveCredentialsEnablePreservingSettings(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, cfg *sdk.ConnectorConfig) error {
			assert.Equal(t, sdk.ConnectorID("cursor"), cfg.ConnectorID)
			assert.Equal(t, testOrgID, cfg.OrganizationID)
			var creds cursorconfig.CursorCredentials
			require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
			assert.Equal(t, "crsr_ok", creds.APIKey)
			// Settings seed a first insert only, so they must be the defaults rather
			// than anything read back from an existing row.
			assert.JSONEq(t, `{}`, string(cfg.Settings))
			return nil
		})
	// Without this the connector stays presenting as broken after a re-paste.
	dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("cursor")).Return(true, nil)

	_, err := svc.SubmitCursorTeamKey(context.Background(), &connintv1.SubmitCursorTeamKeyRequest{
		OrgId: testOrgID, TeamKey: "crsr_ok", ExpectedTeamFingerprint: fp,
	})
	require.NoError(t, err)
}

// TestSubmitCursorTeamKey_TornTeamRefused refuses a save whose confirmed fingerprint
// no longer matches the re-resolved roster — the torn-state guard in
// SubmitCursorTeamKey states why that matters.
func TestSubmitCursorTeamKey_TornTeamRefused(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectBothProbes(t, gw, nil, nil)

	// No Save expectation: refusing before persisting is the assertion.
	svc, _ := newCursorDelegate(t, gw, nil, true)

	_, err := svc.SubmitCursorTeamKey(context.Background(), &connintv1.SubmitCursorTeamKeyRequest{
		OrgId: testOrgID, TeamKey: "crsr_ok", ExpectedTeamFingerprint: "some-other-team",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

// TestSubmitCursorTeamKey_SameSizedDifferentTeamRefused drives the case a member
// count cannot see: a different roster of the same size. It is what makes the guard's
// binding to identity checkable rather than asserted.
func TestSubmitCursorTeamKey_SameSizedDifferentTeamRefused(t *testing.T) {
	t.Parallel()
	fp := fixtureFingerprint(t)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	// A different two-member roster: same count, different people.
	gw.EXPECT().ForGroup(constants.GroupCursorAdmin).Return(gw)
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorListTeamMembersRequest{})).
		Return(proto.Message(&vgpb.CursorListTeamMembersResponse{TeamMembers: [][]byte{
			[]byte(`{"id":"user_X","email":"other1@example.com"}`),
			[]byte(`{"id":"user_Y","email":"other2@example.com"}`),
		}}), nil)
	gw.EXPECT().ForGroup(constants.GroupCursorUsage).Return(gw)
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorFilteredUsageEventsRequest{})).
		Return(proto.Message(&vgpb.CursorFilteredUsageEventsResponse{}), nil)

	// No Save expectation: the swap must be refused before persisting.
	svc, _ := newCursorDelegate(t, gw, nil, true)

	_, err := svc.SubmitCursorTeamKey(context.Background(), &connintv1.SubmitCursorTeamKeyRequest{
		OrgId: testOrgID, TeamKey: "crsr_swapped", ExpectedTeamFingerprint: fp,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

// TestSubmitCursorTeamKey_AbsentExpectationRefuses pins that the expectation is
// required: there is no shape of request that saves a team nobody confirmed.
func TestSubmitCursorTeamKey_AbsentExpectationRefuses(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	expectBothProbes(t, gw, nil, nil)

	// No Save expectation: an unconfirmed save must not reach the DAO.
	svc, _ := newCursorDelegate(t, gw, nil, true)

	_, err := svc.SubmitCursorTeamKey(context.Background(), &connintv1.SubmitCursorTeamKeyRequest{
		OrgId: testOrgID, TeamKey: "crsr_ok",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

// TestCursorTeamFingerprint pins the properties the guard rests on: the vendor promises
// no ordering, so a reordered roster is the same team, and a roster that gains or loses a
// member is not.
func TestCursorTeamFingerprint(t *testing.T) {
	t.Parallel()

	base := cursorTeamFingerprint([]string{"user_A", "user_B", "user_C"})
	assert.Equal(t, base, cursorTeamFingerprint([]string{"user_C", "user_A", "user_B"}),
		"vendor ordering must not change the fingerprint")
	assert.NotEqual(t, base, cursorTeamFingerprint([]string{"user_A", "user_B"}),
		"losing a member must change it")
	assert.NotEqual(t, base, cursorTeamFingerprint([]string{"user_A", "user_B", "user_C", "user_D"}),
		"gaining a member must change it")

	// The length prefix earns its place here: without it these two rosters would
	// concatenate to the same bytes and hash alike.
	assert.NotEqual(t, cursorTeamFingerprint([]string{"ab", "c"}), cursorTeamFingerprint([]string{"a", "bc"}),
		"adjacent identities must not be able to merge into one another")

	// An entry with neither id nor address still contributes, so a roster gaining one
	// is not mistaken for the roster without it.
	assert.NotEqual(t, cursorTeamFingerprint([]string{"user_A"}), cursorTeamFingerprint([]string{"user_A", ""}),
		"an anonymous member still counts")

	// The case the digest cannot pin, and the reason the save refuses it outright: an
	// empty roster carries no distinguishing information, so every empty team hashes
	// alike. TestSubmitCursorTeamKey_UnidentifiableRosterRefused is the guard that makes this safe.
	assert.Equal(t, cursorTeamFingerprint(nil), cursorTeamFingerprint([]string{}),
		"an empty roster is one value, which is why a save may not rely on it")
}

// TestSubmitCursorTeamKey_UnidentifiableRosterRefused covers what stands in for the
// digest wherever the digest cannot distinguish one roster from another of the same size.
// An empty team is the obvious case; a team whose members all arrive without an id or an
// address is the same problem one step along, and the members probe treats that entry
// shape as ordinary — so both must be refused, or a key resolving to a *different* such
// roster would match the confirmed fingerprint and be saved.
func TestSubmitCursorTeamKey_UnidentifiableRosterRefused(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		members [][]byte
	}{
		{name: "no members at all", members: nil},
		{
			// The vendor returning a member with neither field is ordinary — a pending
			// invite, for instance. Two such rosters of equal size are indistinguishable
			// to the digest, so neither may be saved.
			name: "members carrying neither an id nor an address",
			members: [][]byte{
				[]byte(`{"name":"","role":"member"}`),
				[]byte(`{"name":"","role":"member"}`),
			},
		},
		{
			// A vendor type change makes every entry unparseable at once, which counts
			// members the digest never sees. The refusal reads the identities rather
			// than the count precisely so this cannot slip past as a non-empty roster.
			name: "every member unparseable, so the count and the identities disagree",
			members: [][]byte{
				[]byte(`{"id":123,"email":456}`),
				[]byte(`{"id":789,"email":101}`),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			gw := fwmocks.NewMockVendorGateway(ctrl)
			gw.EXPECT().ForGroup(constants.GroupCursorAdmin).Return(gw)
			gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorListTeamMembersRequest{})).
				Return(proto.Message(&vgpb.CursorListTeamMembersResponse{TeamMembers: tt.members}), nil)
			gw.EXPECT().ForGroup(constants.GroupCursorUsage).Return(gw)
			gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorFilteredUsageEventsRequest{})).
				Return(proto.Message(&vgpb.CursorFilteredUsageEventsResponse{}), nil)

			// No persist expectation: the refusal must come before the DAO.
			svc, _ := newCursorDelegate(t, gw, nil, true)

			// The fingerprint the client would legitimately echo for this roster. The
			// refusal must fire even though the digests agree — that agreement is the
			// problem, not the evidence.
			identities := make([]string, len(tt.members))
			_, err := svc.SubmitCursorTeamKey(context.Background(), &connintv1.SubmitCursorTeamKeyRequest{
				OrgId:                   testOrgID,
				TeamKey:                 "crsr_anon",
				ExpectedTeamFingerprint: cursorTeamFingerprint(identities),
			})
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.FailedPrecondition, st.Code())
		})
	}
}

// TestCursorTeamFingerprint_AnonymousRostersCollide pins the fact the refusal exists for:
// a roster carrying no identity hashes by its size alone. That two different teams of
// that shape are indistinguishable cannot be asserted directly — the identities are the
// whole input — which is exactly why the save refuses rather than compares.
func TestCursorTeamFingerprint_AnonymousRostersCollide(t *testing.T) {
	t.Parallel()
	assert.Equal(t, cursorTeamFingerprint(nil), cursorTeamFingerprint([]string{}),
		"an empty roster is one value however it is spelled")
	// And the size still separates them, which is why the refusal is about identity
	// rather than about the count.
	assert.NotEqual(t, cursorTeamFingerprint([]string{""}), cursorTeamFingerprint([]string{"", ""}),
		"a different number of anonymous members is a different digest")
}

// TestValidateCursorTeamKey_EmptyTeamStillResolves pins the asymmetry: validate reports
// an empty team rather than erroring, so an administrator learns why they cannot connect
// instead of being told their valid key is bad.
func TestValidateCursorTeamKey_EmptyTeamStillResolves(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(constants.GroupCursorAdmin).Return(gw)
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorListTeamMembersRequest{})).
		Return(proto.Message(&vgpb.CursorListTeamMembersResponse{}), nil)
	gw.EXPECT().ForGroup(constants.GroupCursorUsage).Return(gw)
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorFilteredUsageEventsRequest{})).
		Return(proto.Message(&vgpb.CursorFilteredUsageEventsResponse{}), nil)

	svc, _ := newCursorDelegate(t, gw, nil, true)
	resp, err := svc.ValidateCursorTeamKey(context.Background(), &connintv1.ValidateCursorTeamKeyRequest{
		OrgId: testOrgID, TeamKey: "crsr_empty",
	})
	require.NoError(t, err)
	assert.Empty(t, resp.GetMemberEmails())
	assert.Zero(t, resp.GetMemberCount())
}

// resolveMembersForTest drives the real members probe over a fixed vendor response, so
// the roster under test is the one production would build rather than a hand-made struct.
func resolveMembersForTest(t *testing.T, members [][]byte) *resolvedCursorTeam {
	t.Helper()
	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(constants.GroupCursorAdmin).Return(gw)
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.CursorListTeamMembersRequest{})).
		Return(proto.Message(&vgpb.CursorListTeamMembersResponse{TeamMembers: members}), nil)

	svc, _ := newCursorDelegate(t, gw, nil, true)
	team, err := svc.probeCursorMembers(context.Background(), gw, testOrgID)
	require.NoError(t, err)
	return team
}

// TestProbeCursorMembers_UnreadableEntryStillFingerprinted closes the gap the earlier
// fingerprint work left open. An entry the parser cannot read was skipped outright, so it
// contributed nothing to the digest while still being counted — and two rosters differing
// ONLY in those entries fingerprinted alike. The confirm step would then bind to a team
// whose differences were exactly the part nobody could see.
//
// Identifiability stays a separate question: an unreadable entry hardens the digest, but
// it puts nothing on screen for a person to recognise, so it must not make an otherwise
// anonymous roster savable.
func TestProbeCursorMembers_UnreadableEntryStillFingerprinted(t *testing.T) {
	t.Parallel()

	// One readable member, plus one entry whose id is the wrong JSON type.
	withFirst := resolveMembersForTest(t, [][]byte{
		[]byte(`{"id":"user_A","email":"a@example.com"}`),
		[]byte(`{"id":111,"email":"x@example.com"}`),
	})
	withSecond := resolveMembersForTest(t, [][]byte{
		[]byte(`{"id":"user_A","email":"a@example.com"}`),
		[]byte(`{"id":222,"email":"y@example.com"}`),
	})

	assert.Equal(t, 2, withFirst.memberCount, "an unreadable entry is still a member")
	assert.Equal(t, 1, withFirst.identifiableMembers,
		"but it is not something a person can recognise the team by")
	assert.Len(t, withFirst.memberEmails, 1, "and it is not listed")

	assert.NotEqual(t,
		cursorTeamFingerprint(withFirst.memberIdentities),
		cursorTeamFingerprint(withSecond.memberIdentities),
		"two rosters differing only in their unreadable entries must not fingerprint alike")

	// And an entry that cannot be read does not on its own make a roster savable.
	allUnreadable := resolveMembersForTest(t, [][]byte{
		[]byte(`{"id":111}`),
		[]byte(`{"id":222}`),
	})
	assert.Equal(t, 2, allUnreadable.memberCount)
	assert.False(t, allUnreadable.hasIdentifiableMember(),
		"nothing here is recognisable, whatever the digest can tell apart")
}
