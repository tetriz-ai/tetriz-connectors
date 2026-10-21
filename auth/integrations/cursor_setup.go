// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cursorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/cursor"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgcursorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const connectorIDCursor = "cursor"

// cursorProbeWindow is how wide a window the billed-events probe asks for. One day
// with a single-row page is the cheapest request that still exercises the usage
// privilege; the probe is about permission, not data.
const cursorProbeWindow = 24 * time.Hour

// resolvedCursorTeam is what a validated key resolved to — the only thing an
// administrator can recognise, since Cursor issues no team identifier.
type resolvedCursorTeam struct {
	memberEmails []string
	memberCount  int
	// memberIdentities is one entry per member the vendor returned, in vendor order,
	// used only to derive the fingerprint. Kept separate from memberEmails because that
	// list is for a human to read and this one is for a machine to compare.
	//
	// Every returned entry contributes, including one the parser cannot read — as its
	// raw bytes. A roster that differs only in the entries nobody can read is still a
	// different roster, and leaving those out let two such rosters fingerprint alike.
	memberIdentities []string
	// identifiableMembers counts the entries carrying an id or an address: the ones that
	// tell this roster apart from another of the same size in a way a person could also
	// recognise. Deliberately not len(memberIdentities), which now counts unreadable
	// entries too — those harden the digest but leave nothing to recognise.
	identifiableMembers int
	// fingerprint identifies the key and the roster together. It is what the save step
	// binds to, because a member count cannot tell two same-sized teams apart and
	// Cursor issues no team id of its own.
	fingerprint string
	// credentials is the marshalled credential blob, returned so the save path does
	// not re-marshal and risk diverging from what was validated.
	credentials []byte
}

// hasIdentifiableMember reports whether the roster carries anything a person could
// recognise it by. It is false for an empty team, for one whose every member arrived
// without an id or an address, and for one whose every entry was unreadable — in each
// case there is nothing on the confirm step to vouch for.
func (t *resolvedCursorTeam) hasIdentifiableMember() bool {
	return t.identifiableMembers > 0
}

// resolveCursorTeamKey validates a pasted Cursor team key and returns who it
// resolves to, persisting nothing.
//
// It makes TWO vendor calls, and the second is the one that earns its place. The
// members call proves the credential authenticates; it cannot prove the key may read
// *usage events*, which is the privilege every scheduled run depends on. A
// members-only probe would accept a key that then fails on its first sync — hours
// later, out of the administrator's sight, with the connector already showing as
// connected. So the probe tests the privilege the design actually needs.
//
// Deliberately separate from resolveConnectorToken rather than a branch inside it.
// That function serves GitHub's live PAT flow and answers with a single
// ConnectorAccount; Cursor has no team handle or identifier to put in one, and
// widening it would put a working credential flow at risk to reach a shape that
// still would not fit.
func (s *Service) resolveCursorTeamKey(ctx context.Context, orgID, teamKey string) (*resolvedCursorTeam, error) {
	if orgID == "" || teamKey == "" {
		logger.Error(ctx, "resolveCursorTeamKey: missing required fields", zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.InvalidArgument, "org_id and team_key are required")
	}

	// This is the environment-wide off switch. A host applies any per-organization gate before it calls this service.
	// No VCS guard: Cursor is an analytics connector, so the single-source-control
	// rule does not apply to it.
	if !s.connectorEnabled(connectorIDCursor) {
		logger.WarnWithCtx(ctx, "resolveCursorTeamKey: connector ops-disabled",
			zap.String(logger.OrgID, orgID))
		return nil, status.Errorf(codes.FailedPrecondition, "connector %q is not enabled", connectorIDCursor)
	}
	// The rate limiter buckets per organisation, and it reads the organisation from
	// request context — which the scheduled path gets from the orchestrator and this one
	// gets from nowhere. Without this, every organisation's validation shares one
	// bucket, so one administrator retrying a bad paste can rate-limit another
	// organisation's connect.
	ctx = reqctx.WithOrgID(ctx, orgID)

	if s.gatewayFactory == nil {
		logger.Error(ctx, "resolveCursorTeamKey: gateway factory not configured")
		return nil, status.Error(codes.FailedPrecondition, "cursor connect not available")
	}

	creds, err := json.Marshal(cursorconfig.CursorCredentials{APIKey: teamKey}) //nolint:gosec // G117: the credential is persisted encrypted at rest; marshalling it here is the point
	if err != nil {
		logger.Error(ctx, "resolveCursorTeamKey: marshal credentials",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}

	// A gateway built from an in-memory config that is never persisted — the same
	// shape GitHub's token resolve uses, so the probe inherits the retry,
	// rate-limit and circuit-breaker behaviour the sync path has.
	gw, err := s.gatewayFactory(ctx, &sdk.ConnectorConfig{
		ConnectorID:    sdk.ConnectorID(connectorIDCursor),
		OrganizationID: orgID,
		Credentials:    creds,
	})
	if err != nil {
		logger.Error(ctx, "resolveCursorTeamKey: build gateway",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "validate team key: %v", err)
	}

	team, err := s.probeCursorMembers(ctx, gw, orgID)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	if err := s.probeCursorUsageAccess(ctx, gw, orgID); err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	team.credentials = creds
	team.fingerprint = cursorTeamFingerprint(team.memberIdentities)
	return team, nil
}

// probeCursorMembers resolves the team roster. An empty roster is not an error: a
// team with no members is a legitimate response, and refusing it would tell an
// administrator their valid key is bad.
func (s *Service) probeCursorMembers(
	ctx context.Context, gw sdk.VendorGateway, orgID string,
) (*resolvedCursorTeam, error) {
	protoResp, err := gw.ForGroup(constants.GroupCursorAdmin).
		Execute(ctx, &vgcursorpb.CursorListTeamMembersRequest{})
	if err != nil {
		code, msg := classifyCursorProbeFailure(ctx, orgID, "list team members", err,
			"invalid or unauthorized Cursor team key")
		return nil, status.Error(code, msg)
	}
	members, ok := protoResp.(*vgcursorpb.CursorListTeamMembersResponse)
	if !ok {
		logger.Error(ctx, "probeCursorMembers: unexpected response type", zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.Internal, "validate team key: unexpected response")
	}

	raw := members.GetTeamMembers()
	emails := make([]string, 0, len(raw))
	identities := make([]string, 0, len(raw))
	identifiable := 0
	for _, m := range raw {
		// Members arrive as raw vendor JSON, so the fields are read out here rather
		// than typed at the gateway. A member without an address is counted but not
		// listed: dropping it from the count would understate the team an
		// administrator is being asked to recognise.
		var member struct {
			ID    string `json:"id"`
			Email string `json:"email"`
		}
		if err := json.Unmarshal(m, &member); err != nil {
			// Counted, not listed, and still fingerprinted — by its raw bytes, since
			// that is the only representation of it we have. Skipping it entirely left
			// the digest blind to the one part of the roster nobody can inspect.
			logger.WarnWithCtx(ctx, "probeCursorMembers: unreadable member entry, counted and fingerprinted but not listed",
				zap.String(logger.OrgID, orgID), zap.Error(err))
			identities = append(identities, "unreadable:"+string(m))
			continue
		}
		if member.Email != "" {
			emails = append(emails, member.Email)
		}
		// The id is preferred over the address because a member can change theirs
		// without becoming a different person, and a rename would otherwise read as a
		// different team. An entry with neither still contributes, as the empty string,
		// so a roster gaining one is not mistaken for the same roster.
		identity := member.ID
		if identity == "" {
			identity = strings.ToLower(member.Email)
		}
		if identity != "" {
			identifiable++
		}
		identities = append(identities, identity)
	}
	return &resolvedCursorTeam{
		memberEmails:        emails,
		memberCount:         len(raw),
		identifiableMembers: identifiable,
		memberIdentities:    identities,
	}, nil
}

// cursorTeamFingerprint identifies the roster the administrator was shown, so the save
// step can refuse a key that now resolves to a different team.
//
// It covers the member identities and nothing else. In particular it does not cover the
// key: a digest of the credential would have to travel to the client, and its safety
// would then rest on the vendor's key having enough entropy to survive an offline guess
// against a roster the same response already discloses — a property this codebase
// neither controls nor checks. The one case a roster-only digest cannot distinguish is
// an empty roster, which carries no information at all; the save refuses that outright
// rather than comparing digests that must match.
//
// Identities are sorted because the vendor makes no ordering promise — an unsorted
// digest would refuse a save for a roster that never changed — and each is
// length-prefixed so no two rosters can concatenate alike ("ab" + "c" against "a" +
// "bc"). A membership change between confirm and save does change this value, and that
// refusal is correct: the roster the administrator approved is no longer the one being
// saved.
func cursorTeamFingerprint(identities []string) string {
	sorted := make([]string, len(identities))
	copy(sorted, identities)
	sort.Strings(sorted)

	h := sha256.New()
	// A domain tag, so the digest cannot be confused with one computed elsewhere for
	// another purpose over the same identities.
	_, _ = fmt.Fprintf(h, "cursor-team-v1\n")
	for _, id := range sorted {
		_, _ = fmt.Fprintf(h, "%d:%s\n", len(id), id)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// probeCursorUsageAccess checks the privilege the design depends on. Cursor reports
// an under-privileged key distinctly — a message about team membership rather than an
// invalid-credential error — so the caller can be told which key to mint instead of
// being told their key is bad.
func (s *Service) probeCursorUsageAccess(ctx context.Context, gw sdk.VendorGateway, orgID string) error {
	now := time.Now().UTC()
	_, err := gw.ForGroup(constants.GroupCursorUsage).Execute(ctx, &vgcursorpb.CursorFilteredUsageEventsRequest{
		StartDate: now.Add(-cursorProbeWindow).UnixMilli(),
		EndDate:   now.UnixMilli(),
		Page:      1,
		PageSize:  1,
	})
	if err != nil {
		code, msg := classifyCursorProbeFailure(ctx, orgID, "read usage events", err,
			"this Cursor key cannot read usage events; mint a team key with read access to billed usage")
		return status.Error(code, msg)
	}
	return nil
}

// classifyCursorProbeFailure decides what to tell the caller about a failed probe.
// Only a classified auth failure means the key is wrong; everything else — a rate
// limit, a vendor outage, a context timeout — must not tell an administrator their
// valid key is bad.
func classifyCursorProbeFailure(
	ctx context.Context, orgID, what string, err error, authMsg string,
) (codes.Code, string) {
	logger.WarnWithCtx(ctx, "cursor probe failed",
		zap.String(logger.OrgID, orgID), zap.String("probe", what), zap.Error(err))
	var syncErr *sdk.SyncError
	if errors.As(err, &syncErr) && syncErr.Kind == sdk.ErrKindAuth {
		return codes.InvalidArgument, authMsg
	}
	return codes.Unavailable,
		"could not reach Cursor to validate the team key, please try again"
}

// ValidateCursorTeamKey resolves a pasted key to its members without persisting.
func (s *Service) ValidateCursorTeamKey(
	ctx context.Context, req *connintv1.ValidateCursorTeamKeyRequest,
) (*connintv1.ValidateCursorTeamKeyResponse, error) {
	team, err := s.resolveCursorTeamKey(ctx, orgid.Canonical(req.GetOrgId()), req.GetTeamKey())
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	return &connintv1.ValidateCursorTeamKeyResponse{
		MemberEmails:    team.memberEmails,
		MemberCount:     int32(team.memberCount), //nolint:gosec // G115: a team roster cannot overflow int32
		TeamFingerprint: team.fingerprint,
	}, nil
}

// SubmitCursorTeamKey persists a confirmed key and enables the connector.
//
// It re-resolves rather than trusting the validate call: the key is re-sent by the
// client between the two steps, so nothing guarantees it is the same one, and the
// count the administrator confirmed is checked against what the key resolves to now.
func (s *Service) SubmitCursorTeamKey(
	ctx context.Context, req *connintv1.SubmitCursorTeamKeyRequest,
) (*connintv1.SubmitCursorTeamKeyResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	team, err := s.resolveCursorTeamKey(ctx, orgID, req.GetTeamKey())
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	// A roster the digest cannot tell apart is refused here rather than compared, and
	// the asymmetry with validate is deliberate. The guard below compares a digest of
	// the member identities, so a roster carrying none of them — no member with an id
	// or an address — hashes to a value fixed by its size alone, and the check would
	// wave through a key resolving to any other such roster of the same size. An empty
	// team is the obvious case; a team of members the vendor returns without either
	// field is the same problem one step along, and the members probe treats that entry
	// shape as ordinary. Refusing is also the honest answer on its own terms: nothing
	// here can be attributed to a person, so there is nothing to ingest.
	if !team.hasIdentifiableMember() {
		logger.WarnWithCtx(ctx, "SubmitCursorTeamKey: refusing a roster with no identifiable member",
			zap.String(logger.OrgID, orgID), zap.Int("member_count", team.memberCount))
		return nil, status.Error(codes.FailedPrecondition,
			"this key resolves to a Cursor team with no identifiable members; add members to the team, then connect")
	}

	// The torn-state guard: a key swapped between confirm and save would resolve to a
	// different team, and saving it would ingest another team's spend under this
	// organisation with nothing able to notice. It compares the roster's fingerprint
	// rather than its size, because two different teams of equal size share a count —
	// which is the case a size check cannot see and this one can. The expectation is
	// required, so there is no shape of request that saves unconfirmed.
	if req.GetExpectedTeamFingerprint() != team.fingerprint {
		logger.WarnWithCtx(ctx, "SubmitCursorTeamKey: resolved team differs from the confirmed one",
			zap.String(logger.OrgID, orgID),
			zap.Int("resolved_member_count", team.memberCount))
		return nil, status.Error(codes.FailedPrecondition,
			"the key now resolves to a different team than the one confirmed; validate again")
	}

	// The settings written here seed a first insert only: SaveCredentialsEnablePreservingSettings omits
	// that column on conflict, so an administrator's stored lookback window survives a
	// key rotation without this path having to read it back first.
	settings, err := json.Marshal(cursorconfig.CursorSettings{})
	if err != nil {
		logger.Error(ctx, "SubmitCursorTeamKey: marshal settings",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal settings: %v", err)
	}

	// Enabled on save, which is the convention rather than a precedent this sets:
	// only the source-control connector defers enablement, because it has
	// repositories to elect first.
	connCfg := &sdk.ConnectorConfig{
		ConnectorID:    sdk.ConnectorID(connectorIDCursor),
		OrganizationID: orgID,
		Credentials:    team.credentials,
		Settings:       settings,
	}
	if err := s.connectorDAO.SaveCredentialsEnablePreservingSettings(ctx, connCfg); err != nil {
		logger.Error(ctx, "SubmitCursorTeamKey: save connector config",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "save connector config: %v", err)
	}
	// Without this, re-pasting a key after the previous one was revoked leaves the
	// connector presenting as broken even though it now works.
	s.resetConnectorHealth(ctx, orgID, sdk.ConnectorID(connectorIDCursor))

	logger.Info(ctx, "cursor setup: connection saved",
		zap.String(logger.OrgID, orgID), zap.Int("member_count", team.memberCount))
	return &connintv1.SubmitCursorTeamKeyResponse{}, nil
}
