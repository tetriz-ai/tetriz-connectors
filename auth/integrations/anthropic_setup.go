// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	anthropicconfig "github.com/tetriz-ai/tetriz-connectors/connectors/anthropic"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vganthropicpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	scheduleradapter "github.com/tetriz-ai/tetriz-connectors/runner/schedule/connectorsource"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorIDClaudeCode aliases the schedule adapter's exported id rather than restating
// the string. The connect path saves under this id and the scheduler selects on it, so a
// drift between the two does not fail — it leaves an organisation showing connected and
// never syncing.
const connectorIDClaudeCode = scheduleradapter.ConnectorIDClaudeCode

// anthropicProbeWindow is how wide a window the cost probe asks for. One day with a
// single-row page is the cheapest request that still exercises the analytics privilege;
// the probe is about permission and identity, not data.
const anthropicProbeWindow = 24 * time.Hour

// resolvedAnthropicOrg is what a validated key resolved to: the organisation to store
// and the roster to show.
type resolvedAnthropicOrg struct {
	// organizationID is the machine-comparable identity the save binds against. Unlike
	// Cursor, which had to synthesise a fingerprint because it issues no team id, this
	// is the vendor's own — so there is no digest here and no roster-shape guard around
	// one.
	organizationID string
	memberEmails   []string
	memberCount    int
	// credentials is the marshalled blob, returned so the save path does not re-marshal
	// and risk diverging from what was validated.
	credentials []byte
}

// resolveAnthropicAnalyticsKey validates a pasted key and returns who it resolves to,
// persisting nothing.
//
// It makes TWO vendor calls, one per scope, and neither is redundant. The roster call
// proves read:members; the cost-report call proves read:analytics. A single call would
// prove one scope and leave the other to fail on its first scheduled run — hours later,
// out of the administrator's sight, with the connector already showing as connected.
//
// The cost report is also the only endpoint that answers WHICH organisation the key
// belongs to: the activity endpoints omit organization_id from their envelope, and the
// Admin API's organisation-info endpoint refuses this key family outright. So the call
// that proves the privilege is the same one that yields the identity.
func (s *Service) resolveAnthropicAnalyticsKey(ctx context.Context, orgID, apiKey string) (*resolvedAnthropicOrg, error) {
	if orgID == "" || apiKey == "" {
		logger.Error(ctx, "resolveAnthropicAnalyticsKey: missing required fields", zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.InvalidArgument, "org_id and api_key are required")
	}

	// This is the environment-wide off switch. A host applies any per-organization gate before it calls this service.
	if !s.connectorEnabled(string(connectorIDClaudeCode)) {
		logger.WarnWithCtx(ctx, "resolveAnthropicAnalyticsKey: connector ops-disabled",
			zap.String(logger.OrgID, orgID))
		return nil, status.Errorf(codes.FailedPrecondition, "connector %q is not enabled", string(connectorIDClaudeCode))
	}

	// The rate limiter buckets per organisation and reads the organisation from
	// request context, which the scheduled path gets from the orchestrator and this one
	// gets from nowhere. Without this every organisation's validation shares one
	// bucket, so one administrator retrying a bad paste rate-limits another
	// organisation's connect.
	ctx = reqctx.WithOrgID(ctx, orgID)

	if s.gatewayFactory == nil {
		logger.Error(ctx, "resolveAnthropicAnalyticsKey: gateway factory not configured")
		return nil, status.Error(codes.FailedPrecondition, "claude code connect not available")
	}

	creds, err := json.Marshal(anthropicconfig.Credentials{APIKey: apiKey}) //nolint:gosec // G117: the credential is persisted encrypted at rest; marshalling it here is the point
	if err != nil {
		logger.Error(ctx, "resolveAnthropicAnalyticsKey: marshal credentials",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}

	// An in-memory config that is never persisted, so the probe inherits the retry,
	// rate-limit and circuit-breaker behaviour the sync path has.
	gw, err := s.gatewayFactory(ctx, &sdk.ConnectorConfig{
		ConnectorID:    connectorIDClaudeCode,
		OrganizationID: orgID,
		Credentials:    creds,
	})
	if err != nil {
		logger.Error(ctx, "resolveAnthropicAnalyticsKey: build gateway",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "validate api key: %v", err)
	}

	resolved, err := s.probeAnthropicMembers(ctx, gw, orgID)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	organizationID, err := s.probeAnthropicAnalyticsAccess(ctx, gw, orgID)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	resolved.organizationID = organizationID
	resolved.credentials = creds
	return resolved, nil
}

// anthropicRosterPageSize is the vendor's maximum, so most organisations resolve in one
// request and the walk below rarely runs twice.
const anthropicRosterPageSize = 1000

// anthropicRosterPageCap bounds the walk. At the page size above this is a million
// members — far past any real organisation — so reaching it means the vendor is not
// advancing after_id rather than that the roster is genuinely that large.
const anthropicRosterPageCap = 1000

// probeAnthropicMembers reads the roster, proving read:members. An empty roster is not an
// error: an organisation with no members is a legitimate response, and refusing it would
// tell an administrator their valid key is bad.
//
// It follows every page. The count and the list are what an administrator is asked to
// recognise and what the response promises, so stopping at the first page would show a
// large organisation a fraction of itself and call it the whole. This is where the roster
// differs from the sibling connector's, whose vendor exposes no paging at all.
func (s *Service) probeAnthropicMembers(
	ctx context.Context, gw sdk.VendorGateway, orgID string,
) (*resolvedAnthropicOrg, error) {
	var (
		emails  []string
		total   int
		afterID string
	)
	for page := 0; page < anthropicRosterPageCap; page++ {
		protoResp, err := gw.ForGroup(constants.GroupAnthropicAnalytics).
			Execute(ctx, &vganthropicpb.AnthropicOrganizationMembersRequest{
				AfterId: afterID,
				Limit:   anthropicRosterPageSize,
			})
		if err != nil {
			code, msg := classifyAnthropicProbeFailure(ctx, orgID, "list organization members", err, scopeReadMembers)
			return nil, status.Error(code, msg)
		}
		members, ok := protoResp.(*vganthropicpb.AnthropicOrganizationMembersResponse)
		if !ok {
			logger.Error(ctx, "probeAnthropicMembers: unexpected response type", zap.String(logger.OrgID, orgID))
			return nil, status.Error(codes.Internal, "validate api key: unexpected response")
		}

		raw := members.GetData()
		total += len(raw)
		for _, m := range raw {
			// Members arrive as raw vendor JSON, so fields are read out here rather than
			// typed at the gateway. A member without an address is counted but not listed:
			// dropping it from the count would understate the organisation an administrator
			// is being asked to recognise.
			var member struct {
				Email string `json:"email"`
			}
			if err := json.Unmarshal(m, &member); err != nil {
				logger.WarnWithCtx(ctx, "probeAnthropicMembers: unreadable member entry, counted but not listed",
					zap.String(logger.OrgID, orgID), zap.Error(err))
				continue
			}
			if member.Email != "" {
				emails = append(emails, member.Email)
			}
		}

		// A page that claims more but hands back no cursor cannot be followed, and looping on
		// the same after_id would spin. It is REFUSED rather than returned as what was read:
		// the roster is the only thing an administrator can actually recognise on the confirm
		// step — the organisation id is a UUID they can only accept — so handing back a
		// truncated list and count would ask them to confirm their organisation against a
		// partial view of it. The scheduled walk already treats this exact shape as an error;
		// returning it as success here left the two paths disagreeing.
		if members.GetHasMore() && members.GetLastId() == "" {
			logger.Error(ctx, "probeAnthropicMembers: vendor reported more pages but returned no cursor",
				zap.String(logger.OrgID, orgID), zap.Int("members_read", total))
			return nil, status.Error(codes.InvalidArgument,
				"the roster could not be read completely, so the organisation cannot be confirmed against it")
		}
		if !members.GetHasMore() {
			return &resolvedAnthropicOrg{memberEmails: emails, memberCount: total}, nil
		}
		afterID = members.GetLastId()
	}

	logger.WarnWithCtx(ctx, "probeAnthropicMembers: roster page cap reached, reporting a partial roster",
		zap.String(logger.OrgID, orgID), zap.Int("members_read", total))
	return &resolvedAnthropicOrg{memberEmails: emails, memberCount: total}, nil
}

// probeAnthropicAnalyticsAccess proves read:analytics and returns the organisation the
// key belongs to. One day, one row: the cheapest request that still exercises the
// privilege every scheduled run depends on.
func (s *Service) probeAnthropicAnalyticsAccess(
	ctx context.Context, gw sdk.VendorGateway, orgID string,
) (string, error) {
	now := time.Now().UTC()
	protoResp, err := gw.ForGroup(constants.GroupAnthropicAnalytics).
		Execute(ctx, &vganthropicpb.AnthropicAnalyticsUserCostReportRequest{
			StartingAt: timestamppb.New(now.Add(-anthropicProbeWindow)),
			EndingAt:   timestamppb.New(now),
			Limit:      1,
		})
	if err != nil {
		code, msg := classifyAnthropicProbeFailure(ctx, orgID, "read cost report", err, scopeReadAnalytics)
		return "", status.Error(code, msg)
	}
	report, ok := protoResp.(*vganthropicpb.AnthropicAnalyticsUserCostReportResponse)
	if !ok {
		logger.Error(ctx, "probeAnthropicAnalyticsAccess: unexpected response type", zap.String(logger.OrgID, orgID))
		return "", status.Error(codes.Internal, "validate api key: unexpected response")
	}

	// The identity is required, not incidental: it is what a later reconnect compares
	// to notice the key now points somewhere else. A response without it means we
	// cannot establish which organisation we would be storing data against, so saying
	// so beats storing an empty identity that silently disables that check forever.
	organizationID := report.GetOrganizationId()
	if organizationID == "" {
		logger.Error(ctx, "probeAnthropicAnalyticsAccess: response carried no organization id",
			zap.String(logger.OrgID, orgID))
		return "", status.Error(codes.InvalidArgument,
			"Anthropic did not return an organisation for this key, so it cannot be verified. Check it is "+
				"an Analytics API key for a single organisation; if it is, open an issue.")
	}
	return organizationID, nil
}

// The two scopes a key must carry. Named rather than inlined because the refusal message
// has to tell an administrator which one is missing, and getting that backwards sends
// them to recreate a key with the scope they already had.
const (
	scopeReadMembers   = "read:members"
	scopeReadAnalytics = "read:analytics"
)

// classifyAnthropicProbeFailure decides what to tell the caller about a failed probe.
//
// Three outcomes, and the middle one is why this is not a copy of the Cursor classifier.
// Anthropic distinguishes a rejected credential (401) from a key whose scopes are too
// narrow (403), and that distinction changes the ADVICE, not just the wording: scopes are
// fixed when a key is minted, so a 403 cannot be resolved by granting permission. The
// administrator has to create a new key with both scopes selected. Telling them
// "insufficient permission" would send them looking for a setting that does not exist.
//
// Everything else — a rate limit, a vendor outage, a timeout — must not tell an
// administrator their valid key is bad.
func classifyAnthropicProbeFailure(
	ctx context.Context, orgID, what string, err error, missingScope string,
) (codes.Code, string) {
	logger.WarnWithCtx(ctx, "anthropic probe failed",
		zap.String(logger.OrgID, orgID), zap.String("probe", what), zap.Error(err))

	var syncErr *sdk.SyncError
	if errors.As(err, &syncErr) && syncErr.Kind == sdk.ErrKindRateLimit {
		return codes.Unavailable,
			"too many validation attempts against Anthropic just now; wait a minute and try again."
	}
	if !errors.As(err, &syncErr) || syncErr.Kind != sdk.ErrKindAuth {
		return codes.Unavailable,
			"could not reach Anthropic to validate the API key, please try again"
	}

	if syncErr.HTTPStatus == http.StatusForbidden {
		return codes.InvalidArgument, fmt.Sprintf(
			"this API key is missing the %s scope. Anthropic fixes a key's scopes when it is created, "+
				"so this key cannot be updated — create a new key with both %s and %s selected, then paste it here.",
			missingScope, scopeReadAnalytics, scopeReadMembers)
	}
	return codes.InvalidArgument,
		"this API key was rejected by Anthropic. Check it was copied in full, and that it is an " +
			"Analytics API key created in Claude by an organisation owner."
}

// ValidateAnthropicAnalyticsKey resolves a pasted key without persisting, so a
// host's UI can ask an administrator to recognise the organisation before anything is
// stored.
func (s *Service) ValidateAnthropicAnalyticsKey(
	ctx context.Context, req *connintv1.ValidateAnthropicAnalyticsKeyRequest,
) (*connintv1.ValidateAnthropicAnalyticsKeyResponse, error) {
	resolved, err := s.resolveAnthropicAnalyticsKey(ctx, orgid.Canonical(req.GetOrgId()), req.GetApiKey())
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	return &connintv1.ValidateAnthropicAnalyticsKeyResponse{
		AnthropicOrganizationId: resolved.organizationID,
		MemberEmails:            resolved.memberEmails,
		MemberCount:             int32(resolved.memberCount), //nolint:gosec // G115: an organisation roster cannot overflow int32
	}, nil
}

// SubmitAnthropicAnalyticsKey persists a confirmed key and enables the connector.
//
// It re-resolves rather than trusting the validate call: the key is re-sent by the client
// between the two steps, so nothing guarantees it is the same one.
func (s *Service) SubmitAnthropicAnalyticsKey(
	ctx context.Context, req *connintv1.SubmitAnthropicAnalyticsKeyRequest,
) (*connintv1.SubmitAnthropicAnalyticsKeyResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	resolved, err := s.resolveAnthropicAnalyticsKey(ctx, orgID, req.GetApiKey())
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	// The torn-state guard: a key swapped between confirm and save would resolve to a
	// different organisation, and saving it would ingest another organisation's spend
	// under this one with nothing able to notice. The comparison is against the vendor's
	// own organisation id, so unlike a roster digest it cannot be confused by two
	// organisations that happen to look alike. The expectation is required, so no shape
	// of request saves unconfirmed.
	if req.GetExpectedAnthropicOrganizationId() != resolved.organizationID {
		logger.WarnWithCtx(ctx, "SubmitAnthropicAnalyticsKey: resolved organisation differs from the confirmed one",
			zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.FailedPrecondition,
			"the key now resolves to a different Anthropic organisation than the one confirmed; validate again")
	}

	// These settings seed a FIRST insert only. On conflict the save preserves the
	// existing settings column, which is what lets an administrator's tuned windows
	// survive a key rotation without this path reading them back.
	settings, err := json.Marshal(anthropicconfig.Settings{AnthropicOrganizationID: resolved.organizationID})
	if err != nil {
		logger.Error(ctx, "SubmitAnthropicAnalyticsKey: marshal settings",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal settings: %v", err)
	}

	connCfg := &sdk.ConnectorConfig{
		ConnectorID:    connectorIDClaudeCode,
		OrganizationID: orgID,
		Credentials:    resolved.credentials,
		Settings:       settings,
	}
	// The save and the organisation id's patch land together: a key that resolves to
	// an Anthropic organisation another tenant holds is refused by the claim index on
	// the patch, and the save it followed must not stay behind.
	saveBoth := func(ctx context.Context) error {
		if err := s.connectorDAO.SaveCredentialsEnablePreservingSettings(ctx, connCfg); err != nil {
			logger.Error(ctx, "SubmitAnthropicAnalyticsKey: save connector config",
				zap.String(logger.OrgID, orgID), zap.Error(err))
			return status.Errorf(codes.Internal, "save connector config: %v", err)
		}

		// Preserving settings is right for the windows and wrong for the organisation id,
		// which is the one field that must follow the KEY rather than the configuration. On
		// a reconnect the insert above is a conflict, so the id it carried was discarded —
		// and an organisation re-pasting a key that belongs somewhere else would keep
		// claiming the old one, defeating the only check the stored id exists for.
		//
		// Patched rather than written by the save because the two settings have opposite
		// requirements, and the patch merges into what is stored instead of replacing it.
		if err := s.connectorDAO.PatchSettings(ctx, orgID, connectorIDClaudeCode,
			func(current json.RawMessage) (json.RawMessage, error) {
				stored := anthropicconfig.Settings{}
				if len(current) > 0 {
					if err := json.Unmarshal(current, &stored); err != nil {
						// Settings we cannot read must not be silently replaced — that would
						// discard the very windows this path exists to preserve.
						return nil, fmt.Errorf("unmarshalling stored anthropic settings: %w", err)
					}
				}
				stored.AnthropicOrganizationID = resolved.organizationID
				out, err := json.Marshal(stored)
				if err != nil {
					return nil, fmt.Errorf("marshalling anthropic settings: %w", err)
				}
				return out, nil
			}); err != nil {
			logger.Error(ctx, "SubmitAnthropicAnalyticsKey: record resolved organisation",
				zap.String(logger.OrgID, orgID), zap.Error(err))
			return status.Errorf(codes.Internal, "record resolved organisation: %v", err)
		}
		return nil
	}
	var saveErr error
	if s.tx != nil {
		saveErr = s.tx.RunInScope(ctx, orgID, store.ScopeEnforced, saveBoth)
	} else {
		saveErr = saveBoth(ctx)
	}
	if isClaimedByAnotherOrganization(saveErr, "uniq_anthropic_organization_per_org") {
		logger.WarnWithCtx(ctx, "SubmitAnthropicAnalyticsKey: Anthropic organization already connected to another organization",
			zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.AlreadyExists, "this Anthropic organization is already connected to another organization")
	}
	if saveErr != nil {
		return nil, saveErr //nolint:wrapcheck // status pass-through
	}

	// Without this, re-pasting a key after the previous one was revoked leaves the
	// connector presenting as broken even though it now works.
	s.resetConnectorHealth(ctx, orgID, connectorIDClaudeCode)

	logger.Info(ctx, "claude code setup: connection saved",
		zap.String(logger.OrgID, orgID), zap.Int("member_count", resolved.memberCount))
	return &connintv1.SubmitAnthropicAnalyticsKeyResponse{}, nil
}
