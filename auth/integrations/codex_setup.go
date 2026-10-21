// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	codexconfig "github.com/tetriz-ai/tetriz-connectors/connectors/codex"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgcodexpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	scheduleradapter "github.com/tetriz-ai/tetriz-connectors/runner/schedule/connectorsource"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorIDCodex is the framework identifier, aliased rather than restated so the
// catalog entry, the connect path and the scheduler's selection cannot drift apart: the
// service matches a stored configuration row to its tile by exact string equality, and a
// mismatch leaves a connected organisation's tile reading disconnected forever — or, on
// the scheduler side, syncing nothing while showing connected.
const connectorIDCodex = scheduleradapter.ConnectorIDCodex

// The probe's window and page size. Seven days rather than one because the probe has a
// second job the sibling's does not: it returns the developer addresses an administrator
// recognises, and a single day is quiet often enough that a correct key would routinely
// confirm against nothing. Twenty-five rows is enough to recognise colleagues by and
// small enough to stay one request — this proves a privilege, it does not read data.
const (
	codexProbeWindow   = 7 * 24 * time.Hour
	codexProbePageSize = 25
)

// codexMalformedWorkspaceMarker is the token OpenAI's malformed-path refusal carries, in the
// pydantic validation shape its API returns:
//
//	{"error":{"message":"[{'type': 'uuid_parsing', 'loc': ('path', 'workspace_id'), ...}]"}}
//
// An undocumented vendor string; a test pins it, and it is named here rather than buried in
// a condition. If OpenAI reshapes the error, the
// branch reading it degrades to the unattributed 400 message rather than to a wrong one — the
// safe direction, but a silent one, so this is the line to re-check when that message stops
// being useful.
const codexMalformedWorkspaceMarker = "uuid_parsing"

// codexProbeScope is the one scope the endpoint requires. Named because the refusal
// message has to say which scope is missing, and a key's scopes cannot be widened after
// it is minted — so the advice is "create a new key", not "grant a permission".
const codexProbeScope = "codex.enterprise.analytics.read"

// resolvedCodexWorkspace is what a validated key and workspace resolved to: the roster to
// show, and the credential blob to store.
type resolvedCodexWorkspace struct {
	// developerEmails are distinct, in the order the vendor first returned them. The
	// probe reads a usage window whose rows are one developer per day, so the raw rows
	// repeat an active colleague once per day worked.
	developerEmails []string
	// developerCount counts distinct developers, and is never smaller than
	// len(developerEmails): a developer is identified by user_id or, failing that, by
	// address, so every listed address has been counted. It can be LARGER, because the
	// address is nullable and a developer known only by identifier is counted and not
	// named.
	developerCount int
	// truncated says the page filled, so the roster is a sample of the window rather than
	// all of it. Without it a count at a large workspace reads as the workspace's size.
	truncated bool
	// credentials is the marshalled credential blob exactly as it was presented to the
	// vendor, so what is stored is byte-identical to what was proven to work.
	credentials []byte
}

// resolveCodexWorkspace probes a pasted key against a named workspace and returns the
// developers it resolved, persisting nothing.
//
// ONE vendor call, where the sibling connector makes two. That is not a shortcut: the
// endpoint requires a single scope, so one request to the endpoint the scheduled path
// actually uses proves exactly the privilege every run depends on. A second call would
// prove a privilege nothing needs.
//
// What this probe cannot do is establish WHICH workspace the key belongs to. The workspace
// is a path parameter and the response carries no workspace field, so a successful probe
// means "this key can read the workspace you named" and never "this is your workspace".
// Nothing downstream may read it as an identity check.
func (s *Service) resolveCodexWorkspace(
	ctx context.Context, orgID, apiKey, workspaceID string,
) (*resolvedCodexWorkspace, error) {
	if orgID == "" || apiKey == "" || workspaceID == "" {
		logger.Error(ctx, "resolveCodexWorkspace: missing required fields", zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.InvalidArgument, "org_id, api_key and workspace_id are required")
	}

	// Refused here and not only at the proto boundary, because this is the last point
	// before a value that cannot be keyed is stored against an ENABLED connector. Past
	// this line the failure moves out of the administrator's sight and into a scheduled
	// run that dies every night.
	if !codexconfig.ValidWorkspaceID(workspaceID) {
		logger.WarnWithCtx(ctx, "resolveCodexWorkspace: unusable workspace id", zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.InvalidArgument,
			"that does not look like a workspace ID. OpenAI identifies a workspace by a UUID — copy it "+
				"from the ChatGPT admin console rather than typing the workspace's name.")
	}

	// This is the environment-wide off switch. A host applies any per-organization gate before it calls this service.
	if !s.connectorEnabled(string(connectorIDCodex)) {
		logger.WarnWithCtx(ctx, "resolveCodexWorkspace: connector ops-disabled", zap.String(logger.OrgID, orgID))
		return nil, status.Errorf(codes.FailedPrecondition, "connector %q is not enabled", string(connectorIDCodex))
	}

	// The rate limiter buckets per organisation and reads the organisation from
	// request context, which the scheduled path gets from the orchestrator and this one gets
	// from nowhere. Without this every organisation's validation shares one bucket, so one
	// administrator retrying a bad paste rate-limits another organisation's connect.
	ctx = reqctx.WithOrgID(ctx, orgID)

	if s.gatewayFactory == nil {
		logger.Error(ctx, "resolveCodexWorkspace: gateway factory not configured")
		return nil, status.Error(codes.FailedPrecondition, "codex connect not available")
	}

	creds, err := json.Marshal(codexconfig.Credentials{APIKey: apiKey}) //nolint:gosec // G117: the credential is persisted encrypted at rest; marshalling it here is the point
	if err != nil {
		logger.Error(ctx, "resolveCodexWorkspace: marshal credentials",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}

	// An in-memory config that is never persisted, so the probe inherits the retry,
	// rate-limit and circuit-breaker behaviour the sync path has.
	gw, err := s.gatewayFactory(ctx, &sdk.ConnectorConfig{
		ConnectorID:    connectorIDCodex,
		OrganizationID: orgID,
		Credentials:    creds,
	})
	if err != nil {
		logger.Error(ctx, "resolveCodexWorkspace: build gateway",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "validate api key: %v", err)
	}

	now := time.Now().UTC()
	protoResp, err := gw.ForGroup(constants.GroupCodexAnalytics).
		Execute(ctx, &vgcodexpb.CodexUsageRequest{
			WorkspaceId: workspaceID,
			StartTime:   now.Add(-codexProbeWindow).Unix(),
			EndTime:     now.Unix(),
			Limit:       codexProbePageSize,
		})
	if err != nil {
		code, msg := classifyCodexProbeFailure(ctx, orgID, err)
		return nil, status.Error(code, msg)
	}
	usage, ok := protoResp.(*vgcodexpb.CodexUsageResponse)
	if !ok {
		logger.Error(ctx, "resolveCodexWorkspace: unexpected response type", zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.Internal, "validate api key: unexpected response")
	}

	// Sliced to the page size we asked for rather than trusting the vendor to honour
	// `limit`. This is an endpoint that publishes no schema, so the bound this probe
	// claims has to be one we impose: a vendor ignoring the parameter would otherwise turn
	// a permission check into a response carrying every address in the workspace.
	rows := usage.GetUsageRows()
	if len(rows) > codexProbePageSize {
		rows = rows[:codexProbePageSize]
	}
	resolved := collectCodexDevelopers(ctx, orgID, rows)
	// A filled page and a vendor-declared next page mean the same thing to the
	// confirmation step: what it shows is a sample. Reading has_more alone would call a
	// full page complete whenever the vendor happens not to set it.
	resolved.truncated = usage.GetHasMore() || len(usage.GetUsageRows()) >= codexProbePageSize
	resolved.credentials = creds
	return resolved, nil
}

// codexUsageRow is the sliver of a usage row this probe reads. Deliberately not the
// connector's storage shape: the payload is stored verbatim precisely because OpenAI
// publishes no schema, and a probe that parsed more would turn an unverified shape into a
// connect-time failure. Both fields are optional, which is what forces the alias pass in
// collectCodexDevelopers below.
type codexUsageRow struct {
	UserID string `json:"user_id"`
	Actor  struct {
		Email string `json:"email"`
	} `json:"actor"`
}

// collectCodexDevelopers folds the probe's rows into the distinct developers behind them.
//
// Distinct by developer, not by row: the window's rows are one developer per day, so an
// active colleague appears once per day worked. Counting rows would report a busy week as a
// large workspace.
//
// It reads the page TWICE, and the second pass is why. Both fields are optional per row, so
// a developer can appear on Monday with only an address and on Friday with an identifier
// too. Counting in one pass would file Monday under the address and Friday under the
// identifier — one person counted as two, on a valid vendor response, overstating the
// workspace on the one screen an administrator is asked to trust. The first pass therefore
// learns which address belongs to which identifier, and the second counts against that.
//
// An unparseable row is skipped rather than failing the probe. This is a permission check
// whose secondary job is recognition — refusing the whole connect because one row of a schema
// we do not control looked unfamiliar would block a valid key. A row that identifies nobody
// counts as skipped rather than passing silently: a page of those is otherwise
// indistinguishable from a quiet window.
func collectCodexDevelopers(ctx context.Context, orgID string, rows [][]byte) *resolvedCodexWorkspace {
	parsed := make([]codexUsageRow, 0, len(rows))
	// aliasOf maps an address to the identifier it was seen beside, so a row carrying only
	// that address resolves to the same developer.
	aliasOf := map[string]string{}
	skipped := 0
	// The first parse failure is kept, not just counted. A log line saying twenty-five rows
	// were skipped tells an operator that something changed and nothing about what; the one
	// cause is what turns it into a diagnosis. Only the first, because a shape change makes
	// every row fail the same way and twenty-five copies say no more than one.
	var firstParseErr error
	for _, raw := range rows {
		var row codexUsageRow
		if err := json.Unmarshal(raw, &row); err != nil {
			if firstParseErr == nil {
				firstParseErr = err
			}
			skipped++
			continue
		}
		if row.UserID == "" && row.Actor.Email == "" {
			skipped++
			continue
		}
		if row.UserID != "" && row.Actor.Email != "" {
			aliasOf[row.Actor.Email] = row.UserID
		}
		parsed = append(parsed, row)
	}

	var (
		out      resolvedCodexWorkspace
		seen     = map[string]struct{}{}
		seenMail = map[string]struct{}{}
	)
	for _, row := range parsed {
		identity := row.UserID
		if identity == "" {
			// An address the vendor named beside an identifier elsewhere in this window
			// resolves to that identifier; otherwise the address is the only identity there
			// is, and stands as its own.
			if canonical, ok := aliasOf[row.Actor.Email]; ok {
				identity = canonical
			} else {
				identity = row.Actor.Email
			}
		}
		if _, dup := seen[identity]; !dup {
			seen[identity] = struct{}{}
			out.developerCount++
		}
		// Deliberately NOT inside the first-sighting branch above. The address is nullable
		// per row, so a developer whose day-one row carried none and whose day-five row did
		// would otherwise be counted and never named — the confirmation step exists to show
		// exactly those names.
		if email := row.Actor.Email; email != "" {
			if _, dup := seenMail[email]; !dup {
				seenMail[email] = struct{}{}
				out.developerEmails = append(out.developerEmails, email)
			}
		}
	}
	if skipped > 0 {
		fields := []zap.Field{
			zap.String(logger.OrgID, orgID), zap.Int("skipped", skipped), zap.Int("total", len(rows)),
		}
		if firstParseErr != nil {
			fields = append(fields, zap.NamedError("first_parse_error", firstParseErr))
		}
		logger.WarnWithCtx(ctx, "codex probe: rows identified no developer", fields...)
	}
	return &out
}

// classifyCodexProbeFailure decides what to tell the caller about a failed probe.
//
// Three inputs can be wrong — the key, its scope, and the workspace — and each gets its
// own message, because an administrator given one opaque failure re-pastes a perfectly
// good credential. The workspace case is the one worth getting right: it names the
// workspace field rather than the key, and it is the failure a Codex connect will hit most
// often, since the workspace is the only value here that is typed rather than copied whole.
//
// Everything else — a rate limit, a vendor outage, a timeout — must not tell an
// administrator their valid key is bad.
func classifyCodexProbeFailure(ctx context.Context, orgID string, err error) (codes.Code, string) {
	logger.WarnWithCtx(ctx, "codex probe failed", zap.String(logger.OrgID, orgID), zap.Error(err))

	var syncErr *sdk.SyncError
	if errors.As(err, &syncErr) && syncErr.Kind == sdk.ErrKindRateLimit {
		// ResourceExhausted, not Unavailable, and the difference is what makes this message
		// showable. Unavailable is also every other transient failure — an unreachable vendor,
		// a timeout — and it reaches the browser as a 503 whose text may be anything, so the
		// a host's UI cannot safely render it and falls back to generic copy. ResourceExhausted
		// means throttled and nothing else, so the interval below survives all the way to the
		// person who has to wait it out.
		//
		// The vendor's own interval when it gave one: telling an administrator to wait a
		// minute against a throttle measured in hours sends them straight back into it, and
		// each retry renews the window.
		if wait := syncErr.RetryAfterSeconds; wait > 0 {
			return codes.ResourceExhausted, fmt.Sprintf(
				"OpenAI is throttling validation attempts and asked us to wait %s; try again after that.",
				(time.Duration(wait) * time.Second).String())
		}
		return codes.ResourceExhausted,
			"too many validation attempts against OpenAI just now; wait a minute and try again."
	}
	if errors.As(err, &syncErr) && syncErr.HTTPStatus == http.StatusNotFound {
		return codes.InvalidArgument,
			"that workspace was not found. Check the workspace ID — the key itself was accepted, so this " +
				"is the workspace field rather than the key."
	}
	// A 400 never reaches the transient branch below, whichever field the vendor objected to.
	// The endpoint validates its path segment before it authenticates, so a 400 also says
	// nothing about the key — unlike the 404 above, which may.
	if errors.As(err, &syncErr) && syncErr.HTTPStatus == http.StatusBadRequest {
		// Attributed to the workspace only on the vendor's own marker, because
		// ValidWorkspaceID has already accepted this workspace as a canonical UUID and the
		// other fields in the probe — the window bounds, the page limit — are our own
		// constants. Blaming a workspace that was already valid sends an administrator to fix
		// something that is not broken.
		if strings.Contains(err.Error(), codexMalformedWorkspaceMarker) {
			return codes.InvalidArgument,
				"OpenAI did not recognise that workspace ID as a workspace identifier. It is a UUID — copy it " +
					"from the ChatGPT admin console rather than typing the workspace's name."
		}
		// No marker is not evidence the workspace was fine. The classifier keeps only the
		// vendor's error message fields of a JSON body, each cut to 200 bytes, and none of a
		// non-JSON body's text, so a workspace-shaped refusal whose marker falls outside those
		// arrives here indistinguishable from one about our own parameters.
		//
		// So this names no field. What it can say is that retrying will not help: a 400 is
		// deterministic, and the transient message below would send someone to retry a request
		// that fails identically every time — the worse error of the two, because it never ends.
		return codes.InvalidArgument,
			"OpenAI rejected this request and did not say which value it objected to. Retrying will not " +
				"change that. Check the workspace ID against the ChatGPT admin console — it is the only " +
				"value here that you supply — and if it is right, tell us: the rest of the request is ours."
	}
	if !errors.As(err, &syncErr) || syncErr.Kind != sdk.ErrKindAuth {
		// Deliberately NOT "could not reach OpenAI". This branch is every unclassified
		// failure, and most of them did reach OpenAI — a status this function has no case
		// for lands here too. Asserting a transport failure sends the reader at the network
		// when the cause is a response we did not expect, which is a diagnosis this message
		// is in no position to make.
		return codes.Unavailable, "OpenAI did not give an answer we could act on. Try again in a moment."
	}
	if syncErr.HTTPStatus == http.StatusForbidden {
		return codes.InvalidArgument, fmt.Sprintf(
			"this key is missing the %s scope. A key's scopes are fixed when it is created, so this one "+
				"cannot be updated — create a new key with that scope, then paste it here.", codexProbeScope)
	}
	return codes.InvalidArgument,
		"this key was rejected by OpenAI. Check it was copied in full, and that it is an admin key " +
			"created for this workspace."
}

// ValidateCodexAnalyticsKey resolves a pasted key against a named workspace without
// persisting, so a host's UI can ask an administrator to recognise their colleagues
// before anything is stored.
func (s *Service) ValidateCodexAnalyticsKey(
	ctx context.Context, req *connintv1.ValidateCodexAnalyticsKeyRequest,
) (*connintv1.ValidateCodexAnalyticsKeyResponse, error) {
	resolved, err := s.resolveCodexWorkspace(ctx, orgid.Canonical(req.GetOrgId()), req.GetApiKey(), req.GetWorkspaceId())
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	return &connintv1.ValidateCodexAnalyticsKeyResponse{
		WorkspaceId:         req.GetWorkspaceId(),
		DeveloperEmails:     resolved.developerEmails,
		DeveloperCount:      int32(resolved.developerCount), //nolint:gosec // G115: a probe page of twenty-five rows cannot overflow int32
		DevelopersTruncated: resolved.truncated,
	}, nil
}

// SubmitCodexAnalyticsKey persists a confirmed key and workspace and enables the
// connector.
//
// It re-probes the key against the submitted workspace before saving. The check proves the
// key can read that workspace. It does not prove it is the workspace the administrator
// confirmed.
func (s *Service) SubmitCodexAnalyticsKey(
	ctx context.Context, req *connintv1.SubmitCodexAnalyticsKeyRequest,
) (*connintv1.SubmitCodexAnalyticsKeyResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	workspaceID := req.GetWorkspaceId()
	resolved, err := s.resolveCodexWorkspace(ctx, orgID, req.GetApiKey(), workspaceID)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	// The credential and the workspace are written in ONE row-locked transaction, and that
	// is not tidiness — it is what stops two overlapping submits from composing a pair
	// neither of them sent. With separate writes, A patches its workspace, B patches its
	// workspace, B saves its key, then A saves its key while preserving B's workspace; the
	// row ends holding A's credential against B's workspace, and both administrators are
	// told they succeeded. Since the scheduler uses the stored workspace as the request
	// path, that pair quietly ingests another workspace's usage under this tenant.
	//
	// The mutator merges rather than replaces, so a tuned lookback — and any settings key
	// this package does not own — survives a reconnect.
	if err := s.connectorDAO.SaveCredentialsEnableWithSettings(ctx, &sdk.ConnectorConfig{
		ConnectorID:    connectorIDCodex,
		OrganizationID: orgID,
		Credentials:    resolved.credentials,
	}, func(current json.RawMessage) (json.RawMessage, error) {
		return mergeCodexWorkspace(current, workspaceID)
	}); err != nil {
		if isClaimedByAnotherOrganization(err, "uniq_codex_workspace_per_org") {
			logger.WarnWithCtx(ctx, "SubmitCodexAnalyticsKey: workspace already connected to another organization",
				zap.String(logger.OrgID, orgID))
			return nil, status.Error(codes.AlreadyExists, "this Codex workspace is already connected to another organization")
		}
		logger.Error(ctx, "SubmitCodexAnalyticsKey: save connector config",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "save connector config: %v", err)
	}

	// Without this, re-pasting a key after the previous one was revoked leaves the
	// connector presenting as broken even though it now works.
	s.resetConnectorHealth(ctx, orgID, connectorIDCodex)

	logger.Info(ctx, "codex setup: connection saved",
		zap.String(logger.OrgID, orgID), zap.Int("developer_count", resolved.developerCount))
	return &connintv1.SubmitCodexAnalyticsKeyResponse{}, nil
}

// mergeCodexWorkspace sets the confirmed workspace on the stored settings, leaving every
// other key untouched.
//
// Decoded as a map rather than through the Settings struct so a key this package does not
// know about survives the round trip; narrowing to the struct would silently drop whatever
// another writer owns.
//
// The nil-map check is the sharp edge. The settings column is `jsonb NOT NULL`, which admits
// the JSON literal `null` — a script or a repair can write one — and unmarshalling `null`
// into a map SUCCEEDS while leaving the map nil. Assigning into it then panics rather than
// returning the settings error this function exists to give.
func mergeCodexWorkspace(current json.RawMessage, workspaceID string) (json.RawMessage, error) {
	stored := map[string]json.RawMessage{}
	if len(current) > 0 {
		if err := json.Unmarshal(current, &stored); err != nil {
			// Settings we cannot read must not be silently replaced — that would discard the
			// lookback this path exists to preserve.
			return nil, fmt.Errorf("unmarshalling stored codex settings: %w", err)
		}
		if stored == nil {
			return nil, fmt.Errorf("stored codex settings are JSON null, not an object; refusing to write over a row we cannot read")
		}
	}
	encoded, err := json.Marshal(workspaceID)
	if err != nil {
		return nil, fmt.Errorf("marshalling codex workspace id: %w", err)
	}
	stored["workspace_id"] = encoded
	out, err := json.Marshal(stored)
	if err != nil {
		return nil, fmt.Errorf("marshalling codex settings: %w", err)
	}
	return out, nil
}
