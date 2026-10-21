// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// HandleWebhook verifies the webhook signature and routes the event to the
// appropriate handler. Returns an error if verification fails.
func (s *Service) HandleWebhook(ctx context.Context, req *connintv1.HandleWebhookRequest) (*emptypb.Empty, error) {
	body := req.GetBody()
	sigHeader := req.GetSignatureHeader()
	eventType := req.GetEventType()

	if !s.verifyWebhookSignature(body, sigHeader) {
		logger.Error(ctx, "HandleWebhook: invalid webhook signature",
			zap.String("event_type", eventType))
		return nil, status.Error(codes.PermissionDenied, "invalid webhook signature")
	}

	switch eventType {
	case "installation":
		if err := s.handleInstallationEvent(ctx, body); err != nil {
			if _, ok := status.FromError(err); ok {
				return nil, err //nolint:wrapcheck // already a status
			}
			// Not acknowledged: the delivery shows as failed in the App's
			// settings. GitHub does not redeliver it by itself.
			return nil, status.Error(codes.Internal, err.Error())
		}
	case "installation_repositories":
		s.handleInstallationReposEvent(ctx, body)
	default:
		logger.Info(ctx, "webhook: ignoring event type", zap.String("event_type", eventType))
	}

	return &emptypb.Empty{}, nil
}

// verifyWebhookSignature checks the HMAC-SHA256 signature of a webhook payload
// against the configured webhook secret.
func (s *Service) verifyWebhookSignature(body []byte, sigHeader string) bool {
	return verifySignature(s.cfg.GitHubAppWebhookSecret, body, sigHeader)
}

// handleInstallationEvent processes a GitHub App installation lifecycle event.
// It acts on deleted, suspend and unsuspend, and acknowledges any other action
// untouched. A row matches on the installation id its settings hold in plain
// text, so a row whose credentials do not decrypt is still matched (through
// the list's UndecryptableRowsError rows). An event
// that matches no row is refused with NotFound, not acknowledged, and so is
// one whose installation could be among rows that cannot be read
// (Unavailable): acknowledging it would drop a suspend or delete that GitHub
// will not send again. An unsuspend re-enables a row only under the connect
// path's tenant gate (see unsuspendAllowed). A store read or write that fails
// returns an error, and a body that does not parse is InvalidArgument.
func (s *Service) handleInstallationEvent(ctx context.Context, body []byte) error {
	var evt installationEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		logger.Error(ctx, "webhook: failed to parse installation event", zap.Error(err))
		return status.Error(codes.InvalidArgument, "installation event does not parse") //nolint:wrapcheck // a status for the RPC
	}

	installationID := fmt.Sprintf("%d", evt.Installation.ID)
	logger.Info(ctx, "webhook: installation event",
		zap.String("action", evt.Action),
		zap.String("installation_id", installationID),
		zap.String("account", evt.Installation.Account.Login))

	var enable bool
	switch evt.Action {
	case "deleted", "suspend":
		enable = false
	case "unsuspend":
		enable = true
	default:
		logger.Info(ctx, "webhook: ignoring installation action", zap.String("action", evt.Action))
		return nil
	}

	configs, err := s.connectorDAO.ListByConnector(ctx, sdk.ConnectorID(connectorIDGitHub))
	var undecryptable *store.UndecryptableRowsError
	switch {
	case errors.As(err, &undecryptable):
		for _, org := range undecryptable.OrgIDs {
			logger.Error(ctx, "webhook: github config could not be read",
				zap.String(logger.OrgID, org), zap.Error(undecryptable.Err))
		}
	case err != nil:
		logger.Error(ctx, "webhook: failed to list github configs", zap.Error(err))
		return fmt.Errorf("list github configs: %w", err)
	}

	// Every live row naming the installation gets the event. SaveInstallation
	// refuses a second organization's claim, so normally there is one; acting on
	// all of them leaves none enabled on a deleted installation. The match is on
	// settings.installation_id, which is stored in plain text (the unique index
	// on it holds the claim), and on the credentials' installation_id for a row
	// whose settings do not carry it, such as one written by `config put`.
	var targets []*sdk.ConnectorConfig
	for _, cfg := range configs {
		if rowInstallationID(cfg) == installationID {
			targets = append(targets, cfg)
		}
	}
	// A row whose credentials do not decrypt still has its settings, which
	// are plain text: match it on them, so a suspend or delete still lands.
	// A row without readable settings cannot be matched and is left to the
	// refusal below.
	if undecryptable != nil {
		for _, row := range undecryptable.Rows {
			if row.Settings != nil && rowInstallationID(&sdk.ConnectorConfig{Settings: row.Settings}) == installationID {
				targets = append(targets, &sdk.ConnectorConfig{OrganizationID: row.OrganizationID, ConnectorID: row.ConnectorID})
			}
		}
	}

	if len(targets) == 0 {
		if undecryptable != nil {
			logger.Error(ctx, "webhook: installation matches no readable config, and some configs could not be read; refusing the event",
				zap.String("installation_id", installationID), zap.Int("unreadable", len(undecryptable.OrgIDs)))
			return status.Errorf(codes.Unavailable, "installation %s matches no readable connector, and %d could not be read", installationID, len(undecryptable.OrgIDs))
		}
		logger.WarnWithCtx(ctx, "webhook: no connector for installation; refusing the event", zap.String("installation_id", installationID))
		return status.Errorf(codes.NotFound, "no connector for installation %s", installationID)
	}

	for _, target := range targets {
		if enable {
			ok, err := s.unsuspendAllowed(ctx, target.OrganizationID)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		// Only enabled is written, so a credential or settings write that
		// landed after the list above survives.
		if err := s.connectorDAO.SetEnabled(ctx, target.OrganizationID, target.ConnectorID, enable); err != nil {
			if enable {
				logger.Error(ctx, "webhook: failed to re-enable connector",
					zap.String(logger.OrgID, target.OrganizationID), zap.Error(err))
				return fmt.Errorf("re-enable connector for installation %s: %w", installationID, err)
			}
			logger.Error(ctx, "webhook: failed to disable connector",
				zap.String(logger.OrgID, target.OrganizationID), zap.Error(err))
			return fmt.Errorf("disable connector for installation %s: %w", installationID, err)
		}
	}
	// A row that could not be read and whose settings name no installation
	// may hold this one in its credentials: the event cannot be known to have
	// reached every row, so it is refused for redelivery rather than acked.
	if undecryptable != nil {
		for _, row := range undecryptable.Rows {
			if row.Settings == nil || installationIDIn(row.Settings) == "" {
				logger.Error(ctx, "webhook: an unreadable config may also hold the installation; refusing the event after applying it to the matched rows",
					zap.String("installation_id", installationID), zap.String(logger.OrgID, row.OrganizationID))
				return status.Errorf(codes.Unavailable, "installation %s: a connector that could not be read may also hold it", installationID)
			}
		}
	}
	return nil
}

// unsuspendAllowed applies the connect path's tenant gate to an unsuspend:
// only an organization tenant's connector is re-enabled, as only an
// organization install lands enabled (SaveInstallation). An individual tenant,
// or one whose intent is unknown, stays disabled until it confirms its
// repositories again. A tenant lookup that fails refuses the event
// (Unavailable) rather than guessing.
func (s *Service) unsuspendAllowed(ctx context.Context, orgID string) (bool, error) {
	if s.tenants == nil {
		logger.WarnWithCtx(ctx, "webhook: no tenant directory; unsuspend leaves the connector disabled", zap.String(logger.OrgID, orgID))
		return false, nil
	}
	kind, err := s.tenants.TenantType(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "webhook: resolve tenant type for unsuspend failed", zap.String(logger.OrgID, orgID), zap.Error(err))
		return false, status.Error(codes.Unavailable, "could not resolve tenant type for unsuspend")
	}
	if kind != sdk.TenantKindOrganization {
		logger.Info(ctx, "webhook: unsuspend leaves a non-organization tenant's connector disabled until it confirms its repositories",
			zap.String(logger.OrgID, orgID), zap.String("tenant_type", kind.String()))
		return false, nil
	}
	return true, nil
}

// rowInstallationID returns the installation id a GitHub row names: the one
// its settings hold, else the one its credentials hold, else "". The id may
// be stored as a JSON string or a JSON number.
func rowInstallationID(cfg *sdk.ConnectorConfig) string {
	for _, raw := range []json.RawMessage{cfg.Settings, cfg.Credentials} {
		if id := installationIDIn(raw); id != "" {
			return id
		}
	}
	return ""
}

// installationIDIn reads installation_id from a JSON object, as a string or
// an integer number, or "" when it is absent or neither.
func installationIDIn(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var v struct {
		InstallationID json.RawMessage `json:"installation_id"`
	}
	if json.Unmarshal(raw, &v) != nil || len(v.InstallationID) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(v.InstallationID, &s) == nil {
		return s
	}
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(v.InstallationID))
	dec.UseNumber()
	if dec.Decode(&n) == nil {
		if i, err := n.Int64(); err == nil {
			return strconv.FormatInt(i, 10)
		}
	}
	return ""
}

// handleInstallationReposEvent processes a GitHub App installation_repositories event.
func (s *Service) handleInstallationReposEvent(ctx context.Context, body []byte) {
	var evt installationReposEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		logger.Error(ctx, "webhook: failed to parse installation_repositories event", zap.Error(err))
		return
	}

	installationID := fmt.Sprintf("%d", evt.Installation.ID)
	logger.Info(ctx, "webhook: installation_repositories event",
		zap.String("action", evt.Action),
		zap.String("installation_id", installationID),
		zap.Int("added", len(evt.RepositoriesAdded)),
		zap.Int("removed", len(evt.RepositoriesRemoved)))
}

// verifySignature performs constant-time HMAC-SHA256 comparison against
// the X-Hub-Signature-256 header value sent by GitHub.
func verifySignature(secret string, payload []byte, sigHeader string) bool {
	if secret == "" || sigHeader == "" {
		return false
	}
	if !strings.HasPrefix(sigHeader, "sha256=") {
		return false
	}
	sigHex := strings.TrimPrefix(sigHeader, "sha256=")
	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		// Malformed signature header — reject. Garbage/attacker input is expected here.
		logger.Warn("webhook: malformed signature header, rejecting", zap.Error(err))
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	expected := mac.Sum(nil)
	return hmac.Equal(sig, expected)
}

type installationEvent struct {
	Action       string `json:"action"`
	Installation struct {
		ID      int64 `json:"id"`
		Account struct {
			Login string `json:"login"`
		} `json:"account"`
	} `json:"installation"`
}

type installationReposEvent struct {
	Action       string `json:"action"`
	Installation struct {
		ID                  int64  `json:"id"`
		RepositorySelection string `json:"repository_selection"`
		Account             struct {
			Login string `json:"login"`
		} `json:"account"`
	} `json:"installation"`
	RepositoriesAdded   []repoInfo `json:"repositories_added"`
	RepositoriesRemoved []repoInfo `json:"repositories_removed"`
}

type repoInfo struct {
	FullName string `json:"full_name"`
}
