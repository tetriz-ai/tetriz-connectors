// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/httputil"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// Slack API endpoints. Variables (not constants) to allow test overrides.
var (
	slackOAuthAccessURL = "https://slack.com/api/oauth.v2.access"
	slackAuthRevokeURL  = "https://slack.com/api/auth.revoke"
	slackUsersInfoURL   = "https://slack.com/api/users.info"
	slackTeamInfoURL    = "https://slack.com/api/team.info"
)

// SlackAppConfig holds the Slack App parameters needed for OAuth.
type SlackAppConfig struct {
	ClientID     string
	ClientSecret string
	RedirectURI  string
	Scopes       string // comma-separated scopes for the OAuth authorize URL
	// APIBaseURL overrides the Slack Web API base (default https://slack.com/api).
	// Set it to a mock so the connect flow runs headlessly; leave it empty
	// against the real vendor.
	APIBaseURL string
}

// SlackOAuthResponse holds the relevant fields from the Slack oauth.v2.access response.
type SlackOAuthResponse struct {
	UserToken string
	TeamID    string
	TeamName  string
	UserID    string
	Scope     string
	// BotToken is the workspace bot token (top-level access_token, xoxb-…), used by
	// the slack_comms connector's app; the data-connector app uses UserToken.
	BotToken string
	BotScope string
}

// SlackUserInfo holds the admin-relevant fields from the Slack users.info response.
type SlackUserInfo struct {
	IsAdmin bool
}

// SlackTeamInfo holds the workspace-level fields from the Slack team.info response.
type SlackTeamInfo struct {
	IconURL string // largest available workspace icon URL
}

// SlackAppClient manages Slack OAuth token exchange and revocation.
//
//go:generate go tool mockgen -source=client.go -destination=../../../internal/mocks/auth/integrations/slack/mock_client.go -package=mocks
type SlackAppClient interface {
	ExchangeOAuthCode(ctx context.Context, code string) (*SlackOAuthResponse, error)
	RevokeToken(ctx context.Context, token string) error
	// GetUserInfo calls users.info with the given user token to check workspace admin status.
	GetUserInfo(ctx context.Context, token, userID string) (*SlackUserInfo, error)
	// GetTeamInfo calls team.info to retrieve the workspace icon URL.
	GetTeamInfo(ctx context.Context, token string) (*SlackTeamInfo, error)
}

// slackAppClient is the production implementation of SlackAppClient.
type slackAppClient struct {
	cfg            *SlackAppConfig
	httpClient     *http.Client
	oauthAccessURL string
	authRevokeURL  string
	usersInfoURL   string
	teamInfoURL    string
}

// NewSlackAppClient creates a SlackAppClient. Returns nil (not an error) when
// cfg is nil — callers check for nil and return a FailedPrecondition error at
// request time rather than failing at startup. When cfg.APIBaseURL is set, the
// Slack Web API endpoints are derived from it (local mock); otherwise the real
// slack.com endpoints are used.
func NewSlackAppClient(cfg *SlackAppConfig) SlackAppClient {
	if cfg == nil || cfg.ClientSecret == "" {
		return nil
	}
	c := &slackAppClient{
		cfg:            cfg,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		oauthAccessURL: slackOAuthAccessURL,
		authRevokeURL:  slackAuthRevokeURL,
		usersInfoURL:   slackUsersInfoURL,
		teamInfoURL:    slackTeamInfoURL,
	}
	if base := strings.TrimRight(cfg.APIBaseURL, "/"); base != "" {
		c.oauthAccessURL = base + "/oauth.v2.access"
		c.authRevokeURL = base + "/auth.revoke"
		c.usersInfoURL = base + "/users.info"
		c.teamInfoURL = base + "/team.info"
	}
	return c
}

// oauthV2AccessResponse mirrors the Slack oauth.v2.access JSON response.
type oauthV2AccessResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	// Top-level access_token/scope are the BOT token + bot scopes (xoxb-…).
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	Team        struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"team"`
	AuthedUser struct {
		ID          string `json:"id"`
		AccessToken string `json:"access_token"`
		Scope       string `json:"scope"`
	} `json:"authed_user"`
}

// ExchangeOAuthCode calls Slack's oauth.v2.access endpoint to exchange an
// authorization code for a bot access token.
func (c *slackAppClient) ExchangeOAuthCode(ctx context.Context, code string) (*SlackOAuthResponse, error) {
	form := url.Values{
		"client_id":     {c.cfg.ClientID},
		"client_secret": {c.cfg.ClientSecret},
		"code":          {code},
		"redirect_uri":  {c.cfg.RedirectURI},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.oauthAccessURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("slack oauth: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "ExchangeOAuthCode: HTTP request failed", zap.Error(err))
		return nil, fmt.Errorf("slack oauth: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "slack: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		logger.Error(ctx, "ExchangeOAuthCode: non-200 response",
			zap.Stringer("response", body))
		return nil, fmt.Errorf("slack oauth: status %d", resp.StatusCode)
	}

	var oauthResp oauthV2AccessResponse
	if err := json.NewDecoder(resp.Body).Decode(&oauthResp); err != nil {
		return nil, fmt.Errorf("slack oauth: decode response: %w", err)
	}

	if !oauthResp.OK {
		logger.Error(ctx, "ExchangeOAuthCode: Slack API error", zap.String("error", oauthResp.Error))
		return nil, fmt.Errorf("slack oauth: %s", oauthResp.Error)
	}

	return &SlackOAuthResponse{
		UserToken: oauthResp.AuthedUser.AccessToken,
		TeamID:    oauthResp.Team.ID,
		TeamName:  oauthResp.Team.Name,
		UserID:    oauthResp.AuthedUser.ID,
		Scope:     oauthResp.AuthedUser.Scope,
		BotToken:  oauthResp.AccessToken,
		BotScope:  oauthResp.Scope,
	}, nil
}

// authRevokeResponse mirrors the Slack auth.revoke JSON response.
type authRevokeResponse struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Revoked bool   `json:"revoked"`
}

// RevokeToken revokes a Slack bot token by calling the auth.revoke API.
func (c *slackAppClient) RevokeToken(ctx context.Context, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.authRevokeURL, nil)
	if err != nil {
		return fmt.Errorf("slack auth.revoke: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "RevokeToken: HTTP request failed", zap.Error(err))
		return fmt.Errorf("slack auth.revoke: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "slack: close response body", zap.Error(cerr))
		}
	}()

	var revokeResp authRevokeResponse
	if err := json.NewDecoder(resp.Body).Decode(&revokeResp); err != nil {
		return fmt.Errorf("slack auth.revoke: decode response: %w", err)
	}

	if !revokeResp.OK {
		logger.Error(ctx, "RevokeToken: Slack auth.revoke failed", zap.String("error", revokeResp.Error))
		return fmt.Errorf("slack auth.revoke: %s", revokeResp.Error)
	}
	if !revokeResp.Revoked {
		return fmt.Errorf("slack auth.revoke: token not revoked")
	}
	return nil
}

// usersInfoResponse mirrors the relevant fields from the Slack users.info JSON response.
type usersInfoResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	User  struct {
		IsAdmin bool `json:"is_admin"`
	} `json:"user"`
}

// GetUserInfo calls Slack's users.info endpoint to retrieve the admin status of the
// authorizing user. token is the user token (xoxp-...) and userID is the Slack user ID.
func (c *slackAppClient) GetUserInfo(ctx context.Context, token, userID string) (*SlackUserInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.usersInfoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("slack users.info: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	q := req.URL.Query()
	q.Set("user", userID)
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "GetUserInfo: HTTP request failed", zap.Error(err))
		return nil, fmt.Errorf("slack users.info: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "slack: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		logger.Error(ctx, "GetUserInfo: non-200 response",
			zap.Stringer("response", body))
		return nil, fmt.Errorf("slack users.info: status %d", resp.StatusCode)
	}

	var infoResp usersInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&infoResp); err != nil {
		return nil, fmt.Errorf("slack users.info: decode response: %w", err)
	}

	if !infoResp.OK {
		logger.Error(ctx, "GetUserInfo: Slack API error", zap.String("error", infoResp.Error))
		return nil, fmt.Errorf("slack users.info: %s", infoResp.Error)
	}

	return &SlackUserInfo{
		IsAdmin: infoResp.User.IsAdmin,
	}, nil
}

// teamInfoResponse mirrors the relevant fields from the Slack team.info JSON response.
type teamInfoResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Team  struct {
		Icon struct {
			Image132     string `json:"image_132"`
			Image230     string `json:"image_230"`
			ImageDefault bool   `json:"image_default"`
		} `json:"icon"`
	} `json:"team"`
}

// GetTeamInfo calls Slack's team.info endpoint to retrieve the workspace icon URL.
func (c *slackAppClient) GetTeamInfo(ctx context.Context, token string) (*SlackTeamInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.teamInfoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("slack team.info: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "GetTeamInfo: HTTP request failed", zap.Error(err))
		return nil, fmt.Errorf("slack team.info: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "slack: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		logger.Error(ctx, "GetTeamInfo: non-200 response",
			zap.Stringer("response", body))
		return nil, fmt.Errorf("slack team.info: status %d", resp.StatusCode)
	}

	var infoResp teamInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&infoResp); err != nil {
		return nil, fmt.Errorf("slack team.info: decode response: %w", err)
	}

	if !infoResp.OK {
		logger.Error(ctx, "GetTeamInfo: Slack API error", zap.String("error", infoResp.Error))
		return nil, fmt.Errorf("slack team.info: %s", infoResp.Error)
	}

	// Prefer larger icon; fall back to smaller.
	iconURL := infoResp.Team.Icon.Image230
	if iconURL == "" {
		iconURL = infoResp.Team.Icon.Image132
	}

	return &SlackTeamInfo{IconURL: iconURL}, nil
}
