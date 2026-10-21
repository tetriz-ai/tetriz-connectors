// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

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

const defaultGitLabBaseURL = "https://gitlab.com"

// GitLabOAuthClient handles OAuth token exchange and group API calls
// during admin setup. It does NOT handle token refresh at sync time —
// that's the VendorGateway's responsibility (see internal/gateway/gitlab/).
//
//go:generate go tool mockgen -source=gitlab_client.go -destination=mock_gitlab_client_test.go -package=integrations
type GitLabOAuthClient interface {
	// ExchangeAuthCode exchanges an authorization code for access + refresh tokens.
	ExchangeAuthCode(ctx context.Context, code string) (*GitLabTokenResponse, error)
	// ExchangeRefreshToken exchanges a refresh token for a new access token.
	ExchangeRefreshToken(ctx context.Context, refreshToken string) (string, error)
	// FetchGroups returns all top-level groups the authenticated user has Developer+ access to.
	FetchGroups(ctx context.Context, accessToken string) ([]GitLabGroup, error)
	// FetchCurrentUser returns the authenticated user's ID, username, and display name.
	FetchCurrentUser(ctx context.Context, accessToken string) (*GitLabUser, error)
	// RevokeToken revokes a refresh token. Best-effort: callers should log but not fail on error.
	RevokeToken(ctx context.Context, refreshToken string) error
	// RegisterGroupWebhook registers a webhook on a GitLab group. Returns the hook ID.
	RegisterGroupWebhook(ctx context.Context, accessToken string, groupID int64, webhookURL, secret string) (int64, error)
	// DeleteGroupWebhook removes a registered group webhook.
	DeleteGroupWebhook(ctx context.Context, accessToken string, groupID, hookID int64) error
}

// GitLabTokenResponse holds the tokens returned by the GitLab token endpoint.
type GitLabTokenResponse struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int
	Scope        string
}

// GitLabGroup represents a GitLab group returned by the /api/v4/groups endpoint.
type GitLabGroup struct {
	ID        int64
	Path      string
	FullPath  string
	Name      string
	AvatarURL string
}

// GitLabUser represents the authenticated GitLab user.
type GitLabUser struct {
	ID       int64
	Username string
	Name     string // display name
}

type gitLabOAuthClientImpl struct {
	clientID     string
	clientSecret string
	callbackURL  string
	baseURL      string
	httpClient   *http.Client
}

// GitLabOAuthClientOption configures the GitLab OAuth client.
type GitLabOAuthClientOption func(*gitLabOAuthClientImpl)

// WithGitLabBaseURL overrides the GitLab base URL (for testing).
func WithGitLabBaseURL(baseURL string) GitLabOAuthClientOption {
	return func(c *gitLabOAuthClientImpl) {
		c.baseURL = baseURL
	}
}

// NewGitLabOAuthClient creates a new GitLabOAuthClient for setup-time OAuth flows.
func NewGitLabOAuthClient(clientID, clientSecret, callbackURL string, opts ...GitLabOAuthClientOption) GitLabOAuthClient {
	c := &gitLabOAuthClientImpl{
		clientID:     clientID,
		clientSecret: clientSecret,
		callbackURL:  callbackURL,
		baseURL:      defaultGitLabBaseURL,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *gitLabOAuthClientImpl) ExchangeAuthCode(ctx context.Context, code string) (*GitLabTokenResponse, error) {
	form := url.Values{
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
		"code":          {code},
		"grant_type":    {"authorization_code"},
		"redirect_uri":  {c.callbackURL},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/oauth/token",
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token exchange request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return nil, fmt.Errorf("token exchange: %s", body)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		Scope        string `json:"scope"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}

	return &GitLabTokenResponse{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresIn:    tokenResp.ExpiresIn,
		Scope:        tokenResp.Scope,
	}, nil
}

func (c *gitLabOAuthClientImpl) ExchangeRefreshToken(ctx context.Context, refreshToken string) (string, error) {
	form := url.Values{
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/oauth/token",
		strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("refresh token request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return "", fmt.Errorf("refresh token: %s", body)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("decode refresh token response: %w", err)
	}

	return tokenResp.AccessToken, nil
}

func (c *gitLabOAuthClientImpl) RevokeToken(ctx context.Context, refreshToken string) error {
	form := url.Values{
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
		"token":         {refreshToken},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/oauth/revoke",
		strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("revoke token request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return fmt.Errorf("revoke token: %s", body)
	}

	return nil
}

func (c *gitLabOAuthClientImpl) RegisterGroupWebhook(ctx context.Context, accessToken string, groupID int64, webhookURL, secret string) (int64, error) {
	body, err := json.Marshal(map[string]interface{}{
		"url":                     webhookURL,
		"token":                   secret,
		"push_events":             true,
		"merge_requests_events":   true,
		"note_events":             true,
		"pipeline_events":         true,
		"tag_push_events":         true,
		"enable_ssl_verification": true,
	})
	if err != nil {
		return 0, fmt.Errorf("marshal webhook body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v4/groups/%d/hooks", c.baseURL, groupID),
		strings.NewReader(string(body)))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("register webhook request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		respBody := httputil.ReadErrorResponse(ctx, resp)
		return 0, fmt.Errorf("register webhook: %s", respBody)
	}

	var hookResp struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&hookResp); err != nil {
		return 0, fmt.Errorf("decode webhook response: %w", err)
	}

	return hookResp.ID, nil
}

func (c *gitLabOAuthClientImpl) DeleteGroupWebhook(ctx context.Context, accessToken string, groupID, hookID int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("%s/api/v4/groups/%d/hooks/%d", c.baseURL, groupID, hookID),
		nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete webhook request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return fmt.Errorf("delete webhook: %s", body)
	}

	return nil
}

func (c *gitLabOAuthClientImpl) FetchCurrentUser(ctx context.Context, accessToken string) (*GitLabUser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v4/user", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("user request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return nil, fmt.Errorf("user: %s", body)
	}

	var user struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Name     string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, fmt.Errorf("decode user response: %w", err)
	}

	return &GitLabUser{ID: user.ID, Username: user.Username, Name: user.Name}, nil
}

func (c *gitLabOAuthClientImpl) FetchGroups(ctx context.Context, accessToken string) ([]GitLabGroup, error) {
	var all []GitLabGroup
	// top_level_only=true is an undocumented GitLab filter that restricts results
	// to root groups (no parent). Intentional: connecting at the group root lets
	// the connector enumerate all sub-groups and projects underneath. Admins with
	// access only to a subgroup (not the root) will see no groups here.
	nextURL := c.baseURL + "/api/v4/groups?min_access_level=30&per_page=100&top_level_only=true"

	for nextURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURL, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("groups request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body := httputil.ReadErrorResponse(ctx, resp)
			if cerr := resp.Body.Close(); cerr != nil {
				logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
			}
			return nil, fmt.Errorf("groups: %s", body)
		}

		var groups []struct {
			ID        int64  `json:"id"`
			Path      string `json:"path"`
			FullPath  string `json:"full_path"`
			Name      string `json:"name"`
			AvatarURL string `json:"avatar_url"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&groups); err != nil {
			if cerr := resp.Body.Close(); cerr != nil {
				logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
			}
			return nil, fmt.Errorf("decode groups response: %w", err)
		}
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
		}

		for _, g := range groups {
			all = append(all, GitLabGroup{
				ID:        g.ID,
				Path:      g.Path,
				FullPath:  g.FullPath,
				Name:      g.Name,
				AvatarURL: g.AvatarURL,
			})
		}

		next := parseLinkHeaderNext(resp.Header.Get("Link"))
		// A compromised GitLab instance can put any URL in the Link header; following
		// it blindly would re-send the Bearer token to an attacker-controlled host
		// (SSRF + token exfiltration). Only follow same-origin (scheme+host) next links.
		if next != "" {
			if err := sameOrigin(c.baseURL, next); err != nil {
				return nil, fmt.Errorf("groups: refusing to follow cross-origin pagination link: %w", err)
			}
		}
		nextURL = next
	}

	return all, nil
}

// sameOrigin returns nil when candidate shares base's scheme and host, else an
// error describing the mismatch. base must be an absolute URL; a parse failure,
// non-absolute base, or scheme/host divergence is a rejection (the caller must
// not follow candidate).
func sameOrigin(base, candidate string) error {
	b, err := url.Parse(base)
	if err != nil {
		return fmt.Errorf("parse base url %q: %w", base, err)
	}
	if b.Scheme == "" || b.Host == "" {
		return fmt.Errorf("base url %q is not absolute", base)
	}
	c, err := url.Parse(candidate)
	if err != nil {
		return fmt.Errorf("parse candidate url %q: %w", candidate, err)
	}
	if c.Scheme != b.Scheme || c.Host != b.Host {
		return fmt.Errorf("cross-origin: candidate %q differs from base %q", candidate, base)
	}
	return nil
}

// parseLinkHeaderNext parses the Link header and returns the URL for rel="next",
// or an empty string if not present.
// Example header: `<https://gitlab.com/api/v4/groups?page=2>; rel="next", <...>; rel="last"`
func parseLinkHeaderNext(linkHeader string) string {
	if linkHeader == "" {
		return ""
	}
	for _, part := range strings.Split(linkHeader, ",") {
		part = strings.TrimSpace(part)
		segments := strings.Split(part, ";")
		if len(segments) < 2 {
			continue
		}
		linkURL := strings.TrimSpace(segments[0])
		linkURL = strings.Trim(linkURL, "<>")
		for _, seg := range segments[1:] {
			seg = strings.TrimSpace(seg)
			if seg == `rel="next"` {
				return linkURL
			}
		}
	}
	return ""
}
