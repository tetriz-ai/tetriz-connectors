// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

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

const (
	defaultAtlassianBaseURL    = "https://auth.atlassian.com"
	defaultAtlassianAPIBaseURL = "https://api.atlassian.com"
)

// OAuthClient handles OAuth token exchange and cloud site discovery
// during admin setup. It does NOT handle token refresh — that's the
// VendorGateway's responsibility at sync time (see internal/gateway/jira/).
//
//go:generate go tool mockgen -source=client.go -destination=../../../internal/mocks/auth/integrations/jira/mock_client.go -package=mocks
type OAuthClient interface {
	// ExchangeAuthCode exchanges an authorization code for access + refresh tokens.
	ExchangeAuthCode(ctx context.Context, code string) (*TokenResponse, error)
	// FetchAccessibleResources returns all Jira Cloud sites the authenticated user can access.
	FetchAccessibleResources(ctx context.Context, accessToken string) ([]CloudSite, error)
	// FetchCurrentUser returns the display name of the authenticated Atlassian user.
	FetchCurrentUser(ctx context.Context, accessToken string) (string, error)
	// RevokeToken revokes a refresh token. Best-effort: callers should log but not fail on error.
	RevokeToken(ctx context.Context, refreshToken string) error
}

type TokenResponse struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int // seconds
	Scopes       string
}

type CloudSite struct {
	ID   string // cloud site ID (e.g., "abc123-def456")
	Name string // display name (e.g., "My Jira Site")
	URL  string // site URL (e.g., "https://myorg.atlassian.net")
}

type oAuthClientImpl struct {
	clientID     string
	clientSecret string
	callbackURL  string
	authBaseURL  string
	apiBaseURL   string
	httpClient   *http.Client
}

// Option configures the Jira OAuth client.
type Option func(*oAuthClientImpl)

// WithAuthBaseURL overrides the Atlassian auth base URL (for testing).
func WithAuthBaseURL(baseURL string) Option {
	return func(c *oAuthClientImpl) {
		c.authBaseURL = baseURL
	}
}

// WithAPIBaseURL overrides the Atlassian API base URL (for testing).
func WithAPIBaseURL(baseURL string) Option {
	return func(c *oAuthClientImpl) {
		c.apiBaseURL = baseURL
	}
}

// NewOAuthClient creates a new OAuthClient for setup-time OAuth flows.
func NewOAuthClient(clientID, clientSecret, callbackURL string, opts ...Option) OAuthClient {
	c := &oAuthClientImpl{
		clientID:     clientID,
		clientSecret: clientSecret,
		callbackURL:  callbackURL,
		authBaseURL:  defaultAtlassianBaseURL,
		apiBaseURL:   defaultAtlassianAPIBaseURL,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *oAuthClientImpl) ExchangeAuthCode(ctx context.Context, code string) (*TokenResponse, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
		"code":          {code},
		"redirect_uri":  {c.callbackURL},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.authBaseURL+"/oauth/token",
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
			logger.WarnWithCtx(ctx, "jira: close response body", zap.Error(cerr))
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

	return &TokenResponse{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresIn:    tokenResp.ExpiresIn,
		Scopes:       tokenResp.Scope,
	}, nil
}

func (c *oAuthClientImpl) FetchAccessibleResources(ctx context.Context, accessToken string) ([]CloudSite, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.apiBaseURL+"/oauth/token/accessible-resources", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("accessible resources request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "jira: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return nil, fmt.Errorf("accessible resources: %s", body)
	}

	var resources []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		URL  string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resources); err != nil {
		return nil, fmt.Errorf("decode accessible resources: %w", err)
	}

	sites := make([]CloudSite, len(resources))
	for i, r := range resources {
		sites[i] = CloudSite{
			ID:   r.ID,
			Name: r.Name,
			URL:  r.URL,
		}
	}

	return sites, nil
}

func (c *oAuthClientImpl) FetchCurrentUser(ctx context.Context, accessToken string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.apiBaseURL+"/me", nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch current user request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "jira: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return "", fmt.Errorf("fetch current user: %s", body)
	}

	var user struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return "", fmt.Errorf("decode current user: %w", err)
	}

	return user.Name, nil
}

// RevokeToken revokes a refresh token via Atlassian's token revocation endpoint (RFC 7009).
// Atlassian's OAuth server (Auth0-based) exposes /oauth/revoke with Basic auth (client_id:client_secret).
func (c *oAuthClientImpl) RevokeToken(ctx context.Context, refreshToken string) error {
	form := url.Values{
		"token":           {refreshToken},
		"token_type_hint": {"refresh_token"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.authBaseURL+"/oauth/revoke",
		strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(c.clientID, c.clientSecret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("revoke token request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "jira: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return fmt.Errorf("revoke token: %s", body)
	}

	return nil
}
