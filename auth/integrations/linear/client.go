// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"bytes"
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
	defaultLinearAPIBaseURL  = "https://api.linear.app"
	defaultLinearGraphQLPath = "/graphql"
)

// OAuthClient handles Linear OAuth token exchange and workspace discovery during
// admin setup. Linear access tokens are long-lived; no refresh is needed here.
//
//go:generate go tool mockgen -source=client.go -destination=../../../internal/mocks/auth/integrations/linear/mock_client.go -package=mocks
type OAuthClient interface {
	// ExchangeAuthCode exchanges an authorization code for an access token.
	ExchangeAuthCode(ctx context.Context, code string) (*TokenResponse, error)
	// FetchWorkspaceInfo returns the Linear organization the token belongs to.
	FetchWorkspaceInfo(ctx context.Context, accessToken string) (*WorkspaceInfo, error)
	// FetchCurrentUser returns the display name of the authenticated Linear user.
	FetchCurrentUser(ctx context.Context, accessToken string) (string, error)
	// RevokeToken revokes the access token. Best-effort: callers log but do not fail.
	RevokeToken(ctx context.Context, accessToken string) error
}

// TokenResponse holds tokens returned by Linear's OAuth token endpoint.
type TokenResponse struct {
	AccessToken  string
	RefreshToken string // present when Linear issues a refresh token alongside the access token
	TokenType    string
	Scope        string
}

// WorkspaceInfo holds the Linear organization attributes returned by the
// organization GraphQL query.
type WorkspaceInfo struct {
	ID     string // Linear organization UUID
	Name   string // display name (e.g., "Acme Corp")
	URLKey string // slug used in Linear URLs (e.g., "acme")
}

type oAuthClientImpl struct {
	clientID     string
	clientSecret string
	callbackURL  string
	apiBaseURL   string
	httpClient   *http.Client
}

// Option configures the Linear OAuth client.
type Option func(*oAuthClientImpl)

// WithAPIBaseURL overrides the Linear API base URL (for testing).
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
		apiBaseURL:   defaultLinearAPIBaseURL,
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
		c.apiBaseURL+"/oauth/token",
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
			logger.WarnWithCtx(ctx, "linear: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		// Surface only Linear's structured OAuth error fields, never the raw body
		// verbatim — it could reflect the client_secret/code back into logs.
		body := httputil.ReadErrorResponse(ctx, resp)
		var e struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		if uErr := body.Decode(&e); uErr == nil && e.Error != "" { // best-effort: a malformed error body intentionally falls through to status-only
			return nil, fmt.Errorf("token exchange: status %d: %s: %s", resp.StatusCode, e.Error, e.ErrorDescription)
		}
		return nil, fmt.Errorf("token exchange: status %d", resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		TokenType    string `json:"token_type"`
		Scope        string `json:"scope"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}

	return &TokenResponse{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		TokenType:    tokenResp.TokenType,
		Scope:        tokenResp.Scope,
	}, nil
}

func (c *oAuthClientImpl) FetchWorkspaceInfo(ctx context.Context, accessToken string) (*WorkspaceInfo, error) {
	query := `{ "query": "{ organization { id name urlKey } }" }`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.apiBaseURL+defaultLinearGraphQLPath,
		bytes.NewBufferString(query))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch workspace info request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "linear: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return nil, fmt.Errorf("fetch workspace info: %s", body)
	}

	var gqlResp struct {
		Data struct {
			Organization struct {
				ID     string `json:"id"`
				Name   string `json:"name"`
				URLKey string `json:"urlKey"`
			} `json:"organization"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return nil, fmt.Errorf("decode workspace info: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return nil, fmt.Errorf("workspace info graphql error: %s", gqlResp.Errors[0].Message)
	}

	org := gqlResp.Data.Organization
	return &WorkspaceInfo{
		ID:     org.ID,
		Name:   org.Name,
		URLKey: org.URLKey,
	}, nil
}

func (c *oAuthClientImpl) FetchCurrentUser(ctx context.Context, accessToken string) (string, error) {
	query := `{ "query": "{ viewer { name } }" }`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.apiBaseURL+defaultLinearGraphQLPath,
		bytes.NewBufferString(query))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch current user request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "linear: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return "", fmt.Errorf("fetch current user: %s", body)
	}

	var gqlResp struct {
		Data struct {
			Viewer struct {
				Name string `json:"name"`
			} `json:"viewer"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return "", fmt.Errorf("decode current user: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return "", fmt.Errorf("current user graphql error: %s", gqlResp.Errors[0].Message)
	}

	return gqlResp.Data.Viewer.Name, nil
}

func (c *oAuthClientImpl) RevokeToken(ctx context.Context, accessToken string) error {
	form := url.Values{
		"access_token": {accessToken},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.apiBaseURL+"/oauth/revoke",
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
			logger.WarnWithCtx(ctx, "linear: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		return fmt.Errorf("revoke token: %s", body)
	}

	return nil
}
