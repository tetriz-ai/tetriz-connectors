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

// Bitbucket splits its surface across two hosts, and the split is load-bearing:
// the OAuth authorize/token endpoints live on bitbucket.org, while every
// token-authenticated REST call must go to api.bitbucket.org. A Bearer request
// sent to bitbucket.org is refused with a 401 whose message ("Token is invalid,
// expired, or not supported for this endpoint") names neither the host nor the
// real cause, so it reads as a credential problem rather than a routing one.
const (
	defaultBitbucketOAuthBaseURL = "https://bitbucket.org"
	defaultBitbucketAPIBaseURL   = "https://api.bitbucket.org"
)

// BitbucketOAuthClient handles OAuth token exchange and workspace API calls
// during admin setup. It does NOT handle token refresh — that's the
// VendorGateway's responsibility at sync time (see internal/gateway/bitbucket/).
//
//go:generate go tool mockgen -source=bitbucket_client.go -destination=mock_bitbucket_client_test.go -package=integrations
type BitbucketOAuthClient interface {
	// ExchangeAuthCode exchanges an authorization code for access + refresh tokens.
	ExchangeAuthCode(ctx context.Context, code string) (*BitbucketTokenResponse, error)
	// FetchWorkspaces returns all workspaces the authenticated user can access.
	FetchWorkspaces(ctx context.Context, accessToken string) ([]BitbucketWorkspace, error)
	// FetchCurrentUser returns the authenticated user's UUID and display name.
	FetchCurrentUser(ctx context.Context, accessToken string) (*BitbucketUser, error)
}

// bitbucketAPIError carries the status Bitbucket answered with, so a caller can
// tell a refusal of our credentials — which no retry changes — from a transient
// failure like a timeout, a 5xx or a throttle. Without the status every failure
// looks alike and the admin gets told to stop retrying during an outage.
type bitbucketAPIError struct {
	Op         string // "token exchange", "user", "workspaces"
	StatusCode int
	// Detail is httputil.ErrorMessage of the response: the status, a request
	// id and Bitbucket's error fields, never the raw body.
	Detail string
}

func (e *bitbucketAPIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Op, e.Detail)
}

// Refused reports whether Bitbucket rejected the credentials outright.
func (e *bitbucketAPIError) Refused() bool {
	return e.StatusCode == http.StatusUnauthorized || e.StatusCode == http.StatusForbidden
}

type BitbucketUser struct {
	UUID        string // e.g., "{user-uuid}"
	DisplayName string // e.g., "Alex Doe"
}

type BitbucketTokenResponse struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int // seconds
	Scopes       string
}

type BitbucketWorkspace struct {
	UUID      string // e.g., "{workspace-uuid}"
	Slug      string // URL-friendly name, e.g., "acme"
	Name      string // Display name, e.g., "Acme Corp"
	AvatarURL string // Workspace avatar/logo URL
}

type bitbucketOAuthClientImpl struct {
	clientID     string
	clientSecret string
	callbackURL  string
	oauthBaseURL string
	apiBaseURL   string
	httpClient   *http.Client
}

// BitbucketOAuthClientOption configures the Bitbucket OAuth client.
type BitbucketOAuthClientOption func(*bitbucketOAuthClientImpl)

// WithBitbucketBaseURL points both the OAuth and REST hosts at one base URL, so
// a test can serve the whole flow from a single httptest server.
func WithBitbucketBaseURL(baseURL string) BitbucketOAuthClientOption {
	return func(c *bitbucketOAuthClientImpl) {
		c.oauthBaseURL = baseURL
		c.apiBaseURL = baseURL
	}
}

// NewBitbucketOAuthClient creates a new BitbucketOAuthClient for setup-time OAuth flows.
func NewBitbucketOAuthClient(clientID, clientSecret, callbackURL string, opts ...BitbucketOAuthClientOption) BitbucketOAuthClient {
	c := &bitbucketOAuthClientImpl{
		clientID:     clientID,
		clientSecret: clientSecret,
		callbackURL:  callbackURL,
		oauthBaseURL: defaultBitbucketOAuthBaseURL,
		apiBaseURL:   defaultBitbucketAPIBaseURL,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *bitbucketOAuthClientImpl) ExchangeAuthCode(ctx context.Context, code string) (*BitbucketTokenResponse, error) {
	form := url.Values{
		"grant_type": {"authorization_code"},
		"code":       {code},
	}
	// OAuth requires the token request to repeat the exact redirect_uri sent on
	// the authorize request (RFC 6749 §4.1.3). connectBitbucket now pins it, so
	// it must be echoed here or Bitbucket rejects the exchange.
	if c.callbackURL != "" {
		form.Set("redirect_uri", c.callbackURL)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.oauthBaseURL+"/site/oauth2/access_token",
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(c.clientID, c.clientSecret)

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
		return nil, &bitbucketAPIError{Op: "token exchange", StatusCode: resp.StatusCode, Detail: body.String()}
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		Scopes       string `json:"scopes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}

	return &BitbucketTokenResponse{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresIn:    tokenResp.ExpiresIn,
		Scopes:       tokenResp.Scopes,
	}, nil
}

func (c *bitbucketOAuthClientImpl) FetchCurrentUser(ctx context.Context, accessToken string) (*BitbucketUser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiBaseURL+"/2.0/user", nil)
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
		return nil, &bitbucketAPIError{Op: "user", StatusCode: resp.StatusCode, Detail: body.String()}
	}

	var user struct {
		UUID        string `json:"uuid"`
		DisplayName string `json:"display_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, fmt.Errorf("decode user response: %w", err)
	}

	return &BitbucketUser{UUID: user.UUID, DisplayName: user.DisplayName}, nil
}

// FetchWorkspaces uses GET /2.0/user/workspaces — the cross-workspace
// /2.0/workspaces endpoint was sunset by Atlassian CHANGE-2770. The new
// endpoint wraps each entry under a "workspace" sub-object and omits the
// human-readable workspace name; we fall back to the slug for display.
func (c *bitbucketOAuthClientImpl) FetchWorkspaces(ctx context.Context, accessToken string) ([]BitbucketWorkspace, error) {
	var all []BitbucketWorkspace
	nextURL := c.apiBaseURL + "/2.0/user/workspaces?pagelen=100"

	for nextURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURL, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("workspaces request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body := httputil.ReadErrorResponse(ctx, resp)
			if cerr := resp.Body.Close(); cerr != nil {
				logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
			}
			return nil, &bitbucketAPIError{Op: "workspaces", StatusCode: resp.StatusCode, Detail: body.String()}
		}

		var page struct {
			Values []struct {
				Workspace struct {
					UUID  string `json:"uuid"`
					Slug  string `json:"slug"`
					Name  string `json:"name"`
					Links struct {
						Avatar struct {
							Href string `json:"href"`
						} `json:"avatar"`
					} `json:"links"`
				} `json:"workspace"`
			} `json:"values"`
			Next string `json:"next"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			if cerr := resp.Body.Close(); cerr != nil {
				logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
			}
			return nil, fmt.Errorf("decode workspaces response: %w", err)
		}
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "connector: close response body", zap.Error(cerr))
		}

		for _, v := range page.Values {
			name := v.Workspace.Name
			if name == "" {
				name = v.Workspace.Slug
			}
			all = append(all, BitbucketWorkspace{
				UUID:      v.Workspace.UUID,
				Slug:      v.Workspace.Slug,
				Name:      name,
				AvatarURL: v.Workspace.Links.Avatar.Href,
			})
		}
		nextURL = page.Next
	}

	return all, nil
}
