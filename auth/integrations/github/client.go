// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"

	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/httputil"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// userAgent identifies this module to the GitHub API.
const userAgent = "tetriz-connectors"

// ErrInstallationNotFound is returned when the GitHub API returns 404 for an installation.
var ErrInstallationNotFound = errors.New("installation not found")

// ErrInvalidInstallationID is returned for an installation id that is not a
// canonical positive decimal number.
var ErrInvalidInstallationID = errors.New("installation id must be a positive decimal number")

// ValidInstallationID reports whether id is a canonical positive decimal
// installation id. The id is placed in App-JWT-signed URL paths, so anything
// else (a "/", "..", "?", a leading zero) could steer the call elsewhere.
func ValidInstallationID(id string) bool {
	n, err := strconv.ParseInt(id, 10, 64)
	return err == nil && n > 0 && strconv.FormatInt(n, 10) == id
}

// GitHubAppConfig holds the GitHub App installation parameters.
type GitHubAppConfig struct {
	AppID         int64
	Slug          string
	ClientID      string
	ClientSecret  string
	PrivateKeyPEM string
	WebhookSecret string
}

// GitHubAppClient validates and manages GitHub App installations using the GitHub API.
//
//go:generate go tool mockgen -source=client.go -destination=../../../internal/mocks/auth/integrations/github/mock_client.go -package=mocks
type GitHubAppClient interface {
	// VerifyUserInstallation exchanges a callback code and checks user access.
	VerifyUserInstallation(ctx context.Context, code, installationID string, installation *InstallationInfo) error
	GetInstallation(ctx context.Context, installationID string) (*InstallationInfo, error)
	DeleteInstallation(ctx context.Context, installationID string) error
	// GetOrgAvatarURL fetches the avatar_url for a GitHub organization.
	GetOrgAvatarURL(ctx context.Context, orgLogin string) (string, error)
	ListRepos(ctx context.Context, installationID string, org string) ([]RepoInfo, error)
	BranchExists(ctx context.Context, installationID string, owner string, repo string, branch string) (bool, error)
}

// RepoInfo holds a repo's key fields from the GitHub REST API.
type RepoInfo struct {
	FullName      string
	DefaultBranch string
	Private       bool
}

// InstallationInfo holds the key fields from a GitHub App installation.
type InstallationInfo struct {
	ID                  int64
	AccountLogin        string
	AccountID           int64
	AccountType         string // "Organization" or "User"
	RepositorySelection string // "all" or "selected"
}

// githubAppClient is the production implementation of GitHubAppClient.
type githubAppClient struct {
	cfg        *GitHubAppConfig
	httpClient *http.Client
	privateKey *rsa.PrivateKey
	// transport carries the installation-token exchange and the installation
	// calls (ListRepos, BranchExists). Nil uses http.DefaultTransport.
	transport http.RoundTripper
}

// GitHubAppClientOption configures a GitHubAppClient.
type GitHubAppClientOption func(*githubAppClient)

// WithGitHubTransport sends every request the client makes, the App's own
// and the installation's, through rt instead of http.DefaultTransport.
func WithGitHubTransport(rt http.RoundTripper) GitHubAppClientOption {
	return func(c *githubAppClient) {
		c.transport = rt
		c.httpClient.Transport = rt
	}
}

// NewGitHubAppClient creates a GitHubAppClient that authenticates to GitHub
// using a JWT signed with the App's private key. Returns (nil, nil) if the
// private key PEM is empty, allowing graceful startup without GitHub App
// credentials.
func NewGitHubAppClient(cfg *GitHubAppConfig, opts ...GitHubAppClientOption) (GitHubAppClient, error) {
	if cfg == nil || cfg.PrivateKeyPEM == "" {
		return nil, nil //nolint:nilnil // absent App credentials is a success contract (graceful startup), not a not-found error
	}
	pk, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(cfg.PrivateKeyPEM))
	if err != nil {
		logger.ErrorNoCtx("NewGitHubAppClient: parse private key failed", zap.Error(err))
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	c := &githubAppClient{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		privateKey: pk,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// generateJWT creates a short-lived JWT for authenticating as the GitHub App.
// The token is valid for 10 minutes and issued 60 seconds in the past to
// accommodate clock skew.
func (c *githubAppClient) generateJWT() (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(now.Add(-60 * time.Second)),
		ExpiresAt: jwt.NewNumericDate(now.Add(10 * time.Minute)),
		Issuer:    c.cfg.ClientID,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(c.privateKey)
	if err != nil {
		return "", fmt.Errorf("signing github app jwt: %w", err)
	}
	return signed, nil
}

// GetInstallation fetches an installation by ID from the GitHub API using
// JWT-based App authentication. Returns ErrInstallationNotFound when the
// installation does not exist (HTTP 404).
func (c *githubAppClient) GetInstallation(ctx context.Context, installationID string) (*InstallationInfo, error) {
	if !ValidInstallationID(installationID) {
		return nil, ErrInvalidInstallationID
	}
	jwtToken, err := c.generateJWT()
	if err != nil {
		logger.Error(ctx, "GetInstallation: JWT generation failed",
			zap.String("installation_id", installationID), zap.Error(err))
		return nil, fmt.Errorf("generate JWT: %w", err)
	}

	url := fmt.Sprintf("https://api.github.com/app/installations/%s", installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Error(ctx, "GetInstallation: create request failed",
			zap.String("installation_id", installationID), zap.Error(err))
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "GetInstallation: HTTP request failed",
			zap.String("installation_id", installationID), zap.Error(err))
		return nil, fmt.Errorf("github api request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "github: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrInstallationNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		logger.Error(ctx, "GetInstallation: unexpected status",
			zap.String("installation_id", installationID),
			zap.Stringer("response", body))
		return nil, fmt.Errorf("github api: %s", body)
	}

	var apiResp struct {
		ID      int64 `json:"id"`
		Account struct {
			Login string `json:"login"`
			ID    int64  `json:"id"`
			Type  string `json:"type"`
		} `json:"account"`
		RepositorySelection string `json:"repository_selection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		logger.Error(ctx, "GetInstallation: decode response failed",
			zap.String("installation_id", installationID), zap.Error(err))
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &InstallationInfo{
		ID:                  apiResp.ID,
		AccountLogin:        apiResp.Account.Login,
		AccountID:           apiResp.Account.ID,
		AccountType:         apiResp.Account.Type,
		RepositorySelection: apiResp.RepositorySelection,
	}, nil
}

// DeleteInstallation revokes a GitHub App installation by ID. A 204 (No Content)
// or 404 (already deleted) response is treated as success.
func (c *githubAppClient) DeleteInstallation(ctx context.Context, installationID string) error {
	if !ValidInstallationID(installationID) {
		return ErrInvalidInstallationID
	}
	jwtToken, err := c.generateJWT()
	if err != nil {
		logger.Error(ctx, "DeleteInstallation: JWT generation failed",
			zap.String("installation_id", installationID), zap.Error(err))
		return fmt.Errorf("generate JWT: %w", err)
	}

	url := fmt.Sprintf("https://api.github.com/app/installations/%s", installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		logger.Error(ctx, "DeleteInstallation: create request failed",
			zap.String("installation_id", installationID), zap.Error(err))
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "DeleteInstallation: HTTP request failed",
			zap.String("installation_id", installationID), zap.Error(err))
		return fmt.Errorf("github api request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "github: close response body", zap.Error(cerr))
		}
	}()

	// 204 No Content = success, 404 = already deleted (treat as success).
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}

	body := httputil.ReadErrorResponse(ctx, resp)
	logger.Error(ctx, "DeleteInstallation: unexpected status",
		zap.String("installation_id", installationID),
		zap.Stringer("response", body))
	return fmt.Errorf("github api: %s", body)
}

// GetOrgAvatarURL fetches the avatar_url for a GitHub organization using the
// REST API (GET /orgs/{org}). This is a public endpoint — no auth required.
func (c *githubAppClient) GetOrgAvatarURL(ctx context.Context, orgLogin string) (string, error) {
	apiURL := fmt.Sprintf("https://api.github.com/orgs/%s", orgLogin)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "GetOrgAvatarURL: HTTP request failed",
			zap.String("org", orgLogin), zap.Error(err))
		return "", fmt.Errorf("github api request: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "github: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body := httputil.ReadErrorResponse(ctx, resp)
		logger.Error(ctx, "GetOrgAvatarURL: unexpected status",
			zap.String("org", orgLogin), zap.Stringer("response", body))
		return "", fmt.Errorf("github api: status %d", resp.StatusCode)
	}

	var apiResp struct {
		AvatarURL string `json:"avatar_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}

	return apiResp.AvatarURL, nil
}

// GitHubAppConfigFromConnectorConfig builds a GitHubAppConfig from the
// connector service's config. Returns an error if the GitHub App section
// is missing or the private key is not configured.
func GitHubAppConfigFromConnectorConfig(cfg *connectorconfig.Config) (*GitHubAppConfig, error) {
	if cfg.GitHubApp == nil {
		return nil, fmt.Errorf("GitHubApp config section is missing")
	}
	privateKey := cfg.GitHubAppPrivateKeyPEM
	// The struct field has valueType:"base64" for the SM path, but env vars
	// bypass that decoding. Decode here if the value isn't already PEM.
	if privateKey != "" && !strings.HasPrefix(strings.TrimSpace(privateKey), "-----BEGIN") {
		if decoded, err := base64.StdEncoding.DecodeString(privateKey); err == nil {
			privateKey = string(decoded)
		} else {
			// Not base64 — leave the value as-is; the emptiness/usage checks below
			// surface a genuinely-bad key. Record the decode miss for diagnosis.
			logger.InfoNoCtx("GitHubAppConfigFromConnectorConfig: private key is not base64, using as-is",
				zap.Error(err))
		}
	}
	if privateKey == "" {
		return nil, fmt.Errorf("GitHub App private key not configured — set GitHubAppPrivateKeyPEM in .env")
	}
	return &GitHubAppConfig{
		AppID:         cfg.GitHubApp.AppID,
		Slug:          cfg.GitHubApp.Slug,
		ClientID:      cfg.GitHubApp.ClientID,
		ClientSecret:  cfg.GitHubAppClientSecret,
		PrivateKeyPEM: privateKey,
		WebhookSecret: cfg.GitHubAppWebhookSecret,
	}, nil
}

// installationHTTPClient creates an HTTP client authenticated as a GitHub App
// installation. The ghinstallation transport auto-acquires and caches tokens.
func (c *githubAppClient) installationHTTPClient(installationID string) (*http.Client, error) {
	installID, err := strconv.ParseInt(installationID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse installation_id: %w", err)
	}
	pemBytes := []byte(c.cfg.PrivateKeyPEM)
	base := c.transport
	if base == nil {
		base = http.DefaultTransport
	}
	itr, err := ghinstallation.New(base, c.cfg.AppID, installID, pemBytes)
	if err != nil {
		return nil, fmt.Errorf("create installation transport: %w", err)
	}
	return &http.Client{Transport: itr, Timeout: 15 * time.Second}, nil
}

// ListRepos returns all non-archived repos for the org using installation auth.
func (c *githubAppClient) ListRepos(ctx context.Context, installationID string, org string) ([]RepoInfo, error) {
	client, err := c.installationHTTPClient(installationID)
	if err != nil {
		return nil, fmt.Errorf("ListRepos: %w", err)
	}

	var repos []RepoInfo
	page := 1
	for {
		url := fmt.Sprintf("https://api.github.com/orgs/%s/repos?per_page=100&page=%d&type=all", org, page)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("User-Agent", userAgent)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list repos: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body := httputil.ReadErrorResponse(ctx, resp)
			if cerr := resp.Body.Close(); cerr != nil {
				logger.WarnWithCtx(ctx, "github: close response body", zap.Error(cerr))
			}
			return nil, fmt.Errorf("list repos: %s", body)
		}

		var apiRepos []struct {
			FullName      string `json:"full_name"`
			DefaultBranch string `json:"default_branch"`
			Private       bool   `json:"private"`
			Archived      bool   `json:"archived"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&apiRepos); err != nil {
			if cerr := resp.Body.Close(); cerr != nil {
				logger.WarnWithCtx(ctx, "github: close response body", zap.Error(cerr))
			}
			return nil, fmt.Errorf("decode repos: %w", err)
		}
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "github: close response body", zap.Error(cerr))
		}

		for _, r := range apiRepos {
			if r.Archived {
				continue
			}
			repos = append(repos, RepoInfo{FullName: r.FullName, DefaultBranch: r.DefaultBranch, Private: r.Private})
		}

		if len(apiRepos) < 100 {
			break
		}
		page++
	}
	return repos, nil
}

// BranchExists checks whether a branch exists on a repo using installation auth.
func (c *githubAppClient) BranchExists(ctx context.Context, installationID string, owner string, repo string, branch string) (bool, error) {
	client, err := c.installationHTTPClient(installationID)
	if err != nil {
		return false, fmt.Errorf("BranchExists: %w", err)
	}

	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/branches/%s", owner, repo, branch)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("check branch: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "github: close response body", zap.Error(cerr))
		}
	}()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("check branch: %s", httputil.ReadErrorResponse(ctx, resp))
}
