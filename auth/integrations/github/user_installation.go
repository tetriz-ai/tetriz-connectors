// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrUserInstallationDenied deliberately contains no OAuth response or credentials.
var ErrUserInstallationDenied = errors.New("GitHub user installation authorization failed")

// VerifyUserInstallation uses the App's OAuth credentials, never App-global JWT
// access, to prove the user owns its personal account or administers its organization.
// Visibility alone permits repository collaborators and cannot authorize App-wide access. Tokens
// exist only for this bounded request sequence and are never persisted or logged.
func (c *githubAppClient) VerifyUserInstallation(ctx context.Context, code, installationID string, expected *InstallationInfo) error {
	if code == "" || len(code) > 1024 || c.cfg.ClientID == "" || c.cfg.ClientSecret == "" || !ValidInstallationID(installationID) || expected == nil || fmt.Sprint(expected.ID) != installationID || expected.AccountID <= 0 || !validAccountLogin(expected.AccountLogin) || (expected.AccountType != "User" && expected.AccountType != "Organization") {
		return ErrUserInstallationDenied
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	form := url.Values{"client_id": {c.cfg.ClientID}, "client_secret": {c.cfg.ClientSecret}, "code": {code}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://github.com/login/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return ErrUserInstallationDenied
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	var grant struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		Error       string `json:"error"`
	}
	if err = c.userAuthorizationJSON(req, &grant); err != nil || grant.Error != "" || grant.AccessToken == "" || !strings.EqualFold(grant.TokenType, "bearer") {
		return ErrUserInstallationDenied
	}
	// Construct every URL ourselves: never follow vendor-supplied pagination URLs.
	// A full last page requires another page; exceeding the cap fails closed.
	for page := 1; page <= 10; page++ {
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://api.github.com/user/installations?per_page=100&page=%d", page), nil)
		if err != nil {
			return ErrUserInstallationDenied
		}
		req.Header.Set("Authorization", "Bearer "+grant.AccessToken)
		var listing struct {
			Installations []struct {
				ID      int64 `json:"id"`
				Account struct {
					ID    int64  `json:"id"`
					Login string `json:"login"`
					Type  string `json:"type"`
				} `json:"account"`
			} `json:"installations"`
		}
		if err = c.userAuthorizationJSON(req, &listing); err != nil {
			return ErrUserInstallationDenied
		}
		if len(listing.Installations) > 100 {
			return ErrUserInstallationDenied
		}
		for _, installation := range listing.Installations {
			if fmt.Sprintf("%d", installation.ID) == installationID {
				if installation.Account.ID != expected.AccountID || !strings.EqualFold(installation.Account.Login, expected.AccountLogin) || installation.Account.Type != expected.AccountType {
					return ErrUserInstallationDenied
				}
				return c.verifyInstallationAccountAdmin(ctx, grant.AccessToken, expected)
			}
		}
		if len(listing.Installations) < 100 {
			return ErrUserInstallationDenied
		}
	}
	return ErrUserInstallationDenied
}

func (c *githubAppClient) userAuthorizationJSON(req *http.Request, target any) error {
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	// Refuse redirects, including same-origin: the code/secret/token must never be
	// forwarded to an unexpected URL or exposed in a redirect error.
	client := *c.httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return ErrUserInstallationDenied
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return ErrUserInstallationDenied
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return ErrUserInstallationDenied
	}
	if err = json.Unmarshal(body, target); err != nil {
		return ErrUserInstallationDenied
	}
	return nil
}

func validAccountLogin(login string) bool {
	if len(login) == 0 || len(login) > 39 {
		return false
	}
	for _, r := range login {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
		default:
			return false
		}
	}
	return true
}

func (c *githubAppClient) verifyInstallationAccountAdmin(ctx context.Context, token string, expected *InstallationInfo) error {
	endpoint := "https://api.github.com/user"
	if expected.AccountType == "Organization" {
		endpoint = "https://api.github.com/user/memberships/orgs/" + expected.AccountLogin
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return ErrUserInstallationDenied
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if expected.AccountType == "User" {
		var user struct {
			ID int64 `json:"id"`
		}
		if err = c.userAuthorizationJSON(req, &user); err != nil || user.ID != expected.AccountID {
			return ErrUserInstallationDenied
		}
		return nil
	}
	var membership struct {
		State        string `json:"state"`
		Role         string `json:"role"`
		Organization struct {
			ID    int64  `json:"id"`
			Login string `json:"login"`
		} `json:"organization"`
	}
	if err = c.userAuthorizationJSON(req, &membership); err != nil || membership.State != "active" || membership.Role != "admin" || membership.Organization.ID != expected.AccountID || !strings.EqualFold(membership.Organization.Login, expected.AccountLogin) {
		return ErrUserInstallationDenied
	}
	return nil
}
