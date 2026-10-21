// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/redis/go-redis/v9"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	kiroconnector "github.com/tetriz-ai/tetriz-connectors/connectors/kiro"
	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/inprocess"
	rlstore "github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/store"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
	anthropicgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/anthropic"
	bitbucketgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/bitbucket"
	codexgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/codex"
	cursorgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/cursor"
	githubgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/github"
	gitlabgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/gitlab"
	jiragw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/jira"
	kirogw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/kiro"
	lineargw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/linear"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// simulatorToken is sent to the simulator, which accepts any bearer token.
const simulatorToken = "simulator-token"

type httpDoer struct{ client *http.Client }

func (d httpDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, err := d.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	return resp, nil
}

// GatewayDeps are what NewGatewayFactory builds the built-in connectors'
// gateways from. RedisGatewayDeps fills the stores from a Redis client, as
// `serve` and `sync` do, and InProcessGatewayDeps from in-process stores, for
// a program with no Redis.
type GatewayDeps struct {
	// Config gives the OAuth applications (Config.OAuth) and the GitHub App
	// (Config.GitHubAppID and Config.GitHubAppPrivateKey). The other fields
	// are not read.
	Config Config
	// RateLimits is the store every gateway's rate limiter books against.
	RateLimits ratelimiter.Store
	// Budgets are the vendor budget floors of the GitHub, GitLab and
	// Bitbucket gateways, keyed by connector. A missing entry disables that
	// vendor's floor.
	Budgets map[sdk.ConnectorID]*ratelimiter.BudgetLimiter
	// Refresh is what the OAuth gateways (GitLab, Bitbucket, Jira, Linear)
	// refresh access tokens with. Creds and Tx may be nil
	// when no OAuth connector runs: an OAuth gateway then fails to build.
	Refresh auth.RefreshDeps
	// Encryptor encrypts the access tokens Linear, GitLab, Bitbucket and
	// Jira cache. Nil caches them in the clear.
	Encryptor cryptutil.Encryptor
	// BitbucketCircuit is the Bitbucket gateways' 429 circuit-breaker store.
	// Nil disables the breaker; rate limiting still applies.
	BitbucketCircuit gateway.CircuitBreakerStore
	// GitLabCircuit is the GitLab gateways' 429 circuit-breaker store, the
	// same breaker under its own keys. Nil disables it.
	GitLabCircuit gateway.CircuitBreakerStore
	// HTTP sends every vendor request that needs no GitHub App transport.
	// Nil is a client with a 60-second timeout.
	HTTP vendorapi.HTTPDoer
}

// RedisGatewayDeps returns GatewayDeps on rdb, the stores `serve` and `sync`
// use: rate limits, budgets, cached access tokens, refresh locks and the
// Bitbucket circuit breaker are shared by every process on that Redis, with
// each key in cfg.Namespace. creds and tx are Refresh.Creds and Refresh.Tx.
func RedisGatewayDeps(cfg Config, rdb *redis.Client, creds auth.CredentialStore, tx store.TxRunner) GatewayDeps {
	ns := cfg.Namespace
	budgets := NamespacedBudgets(ns, ratelimiter.NewRedisBudgetStore(rdb))
	return GatewayDeps{
		Config:     cfg,
		RateLimits: NamespacedRateLimits(ns, rlstore.NewSlidingWindowLogWithRedis(rdb)),
		Budgets: map[sdk.ConnectorID]*ratelimiter.BudgetLimiter{
			"github":    ratelimiter.NewBudgetLimiterWithStore(budgets, ratelimiter.GitHubAdapter{}),
			"gitlab":    ratelimiter.NewBudgetLimiterWithStore(budgets, ratelimiter.GitLabAdapter{}),
			"bitbucket": ratelimiter.NewBudgetLimiterWithStore(budgets, ratelimiter.BitbucketAdapter{}),
		},
		Refresh: auth.RefreshDeps{
			Cache:  NamespacedTokenCache(ns, authredis.NewTokenCache(rdb)),
			Locker: NamespacedLocker(ns, authredis.NewLocker(rdb)),
			Creds:  creds,
			Tx:     tx,
		},
		BitbucketCircuit: NamespacedCircuit(ns, rdb),
		GitLabCircuit:    NamespacedCircuit(ns, rdb),
	}
}

// InProcessGatewayDeps returns GatewayDeps on stores, for a program that
// embeds the built-in connectors with no Redis: rate limits, budgets, cached
// access tokens and the Bitbucket and GitLab circuit breakers live in this process (see
// package inprocess). Build stores once with inprocess.New and share it. Set
// Refresh.Creds and Refresh.Tx on the result to run an OAuth connector.
func InProcessGatewayDeps(cfg Config, stores *inprocess.Stores) GatewayDeps {
	return GatewayDeps{
		Config:           cfg,
		RateLimits:       stores.RateLimits,
		Budgets:          stores.BudgetLimiters(),
		Refresh:          auth.RefreshDeps{Cache: stores.Tokens, Locker: stores.Tokens},
		BitbucketCircuit: stores.BitbucketCircuit,
		GitLabCircuit:    stores.GitLabCircuit,
	}
}

// budgetObservers are the orchestrator's view of the budget limiters.
func (d GatewayDeps) budgetObservers() map[sdk.ConnectorID]sdk.BudgetObserver {
	out := make(map[sdk.ConnectorID]sdk.BudgetObserver, len(d.Budgets))
	for k, v := range d.Budgets {
		if v != nil {
			out[k] = v
		}
	}
	return out
}

// gatewayDeps is GatewayDeps with the GitHub App key decoded and the
// defaults filled in.
type gatewayDeps struct {
	cfg       Config
	rlStore   ratelimiter.Store
	budgets   map[sdk.ConnectorID]*ratelimiter.BudgetLimiter
	refresh   auth.RefreshDeps
	enc       cryptutil.Encryptor
	appKey    []byte
	bbCircuit gateway.CircuitBreakerStore
	glCircuit gateway.CircuitBreakerStore
	plainHTTP vendorapi.HTTPDoer
}

// NewGatewayFactory returns the VendorGatewayFactory of the built-in
// connectors, the one `serve` and `sync` run: the orchestrator calls it once
// per run with the organization's decrypted config. Organizations the gateway
// config routes to the simulator get a gateway pointed at it, with a fixed
// token and no OAuth refresh. Any other connector id is an error, so a host
// with connectors of its own wraps the factory, as New does for WithConnector.
// It fails when Config.GitHubAppPrivateKey is neither PEM nor base64 PEM.
func NewGatewayFactory(deps GatewayDeps) (sdk.VendorGatewayFactory, error) {
	appKey, err := pemBytes(deps.Config.GitHubAppPrivateKey)
	if err != nil {
		return nil, err
	}
	d := gatewayDeps{
		cfg:       deps.Config,
		rlStore:   deps.RateLimits,
		budgets:   deps.Budgets,
		refresh:   deps.Refresh,
		enc:       deps.Encryptor,
		appKey:    appKey,
		bbCircuit: deps.BitbucketCircuit,
		glCircuit: deps.GitLabCircuit,
		plainHTTP: deps.HTTP,
	}
	if d.refresh.Encryptor == nil {
		d.refresh.Encryptor = deps.Encryptor
	}
	if d.rlStore == nil {
		return nil, errors.New("gateway deps: RateLimits is nil; use RedisGatewayDeps or InProcessGatewayDeps")
	}
	if d.plainHTTP == nil {
		d.plainHTTP = httpDoer{client: &http.Client{Timeout: 60 * time.Second}}
	}
	return newGatewayFactory(d), nil
}

// newGatewayFactory is NewGatewayFactory over decoded deps. The GitHub and
// Bitbucket gateways are paced at the lower of the configured rate and the
// hourly cap the connectors observed from the vendor (withObservedCaps).
func newGatewayFactory(d gatewayDeps) sdk.VendorGatewayFactory {
	build := buildGateway(d)
	return func(ctx context.Context, cc *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		gw, err := build(ctx, cc)
		if err != nil {
			return nil, err
		}
		var endpoints func(*gateway.Config) map[string]gateway.APIEndpointConfig
		switch cc.ConnectorID {
		case "github":
			endpoints = (*gateway.Config).GitHubEndpoints
		case "bitbucket":
			endpoints = (*gateway.Config).BitbucketEndpoints
		default:
			return gw, nil
		}
		gcfg, err := gateway.LoadConfig()
		if err != nil {
			return nil, fmt.Errorf("load gateway config: %w", err)
		}
		return withObservedCaps(ctx, gw, endpoints(gcfg), cc.Settings, time.Now()), nil
	}
}

// buildGateway builds the vendor gateway for one connector config.
func buildGateway(d gatewayDeps) sdk.VendorGatewayFactory {
	return func(ctx context.Context, cc *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		gcfg, err := gateway.LoadConfig()
		if err != nil {
			return nil, fmt.Errorf("load gateway config: %w", err)
		}
		sim := gcfg.IsSimulatorOrg(cc.OrganizationID)
		switch cc.ConnectorID {
		case "github":
			if sim {
				return githubgw.NewGitHubVendorGatewayForSimulator(d.plainHTTP, simulatorToken, d.rlStore, d.budgets["github"], gcfg.SimulatorBaseURL)
			}
			doer, token, err := d.githubDoer(cc.Credentials)
			if err != nil {
				return nil, fmt.Errorf("github gateway: %w", err)
			}
			return githubgw.NewGitHubVendorGateway(doer, token, d.rlStore, d.budgets["github"])
		case "gitlab":
			if sim {
				return gitlabgw.NewGitLabVendorGatewayForSimulator(d.plainHTTP, simulatorToken, d.rlStore, d.glCircuit, d.budgets["gitlab"], gcfg.SimulatorBaseURL)
			}
			oc, err := d.oauth("gitlab")
			if err != nil {
				return nil, err
			}
			return gitlabgw.NewGitLabVendorGateway(gitlabgw.NewAutoRefreshDoer(d.plainHTTP, d.refresh, oc.ClientID, oc.ClientSecret, cc.OrganizationID), d.rlStore, d.glCircuit, d.budgets["gitlab"])
		case "bitbucket":
			if sim {
				return bitbucketgw.NewBitbucketVendorGatewayForSimulator(d.plainHTTP, simulatorToken, d.rlStore, d.bbCircuit, d.budgets["bitbucket"], gcfg.SimulatorBaseURL)
			}
			oc, err := d.oauth("bitbucket")
			if err != nil {
				return nil, err
			}
			return bitbucketgw.NewBitbucketVendorGateway(bitbucketgw.NewAutoRefreshDoer(d.plainHTTP, d.refresh, oc.ClientID, oc.ClientSecret, cc.OrganizationID), d.rlStore, d.bbCircuit, d.budgets["bitbucket"])
		case "jira":
			// No budget: ratelimiter has no adapter for Jira's rate-limit headers.
			if sim {
				return jiragw.NewJiraVendorGatewayForSimulator(d.plainHTTP, simulatorToken, d.rlStore, gcfg.SimulatorBaseURL)
			}
			oc, err := d.oauth("jira")
			if err != nil {
				return nil, err
			}
			return jiragw.NewJiraVendorGateway(jiragw.NewAutoRefreshDoer(d.plainHTTP, d.refresh, oc.ClientID, oc.ClientSecret, cc.OrganizationID), d.rlStore)
		case "linear":
			// No budget: ratelimiter has no adapter for Linear's rate-limit headers.
			oc, err := d.oauth("linear")
			if err != nil {
				return nil, err
			}
			return lineargw.NewLinearVendorGateway(lineargw.NewAutoRefreshDoer(d.plainHTTP, d.refresh, d.enc, oc.ClientID, oc.ClientSecret, cc.OrganizationID), d.rlStore)
		case "cursor", "claude-code", "codex":
			// No budget: ratelimiter has no adapter for these vendors' headers.
			var c struct {
				APIKey string `json:"api_key"`
			}
			if err := json.Unmarshal(cc.Credentials, &c); err != nil || c.APIKey == "" {
				return nil, fmt.Errorf("%s gateway: credentials carry no api_key", cc.ConnectorID)
			}
			switch cc.ConnectorID {
			case "cursor":
				return cursorgw.NewCursorVendorGateway(d.plainHTTP, c.APIKey, d.rlStore)
			case "claude-code":
				return anthropicgw.NewAnthropicVendorGateway(d.plainHTTP, c.APIKey, d.rlStore)
			default:
				return codexgw.NewCodexVendorGateway(d.plainHTTP, c.APIKey, d.rlStore)
			}
		case "kiro":
			// No budget: Kiro reads S3, which sends no rate-limit budget headers.
			_, settings, err := kiroconnector.ParseConfig(cc)
			if err != nil {
				return nil, fmt.Errorf("kiro gateway: %w", err)
			}
			return kirogw.NewGatewayForOrganization(ctx, settings.Region, cc.OrganizationID)
		default:
			return nil, fmt.Errorf("no gateway for connector %q", cc.ConnectorID)
		}
	}
}

func (d gatewayDeps) oauth(vendor string) (OAuthClient, error) {
	oc, ok := d.cfg.OAuth[vendor]
	if !ok || oc.ClientID == "" || oc.ClientSecret == "" {
		v := "TETRIZ_" + strings.ToUpper(strings.ReplaceAll(vendor, "-", "_")) + "_OAUTH_CLIENT_ID and _CLIENT_SECRET"
		return OAuthClient{}, fmt.Errorf("%s gateway: OAuth application not configured (%s)", vendor, v)
	}
	if d.refresh.Cache == nil || d.refresh.Locker == nil || d.refresh.Creds == nil || d.refresh.Tx == nil {
		return OAuthClient{}, fmt.Errorf("%s gateway: GatewayDeps.Refresh needs a Cache, a Locker, Creds and Tx to refresh OAuth tokens", vendor)
	}
	return oc, nil
}

// githubDoer returns the doer and token for a GitHub connector: a personal
// access token as a bearer token, or an App installation whose tokens are
// minted per request from the App's private key.
func (d gatewayDeps) githubDoer(creds json.RawMessage) (vendorapi.HTTPDoer, string, error) {
	var c struct {
		InstallationID      string `json:"installation_id"`
		PersonalAccessToken string `json:"personal_access_token"`
	}
	if err := json.Unmarshal(creds, &c); err != nil {
		return nil, "", fmt.Errorf("parse credentials: %w", err)
	}
	if c.PersonalAccessToken != "" {
		return d.plainHTTP, c.PersonalAccessToken, nil
	}
	if c.InstallationID == "" {
		return nil, "", errors.New("github credentials give neither a personal_access_token nor an installation_id")
	}
	if d.cfg.GitHubAppID == 0 || len(d.appKey) == 0 {
		return nil, "", errors.New("credentials name an App installation but TETRIZ_GITHUB_APP_ID or TETRIZ_GITHUB_APP_PRIVATE_KEY is unset")
	}
	installID, err := strconv.ParseInt(c.InstallationID, 10, 64)
	if err != nil {
		return nil, "", fmt.Errorf("parse installation_id: %w", err)
	}
	tr, err := ghinstallation.New(http.DefaultTransport, d.cfg.GitHubAppID, installID, d.appKey)
	if err != nil {
		return nil, "", fmt.Errorf("github app transport: %w", err)
	}
	return httpDoer{client: &http.Client{Transport: tr}}, "", nil
}

// pemBytes accepts a PEM block or its base64 encoding.
func pemBytes(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	if strings.HasPrefix(strings.TrimSpace(s), "-----BEGIN") {
		return []byte(s), nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("TETRIZ_GITHUB_APP_PRIVATE_KEY is neither PEM nor base64 PEM: %w", err)
	}
	return b, nil
}
