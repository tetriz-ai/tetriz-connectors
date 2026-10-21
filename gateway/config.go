// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gateway

import (
	"bytes"
	_ "embed"
	"fmt"
	"math"
	"os"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ConfigEnv names the environment variable LoadConfig reads a config file
// path from.
const ConfigEnv = "TETRIZ_GATEWAY_CONFIG"

//go:embed defaults.yaml
var defaultsYAML []byte

var (
	current    *Config
	configErr  error
	configOnce sync.Once
	configMu   sync.RWMutex
)

// LoadConfig returns the process-wide gateway config. The first call loads
// the file named by $TETRIZ_GATEWAY_CONFIG, or the built-in defaults when the
// variable is empty, unless SetConfig ran first. Later calls return the same
// result.
func LoadConfig() (*Config, error) {
	configOnce.Do(func() {
		configMu.Lock()
		defer configMu.Unlock()
		if current != nil {
			return
		}
		if path := os.Getenv(ConfigEnv); path != "" {
			current, configErr = LoadConfigFile(path)
			return
		}
		current, configErr = ParseConfig(defaultsYAML)
	})
	configMu.RLock()
	defer configMu.RUnlock()
	if configErr != nil {
		return nil, configErr
	}
	return current, nil
}

// SetConfig replaces the process-wide config. Call it before building any
// gateway; gateways read the config when they are built.
func SetConfig(c *Config) {
	configOnce.Do(func() {})
	configMu.Lock()
	defer configMu.Unlock()
	current, configErr = c, nil
}

// DefaultConfig returns a fresh copy of the built-in defaults: each vendor's
// public API base URL and a conservative request rate per rate-limit group.
func DefaultConfig() *Config {
	c, err := ParseConfig(defaultsYAML)
	if err != nil {
		panic(fmt.Sprintf("gateway: built-in defaults do not parse: %v", err))
	}
	return c
}

// LoadConfigFile reads a YAML config file. Vendors the file leaves out get
// no endpoints; start from a copy of defaults.yaml to change one value.
func LoadConfigFile(path string) (*Config, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the path is the operator's own config file
	if err != nil {
		return nil, fmt.Errorf("gateway: read config: %w", err)
	}
	return ParseConfig(b)
}

// ParseConfig decodes a YAML config. Unknown keys are an error.
func ParseConfig(b []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("gateway: parse config: %w", err)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// validate rejects an endpoint whose rate is zero, negative or not finite:
// such a group would fail every request as rate limited, or have no usable
// integer budget. It also rejects a group named twice for one vendor, since
// the later entry would silently replace the earlier one.
func (c *Config) validate() error {
	for vendor, eps := range map[string][]APIEndpointConfig{
		"GitHub": c.GitHub, "Bitbucket": c.Bitbucket, "Jira": c.Jira,
		"Slack": c.Slack, "GitLab": c.GitLab, "Cursor": c.Cursor, "Anthropic": c.Anthropic, "Codex": c.Codex,
	} {
		seen := make(map[string]bool, len(eps))
		for _, ep := range eps {
			if !(ep.RequestsPerSecond > 0) || math.IsInf(ep.RequestsPerSecond, 0) {
				return fmt.Errorf("gateway: %s group %q: RequestsPerSecond must be above 0 and finite, got %v", vendor, ep.Group, ep.RequestsPerSecond)
			}
			if seen[ep.Group] {
				return fmt.Errorf("gateway: %s group %q is listed more than once", vendor, ep.Group)
			}
			seen[ep.Group] = true
		}
	}
	return nil
}

// Config holds each vendor's API endpoints and the simulator routing.
type Config struct {
	GitHub    []APIEndpointConfig `yaml:"GitHub"`
	Bitbucket []APIEndpointConfig `yaml:"Bitbucket"`
	Jira      []APIEndpointConfig `yaml:"Jira"`
	Slack     []APIEndpointConfig `yaml:"Slack"`
	GitLab    []APIEndpointConfig `yaml:"GitLab"`
	Cursor    []APIEndpointConfig `yaml:"Cursor"`
	Anthropic []APIEndpointConfig `yaml:"Anthropic"`
	Codex     []APIEndpointConfig `yaml:"Codex"`
	// SimulatorOrgIDs lists organization IDs whose connector traffic is routed to
	// the simulator service instead of the real vendor API.
	SimulatorOrgIDs []string `yaml:"SimulatorOrgIDs"`
	// SimulatorBaseURL is the simulator service root (no vendor path). Each vendor's
	// simulator gateway appends its own prefix ("/github", "/bitbucket/2.0"), so one
	// root serves every vendor. For orgs in SimulatorOrgIDs, that vendor's requests
	// use this root instead of the real vendor API base.
	SimulatorBaseURL string `yaml:"SimulatorBaseURL"`
}

// APIEndpointConfig defines a vendor API endpoint with its base URL and rate limiting.
type APIEndpointConfig struct {
	// Group is the logical rate-limit pool name (e.g., "admin-sdk", "reports-api").
	Group string `yaml:"Group"`

	// BaseURL is the root URL for this API group (e.g., "https://admin.googleapis.com/admin/directory/v1").
	BaseURL string `yaml:"BaseURL"`

	// RequestsPerSecond is the sustained rate limit for this group.
	RequestsPerSecond float64 `yaml:"RequestsPerSecond"`

	// BurstSize is the maximum burst allowed above the sustained rate.
	BurstSize int `yaml:"BurstSize"`

	// OperationShares reserves a fraction of the group's budget for named
	// operations, so one operation cannot consume the whole of it and starve the
	// rest. Keys are operation ids; values are fractions of RequestsPerSecond.
	//
	// Empty (the default) puts every operation of the group in one shared bucket.
	//
	// The fractions must leave a remainder: operations not named here share a
	// bucket sized at whatever is left. Every operation has exactly one bucket,
	// and the buckets together sum to RequestsPerSecond. A set summing to 1.0 or more is rejected at construction,
	// because it would leave unnamed operations either unbucketed or over budget.
	OperationShares map[string]float64 `yaml:"OperationShares"`
}

// ToRateLimitConfig converts to the SDK's RateLimitConfig.
func (a *APIEndpointConfig) ToRateLimitConfig() sdk.RateLimitConfig {
	return sdk.RateLimitConfig{
		Group:             a.Group,
		RequestsPerSecond: a.RequestsPerSecond,
		BurstSize:         a.BurstSize,
		// Carried through, not dropped: this conversion is what feeds the observed-cap
		// rebuild, so omitting the shares would silently collapse the partition back to
		// one shared bucket the moment an org gets a live cap observation — reinstating
		// the starvation on exactly the orgs busy enough to have one.
		OperationShares: a.OperationShares,
	}
}

// GitHubEndpoints returns a map of group name → APIEndpointConfig
// for convenient lookup by operations.
func (c *Config) GitHubEndpoints() map[string]APIEndpointConfig {
	m := make(map[string]APIEndpointConfig, len(c.GitHub))
	for _, ep := range c.GitHub {
		m[ep.Group] = ep
	}
	return m
}

// BitbucketEndpoints returns a map of group name → APIEndpointConfig
// for convenient lookup by operations.
func (c *Config) BitbucketEndpoints() map[string]APIEndpointConfig {
	m := make(map[string]APIEndpointConfig, len(c.Bitbucket))
	for _, ep := range c.Bitbucket {
		m[ep.Group] = ep
	}
	return m
}

// JiraEndpoints returns a map of group name → APIEndpointConfig
// for convenient lookup by operations.
func (c *Config) JiraEndpoints() map[string]APIEndpointConfig {
	m := make(map[string]APIEndpointConfig, len(c.Jira))
	for _, ep := range c.Jira {
		m[ep.Group] = ep
	}
	return m
}

// SlackEndpoints returns a map of group name → APIEndpointConfig
// for convenient lookup by operations.
func (c *Config) SlackEndpoints() map[string]APIEndpointConfig {
	m := make(map[string]APIEndpointConfig, len(c.Slack))
	for _, ep := range c.Slack {
		m[ep.Group] = ep
	}
	return m
}

// GitLabEndpoints returns a map of group name → APIEndpointConfig
// for convenient lookup by operations.
func (c *Config) GitLabEndpoints() map[string]APIEndpointConfig {
	m := make(map[string]APIEndpointConfig, len(c.GitLab))
	for _, ep := range c.GitLab {
		m[ep.Group] = ep
	}
	return m
}

// CursorEndpoints returns a map of group name → APIEndpointConfig
// for convenient lookup by operations.
func (c *Config) CursorEndpoints() map[string]APIEndpointConfig {
	m := make(map[string]APIEndpointConfig, len(c.Cursor))
	for _, ep := range c.Cursor {
		m[ep.Group] = ep
	}
	return m
}

// AnthropicEndpoints returns a map of group name → APIEndpointConfig for
// convenient lookup by operations. Anthropic declares one group for all five
// endpoints, so this map holds a single entry — see constants.GroupAnthropicAnalytics
// for why the pool is shared rather than per-endpoint.
func (c *Config) AnthropicEndpoints() map[string]APIEndpointConfig {
	m := make(map[string]APIEndpointConfig, len(c.Anthropic))
	for _, ep := range c.Anthropic {
		m[ep.Group] = ep
	}
	return m
}

// CodexEndpoints returns a map of group name → APIEndpointConfig for convenient
// lookup by operations. Codex declares one group for the whole endpoint family, so
// this map holds a single entry — see constants.GroupCodexAnalytics for why the pool
// is shared and why its rate is not a vendor-published figure.
func (c *Config) CodexEndpoints() map[string]APIEndpointConfig {
	m := make(map[string]APIEndpointConfig, len(c.Codex))
	for _, ep := range c.Codex {
		m[ep.Group] = ep
	}
	return m
}

// IsSimulatorOrg reports whether the given organization ID should have its
// connector traffic routed to the simulator rather than the real vendor API.
// The decision is vendor-agnostic: each vendor's gateway and direct-URL routing
// consult it, then append their own prefix onto SimulatorBaseURL.
func (c *Config) IsSimulatorOrg(orgID string) bool {
	for _, id := range c.SimulatorOrgIDs {
		if id == orgID {
			return true
		}
	}
	return false
}
