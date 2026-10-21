// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package app assembles a runnable connector runner from the module's parts:
// the Postgres store, Redis, the eleven connectors, their gateways, a sink,
// the orchestrator and the scheduler. The tetriz-connectors command is a thin
// layer over it.
package app

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Config is the runner's configuration. FromEnv reads it from TETRIZ_*
// variables; every field has a local default for the docker-compose stack.
type Config struct {
	// PostgresDSN is a libpq keyword or URL DSN for the connector store.
	PostgresDSN string
	// RedisAddr is host:port of the Redis the scheduler, rate limiters and
	// token cache share. RedisPassword, when set, authenticates to it.
	RedisAddr     string
	RedisPassword string
	// RedisURL, when set, replaces RedisAddr and RedisPassword: a redis:// or
	// rediss:// (TLS) URL with an optional user, password and database
	// number, as redis.ParseURL reads it.
	RedisURL string
	// Namespace, when set, prefixes the asynq queue names, the leader lock key
	// and every other Redis key the runner writes with "Namespace:", so
	// deployments can share one Redis. Empty keeps today's key names.
	Namespace string
	// CredentialsKey is a base64 32-byte AES-256 key that encrypts stored
	// connector credentials. Empty stores them in the clear, which the runner
	// refuses unless AllowPlaintextCredentials is set.
	CredentialsKey            string
	AllowPlaintextCredentials bool
	// SinkPath is where records go: a file path for JSON lines, "-" for
	// standard output, "log" to log one line per record, or "postgres" for
	// the connector_records table of SinkPostgresDSN (sink/postgres).
	SinkPath string
	// SinkPostgresDSN is the database the postgres sink writes to. Empty
	// means PostgresDSN.
	SinkPostgresDSN string
	// BlobDir is the directory large bodies (diffs, descriptions, trees) are
	// written under.
	BlobDir string
	// HTTPAddr serves /_health and /metrics.
	HTTPAddr string
	// MetricsPrefix is prepended to every metric name.
	MetricsPrefix string
	// Concurrency is the worker's concurrency.
	Concurrency int

	// DefaultTenantKind is the tenant kind of every organization that
	// TenantKindsFile does not name. TenantKindsFile, when set, holds one
	// organization id and kind per line.
	DefaultTenantKind sdk.TenantKind
	TenantKindsFile   string

	// GitHubAppID and GitHubAppPrivateKey (PEM, or base64 of it) let the
	// runner mint installation tokens for GitHub App installs. A connector
	// whose credentials hold a personal access token needs neither.
	GitHubAppID         int64
	GitHubAppPrivateKey string

	// OAuth client ids and secrets of the vendors whose access tokens the
	// runner refreshes.
	OAuth map[string]OAuthClient

	// withoutCredentials is set by FromEnvWithoutCredentials.
	withoutCredentials bool
}

// OAuthClient is one vendor's OAuth application.
type OAuthClient struct {
	ClientID     string
	ClientSecret string
}

// OAuthVendors are the connectors whose gateways refresh an OAuth token.
var OAuthVendors = []string{"jira", "gitlab", "bitbucket", "linear"}

// FromEnv reads the configuration from the environment.
func FromEnv() (Config, error) {
	c, err := fromEnv()
	if err != nil {
		return c, err
	}
	return c, c.validate()
}

// FromEnvWithoutCredentials reads the configuration for a command that reads
// and writes no credentials, such as migrate or cursor show. It ignores
// TETRIZ_CREDENTIALS_KEY, and an App built from it refuses any credential read
// or write.
func FromEnvWithoutCredentials() (Config, error) {
	c, err := fromEnv()
	c.CredentialsKey, c.withoutCredentials = "", true
	return c, err
}

func fromEnv() (Config, error) {
	c := Config{
		PostgresDSN:               envOr("TETRIZ_POSTGRES_DSN", "host=localhost port=55432 user=tetriz password=tetriz-local-only dbname=tetriz sslmode=disable"),
		RedisAddr:                 envOr("TETRIZ_REDIS_ADDR", "localhost:56379"),
		RedisPassword:             os.Getenv("TETRIZ_REDIS_PASSWORD"),
		RedisURL:                  os.Getenv("TETRIZ_REDIS_URL"),
		Namespace:                 os.Getenv("TETRIZ_NAMESPACE"),
		SinkPostgresDSN:           os.Getenv("TETRIZ_SINK_POSTGRES_DSN"),
		CredentialsKey:            os.Getenv("TETRIZ_CREDENTIALS_KEY"),
		AllowPlaintextCredentials: os.Getenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS") == "1",
		SinkPath:                  envOr("TETRIZ_SINK", "log"),
		BlobDir:                   envOr("TETRIZ_BLOB_DIR", "blobs"),
		HTTPAddr:                  envOr("TETRIZ_HTTP_ADDR", ":9090"),
		MetricsPrefix:             envOr("TETRIZ_METRICS_PREFIX", "tetriz_"),
		GitHubAppPrivateKey:       os.Getenv("TETRIZ_GITHUB_APP_PRIVATE_KEY"),
		OAuth:                     map[string]OAuthClient{},
	}
	if err := validateNamespace(c.Namespace); err != nil {
		return c, err
	}
	if _, err := c.RedisOptions(); err != nil {
		return c, err
	}
	var err error
	if c.Concurrency, err = envInt("TETRIZ_CONCURRENCY", 10); err != nil {
		return c, err
	}
	if c.DefaultTenantKind, err = ParseTenantKind(envOr("TETRIZ_DEFAULT_TENANT_KIND", "organization")); err != nil {
		return c, fmt.Errorf("TETRIZ_DEFAULT_TENANT_KIND: %w", err)
	}
	c.TenantKindsFile = os.Getenv("TETRIZ_TENANT_KINDS_FILE")
	if v := os.Getenv("TETRIZ_GITHUB_APP_ID"); v != "" {
		if c.GitHubAppID, err = strconv.ParseInt(v, 10, 64); err != nil {
			return c, fmt.Errorf("TETRIZ_GITHUB_APP_ID: %w", err)
		}
	}
	for _, v := range OAuthVendors {
		p := "TETRIZ_" + strings.ToUpper(strings.ReplaceAll(v, "-", "_")) + "_OAUTH_"
		id, secret := os.Getenv(p+"CLIENT_ID"), os.Getenv(p+"CLIENT_SECRET")
		if id != "" || secret != "" {
			c.OAuth[v] = OAuthClient{ClientID: id, ClientSecret: secret}
		}
	}
	return c, nil
}

// SinkPostgres is the SinkPath that writes records to Postgres.
const SinkPostgres = "postgres"

// RedisOptions returns the options of the runner's Redis client: RedisURL
// parsed by redis.ParseURL when it is set, and otherwise RedisAddr with
// RedisPassword. The scheduler's asynq servers share the client, and its
// periodic task manager dials with the same address, credentials, database
// and TLS settings.
func (c Config) RedisOptions() (*redis.Options, error) {
	if c.RedisURL != "" {
		o, err := redis.ParseURL(c.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("TETRIZ_REDIS_URL: %w", err)
		}
		return o, nil
	}
	return &redis.Options{Addr: c.RedisAddr, Password: c.RedisPassword}, nil
}

// sinkPostgresDSN is the database the postgres sink writes to.
func (c Config) sinkPostgresDSN() string {
	if c.SinkPostgresDSN != "" {
		return c.SinkPostgresDSN
	}
	return c.PostgresDSN
}

func (c Config) validate() error {
	if c.CredentialsKey == "" {
		if !c.AllowPlaintextCredentials {
			return errors.New("TETRIZ_CREDENTIALS_KEY is empty; set it to a base64 32-byte key, or set TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS=1 for a local run")
		}
		return nil
	}
	key, err := base64.StdEncoding.DecodeString(c.CredentialsKey)
	if err != nil {
		return fmt.Errorf("TETRIZ_CREDENTIALS_KEY is not base64: %w", err)
	}
	if len(key) != 32 {
		return fmt.Errorf("TETRIZ_CREDENTIALS_KEY decodes to %d bytes, want 32", len(key))
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return n, nil
}
