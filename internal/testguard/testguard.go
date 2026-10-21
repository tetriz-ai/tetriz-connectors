// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package testguard keeps tests away from the machine's credentials and from
// the network. Run points HOME at an empty temporary directory, clears cloud
// credential variables, makes the AWS credential loader panic, refuses any
// HTTP request through the default transport to a host that is not loopback,
// and refuses a test database DSN that names a remote host, so a test that
// would have reached a real credential store, vendor or database fails
// loudly instead.
package testguard

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/tetriz-ai/tetriz-connectors/internal/awscreds"
)

// Run guards the environment, runs the tests and returns their exit code.
// Call it from TestMain: os.Exit(testguard.Run(m)).
func Run(m *testing.M) int {
	home, err := os.MkdirTemp("", "tetriz-test-home-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "testguard:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(home) }()
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "AWS_") || strings.HasPrefix(k, "GOOGLE_") || k == "GH_TOKEN" || k == "GITHUB_TOKEN" {
			_ = os.Unsetenv(k)
		}
	}
	_ = os.Setenv("HOME", home)
	_ = os.Setenv("USERPROFILE", home)
	_ = os.Setenv("XDG_CONFIG_HOME", home)
	_ = os.Setenv("AWS_CONFIG_FILE", home+"/no-aws-config")
	_ = os.Setenv("AWS_SHARED_CREDENTIALS_FILE", home+"/no-aws-credentials")
	_ = os.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	awscreds.Load = func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		panic("testguard: a test tried to load real AWS credentials; inject a fake instead")
	}
	if err := checkTestDatabase(os.Getenv("TETRIZ_TEST_POSTGRES_DSN")); err != nil && os.Getenv("TETRIZ_TEST_ALLOW_REMOTE_DB") != "1" {
		fmt.Fprintln(os.Stderr, "testguard:", err)
		return 1
	}
	guardHTTP()
	code := m.Run()
	if refused := Refusals(); len(refused) > 0 {
		fmt.Fprintf(os.Stderr, "testguard: tests sent requests to non-loopback hosts, which were refused: %s\n", strings.Join(refused, ", "))
		if code == 0 {
			code = 1
		}
	}
	return code
}

// The guard refuses on the request's host as written, before any name is
// resolved or any connection is opened, so a refused request sends no packet.
// A refusal is returned to the caller as an error and also recorded: Run fails
// the package even when the test swallowed the error, so a test cannot pass
// by reaching (or trying to reach) the network.
var (
	refusedMu sync.Mutex
	refused   = map[string]bool{}
)

// Refusals returns, sorted, the hosts the guard has refused, and forgets
// them. The guard's own tests call it to consume refusals they caused on
// purpose; Run calls it after the tests to fail a package that made any.
func Refusals() []string {
	refusedMu.Lock()
	defer refusedMu.Unlock()
	out := make([]string, 0, len(refused))
	for h := range refused {
		out = append(out, h)
	}
	sort.Strings(out)
	refused = map[string]bool{}
	return out
}

// guardedTransport sends a request on only when its host is loopback.
type guardedTransport struct{ next http.RoundTripper }

func (g guardedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()
	if !IsLoopbackHost(host) {
		refusedMu.Lock()
		refused[host] = true
		refusedMu.Unlock()
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("testguard: refused non-loopback request to %s", host)
	}
	return g.next.RoundTrip(req) //nolint:wrapcheck // a transport passes its inner transport's error through
}

// guardHTTP wraps http.DefaultTransport, which every client the repository
// builds without its own transport uses (ghinstallation and the vendor
// clients read it when they are built), and http.DefaultClient's transport
// when one is set.
func guardHTTP() {
	if _, done := http.DefaultTransport.(guardedTransport); !done {
		http.DefaultTransport = guardedTransport{next: http.DefaultTransport}
	}
	if t := http.DefaultClient.Transport; t != nil {
		if _, done := t.(guardedTransport); !done {
			http.DefaultClient.Transport = guardedTransport{next: t}
		}
	}
}

// IsLoopbackHost reports whether host, as written, is loopback: "localhost"
// or a literal address in 127.0.0.0/8 or ::1. It resolves no name.
func IsLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// checkTestDatabase refuses a test database DSN (a libpq keyword DSN or a
// postgres:// URL) whose host is not loopback or a local socket.
func checkTestDatabase(dsn string) error {
	if dsn == "" {
		return nil
	}
	var hosts []string
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return fmt.Errorf("TETRIZ_TEST_POSTGRES_DSN does not parse: %w", err)
		}
		hosts = strings.Split(u.Hostname(), ",")
	} else {
		for _, field := range strings.Fields(dsn) {
			if k, v, ok := strings.Cut(field, "="); ok && (k == "host" || k == "hostaddr") {
				hosts = append(hosts, strings.Split(strings.Trim(v, "'"), ",")...)
			}
		}
	}
	for _, h := range hosts {
		if h == "" || strings.HasPrefix(h, "/") || IsLoopbackHost(h) {
			continue
		}
		return fmt.Errorf("TETRIZ_TEST_POSTGRES_DSN names the non-loopback host %q; tests use a local database. Set TETRIZ_TEST_ALLOW_REMOTE_DB=1 to allow it", h)
	}
	return nil
}
