// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package testguard

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/awscreds"
)

func TestMain(m *testing.M) { os.Exit(Run(m)) }

// Under the guard HOME is an empty temporary directory, cloud credential
// variables are gone, and loading AWS credentials panics.
func TestGuardHidesMachineCredentials(t *testing.T) {
	home := os.Getenv("HOME")
	if !strings.Contains(home, "tetriz-test-home-") {
		t.Fatalf("HOME = %q, want the guard's temporary directory", home)
	}
	if entries, err := os.ReadDir(home); err != nil || len(entries) != 0 {
		t.Fatalf("HOME is not an empty directory: %v %v", entries, err)
	}
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_PROFILE", "GITHUB_TOKEN"} {
		if v := os.Getenv(k); v != "" {
			t.Fatalf("%s is set under the guard", k)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("awscreds.Load did not panic")
		}
	}()
	_, _ = awscreds.Load(context.Background())
}

// Under the guard a request through the default transport to a host that is
// not loopback is refused with the testguard error before any name is
// resolved or any connection is opened, and the host is recorded so Run fails
// the package even if a test swallowed the error. Loopback still works.
func TestGuardRefusesNonLoopbackHTTP(t *testing.T) {
	var dials atomic.Int32
	base, ok := http.DefaultTransport.(guardedTransport)
	if !ok {
		t.Fatalf("http.DefaultTransport is %T, want the guard", http.DefaultTransport)
	}
	inner := base.next.(*http.Transport).Clone()
	inner.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	client := &http.Client{Transport: guardedTransport{next: inner}}
	for _, target := range []string{"http://192.0.2.1/x", "https://api.github.com.invalid/app/installations/1/access_tokens"} {
		resp, err := client.Get(target) //nolint:noctx // a refused request never leaves; no context needed
		if err == nil {
			_ = resp.Body.Close()
			t.Fatalf("%s was not refused", target)
		}
		u, _ := url.Parse(target)
		if want := "testguard: refused non-loopback request to " + u.Hostname(); !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not say %q", err, want)
		}
	}
	if got := Refusals(); strings.Join(got, ",") != "192.0.2.1,api.github.com.invalid" {
		t.Fatalf("recorded refusals %v", got)
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("the guard dialled %d times for refused requests", n)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer srv.Close()
	resp, err := http.Get(srv.URL) //nolint:noctx // a loopback test server
	if err != nil {
		t.Fatalf("loopback request refused: %v", err)
	}
	_ = resp.Body.Close()
	if got := Refusals(); len(got) != 0 {
		t.Fatalf("loopback recorded as refused: %v", got)
	}
}

// Loopback hosts pass as written, and nothing is resolved to decide.
func TestIsLoopbackHost(t *testing.T) {
	for host, want := range map[string]bool{
		"localhost": true, "LOCALHOST": true, "127.0.0.1": true, "127.8.9.10": true, "::1": true, "[::1]": true,
		"192.0.2.1": false, "api.github.com.invalid": false, "10.0.0.1": false, "localhost.invalid": false, "": false,
	} {
		if got := IsLoopbackHost(host); got != want {
			t.Errorf("IsLoopbackHost(%q) = %v, want %v", host, got, want)
		}
	}
}

// A test database DSN must name a loopback host or a local socket.
func TestCheckTestDatabase(t *testing.T) {
	for dsn, ok := range map[string]bool{
		"": true,
		"host=localhost port=5432 user=x dbname=postgres": true,
		"host=127.0.0.1 port=5432":                        true,
		"host=/var/run/postgresql":                        true,
		"postgres://u:p@localhost:5432/db":                true,
		"host=db.example.invalid port=5432":               false,
		"postgres://u:p@192.0.2.1:5432/db":                false,
	} {
		err := checkTestDatabase(dsn)
		if (err == nil) != ok {
			t.Errorf("checkTestDatabase(%q) = %v, want ok=%v", dsn, err, ok)
		}
		if err != nil && !strings.Contains(err.Error(), "TETRIZ_TEST_ALLOW_REMOTE_DB=1") {
			t.Errorf("error %q does not name the opt-in", err)
		}
	}
}
