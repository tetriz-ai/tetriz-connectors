// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	simserver "github.com/tetriz-ai/tetriz-connectors/internal/simulator/server"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
)

// externalIDs returns the external ids in a JSON-lines sink file, one batch
// of events per line.
func externalIDs(t *testing.T, path string) map[string]bool {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // test file under t.TempDir
	if os.IsNotExist(err) {
		return map[string]bool{}
	}
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck // read-only
	ids := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var batch struct {
			Events []struct {
				ExternalID string `json:"externalId"`
			} `json:"events"`
		}
		require.NoError(t, json.Unmarshal(sc.Bytes(), &batch))
		for _, e := range batch.Events {
			ids[e.ExternalID] = true
		}
	}
	require.NoError(t, sc.Err())
	return ids
}

// simCLI runs the CLI against a seeded in-process simulator and a fresh
// store, with the demo gateway file routing org to it.
type simCLI struct {
	t   *testing.T
	dir string
}

const simOrg = "00000000-0000-4000-8000-00000000d3e0"

func newSimCLI(t *testing.T) *simCLI {
	t.Helper()
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("simulator CLI", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	_, simDSN, simCleanup, err := testdb.Setup(ctx, testdb.Simulator)
	require.NoError(t, err)
	t.Cleanup(simCleanup)
	require.NoError(t, simserver.Seed(ctx, simDSN))
	pool, err := pgxpool.New(ctx, simDSN)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	sim := httptest.NewServer(simserver.Handler(pool))
	t.Cleanup(sim.Close)

	_, dsn, cleanup, err := testdb.Setup(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	// The demo gateway file: simulator rates, with this test's simulator.
	demo, err := os.ReadFile("../../deploy/gateway.demo.yaml")
	require.NoError(t, err)
	gw := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(gw, []byte(strings.ReplaceAll(string(demo), "http://simulator:8080", sim.URL)), 0o600))
	// The gateway config is loaded once per process, so set it directly.
	gcfg, err := gateway.LoadConfigFile(gw)
	require.NoError(t, err)
	gateway.SetConfig(gcfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	t.Setenv("TETRIZ_BLOB_DIR", t.TempDir())
	// The OAuth gateways need an application configured; a simulated
	// organization never uses it.
	for _, v := range []string{"GITLAB", "BITBUCKET", "JIRA"} {
		t.Setenv("TETRIZ_"+v+"_OAUTH_CLIENT_ID", "simulated")
		t.Setenv("TETRIZ_"+v+"_OAUTH_CLIENT_SECRET", "simulated")
	}
	return &simCLI{t: t, dir: t.TempDir()}
}

func (c *simCLI) put(conn, settings string) {
	c.t.Helper()
	code, _, errOut := runCLI(c.t, "config", "put", "--org", simOrg, "--connector", conn, "--settings", settings, "--credentials", `{}`)
	require.Equal(c.t, 0, code, errOut)
}

// sync runs one operation, writing records to sink under the test's
// directory, and returns the exit code and the printed run.
func (c *simCLI) sync(conn, op, sink string) (int, string) {
	c.t.Helper()
	c.t.Setenv("TETRIZ_SINK", filepath.Join(c.dir, sink))
	code, out, _ := runCLI(c.t, "sync", "--org", simOrg, "--connector", conn, "--op", op)
	return code, out
}

func (c *simCLI) ids(sink string) map[string]bool {
	return externalIDs(c.t, filepath.Join(c.dir, sink))
}

// pinReEmission runs op twice and checks that the second run emits a
// non-empty subset of the first run's external ids.
func (c *simCLI) pinReEmission(t *testing.T, conn, op string) {
	t.Helper()
	for _, run := range []string{"1", "2"} {
		code, out := c.sync(conn, op, conn+"."+op+"."+run+".jsonl")
		require.Equal(t, 0, code, "%s %s run %s: %s", conn, op, run, out)
	}
	first, second := c.ids(conn+"."+op+".1.jsonl"), c.ids(conn+"."+op+".2.jsonl")
	require.NotEmpty(t, second, "the second run emits the boundary again")
	assert.Less(t, len(second), len(first), "the second run resumes from its cursor")
	for id := range second {
		assert.True(t, first[id], "%s was emitted by the first run too", id)
	}
}

// Each incremental operation below emits records again on a second run
// against unchanged simulator data: the records at its cursor boundary, with
// the same external ids.
func TestLimitation_IncrementalGitHubOpsReEmitTheBoundary(t *testing.T) {
	c := newSimCLI(t)
	c.put("github", `{"organization":"demo-gh-startup"}`)
	code, out := c.sync("github", "fetch_repos", "repos.jsonl")
	require.Equal(t, 0, code, out)
	for _, op := range []string{"fetch_prs", "fetch_issues", "fetch_reviews", "fetch_pr_comments", "fetch_copilot_usage", "fetch_prs_data"} {
		t.Run(op, func(t *testing.T) { c.pinReEmission(t, "github", op) })
	}
}

func TestLimitation_IncrementalGitLabBitbucketJiraOpsReEmitTheBoundary(t *testing.T) {
	c := newSimCLI(t)
	c.put("gitlab", `{"group_id":91002}`)
	c.put("bitbucket", `{"workspace":"demo-bb-startup"}`)
	c.put("jira", `{"cloud_id":"demo-bb-startup"}`)
	t.Run("gitlab fetch_prs", func(t *testing.T) { c.pinReEmission(t, "gitlab", "fetch_prs") })
	t.Run("bitbucket fetch_prs", func(t *testing.T) { c.pinReEmission(t, "bitbucket", "fetch_prs") })
	t.Run("jira fetch_issues", func(t *testing.T) { c.pinReEmission(t, "jira", "fetch_issues") })
}

// The simulator serves what the vendor serves where it once fell short:
// Jira issue comments and GitHub branch listings sync, and an unknown GitHub
// organization or Jira cloud_id fails not_found, as against the vendor,
// instead of completing empty. A personal-access-token config, which lists
// /user/repos, syncs.
func TestSimulator_AnswersAsTheVendorDoes(t *testing.T) {
	c := newSimCLI(t)
	c.put("github", `{"organization":"demo-gh-startup"}`)
	c.put("jira", `{"cloud_id":"demo-bb-startup"}`)

	code, out := c.sync("jira", "fetch_comments", "jira-comments.jsonl")
	require.Equal(t, 0, code, "Jira fetch_comments syncs: %s", out)
	assert.NotEmpty(t, c.ids("jira-comments.jsonl"), "issue comments are emitted")

	code, out = c.sync("github", "fetch_repos", "gh-repos.jsonl")
	require.Equal(t, 0, code, out)
	code, out = c.sync("github", "fetch_branches", "gh-branches.jsonl")
	require.Equal(t, 0, code, "GitHub fetch_branches syncs: %s", out)
	branches := c.ids("gh-branches.jsonl")
	assert.NotEmpty(t, branches, "branches are emitted")

	c.put("github", `{"organization":"no-such-org"}`)
	code, out = c.sync("github", "fetch_repos", "unknown-org.jsonl")
	assert.Equal(t, 1, code, "an unknown GitHub organization fails: %s", out)
	assert.Contains(t, out, `"error_kind": "not_found"`)

	c.put("jira", `{"cloud_id":"no-such-site"}`)
	code, out = c.sync("jira", "fetch_issues", "unknown-site.jsonl")
	assert.Equal(t, 1, code, "an unknown Jira cloud_id fails: %s", out)
	assert.Contains(t, out, `"error_kind": "not_found"`)

	code, _, errOut := runCLI(t, "config", "put", "--org", simOrg, "--connector", "github", "--settings", `{"organization":"demo-gh-startup"}`, "--credentials", `{"personal_access_token":"simulated"}`)
	require.Equal(t, 0, code, errOut)
	code, out = c.sync("github", "fetch_repos", "pat-repos.jsonl")
	require.Equal(t, 0, code, "a personal access token lists /user/repos: %s", out)
	assert.Contains(t, out, `"status": "completed"`)
	repos, err := os.ReadFile(filepath.Join(c.dir, "pat-repos.jsonl"))
	require.NoError(t, err)
	for _, name := range []string{"web-app", "api-gateway", "mobile"} {
		assert.Contains(t, string(repos), "demo-gh-startup/"+name, "the organization's repositories are listed")
	}
	code, out = c.sync("github", "fetch_prs", "pat-prs.jsonl")
	require.Equal(t, 0, code, "fetch_prs with a personal access token: %s", out)
	assert.NotEmpty(t, c.ids("pat-prs.jsonl"), "fetch_prs emits the organization's pull requests")
}

// An operation that needs a host service, run with `sync`, which provides
// none, fails and exits 1 naming the missing service, instead of completing
// with 0 records.
func TestSync_HostOperationWithoutAHostFails(t *testing.T) {
	c := newSimCLI(t)
	c.put("github", `{"organization":"demo-gh-startup"}`)
	for _, tc := range []struct{ op, want string }{
		{"fetch_file_content", "operation github/fetch_file_content needs host services: work_queue"},
		{"fetch_pr_diffs", "operation github/fetch_pr_diffs needs host services: work_queue"},
		{"drain_defect_lineage", "operation github/drain_defect_lineage needs host services: blame_sink, lineage_walker"},
	} {
		code, out := c.sync("github", tc.op, tc.op+".jsonl")
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, tc.want)
		assert.NotContains(t, out, `"status": "completed"`)
	}
}
