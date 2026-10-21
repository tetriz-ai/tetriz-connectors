// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package demoseed

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
)

// What only real SQL can exercise here is the read half of the seed path:
// seedJiraIssues derives its issues from pull-request rows it reads back out of
// sim_records, so the JSON round-trip and the per-vendor field projection sit
// between the two builders and no builder-level test crosses them.
//
// Worth stating what these do NOT prove, so the next reader does not over-read them:
// the payloads are stable across runs because the builder is a pure function of
// immutable pull-request rows and reads no clock — not because the rows are written
// non-destructively. Flipping seedJiraIssues to overwrite the rows leaves every
// assertion below green, since re-deriving yields identical bytes. The properties
// that would actually catch a regression are TestJiraIssuesAccumulateAcrossRuns and
// TestJiraIssueBuilderReadsNoClock.

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	if err := testdb.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "skipping demoseed integration tests: cannot reach test database: %v\n", err)
		os.Exit(0)
	}
	ctx := context.Background()
	_, leasedDSN, cleanup, err := testdb.Setup(ctx, testdb.Simulator)
	if err != nil {
		fmt.Fprintf(os.Stderr, "demoseed test schema load failed: %v\n", err)
		os.Exit(1)
	}
	dsn := leasedDSN
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open pgx pool: %v\n", err)
		os.Exit(1)
	}
	testPool = pool
	code := testguard.Run(m)
	pool.Close()
	cleanup()
	os.Exit(code)
}

type seededIssue struct {
	payload   string
	cursorVal string
}

func readSeededIssues(t *testing.T, org string) map[string]seededIssue {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT entity_id, payload::text, cursor_val::text FROM sim_records
		 WHERE vendor='jira' AND org_name=$1 AND entity_type='issue'`, org)
	require.NoError(t, err)
	defer rows.Close()

	out := map[string]seededIssue{}
	for rows.Next() {
		var id, payload, cursor string
		require.NoError(t, rows.Scan(&id, &payload, &cursor))
		out[id] = seededIssue{payload: payload, cursorVal: cursor}
	}
	require.NoError(t, rows.Err())
	return out
}

// inTx runs fn in a committed transaction, the way seedOrg drives each entity pass.
func inTx(t *testing.T, fn func(tx pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	require.NoError(t, err)
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx) //nolint:errcheck // the fn error below is the one that matters
		t.Fatalf("seed pass failed: %v", err)
	}
	require.NoError(t, tx.Commit(ctx))
}

func TestSeedJiraIssuesAccumulatesInDB(t *testing.T) {
	org := jiraPMOrgs()[0]
	org.OrgName = "demo-itest-accumulate" // isolated from any other test's rows
	members := jiraOrgMembers(org)
	ctx := context.Background()

	t.Cleanup(func() {
		_, err := testPool.Exec(ctx, `DELETE FROM sim_records WHERE org_name=$1`, org.OrgName)
		require.NoError(t, err)
	})

	// First run: pull requests, then the issues derived from them.
	inTx(t, func(tx pgx.Tx) error { return seedPRs(ctx, tx, org, members) })
	inTx(t, func(tx pgx.Tx) error { return seedJiraIssues(ctx, tx, org, members) })
	first := readSeededIssues(t, org.OrgName)
	require.NotEmpty(t, first, "first run seeded no jira issues")

	// Second run: seedPRs appends a week of new pull requests, so seedJiraIssues
	// sees a longer history and must add the issues those bring.
	inTx(t, func(tx pgx.Tx) error { return seedPRs(ctx, tx, org, members) })
	inTx(t, func(tx pgx.Tx) error { return seedJiraIssues(ctx, tx, org, members) })
	second := readSeededIssues(t, org.OrgName)

	require.Greater(t, len(second), len(first), "second run added no jira issues")
	for key, was := range first {
		now, ok := second[key]
		require.True(t, ok, "issue %s disappeared on the second run", key)
		require.JSONEq(t, was.payload, now.payload, "issue %s's payload was rewritten", key)
		require.Equal(t, was.cursorVal, now.cursorVal,
			"issue %s's cursor_val moved; the connector's high-water mark would re-emit or skip it", key)
		// cursor_val must be the issue's own fields.updated, since the ascending
		// search keysets on it — a row whose cursor disagrees with its payload is
		// returned in the wrong page.
		require.Contains(t, now.payload, `"updated"`, "issue %s carries no fields.updated to key the cursor on", key)
	}
}

// TestSeedJiraIssuesIsIdempotent covers the re-run-without-new-PRs case: nothing new
// to derive means nothing may change, so a demo-seed run that no-ops leaves the demo
// exactly as it was.
func TestSeedJiraIssuesIsIdempotent(t *testing.T) {
	org := jiraPMOrgs()[0]
	org.OrgName = "demo-itest-idempotent"
	members := jiraOrgMembers(org)
	ctx := context.Background()

	t.Cleanup(func() {
		_, err := testPool.Exec(ctx, `DELETE FROM sim_records WHERE org_name=$1`, org.OrgName)
		require.NoError(t, err)
	})

	inTx(t, func(tx pgx.Tx) error { return seedPRs(ctx, tx, org, members) })
	inTx(t, func(tx pgx.Tx) error { return seedJiraIssues(ctx, tx, org, members) })
	before := readSeededIssues(t, org.OrgName)
	require.NotEmpty(t, before)

	// Re-derive from the same pull-request rows.
	inTx(t, func(tx pgx.Tx) error { return seedJiraIssues(ctx, tx, org, members) })
	after := readSeededIssues(t, org.OrgName)

	require.Len(t, after, len(before), "re-running the issue pass changed the row count")
	for key, was := range before {
		now := after[key]
		require.JSONEq(t, was.payload, now.payload, "issue %s changed on an unchanged re-run", key)
		require.Equal(t, was.cursorVal, now.cursorVal, "issue %s's cursor_val changed on an unchanged re-run", key)
	}
}

// TestSeededIssuesReferenceSeededProjects catches a project-key drift between the two
// builders: an issue whose fields.project.key names no seeded project row is invisible
// to the JQL search, which filters on exactly that key.
func TestSeededIssuesReferenceSeededProjects(t *testing.T) {
	org := jiraPMOrgs()[0]
	org.OrgName = "demo-itest-projects"
	members := jiraOrgMembers(org)
	ctx := context.Background()

	t.Cleanup(func() {
		_, err := testPool.Exec(ctx, `DELETE FROM sim_records WHERE org_name=$1`, org.OrgName)
		require.NoError(t, err)
	})

	inTx(t, func(tx pgx.Tx) error { return seedJiraProjects(ctx, tx, org) })
	inTx(t, func(tx pgx.Tx) error { return seedPRs(ctx, tx, org, members) })
	inTx(t, func(tx pgx.Tx) error { return seedJiraIssues(ctx, tx, org, members) })

	var orphans int
	err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM sim_records i
		WHERE i.vendor='jira' AND i.org_name=$1 AND i.entity_type='issue'
		  AND NOT EXISTS (
		    SELECT 1 FROM sim_records p
		    WHERE p.vendor='jira' AND p.org_name=i.org_name AND p.entity_type='project'
		      AND p.payload->>'key' = i.payload->'fields'->'project'->>'key')`,
		org.OrgName).Scan(&orphans)
	require.NoError(t, err)
	require.Zero(t, orphans, "%d seeded issues name a project that has no project row", orphans)

	// And the reverse direction of the same contract: every issue is findable by the
	// project filter the JQL search applies.
	var issues, matched int
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT count(*) FROM sim_records WHERE vendor='jira' AND org_name=$1 AND entity_type='issue'`,
		org.OrgName).Scan(&issues))
	require.Positive(t, issues)
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT count(*) FROM sim_records WHERE vendor='jira' AND org_name=$1 AND entity_type='issue'
		   AND payload->'fields'->'project'->>'key' IS NOT NULL`,
		org.OrgName).Scan(&matched))
	require.Equal(t, issues, matched, "an issue with no fields.project.key can never be returned by the JQL search")
}
