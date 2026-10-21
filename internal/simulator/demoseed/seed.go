// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package demoseed inserts deterministic GitHub, Bitbucket, and GitLab simulator
// data into sim_records. SeedAll is safe to call repeatedly (weekly, or on
// demand): each run appends new timeseries rows (PRs, copilot) without touching
// existing ones, while static entities (members, repos, projects) are
// upserted to stay current.
//
// All randomness is seeded with FNV-1a hashes of (org, repo, entity, index),
// so re-running with the same inputs produces the same payload — safe to replay.
//
// GitHub, Bitbucket, and GitLab orgs are seeded (each with its own vendor-shaped builders).
package demoseed

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// prBackfillDays is the window used on first run to seed historical PR data.
const prBackfillDays = 90

// Payload map keys shared across the builders below — kept as constants so a
// typo can't silently desync a producer from a consumer (e.g. the "login" key
// buildMembers writes and the Jira builder reads back).
const (
	keyDatabaseID = "databaseId"
	keyLogin      = "login"
	keyName       = "name"
	keyEmail      = "email"
	keyNumber     = "number"
	keyTitle      = "title"
	keyCreatedAt  = "createdAt"
	keyState      = "state"
	keyAuthor     = "author"
	keyNodes      = "nodes"

	// Jira payload field names (repeated across the jira builders).
	keyKey         = "key"
	keyType        = "type"
	keyDescription = "description"

	prStateClosed = "CLOSED"
)

// recentWeek anchors non-PR timeseries entities (copilot) to the last 7 days.
func recentWeek() time.Time {
	return time.Now().UTC().Add(-7 * 24 * time.Hour)
}

// truncateToDay zeroes the time-of-day component, in UTC. It anchors a
// full PR backfill's window end to a stable point so re-running it within
// the same day is reproducible — see seedPRs' doc comment.
func truncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// asString and asInt extract typed values from the map[string]any payloads
// built by this package's own buildX functions: every call site reads a key
// its own producer just set two lines above, so a type mismatch here is a
// bug in this package, not external input — panicking surfaces it immediately
// instead of masking it behind a zero value.
func asString(m map[string]any, key string) string { return m[key].(string) } //nolint:forcetypeassert // see doc comment above
func asInt(m map[string]any, key string) int       { return m[key].(int) }    //nolint:forcetypeassert // see doc comment above

// SeedAll seeds every org in DefaultCatalog into simPool.
func SeedAll(ctx context.Context, simPool *pgxpool.Pool) error {
	for _, org := range DefaultCatalog {
		logger.Info(ctx, fmt.Sprintf("demoseed: seeding org %q (pm=%s)", org.OrgName, org.PMTool))
		if err := seedOrg(ctx, simPool, org); err != nil {
			return fmt.Errorf("seed org %q: %w", org.OrgName, err)
		}
	}
	return nil
}

// jiraAwareEntities returns the entity specs to seed for an org: its catalog set,
// plus the Jira overlay when its PM tool is Jira — one rule rather than a per-org
// catalog edit, so a future jira-PM org is covered without touching it.
//
// The Jira issue spec must come after "pr": issues are derived from the pull-request
// rows a prior spec already committed, so an order that put it first would seed an
// org's issues from the previous run's pull requests.
func jiraAwareEntities(org OrgCatalog) []EntitySpec {
	if org.PMTool != pmToolJira {
		return org.Entities
	}
	return append(append([]EntitySpec{}, org.Entities...),
		EntitySpec{Type: entityTypeJiraProject},
		EntitySpec{Type: entityTypeJiraIssue},
		EntitySpec{Type: entityTypeJiraSprint},
	)
}

func seedOrg(ctx context.Context, pool *pgxpool.Pool, org OrgCatalog) error {
	// Each vendor's member/pr/repo payloads share no shape, so the pr/member/repo
	// passes route by vendor.
	isBitbucket := org.Vendor == vendorBitbucket
	isGitLab := org.Vendor == vendorGitLab
	var members []map[string]any
	switch {
	case isBitbucket:
		members = BuildBitbucketMembers(org)
	case isGitLab:
		members = BuildGitLabMembers(org)
	default:
		members = buildMembers(org)
	}

	for _, spec := range jiraAwareEntities(org) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s/%s: %w", org.OrgName, spec.Type, err)
		}

		var seedErr error
		switch spec.Type {
		case "pr":
			switch {
			case isBitbucket:
				seedErr = seedBitbucketPRs(ctx, tx, org, members)
			case isGitLab:
				seedErr = seedGitLabMRs(ctx, tx, org, members)
			default:
				seedErr = seedPRs(ctx, tx, org, members)
			}
		case entityTypeCommit:
			if isGitLab {
				seedErr = seedGitLabCommits(ctx, tx, org, members)
			} else {
				seedErr = seedBitbucketCommits(ctx, tx, org, members)
			}
		case entityTypeMember:
			switch {
			case isBitbucket:
				seedErr = seedBitbucketMembers(ctx, tx, org, members)
			case isGitLab:
				seedErr = seedGitLabMembers(ctx, tx, org, members)
			default:
				seedErr = seedMembers(ctx, tx, org, members)
			}
		case entityTypeRepo:
			switch {
			case isBitbucket:
				seedErr = seedBitbucketRepos(ctx, tx, org)
			case isGitLab:
				seedErr = seedGitLabRepos(ctx, tx, org)
			default:
				seedErr = seedRepos(ctx, tx, org)
			}
		case entityTypeIssue:
			seedErr = seedGitLabIssues(ctx, tx, org, members)
		case entityTypeProject:
			seedErr = seedProjects(ctx, tx, org)
		case entityTypeJiraProject:
			seedErr = seedJiraProjects(ctx, tx, org)
		case entityTypeJiraIssue:
			seedErr = seedJiraIssues(ctx, tx, org, members)
		case entityTypeJiraSprint:
			seedErr = seedJiraSprints(ctx, tx, org)
		case entityTypeCopilotMetrics:
			seedErr = seedCopilotMetrics(ctx, tx, org, spec.PerRunCount)
		case entityTypeCopilotUser:
			seedErr = seedCopilotUsers(ctx, tx, org, members)
		default:
			logger.WarnWithCtx(ctx, fmt.Sprintf("demoseed: skipping unknown entity type %q (not yet implemented)", spec.Type))
		}

		if seedErr != nil {
			tx.Rollback(ctx) //nolint:errcheck,gosec // rollback on the already-failing path; the seedErr below is the one that matters
			return fmt.Errorf("seed %s for %s: %w", spec.Type, org.OrgName, seedErr)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s for %s: %w", spec.Type, org.OrgName, err)
		}
	}

	return nil
}

// seedPRs appends new PRs for each repo using that repo's FlavorProfile.
// On first run (no existing rows) a full prBackfillDays-day history is generated;
// subsequent runs add FlavorProfile.PRsPerWeek new PRs continuing from the last
// inserted PR number, spread over the trailing 7-day window.
func seedPRs(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	now := time.Now().UTC()
	for _, repo := range org.Repos {
		profile := Profiles[repo.Flavor]
		if profile.PRsPerWeek == 0 {
			// direct_push repos do not use PRs.
			continue
		}

		lastNum := maxPRNumber(ctx, tx, org.Vendor, org.OrgName, repo.Name)

		var prs []map[string]any
		if lastNum == 0 {
			// Anchored to the start of today (not now) so a full backfill is
			// reproducible within the same day: re-running it after a delete
			// (e.g. a demo-data reset) reproduces the exact same opened_at for
			// PR #N every time. A consumer may key a pull request on its
			// opened_at, so a backfill anchored to a drifting "now" would give
			// the "same" PR number a different opened_at on each fresh
			// backfill, and a second copy instead of an update.
			windowEnd := truncateToDay(now)
			windowStart := windowEnd.Add(-time.Duration(prBackfillDays) * 24 * time.Hour)
			count := profile.PRsPerWeek * (prBackfillDays / 7)
			prs = buildPRs(org, repo, members, count, 0, windowStart, windowEnd)
			logger.Info(ctx, fmt.Sprintf("demoseed:   %s/%s (%s): backfilling %d PRs over %d days", org.OrgName, repo.Name, repo.Flavor, count, prBackfillDays))
		} else {
			windowStart := now.Add(-7 * 24 * time.Hour)
			prs = buildPRs(org, repo, members, profile.PRsPerWeek, lastNum, windowStart, now)
			logger.Info(ctx, fmt.Sprintf("demoseed:   %s/%s (%s): adding %d PRs (last was #%d)", org.OrgName, repo.Name, repo.Flavor, profile.PRsPerWeek, lastNum))
		}

		for _, pr := range prs {
			entityID := fmt.Sprintf("%s#%d", repo.Name, asInt(pr, keyNumber))
			updatedAt, err := time.Parse(time.RFC3339, asString(pr, "updatedAt"))
			if err != nil {
				return fmt.Errorf("parse updatedAt for %s: %w", entityID, err)
			}
			if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, "pr", entityID, pr, updatedAt, false); err != nil {
				return fmt.Errorf("seed pr %s: %w", entityID, err)
			}
		}
	}
	return nil
}

func seedMembers(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	for _, m := range members {
		entityID := asString(m, keyLogin)
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeMember, entityID, m, recentWeek(), true); err != nil {
			return fmt.Errorf("seed member %s: %w", entityID, err)
		}
	}
	return nil
}

func seedRepos(ctx context.Context, tx pgx.Tx, org OrgCatalog) error {
	now := time.Now().UTC()
	prWindowStart := now.Add(-time.Duration(prBackfillDays) * 24 * time.Hour)
	for _, repo := range buildRepos(org, prWindowStart) {
		entityID := asString(repo, "name")
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeRepo, entityID, repo, recentWeek(), true); err != nil {
			return fmt.Errorf("seed repo %s: %w", entityID, err)
		}
	}
	return nil
}

func seedProjects(ctx context.Context, tx pgx.Tx, org OrgCatalog) error {
	for _, proj := range buildProjects(org) {
		entityID := asString(proj, keyTitle)
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeProject, entityID, proj, recentWeek(), true); err != nil {
			return fmt.Errorf("seed project %s: %w", entityID, err)
		}
	}
	return nil
}

func seedCopilotMetrics(ctx context.Context, tx pgx.Tx, org OrgCatalog, days int) error {
	for _, cm := range buildCopilotMetrics(org, days) {
		entityID := asString(cm, "date")
		d, _ := time.Parse("2006-01-02", asString(cm, "date")) //nolint:errcheck // value round-trips buildCopilotMetrics' own d.Format("2006-01-02") two lines above; cannot fail
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeCopilotMetrics, entityID, cm, d, false); err != nil {
			return fmt.Errorf("seed copilot_metrics %s: %w", entityID, err)
		}
	}
	return nil
}

func seedCopilotUsers(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	for _, cu := range buildCopilotUsers(org, members) {
		entityID := fmt.Sprintf("%s#%s", cu["github_com_user_id"], cu["day"])
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeCopilotUser, entityID, cu, recentWeek(), false); err != nil {
			return fmt.Errorf("seed copilot_user %s: %w", entityID, err)
		}
	}
	return nil
}

// upsertRow inserts a row with entity_id. When overwrite is true (static entities
// like members/repos), an existing row's payload is updated to reflect catalog changes.
// When false (timeseries like PRs, copilot), existing rows are left untouched so
// history is never rewritten.
func upsertRow(ctx context.Context, tx pgx.Tx, vendor, org, entityType, entityID string, payload any, cursorVal time.Time, overwrite bool) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s/%s: %w", entityType, entityID, err)
	}

	var q string
	if overwrite {
		q = `INSERT INTO sim_records (vendor, org_name, entity_type, entity_id, payload, cursor_val)
		     VALUES ($1, $2, $3, $4, $5, $6)
		     ON CONFLICT (vendor, org_name, entity_type, entity_id) WHERE entity_id IS NOT NULL
		     DO UPDATE SET payload = EXCLUDED.payload, cursor_val = EXCLUDED.cursor_val`
	} else {
		q = `INSERT INTO sim_records (vendor, org_name, entity_type, entity_id, payload, cursor_val)
		     VALUES ($1, $2, $3, $4, $5, $6)
		     ON CONFLICT (vendor, org_name, entity_type, entity_id) WHERE entity_id IS NOT NULL
		     DO NOTHING`
	}
	if _, err := tx.Exec(ctx, q, vendor, org, entityType, entityID, b, cursorVal); err != nil {
		return fmt.Errorf("upsert %s/%s: %w", entityType, entityID, err)
	}
	return nil
}

// maxPRNumber returns the highest PR number currently stored for (vendor, org, repo), or 0 if none.
func maxPRNumber(ctx context.Context, tx pgx.Tx, vendor, org, repo string) int {
	var maxNum int
	row := tx.QueryRow(ctx, //nolint:errcheck,gosec // COALESCE guarantees a zero-value row; Scan's only failure mode is total connection loss, in which case the tx is already dead and the caller's next call errors instead.
		`SELECT COALESCE(MAX((payload->>'number')::int), 0)
		 FROM sim_records
		 WHERE vendor=$1 AND org_name=$2 AND entity_type='pr' AND payload->>'repo'=$3`,
		vendor, org, repo,
	)
	row.Scan(&maxNum) //nolint:errcheck,gosec // see above
	return maxNum
}

// fnvRand returns a seeded *rand.Rand for deterministic generation.
func fnvRand(parts ...string) *rand.Rand {
	h := fnv.New64a()
	for _, p := range parts {
		h.Write([]byte(p)) //nolint:errcheck,gosec // hash.Hash.Write never returns an error
		h.Write([]byte{0}) //nolint:errcheck,gosec // hash.Hash.Write never returns an error
	}
	return rand.New(rand.NewSource(int64(h.Sum64()))) //nolint:gosec // deterministic replay seed, not a security context
}

// --- builders ---

func buildMembers(org OrgCatalog) []map[string]any {
	members := make([]map[string]any, 0, len(org.MemberNames))
	for _, login := range org.MemberNames {
		rng := fnvRand(org.OrgName, entityTypeMember, login)
		members = append(members, map[string]any{
			keyDatabaseID: rng.Intn(9000000) + 1000000,
			keyLogin:      login,
			keyName:       capitalize(login) + " Demo",
			keyEmail:      login + "@" + org.OrgName + ".example.com",
			"avatarUrl":   "https://avatars.githubusercontent.com/u/" + fmt.Sprint(rng.Intn(9000000)+1000000),
		})
	}
	return members
}

func buildProjects(org OrgCatalog) []map[string]any {
	projects := make([]map[string]any, 0, len(org.ProjectTitles))
	for i, title := range org.ProjectTitles {
		rng := fnvRand(org.OrgName, entityTypeProject, title)
		projects = append(projects, map[string]any{
			keyNumber:          i + 1,
			keyTitle:           title,
			"shortDescription": "Demo project: " + title,
			"url":              fmt.Sprintf("https://github.com/orgs/%s/projects/%d", org.OrgName, i+1),
			"closed":           false,
			keyCreatedAt:       recentWeek().Add(-time.Duration(rng.Intn(30)) * 24 * time.Hour).Format(time.RFC3339),
		})
	}
	return projects
}

func buildRepos(org OrgCatalog, prWindowStart time.Time) []map[string]any {
	repos := make([]map[string]any, 0, len(org.Repos))
	for _, repo := range org.Repos {
		rng := fnvRand(org.OrgName, entityTypeRepo, repo.Name)
		profile := Profiles[repo.Flavor]
		repos = append(repos, map[string]any{
			"id":                rng.Intn(9000000) + 1000000,
			"name":              repo.Name,
			"full_name":         org.OrgName + "/" + repo.Name,
			"private":           false,
			"html_url":          "https://github.com/" + org.OrgName + "/" + repo.Name,
			"description":       "Demo repository: " + repo.Name,
			"fork":              repo.Flavor == FlavorForkAndPull,
			"default_branch":    profile.DefaultBranch,
			"language":          pickLang(rng),
			"stargazers_count":  rng.Intn(500),
			"forks_count":       rng.Intn(50),
			"open_issues_count": rng.Intn(20),
			"archived":          false,
			"updated_at":        recentWeek().Format(time.RFC3339),
			"pushed_at":         recentWeek().Format(time.RFC3339),
			"created_at":        prWindowStart.Add(-time.Duration(rng.Intn(365)) * 24 * time.Hour).Format(time.RFC3339),
		})
	}
	return repos
}

// buildPRs generates count PRs for the given repo starting at PR number startNum+1.
// Timestamps are spread evenly between windowStart and windowEnd.
// For github-issues orgs, a fraction of PRs include a closingIssuesReferences entry
// so the simulator can serve GitHub Issues linked to those PRs.
func buildPRs(org OrgCatalog, repo RepoSpec, members []map[string]any, count, startNum int, windowStart, windowEnd time.Time) []map[string]any {
	profile := Profiles[repo.Flavor]
	window := windowEnd.Sub(windowStart)
	var spacing time.Duration
	if count > 1 {
		spacing = window / time.Duration(count)
	}

	prs := make([]map[string]any, count)
	for i := range prs {
		num := startNum + i + 1
		rng := fnvRand(org.OrgName, repo.Name, "pr", fmt.Sprint(num))
		author := members[rng.Intn(len(members))]
		authorLogin := asString(author, keyLogin)
		authorID := asInt(author, keyDatabaseID)

		// Determine merge state using the flavor's merge ratio.
		merged := rng.Float64() < profile.MergeRatio
		var state string
		if merged {
			state = "MERGED"
		} else if rng.Float64() < 0.5 {
			state = "OPEN"
		} else {
			state = prStateClosed
		}

		t := windowStart.Add(time.Duration(i) * spacing)
		createdAt := t.Format(time.RFC3339)
		updatedAt := t.Add(2 * time.Hour).Format(time.RFC3339)
		var mergedAt any
		if state == "MERGED" {
			mergedAt = t.Add(2*time.Hour + time.Duration(rng.Intn(48))*time.Hour).Format(time.RFC3339)
		}

		// LOC varies by flavor profile ± 50% jitter.
		additions := profile.BaseAdds/2 + rng.Intn(profile.BaseAdds)
		deletions := profile.BaseDels/2 + rng.Intn(profile.BaseDels)

		baseSHA := fmt.Sprintf("%040x", fnvRand(org.OrgName, repo.Name, "base", fmt.Sprint(num)).Int63())
		commits := buildCommits(rng, authorLogin, authorID, createdAt, profile.Reviewers+1)
		reviews := buildReviews(rng, members, updatedAt, profile.Reviewers)
		comments := buildPRComments(rng, members, updatedAt, rng.Intn(3))

		// Branch naming follows the flavor convention.
		headRef := fmt.Sprintf("%s%s-%d", profile.FeaturePrefix, repo.Name, num)
		if profile.FeaturePrefix == "" {
			headRef = fmt.Sprintf("%s-%d", repo.Name, num)
		}

		// For github-issues orgs, ~60% of PRs link a closing issue.
		closingIssues := buildClosingIssues(org, repo, num, rng)

		title := fmt.Sprintf("%s: %s #%d", repo.Name, prVerb(rng), num)
		body := fmt.Sprintf("Demo PR body for %s #%d", repo.Name, num)
		// For a jira-PM org, stamp a linking issue key into the PR title, body, and
		// branch so the prissue linkage engine resolves it against the seeded Jira
		// issues. The key comes from a key-dedicated seed, so the PR rng
		// sequence — and thus every non-jira org's deterministic history — is untouched.
		if key, ok := jiraLinkKeyForPR(org, repo, num); ok {
			title = key + ": " + title
			body = key + " " + body
			headRef = headRef + "-" + key
		}

		prs[i] = map[string]any{
			keyNumber:    num,
			keyTitle:     title,
			keyState:     state,
			"body":       body,
			"url":        fmt.Sprintf("https://github.com/%s/%s/pull/%d", org.OrgName, repo.Name, num),
			keyCreatedAt: createdAt,
			"updatedAt":  updatedAt,
			"mergedAt":   mergedAt,
			"repo":       repo.Name,
			"baseSha":    baseSHA,
			"diff":       buildDiff(fnvRand(org.OrgName, repo.Name, "diff", fmt.Sprint(num)), additions, deletions),
			keyAuthor: map[string]any{
				keyLogin:      authorLogin,
				keyDatabaseID: authorID,
				keyEmail:      authorLogin + "@example.com",
			},
			"baseRefName":  profile.DefaultBranch,
			"headRefName":  headRef,
			"additions":    additions,
			"deletions":    deletions,
			"changedFiles": rng.Intn(20) + 1,
			"commits": map[string]any{
				keyNodes: commits,
			},
			"reviews": map[string]any{
				keyNodes: reviews,
			},
			"comments": map[string]any{
				"totalCount": len(comments),
				keyNodes:     comments,
				"pageInfo":   map[string]any{"hasNextPage": false, "endCursor": ""},
			},
			"closingIssuesReferences": map[string]any{
				keyNodes: closingIssues,
			},
			"files":     map[string]any{keyNodes: []any{}},
			"labels":    map[string]any{keyNodes: []any{}},
			"assignees": map[string]any{keyNodes: []any{}},
		}
	}
	return prs
}

// buildClosingIssues returns a linked GitHub Issue for ~60% of PRs in
// github-issues orgs. Returns empty slice for jira orgs (no GitHub Issues PM).
func buildClosingIssues(org OrgCatalog, repo RepoSpec, prNum int, rng *rand.Rand) []any {
	if org.PMTool != pmToolGitHubIssues {
		return []any{}
	}
	if rng.Float64() >= 0.60 {
		return []any{}
	}
	issueNum := prNum // one issue per PR for simplicity; issue number mirrors PR number
	states := []string{prStateClosed, prStateClosed, "OPEN"}
	return []any{
		map[string]any{
			keyNumber: issueNum,
			keyTitle:  fmt.Sprintf("%s: issue #%d", repo.Name, issueNum),
			keyState:  states[rng.Intn(len(states))],
		},
	}
}

func buildDiff(rng *rand.Rand, additions, deletions int) string {
	treeFiles := []string{"main.go", "README.md", "go.mod", "Makefile"}
	numFiles := rng.Intn(3) + 1
	var sb strings.Builder
	for f := 0; f < numFiles; f++ {
		file := treeFiles[rng.Intn(len(treeFiles))]
		oldIdx := fmt.Sprintf("%06x", rng.Intn(0xffffff))
		newIdx := fmt.Sprintf("%06x", rng.Intn(0xffffff))
		fmt.Fprintf(&sb, "diff --git a/%s b/%s\n", file, file)
		fmt.Fprintf(&sb, "index %s..%s 100644\n", oldIdx, newIdx)
		fmt.Fprintf(&sb, "--- a/%s\n+++ b/%s\n", file, file)

		lineStart := rng.Intn(50) + 1
		adds := additions/numFiles + 1
		dels := deletions/numFiles + 1
		if adds > 8 {
			adds = 8
		}
		if dels > 4 {
			dels = 4
		}
		fmt.Fprintf(&sb, "@@ -%d,%d +%d,%d @@\n", lineStart, dels+2, lineStart, adds+2)
		sb.WriteString(" // context\n")
		for d := 0; d < dels; d++ {
			fmt.Fprintf(&sb, "-\t// removed: line %d\n", lineStart+d)
		}
		for a := 0; a < adds; a++ {
			fmt.Fprintf(&sb, "+\t// added: line %d\n", lineStart+a)
		}
		sb.WriteString(" // context\n")
	}
	return sb.String()
}

// buildCommits fills each node with commit dates in two shapes because the
// simulator serves this same node array to two different real consumers: the
// PR-embedded commits.nodes[].commit.{authoredDate,committedDate} shape read
// by the connector's fetch_prs parser (parsePREntityFields's
// SupplementalFirstCommitAt), and the flat top-level
// authoredDate/committedDate shape read by the separate fetch_commits stream
// parser (ParseCommit). Without both, first_commit_at never lands in
// a consumer's pull-request metadata and Cycle Time has nothing to aggregate.
func buildCommits(rng *rand.Rand, authorLogin string, authorID int, baseTime string, count int) []map[string]any {
	committedAt, _ := time.Parse(time.RFC3339, baseTime) //nolint:errcheck // baseTime is always this package's own t.Format(time.RFC3339) (buildPRs' createdAt); cannot fail
	commits := make([]map[string]any, count)
	for i := range commits {
		// authoredDate precedes committedDate by a small jitter, mirroring a
		// real author-writes-then-pushes gap.
		authoredAt := committedAt.Add(-time.Duration(rng.Intn(180)+5) * time.Minute)
		authoredStr := authoredAt.Format(time.RFC3339)
		committedStr := committedAt.Format(time.RFC3339)
		commits[i] = map[string]any{
			"oid":                     fmt.Sprintf("%040x", rng.Int63()),
			"message":                 fmt.Sprintf("chore: commit %d", i+1),
			"authoredDate":            authoredStr,
			"committedDate":           committedStr,
			"additions":               rng.Intn(100),
			"deletions":               rng.Intn(50),
			"changedFilesIfAvailable": rng.Intn(5) + 1,
			keyAuthor: map[string]any{
				keyName:  capitalize(authorLogin),
				keyEmail: authorLogin + "@example.com",
				"user": map[string]any{
					keyDatabaseID: authorID,
					keyLogin:      authorLogin,
				},
			},
			// Nested form for the PR-embedded commits.nodes[].commit shape — see
			// doc comment above.
			"commit": map[string]any{
				"authoredDate":  authoredStr,
				"committedDate": committedStr,
			},
		}
	}
	return commits
}

func buildReviews(rng *rand.Rand, members []map[string]any, submittedAt string, count int) []map[string]any {
	reviewStates := []string{"APPROVED", "CHANGES_REQUESTED", "COMMENTED"}
	reviews := make([]map[string]any, count)
	for i := range reviews {
		reviewer := members[rng.Intn(len(members))]
		reviews[i] = map[string]any{
			keyDatabaseID: rng.Intn(9000000) + 1000000,
			keyState:      reviewStates[rng.Intn(len(reviewStates))],
			"submittedAt": submittedAt,
			keyAuthor: map[string]any{
				keyLogin:      reviewer[keyLogin],
				keyDatabaseID: reviewer[keyDatabaseID],
			},
		}
	}
	return reviews
}

func buildPRComments(rng *rand.Rand, members []map[string]any, createdAt string, count int) []map[string]any {
	comments := make([]map[string]any, count)
	for i := range comments {
		commenter := members[rng.Intn(len(members))]
		comments[i] = map[string]any{
			keyDatabaseID: rng.Intn(9000000) + 1000000,
			"body":        fmt.Sprintf("Demo comment %d", i+1),
			keyCreatedAt:  createdAt,
			keyAuthor: map[string]any{
				keyLogin:      commenter[keyLogin],
				keyDatabaseID: commenter[keyDatabaseID],
			},
		}
	}
	return comments
}

func buildCopilotMetrics(org OrgCatalog, days int) []map[string]any {
	metrics := make([]map[string]any, days)
	for i := range metrics {
		d := recentWeek().Add(time.Duration(i) * 24 * time.Hour)
		rng := fnvRand(org.OrgName, entityTypeCopilotMetrics, d.Format("2006-01-02"))
		metrics[i] = map[string]any{
			"date":                d.Format("2006-01-02"),
			"total_active_users":  rng.Intn(15) + 5,
			"total_engaged_users": rng.Intn(10) + 3,
			"copilot_ide_code_completions": map[string]any{
				"total_engaged_users": rng.Intn(8) + 2,
			},
		}
	}
	return metrics
}

func buildCopilotUsers(org OrgCatalog, members []map[string]any) []map[string]any {
	users := make([]map[string]any, len(members))
	for i, m := range members {
		rng := fnvRand(org.OrgName, entityTypeCopilotUser, asString(m, keyLogin))
		users[i] = map[string]any{
			"github_com_user_id":      fmt.Sprint(m[keyDatabaseID]),
			"day":                     recentWeek().Format("2006-01-02"),
			"total_suggestions_count": rng.Intn(100),
			"total_acceptances_count": rng.Intn(50),
		}
	}
	return users
}

func prVerb(rng *rand.Rand) string {
	verbs := []string{"feat", "fix", "refactor", "chore", "docs", "perf", "test"}
	return verbs[rng.Intn(len(verbs))]
}

func pickLang(rng *rand.Rand) string {
	langs := []string{"Go", "TypeScript", "Python", "Rust", "Java", "Ruby"}
	return langs[rng.Intn(len(langs))]
}

func capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	return string(s[0]-32) + s[1:]
}
