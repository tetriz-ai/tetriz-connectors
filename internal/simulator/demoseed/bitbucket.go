// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package demoseed

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
)

// Bitbucket seed builders. Unlike GitHub's GraphQL-shaped payloads, these emit the
// Bitbucket REST shapes the connector's bb* structs unmarshal (api_types.go): repos
// keyed by `slug`, PRs by numeric `id` (not GitHub's `number`), commits by `hash`,
// members as {"user":{…}}. The seed↔connector contract is pinned at build time by
// connectors/bitbucket/demoseed_contract_test.go. Every nested user carries
// account_id, because a consumer keys identity on it.
//
// Payload map keys — kept as constants so a producer typo can't silently desync from
// a consumer (the handler filters and the connector structs).
const (
	bbKeyRepo      = "repo" // top-level slug duplicate: the handlers' repo-scoped filter column
	bbKeyID        = "id"
	bbKeyHash      = "hash"
	bbKeyAccountID = "account_id"
	bbKeyUser      = "user"
	bbKeyNickname  = "nickname"
	bbKeyUUID      = "uuid"
	bbKeyFullName  = "full_name"
	bbKeyBranch    = "branch"
	bbKeyMessage   = "message"
	bbKeyDiffstat  = "diffstat"
	bbKeyCreatedOn = "created_on"
	bbKeyUpdatedOn = "updated_on"
	bbKeyDate      = "date"
	bbStateMerged  = "MERGED"
	bbStateOpen    = "OPEN"
)

// bbUserMap builds the nested bbUser object for a member login, deterministically so
// the identity-correlation invariant holds: the same login yields the same uuid and
// account_id everywhere it appears (member row, pr.author, reviewers, commit author),
// which is what lets a consumer resolve every appearance to one person.
func bbUserMap(org, login string) map[string]any {
	return map[string]any{
		bbKeyUUID:      bbUUID(org, "user", login),
		"display_name": capitalize(login) + " Demo",
		bbKeyNickname:  login,
		bbKeyAccountID: login + "-acct",
	}
}

// bbUUID returns a stable braced-hex UUID for (parts…) — the connector treats it as
// an opaque string, so only stability and non-emptiness matter.
func bbUUID(parts ...string) string {
	return fmt.Sprintf("{%016x}", fnvRand(parts...).Uint64())
}

// bbAuthorRaw is the commit author's raw string, in the exact
// "{Display Name} <{login}@{org}.example.com>" form that the connector's
// commit-email fallback parses (ADR-0004), so a seeded commit resolves to the
// same login as the member row.
func bbAuthorRaw(org, login string) string {
	return fmt.Sprintf("%s Demo <%s@%s.example.com>", capitalize(login), login, org)
}

// BuildBitbucketMembers returns one member payload per roster login, in the
// {"user":{…}} shape fetch_workspace_members serves. Exported for the contract test.
func BuildBitbucketMembers(org OrgCatalog) []map[string]any {
	members := make([]map[string]any, 0, len(org.MemberNames))
	for _, login := range org.MemberNames {
		members = append(members, map[string]any{bbKeyUser: bbUserMap(org.OrgName, login)})
	}
	return members
}

// bbMemberLogin extracts the login (nickname) from a member payload built above.
func bbMemberLogin(m map[string]any) string {
	return m[bbKeyUser].(map[string]any)[bbKeyNickname].(string) //nolint:forcetypeassert // this package built the map two calls above
}

// bbMemberAccountID extracts the account_id from a member payload built above.
func bbMemberAccountID(m map[string]any) string {
	return m[bbKeyUser].(map[string]any)[bbKeyAccountID].(string) //nolint:forcetypeassert // this package built the map two calls above
}

// BuildBitbucketRepos returns the repo payloads (full bbRepository shape incl.
// mainbranch, workspace, project) for the org. Exported for the contract test.
func BuildBitbucketRepos(org OrgCatalog, createdWindowStart time.Time) []map[string]any {
	repos := make([]map[string]any, 0, len(org.Repos))
	for _, repo := range org.Repos {
		rng := fnvRand(org.OrgName, entityTypeRepo, repo.Name)
		profile := Profiles[repo.Flavor]
		repos = append(repos, map[string]any{
			bbKeyRepo:      repo.Name,
			bbKeyUUID:      bbUUID(org.OrgName, "repo", repo.Name),
			"slug":         repo.Name,
			bbKeyFullName:  org.OrgName + "/" + repo.Name,
			"name":         repo.Name,
			"is_private":   true,
			"scm":          "git",
			bbKeyUpdatedOn: recentWeek().Format(time.RFC3339),
			bbKeyCreatedOn: createdWindowStart.Add(-time.Duration(rng.Intn(365)) * 24 * time.Hour).Format(time.RFC3339),
			"mainbranch":   map[string]any{bbKeyName: profile.DefaultBranch, "type": bbKeyBranch},
			"workspace":    map[string]any{bbKeyUUID: bbUUID(org.OrgName, "workspace"), "slug": org.OrgName, bbKeyName: org.OrgName},
			"project":      map[string]any{"key": bbProjectKey(repo.Name), bbKeyName: capitalize(repo.Name), bbKeyUUID: bbUUID(org.OrgName, "project", repo.Name)},
		})
	}
	return repos
}

func bbProjectKey(repo string) string {
	key := ""
	for _, r := range repo {
		if r >= 'a' && r <= 'z' {
			key += string(r - 32)
		}
		if len(key) == 4 {
			break
		}
	}
	if key == "" {
		key = "PROJ"
	}
	return key
}

// BuildBitbucketPRs generates count PRs for the repo starting at id startID+1,
// timestamps spread over [windowStart, windowEnd]. Each PR carries its nested
// diffstat/activity/comments/commits in final REST shape (the handlers extract them
// verbatim). Exported for the contract test.
func BuildBitbucketPRs(org OrgCatalog, repo RepoSpec, members []map[string]any, count, startID int, windowStart, windowEnd time.Time) []map[string]any {
	profile := Profiles[repo.Flavor]
	window := windowEnd.Sub(windowStart)
	var spacing time.Duration
	if count > 1 {
		spacing = window / time.Duration(count)
	}

	prs := make([]map[string]any, count)
	for i := range prs {
		id := startID + i + 1
		rng := fnvRand(org.OrgName, repo.Name, "bbpr", fmt.Sprint(id))
		authorLogin := bbMemberLogin(members[rng.Intn(len(members))])

		merged := rng.Float64() < profile.MergeRatio
		state := bbStateOpen
		if merged {
			state = bbStateMerged
		} else if rng.Float64() < 0.5 {
			state = "DECLINED"
		}

		t := windowStart.Add(time.Duration(i) * spacing)
		createdOn := t.Format(time.RFC3339)
		updatedOn := t.Add(2 * time.Hour).Format(time.RFC3339)

		additions := profile.BaseAdds/2 + rng.Intn(profile.BaseAdds+1)
		deletions := profile.BaseDels/2 + rng.Intn(profile.BaseDels+1)
		headRef := fmt.Sprintf("%s%s-%d", profile.FeaturePrefix, repo.Name, id)
		if profile.FeaturePrefix == "" {
			headRef = fmt.Sprintf("%s-%d", repo.Name, id)
		}

		reviewers, participants := bbReviewersAndParticipants(rng, org.OrgName, members, profile.Reviewers, updatedOn)
		commits := bbCommitObjs(org.OrgName, repo.Name, id, authorLogin, t, rng.Intn(3)+1)
		activity := bbActivityEntries(rng, org.OrgName, members, updatedOn)
		comments := bbCommentEntries(rng, org.OrgName, members, updatedOn)

		title := fmt.Sprintf("%s: %s #%d", repo.Name, prVerb(rng), id)
		description := fmt.Sprintf("Demo PR for %s #%d", repo.Name, id)
		// For a jira-PM org, stamp a linking issue key into the PR title, description,
		// and source branch. Key-dedicated seed → PR rng untouched.
		if key, ok := jiraLinkKeyForPR(org, repo, id); ok {
			title = key + ": " + title
			description = key + " " + description
			headRef = headRef + "-" + key
		}

		prs[i] = map[string]any{
			bbKeyRepo:      repo.Name,
			bbKeyID:        id,
			bbKeyTitle:     title,
			keyDescription: description,
			"state":        state,
			bbKeyCreatedOn: createdOn,
			bbKeyUpdatedOn: updatedOn,
			bbKeyAuthor:    bbUserMap(org.OrgName, authorLogin),
			"source":       map[string]any{bbKeyBranch: map[string]any{bbKeyName: headRef}},
			"destination": map[string]any{
				bbKeyBranch:  map[string]any{bbKeyName: profile.DefaultBranch},
				"repository": map[string]any{bbKeyUUID: bbUUID(org.OrgName, "repo", repo.Name), bbKeyFullName: org.OrgName + "/" + repo.Name},
			},
			"reviewers":    reviewers,
			"participants": participants,
			bbKeyDiffstat:  bbDiffstat(rng, additions, deletions),
			"activity":     activity,
			"comments":     comments,
			"commits":      commits,
			"diff":         buildDiff(fnvRand(org.OrgName, repo.Name, "bbdiff", fmt.Sprint(id)), additions, deletions),
		}
	}
	return prs
}

// bbKeyTitle/bbKeyAuthor/bbKeyName reuse the seed.go title/author/name constants.
const (
	bbKeyTitle  = keyTitle
	bbKeyAuthor = keyAuthor
	bbKeyName   = keyName
)

func bbReviewersAndParticipants(rng *rand.Rand, org string, members []map[string]any, count int, approvalDate string) ([]map[string]any, []map[string]any) {
	reviewers := make([]map[string]any, 0, count)
	participants := make([]map[string]any, 0, count)
	for i := 0; i < count; i++ {
		login := bbMemberLogin(members[rng.Intn(len(members))])
		user := bbUserMap(org, login)
		reviewers = append(reviewers, user)
		participants = append(participants, map[string]any{
			bbKeyUser:  user,
			"role":     "REVIEWER",
			"approved": rng.Float64() < 0.7,
			"state":    "approved",
		})
	}
	return reviewers, participants
}

// bbCommitObjs builds the PR-nested commits[] — full bbCommit objects (hash/date/
// message/author.raw+user), oldest-first, ending just before the PR's own time so
// the per-PR commits route's sort=date enrichment resolves first_commit_at.
func bbCommitObjs(org, repo string, prID int, authorLogin string, prTime time.Time, count int) []map[string]any {
	commits := make([]map[string]any, count)
	for i := range commits {
		rng := fnvRand(org, repo, "bbcommit", fmt.Sprint(prID), fmt.Sprint(i))
		// Spread commit dates before the PR update, oldest first.
		date := prTime.Add(-time.Duration(count-i) * time.Duration(rng.Intn(180)+30) * time.Minute)
		commits[i] = map[string]any{
			bbKeyHash:    fmt.Sprintf("%040x", rng.Int63()),
			bbKeyDate:    date.Format(time.RFC3339),
			bbKeyMessage: fmt.Sprintf("%s: commit %d for #%d", prVerb(rng), i+1, prID),
			bbKeyAuthor: map[string]any{
				"raw":     bbAuthorRaw(org, authorLogin),
				bbKeyUser: bbUserMap(org, authorLogin),
			},
		}
	}
	return commits
}

func bbActivityEntries(rng *rand.Rand, org string, members []map[string]any, when string) []map[string]any {
	approver := bbUserMap(org, bbMemberLogin(members[rng.Intn(len(members))]))
	commenter := bbUserMap(org, bbMemberLogin(members[rng.Intn(len(members))]))
	return []map[string]any{
		{"approval": map[string]any{bbKeyDate: when, bbKeyUser: approver}},
		{"comment": map[string]any{bbKeyID: rng.Intn(9000) + 1000, bbKeyCreatedOn: when, bbKeyUser: commenter}},
	}
}

func bbCommentEntries(rng *rand.Rand, org string, members []map[string]any, when string) []map[string]any {
	n := rng.Intn(3)
	comments := make([]map[string]any, n)
	for i := range comments {
		comments[i] = map[string]any{
			bbKeyID:        rng.Intn(9000) + 1000,
			bbKeyCreatedOn: when,
			bbKeyUser:      bbUserMap(org, bbMemberLogin(members[rng.Intn(len(members))])),
		}
	}
	return comments
}

func bbDiffstat(rng *rand.Rand, additions, deletions int) []map[string]any {
	files := []string{"service.go", "handler.go", "model.go", "README.md"}
	n := rng.Intn(3) + 1
	entries := make([]map[string]any, n)
	for i := range entries {
		entries[i] = map[string]any{
			"status":        "modified",
			"lines_added":   additions/n + 1,
			"lines_removed": deletions/n + 1,
			"new":           map[string]any{"path": files[rng.Intn(len(files))]},
		}
	}
	return entries
}

// bbCommitRow converts a PR-nested commit object (from bbCommitObjs) into a
// repo-level `commit` row payload for the fetch_commits stream, adding the
// repo/branch/diffstat fields that route serves. The hash is carried through
// verbatim, so the repo commit stream cross-references the PR's commits[].
func bbCommitRow(org, repo, branch string, commitObj map[string]any) map[string]any {
	rng := fnvRand(org, repo, "bbcommitrow", commitObj[bbKeyHash].(string)) //nolint:forcetypeassert // built by bbCommitObjs
	row := map[string]any{
		bbKeyRepo:     repo,
		bbKeyHash:     commitObj[bbKeyHash],
		bbKeyDate:     commitObj[bbKeyDate],
		bbKeyMessage:  commitObj[bbKeyMessage],
		bbKeyAuthor:   commitObj[bbKeyAuthor],
		bbKeyBranch:   branch,
		bbKeyDiffstat: bbDiffstat(rng, rng.Intn(120)+10, rng.Intn(40)+2),
	}
	return row
}

// BuildBitbucketDirectPushCommits generates standalone direct-push commit rows for a
// repo — commits that landed on the default branch outside any PR. count is spread
// over [windowStart, windowEnd]. These are the only commits for a zero-PR
// (direct-push) repo, so demo-bb-solo is not left empty. Exported for the contract
// test.
func BuildBitbucketDirectPushCommits(org OrgCatalog, repo RepoSpec, members []map[string]any, count int, windowStart, windowEnd time.Time) []map[string]any {
	if count <= 0 {
		return nil
	}
	profile := Profiles[repo.Flavor]
	window := windowEnd.Sub(windowStart)
	var spacing time.Duration
	if count > 1 {
		spacing = window / time.Duration(count)
	}
	rows := make([]map[string]any, count)
	for i := range rows {
		rng := fnvRand(org.OrgName, repo.Name, "bbpush", fmt.Sprint(windowStart.Unix()), fmt.Sprint(i))
		login := bbMemberLogin(members[rng.Intn(len(members))])
		date := windowStart.Add(time.Duration(i) * spacing)
		rows[i] = map[string]any{
			bbKeyRepo:    repo.Name,
			bbKeyHash:    fmt.Sprintf("%040x", rng.Int63()),
			bbKeyDate:    date.Format(time.RFC3339),
			bbKeyMessage: fmt.Sprintf("%s: direct push", prVerb(rng)),
			bbKeyAuthor: map[string]any{
				"raw":     bbAuthorRaw(org.OrgName, login),
				bbKeyUser: bbUserMap(org.OrgName, login),
			},
			bbKeyBranch:   profile.DefaultBranch,
			bbKeyDiffstat: bbDiffstat(rng, profile.BaseAdds/2+rng.Intn(profile.BaseAdds+1), profile.BaseDels/2+rng.Intn(profile.BaseDels+1)),
		}
	}
	return rows
}

// maxBBPRID returns the highest Bitbucket PR id stored for (vendor, org, repo), or 0.
// Bitbucket PRs key on payload->>'id' (not GitHub's 'number'), so incremental runs
// continue from this rather than maxPRNumber.
func maxBBPRID(ctx context.Context, tx pgx.Tx, vendor, org, repo string) int {
	var maxID int
	row := tx.QueryRow(ctx, //nolint:errcheck,gosec // COALESCE guarantees a row; see maxPRNumber
		`SELECT COALESCE(MAX((payload->>'id')::int), 0)
		 FROM sim_records
		 WHERE vendor=$1 AND org_name=$2 AND entity_type='pr' AND payload->>'repo'=$3`,
		vendor, org, repo,
	)
	row.Scan(&maxID) //nolint:errcheck,gosec // see maxPRNumber
	return maxID
}

// --- Bitbucket seed passes (per-entity, mirror the GitHub seedX functions) ---

func seedBitbucketMembers(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	for _, m := range members {
		entityID := bbMemberAccountID(m)
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeMember, entityID, m, recentWeek(), true); err != nil {
			return fmt.Errorf("seed bitbucket member %s: %w", entityID, err)
		}
	}
	return nil
}

func seedBitbucketRepos(ctx context.Context, tx pgx.Tx, org OrgCatalog) error {
	now := time.Now().UTC()
	windowStart := now.Add(-time.Duration(prBackfillDays) * 24 * time.Hour)
	for _, repo := range BuildBitbucketRepos(org, windowStart) {
		entityID := repo["slug"].(string) //nolint:forcetypeassert // built by BuildBitbucketRepos
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeRepo, entityID, repo, recentWeek(), true); err != nil {
			return fmt.Errorf("seed bitbucket repo %s: %w", entityID, err)
		}
	}
	return nil
}

// seedBitbucketPRs mirrors seedPRs but keys continuation on the Bitbucket `id` and
// writes `updated_on` as the cursor. Direct-push repos (PRsPerWeek==0) are skipped
// (their activity is the commit stream, seeded separately).
func seedBitbucketPRs(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	now := time.Now().UTC()
	for _, repo := range org.Repos {
		profile := Profiles[repo.Flavor]
		if profile.PRsPerWeek == 0 {
			continue
		}
		lastID := maxBBPRID(ctx, tx, org.Vendor, org.OrgName, repo.Name)
		var prs []map[string]any
		if lastID == 0 {
			windowEnd := truncateToDay(now)
			windowStart := windowEnd.Add(-time.Duration(prBackfillDays) * 24 * time.Hour)
			prs = BuildBitbucketPRs(org, repo, members, profile.PRsPerWeek*(prBackfillDays/7), 0, windowStart, windowEnd)
		} else {
			prs = BuildBitbucketPRs(org, repo, members, profile.PRsPerWeek, lastID, now.Add(-7*24*time.Hour), now)
		}
		for _, pr := range prs {
			entityID := fmt.Sprintf("%s#%d", repo.Name, pr[bbKeyID].(int))          //nolint:forcetypeassert // built above
			updatedOn, err := time.Parse(time.RFC3339, pr[bbKeyUpdatedOn].(string)) //nolint:forcetypeassert // built above
			if err != nil {
				return fmt.Errorf("parse updated_on for %s: %w", entityID, err)
			}
			if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, "pr", entityID, pr, updatedOn, false); err != nil {
				return fmt.Errorf("seed bitbucket pr %s: %w", entityID, err)
			}
		}
	}
	return nil
}

// seedBitbucketCommits builds the repo default-branch commit stream: the commits of
// every already-seeded PR (so hashes cross-reference the PR's commits[]) plus
// standalone direct-push commits sized by the repo flavor. A zero-PR repo yields only
// direct-push commits, so demo-bb-solo is populated. Timeseries (DO NOTHING upsert),
// so re-runs never rewrite history.
//
// Requires the repo's `pr` rows to be persisted already (bbPRCommitRows re-reads
// them across the committed pr transaction): the catalog orders `pr` before `commit`
// for this reason.
func seedBitbucketCommits(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	now := time.Now().UTC()
	for _, repo := range org.Repos {
		profile := Profiles[repo.Flavor]
		branch := profile.DefaultBranch

		// Capture the first-run decision BEFORE any commit upsert — the PR-derived
		// upserts below would otherwise make the count non-zero and starve the
		// direct-push 90-day backfill on a genuine first run.
		firstRun := bbCommitCount(ctx, tx, org.Vendor, org.OrgName, repo.Name) == 0

		prCommits, err := bbPRCommitRows(ctx, tx, org, repo.Name, branch)
		if err != nil {
			return fmt.Errorf("seed bitbucket commits (pr-derived) for %s: %w", repo.Name, err)
		}
		for _, row := range prCommits {
			if err := upsertBBCommit(ctx, tx, org, repo.Name, row); err != nil {
				return fmt.Errorf("seed bitbucket commits (pr-derived) for %s: %w", repo.Name, err)
			}
		}

		// Direct-push volume: the full flow for a direct-push repo, a light trickle
		// otherwise. Backfill 90 days on first run, else the trailing week —
		// mirroring seedPRs' first-run/incremental split.
		perWeek := 8
		if profile.PRsPerWeek > 0 {
			perWeek = 2
		}
		var windowStart, windowEnd time.Time
		var count int
		if firstRun {
			windowEnd = truncateToDay(now)
			windowStart = windowEnd.Add(-time.Duration(prBackfillDays) * 24 * time.Hour)
			count = perWeek * (prBackfillDays / 7)
		} else {
			windowStart, windowEnd = now.Add(-7*24*time.Hour), now
			count = perWeek
		}
		for _, row := range BuildBitbucketDirectPushCommits(org, repo, members, count, windowStart, windowEnd) {
			if err := upsertBBCommit(ctx, tx, org, repo.Name, row); err != nil {
				return fmt.Errorf("seed bitbucket commits (direct-push) for %s: %w", repo.Name, err)
			}
		}
	}
	return nil
}

// bbPRCommitRows reads the seeded PR rows for a repo and projects each PR's nested
// commits[] into repo-level commit rows, so the fetch_commits stream shares hashes
// with the PR commits it came from.
func bbPRCommitRows(ctx context.Context, tx pgx.Tx, org OrgCatalog, repo, branch string) ([]map[string]any, error) {
	rows, err := tx.Query(ctx,
		`SELECT payload FROM sim_records
		 WHERE vendor=$1 AND org_name=$2 AND entity_type='pr' AND payload->>'repo'=$3`,
		org.Vendor, org.OrgName, repo)
	if err != nil {
		return nil, fmt.Errorf("read bitbucket pr rows for %s: %w", repo, err)
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan pr row for %s: %w", repo, err)
		}
		var pr struct {
			Commits []map[string]any `json:"commits"`
		}
		if err := json.Unmarshal(raw, &pr); err != nil {
			return nil, fmt.Errorf("unmarshal pr row for %s: %w", repo, err)
		}
		for _, c := range pr.Commits {
			out = append(out, bbCommitRow(org.OrgName, repo, branch, c))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pr rows for %s: %w", repo, err)
	}
	return out, nil
}

func upsertBBCommit(ctx context.Context, tx pgx.Tx, org OrgCatalog, repo string, row map[string]any) error {
	hash := row[bbKeyHash].(string) //nolint:forcetypeassert // built by bbCommitRow/bbDirectPushCommits
	entityID := fmt.Sprintf("%s#%s", repo, hash)
	date, err := time.Parse(time.RFC3339, row[bbKeyDate].(string)) //nolint:forcetypeassert // built above
	if err != nil {
		return fmt.Errorf("parse commit date for %s: %w", entityID, err)
	}
	if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeCommit, entityID, row, date, false); err != nil {
		return fmt.Errorf("seed bitbucket commit %s: %w", entityID, err)
	}
	return nil
}

// bbCommitCount returns how many commit rows exist for (vendor, org, repo).
func bbCommitCount(ctx context.Context, tx pgx.Tx, vendor, org, repo string) int {
	var n int
	row := tx.QueryRow(ctx, //nolint:errcheck,gosec // COALESCE/COUNT guarantees a row; see maxPRNumber
		`SELECT COUNT(*) FROM sim_records
		 WHERE vendor=$1 AND org_name=$2 AND entity_type='commit' AND payload->>'repo'=$3`,
		vendor, org, repo,
	)
	row.Scan(&n) //nolint:errcheck,gosec // see maxPRNumber
	return n
}
