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

// Synthetic GitLab group ids for the three catalog orgs. GitLabSettings.GroupID is
// int64 and connector URLs are /groups/%d, so the catalog name cannot live in the
// setting the way GitHub's organization / Bitbucket's workspace do. These integers
// are the only site key the demo seed writes; the simulator maps them back
// to org_name. Must stay in lockstep with internal/simulator/gitlab/ids.go.
const (
	gitLabGroupRegulated int64 = 91001
	gitLabGroupScaleup   int64 = 91002
	gitLabGroupSolo      int64 = 91003
)

const (
	glKeyID       = "id"
	glKeyIID      = "iid"
	glKeyUsername = "username"
	glStateMerged = "merged"
	glStateOpened = "opened"
	glStateClosed = "closed"
)

// GitLabGroupID returns the synthetic group_id for a GitLab catalog org name, or 0
// if the name is not one of the three demo GitLab orgs.
func GitLabGroupID(orgName string) int64 {
	switch orgName {
	case "demo-gl-regulated":
		return gitLabGroupRegulated
	case "demo-gl-scaleup":
		return gitLabGroupScaleup
	case "demo-gl-solo":
		return gitLabGroupSolo
	default:
		return 0
	}
}

// GitLabProjectID encodes project_id = group_id*100 + repoIndex (1-based).
func GitLabProjectID(orgName string, repoIndex int) int64 {
	return GitLabGroupID(orgName)*100 + int64(repoIndex)
}

// GitLabUserID encodes user_id = group_id*1000 + memberIndex (1-based, never 0).
func GitLabUserID(orgName string, memberIndex int) int64 {
	return GitLabGroupID(orgName)*1000 + int64(memberIndex)
}

func glMemberIndex(org OrgCatalog, login string) int {
	for i, name := range org.MemberNames {
		if name == login {
			return i + 1
		}
	}
	return 1
}

func gitLabRepoIndex(org OrgCatalog, repo RepoSpec) int {
	for i, r := range org.Repos {
		if r.Name == repo.Name {
			return i + 1
		}
	}
	return 1
}

// gitLabCatalogRepoName maps a synthetic project_id back to the catalog repo
// name (project_id = group_id*100 + 1-based index). Empty when the id is not
// in this org's catalog — callers skip those rows rather than group them under "".
func gitLabCatalogRepoName(org OrgCatalog, projectID int64) string {
	gid := GitLabGroupID(org.OrgName)
	if gid == 0 || projectID == 0 {
		return ""
	}
	idx := int(projectID - gid*100)
	if idx < 1 || idx > len(org.Repos) {
		return ""
	}
	return org.Repos[idx-1].Name
}

func projectIDFromPayload(raw []byte) int64 {
	var p struct {
		ProjectID int64 `json:"project_id"`
	}
	_ = json.Unmarshal(raw, &p) //nolint:errcheck // malformed seed rows are skipped via ProjectID==0
	return p.ProjectID
}

func glUserMap(org OrgCatalog, login string) map[string]any {
	idx := glMemberIndex(org, login)
	return map[string]any{
		glKeyID:       GitLabUserID(org.OrgName, idx),
		glKeyUsername: login,
		keyEmail:      login + "@" + org.OrgName + ".example.com",
		"bot":         false,
	}
}

func glMemberLogin(m map[string]any) string {
	return asString(m, glKeyUsername)
}

// BuildGitLabMembers returns one member payload per roster login, in the
// GET /groups/:id/members/all shape. Exported for the contract test.
func BuildGitLabMembers(org OrgCatalog) []map[string]any {
	members := make([]map[string]any, 0, len(org.MemberNames))
	for i, login := range org.MemberNames {
		members = append(members, map[string]any{
			glKeyID:        GitLabUserID(org.OrgName, i+1),
			glKeyUsername:  login,
			keyName:        capitalize(login) + " Demo",
			keyEmail:       login + "@" + org.OrgName + ".example.com",
			"state":        "active",
			"access_level": 50,
		})
	}
	return members
}

// BuildGitLabRepos returns GitLab project payloads. Exported for the contract test.
func BuildGitLabRepos(org OrgCatalog, createdWindowStart time.Time) []map[string]any {
	gid := GitLabGroupID(org.OrgName)
	repos := make([]map[string]any, 0, len(org.Repos))
	for i, repo := range org.Repos {
		rng := fnvRand(org.OrgName, entityTypeRepo, repo.Name)
		profile := Profiles[repo.Flavor]
		pid := GitLabProjectID(org.OrgName, i+1)
		created := createdWindowStart.Add(-time.Duration(rng.Intn(365)) * 24 * time.Hour)
		repos = append(repos, map[string]any{
			glKeyID:               pid,
			"path_with_namespace": org.OrgName + "/" + repo.Name,
			"path":                repo.Name,
			keyName:               repo.Name,
			"default_branch":      profile.DefaultBranch,
			"visibility":          "private",
			"created_at":          created.Format(time.RFC3339),
			"last_activity_at":    recentWeek().Format(time.RFC3339),
			"namespace": map[string]any{
				glKeyID: gid,
				"path":  org.OrgName,
				"kind":  "group",
			},
		})
	}
	return repos
}

// BuildGitLabMRs generates count MRs for the repo starting at iid startIID+1.
// Nested commits/notes/approvals/diffs are the shapes the per-MR simulator
// routes extract. Exported for the contract test.
func BuildGitLabMRs(org OrgCatalog, repo RepoSpec, repoIndex int, members []map[string]any, count, startIID int, windowStart, windowEnd time.Time) []map[string]any {
	profile := Profiles[repo.Flavor]
	window := windowEnd.Sub(windowStart)
	var spacing time.Duration
	if count > 1 {
		spacing = window / time.Duration(count)
	}
	pid := GitLabProjectID(org.OrgName, repoIndex)

	mrs := make([]map[string]any, count)
	for i := range mrs {
		iid := startIID + i + 1
		rng := fnvRand(org.OrgName, repo.Name, "glmr", fmt.Sprint(iid))
		author := members[rng.Intn(len(members))]
		authorLogin := glMemberLogin(author)

		merged := rng.Float64() < profile.MergeRatio
		state := glStateOpened
		if merged {
			state = glStateMerged
		} else if rng.Float64() < 0.5 {
			state = glStateClosed
		}

		t := windowStart.Add(time.Duration(i) * spacing)
		createdAt := t.Format(time.RFC3339)
		updatedAt := t.Add(2 * time.Hour).Format(time.RFC3339)

		var mergedAt any
		var closedAt any
		switch state {
		case glStateMerged:
			mergedAt = t.Add(2*time.Hour + time.Duration(rng.Intn(48))*time.Hour).Format(time.RFC3339)
		case glStateClosed:
			closedAt = t.Add(3 * time.Hour).Format(time.RFC3339)
		}

		additions := profile.BaseAdds/2 + rng.Intn(profile.BaseAdds+1)
		deletions := profile.BaseDels/2 + rng.Intn(profile.BaseDels+1)
		headRef := fmt.Sprintf("%s%s-%d", profile.FeaturePrefix, repo.Name, iid)
		if profile.FeaturePrefix == "" {
			headRef = fmt.Sprintf("%s-%d", repo.Name, iid)
		}

		title := fmt.Sprintf("%s: %s !%d", repo.Name, prVerb(rng), iid)
		description := fmt.Sprintf("Demo MR for %s !%d", repo.Name, iid)
		if key, ok := jiraLinkKeyForPR(org, repo, iid); ok {
			title = key + ": " + title
			description = key + " " + description
			headRef = headRef + "-" + key
		}
		if org.PMTool == pmToolGitLabIssues && rng.Float64() < 0.60 {
			description += fmt.Sprintf(" Closes #%d", iid)
		}

		reviewers := glReviewers(rng, org, members, profile.Reviewers)
		commits := glCommitObjs(org, repo.Name, iid, authorLogin, t, rng.Intn(3)+1)
		notes := glNoteObjs(rng, org, members, pid, updatedAt)
		approvals := glApprovalsObj(rng, org, members, updatedAt, state == glStateMerged)

		mrs[i] = map[string]any{
			glKeyID:            pid*10000 + int64(iid),
			glKeyIID:           iid,
			"project_id":       pid,
			keyTitle:           title,
			keyDescription:     description,
			"state":            state,
			"created_at":       createdAt,
			"updated_at":       updatedAt,
			"merged_at":        mergedAt,
			"closed_at":        closedAt,
			"source_branch":    headRef,
			"target_branch":    profile.DefaultBranch,
			keyAuthor:          glUserMap(org, authorLogin),
			"reviewers":        reviewers,
			"user_notes_count": int64(len(notes)),
			"additions":        int64(additions),
			"deletions":        int64(deletions),
			"notes":            notes,
			"approvals":        approvals,
			"commits":          commits,
			"diffs":            glDiffs(rng, additions, deletions),
		}
	}
	return mrs
}

func glReviewers(rng *rand.Rand, org OrgCatalog, members []map[string]any, count int) []map[string]any {
	if count < 0 {
		count = 0
	}
	out := make([]map[string]any, 0, count)
	for i := 0; i < count && len(members) > 0; i++ {
		login := glMemberLogin(members[rng.Intn(len(members))])
		u := glUserMap(org, login)
		out = append(out, map[string]any{glKeyID: u[glKeyID], glKeyUsername: login})
	}
	return out
}

func glCommitObjs(org OrgCatalog, repo string, iid int, authorLogin string, prTime time.Time, count int) []map[string]any {
	commits := make([]map[string]any, count)
	for i := range commits {
		rng := fnvRand(org.OrgName, repo, "glcommit", fmt.Sprint(iid), fmt.Sprint(i))
		date := prTime.Add(-time.Duration(count-i) * time.Duration(rng.Intn(180)+30) * time.Minute)
		sha := fmt.Sprintf("%040x", rng.Uint64())
		email := authorLogin + "@" + org.OrgName + ".example.com"
		commits[i] = map[string]any{
			glKeyID:          sha,
			"authored_date":  date.Format(time.RFC3339),
			"committed_date": date.Format(time.RFC3339),
			"author_email":   email,
			"author_name":    capitalize(authorLogin) + " Demo",
			"message":        fmt.Sprintf("%s: commit %d for !%d", prVerb(rng), i+1, iid),
			"stats":          glCommitStats(rng),
		}
	}
	return commits
}

func glNoteObjs(rng *rand.Rand, org OrgCatalog, members []map[string]any, projectID int64, when string) []map[string]any {
	n := rng.Intn(3)
	notes := make([]map[string]any, n)
	for i := range notes {
		login := glMemberLogin(members[rng.Intn(len(members))])
		notes[i] = map[string]any{
			glKeyID:      rng.Intn(9000) + 1000,
			"project_id": projectID,
			"body":       "lgtm",
			"system":     false,
			"created_at": when,
			"updated_at": when,
			keyAuthor:    glUserMap(org, login),
		}
	}
	return notes
}

func glApprovalsObj(rng *rand.Rand, org OrgCatalog, members []map[string]any, when string, approved bool) map[string]any {
	approvedBy := []map[string]any{}
	if approved && len(members) > 0 {
		login := glMemberLogin(members[rng.Intn(len(members))])
		approvedBy = []map[string]any{{
			"user":       glUserMap(org, login),
			"created_at": when,
		}}
	}
	return map[string]any{"approved": approved && len(approvedBy) > 0, "approved_by": approvedBy}
}

func glDiffs(rng *rand.Rand, additions, deletions int) []map[string]any {
	files := []string{"service.go", "handler.go", "model.go", "README.md"}
	n := rng.Intn(3) + 1
	entries := make([]map[string]any, n)
	for i := range entries {
		path := files[rng.Intn(len(files))]
		entries[i] = map[string]any{
			"old_path":     path,
			"new_path":     path,
			"diff":         fmt.Sprintf("@@ -1,0 +1,%d @@\n+package demo\n", additions/n+1),
			"new_file":     false,
			"renamed_file": false,
			"deleted_file": false,
			"additions":    additions/n + 1,
			"deletions":    deletions/n + 1,
		}
	}
	return entries
}

// BuildGitLabDirectPushCommits returns project-level commit payloads with no MR.
func BuildGitLabDirectPushCommits(org OrgCatalog, repo RepoSpec, repoIndex int, members []map[string]any, count int, windowStart, windowEnd time.Time) []map[string]any {
	pid := GitLabProjectID(org.OrgName, repoIndex)
	window := windowEnd.Sub(windowStart)
	var spacing time.Duration
	if count > 1 {
		spacing = window / time.Duration(count)
	}
	out := make([]map[string]any, count)
	for i := range out {
		rng := fnvRand(org.OrgName, repo.Name, "gldirect", fmt.Sprint(windowStart.Unix()), fmt.Sprint(i))
		login := glMemberLogin(members[rng.Intn(len(members))])
		t := windowStart.Add(time.Duration(i) * spacing)
		sha := fmt.Sprintf("%040x", rng.Uint64())
		out[i] = map[string]any{
			glKeyID:          sha,
			"project_id":     pid,
			"committed_date": t.Format(time.RFC3339),
			"authored_date":  t.Format(time.RFC3339),
			"author_email":   login + "@" + org.OrgName + ".example.com",
			"author_name":    capitalize(login) + " Demo",
			"message":        fmt.Sprintf("%s: direct push %d", prVerb(rng), i+1),
			"stats":          glCommitStats(rng),
		}
	}
	return out
}

func glCommitStats(rng *rand.Rand) map[string]any {
	adds := rng.Intn(40) + 1
	dels := rng.Intn(20)
	return map[string]any{"additions": adds, "deletions": dels, "total": adds + dels}
}

// BuildGitLabIssues returns native GitLab issue payloads. Every issue carries
// author — ParseIssue skips records without one. startIID is the last already-
// seeded iid (0 on first run); new issues continue from startIID+1 so a later
// DEMO_SEED week does not collide with overwrite=false.
func BuildGitLabIssues(org OrgCatalog, repo RepoSpec, repoIndex int, members []map[string]any, count, startIID int, windowStart, windowEnd time.Time) []map[string]any {
	pid := GitLabProjectID(org.OrgName, repoIndex)
	window := windowEnd.Sub(windowStart)
	var spacing time.Duration
	if count > 1 {
		spacing = window / time.Duration(count)
	}
	out := make([]map[string]any, count)
	for i := range out {
		iid := startIID + i + 1
		rng := fnvRand(org.OrgName, repo.Name, "glissue", fmt.Sprint(iid))
		author := members[rng.Intn(len(members))]
		t := windowStart.Add(time.Duration(i) * spacing)
		closed := rng.Float64() < 0.4
		state := glStateOpened
		var closedAt any
		if closed {
			state = glStateClosed
			closedAt = t.Add(24 * time.Hour).Format(time.RFC3339)
		}
		out[i] = map[string]any{
			glKeyID:      pid*10000 + int64(iid),
			glKeyIID:     iid,
			"project_id": pid,
			keyTitle:     fmt.Sprintf("%s issue %d", repo.Name, iid),
			"state":      state,
			"issue_type": "issue",
			"created_at": t.Format(time.RFC3339),
			"updated_at": t.Add(2 * time.Hour).Format(time.RFC3339),
			"closed_at":  closedAt,
			keyAuthor:    glUserMap(org, glMemberLogin(author)),
			"assignees":  []map[string]any{},
			"labels":     []string{},
			"references": map[string]any{"full": fmt.Sprintf("%s/%s#%d", org.OrgName, repo.Name, iid)},
		}
	}
	return out
}

func seedGitLabMembers(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	for _, m := range members {
		login := glMemberLogin(m)
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeMember, login, m, time.Now().UTC(), true); err != nil {
			return fmt.Errorf("seed gitlab member %s: %w", login, err)
		}
	}
	return nil
}

func seedGitLabRepos(ctx context.Context, tx pgx.Tx, org OrgCatalog) error {
	windowStart := time.Now().UTC().Add(-time.Duration(prBackfillDays) * 24 * time.Hour)
	for _, repo := range BuildGitLabRepos(org, windowStart) {
		path := asString(repo, "path")
		if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeRepo, path, repo, time.Now().UTC(), true); err != nil {
			return fmt.Errorf("seed gitlab repo %s: %w", path, err)
		}
	}
	return nil
}

func seedGitLabMRs(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	now := time.Now().UTC()
	for _, repo := range org.Repos {
		profile := Profiles[repo.Flavor]
		if profile.PRsPerWeek == 0 {
			continue
		}
		repoIndex := gitLabRepoIndex(org, repo)
		pid := GitLabProjectID(org.OrgName, repoIndex)
		lastIID := maxGitLabIID(ctx, tx, org.Vendor, org.OrgName, pid)

		var mrs []map[string]any
		if lastIID == 0 {
			windowEnd := truncateToDay(now)
			windowStart := windowEnd.Add(-time.Duration(prBackfillDays) * 24 * time.Hour)
			count := profile.PRsPerWeek * (prBackfillDays / 7)
			mrs = BuildGitLabMRs(org, repo, repoIndex, members, count, 0, windowStart, windowEnd)
		} else {
			mrs = BuildGitLabMRs(org, repo, repoIndex, members, profile.PRsPerWeek, lastIID, now.Add(-7*24*time.Hour), now)
		}
		for _, mr := range mrs {
			entityID := fmt.Sprintf("mr-%d!%d", pid, asInt(mr, glKeyIID))
			updatedAt, err := time.Parse(time.RFC3339, asString(mr, "updated_at"))
			if err != nil {
				return fmt.Errorf("parse updated_at for %s: %w", entityID, err)
			}
			if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, "pr", entityID, mr, updatedAt, false); err != nil {
				return fmt.Errorf("seed gitlab mr %s: %w", entityID, err)
			}
		}
	}
	return nil
}

func seedGitLabCommits(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	now := time.Now().UTC()
	for i, repo := range org.Repos {
		pid := GitLabProjectID(org.OrgName, i+1)
		firstRun := gitLabCommitCount(ctx, tx, org.Vendor, org.OrgName, pid) == 0

		prCommits, err := glPRCommitRows(ctx, tx, org, pid)
		if err != nil {
			return fmt.Errorf("seed gitlab commits (pr-derived) for %s: %w", repo.Name, err)
		}
		for _, row := range prCommits {
			if err := upsertGitLabCommit(ctx, tx, org, pid, row); err != nil {
				return fmt.Errorf("seed gitlab commits (pr-derived) for %s: %w", repo.Name, err)
			}
		}

		perWeek := 8
		if Profiles[repo.Flavor].PRsPerWeek > 0 {
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
		for _, row := range BuildGitLabDirectPushCommits(org, repo, i+1, members, count, windowStart, windowEnd) {
			if err := upsertGitLabCommit(ctx, tx, org, pid, row); err != nil {
				return fmt.Errorf("seed gitlab commits (direct-push) for %s: %w", repo.Name, err)
			}
		}
	}
	return nil
}

func seedGitLabIssues(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	if org.PMTool != pmToolGitLabIssues {
		return nil
	}
	now := time.Now().UTC()
	for i, repo := range org.Repos {
		pid := GitLabProjectID(org.OrgName, i+1)
		lastIID := maxGitLabIssueIID(ctx, tx, org.Vendor, org.OrgName, pid)

		var issues []map[string]any
		if lastIID == 0 {
			windowEnd := truncateToDay(now)
			windowStart := windowEnd.Add(-time.Duration(prBackfillDays) * 24 * time.Hour)
			count := 8
			if Profiles[repo.Flavor].PRsPerWeek > 0 {
				count = Profiles[repo.Flavor].PRsPerWeek * (prBackfillDays / 7) / 4
				if count < 4 {
					count = 4
				}
			}
			issues = BuildGitLabIssues(org, repo, i+1, members, count, 0, windowStart, windowEnd)
		} else {
			weekly := 2
			if p := Profiles[repo.Flavor].PRsPerWeek; p > 0 {
				weekly = p / 4
				if weekly < 1 {
					weekly = 1
				}
			}
			issues = BuildGitLabIssues(org, repo, i+1, members, weekly, lastIID, now.Add(-7*24*time.Hour), now)
		}
		for _, iss := range issues {
			entityID := fmt.Sprintf("issue-%d!%d", pid, asInt(iss, glKeyIID))
			updatedAt, err := time.Parse(time.RFC3339, asString(iss, "updated_at"))
			if err != nil {
				return fmt.Errorf("parse updated_at for %s: %w", entityID, err)
			}
			if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeIssue, entityID, iss, updatedAt, false); err != nil {
				return fmt.Errorf("seed gitlab issue %s: %w", entityID, err)
			}
		}
	}
	return nil
}

func upsertGitLabCommit(ctx context.Context, tx pgx.Tx, org OrgCatalog, projectID int64, row map[string]any) error {
	sha := asString(row, glKeyID)
	entityID := fmt.Sprintf("%d#%s", projectID, sha)
	committed, err := time.Parse(time.RFC3339, asString(row, "committed_date"))
	if err != nil {
		return fmt.Errorf("parse committed_date for %s: %w", entityID, err)
	}
	if err := upsertRow(ctx, tx, org.Vendor, org.OrgName, entityTypeCommit, entityID, row, committed, false); err != nil {
		return fmt.Errorf("seed gitlab commit %s: %w", entityID, err)
	}
	return nil
}

func glPRCommitRows(ctx context.Context, tx pgx.Tx, org OrgCatalog, projectID int64) ([]map[string]any, error) {
	rows, err := tx.Query(ctx,
		`SELECT payload FROM sim_records
		 WHERE vendor=$1 AND org_name=$2 AND entity_type='pr' AND (payload->>'project_id')::bigint=$3`,
		org.Vendor, org.OrgName, projectID)
	if err != nil {
		return nil, fmt.Errorf("read gitlab pr rows for project %d: %w", projectID, err)
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan pr row for project %d: %w", projectID, err)
		}
		var pr struct {
			Commits []map[string]any `json:"commits"`
		}
		if err := json.Unmarshal(raw, &pr); err != nil {
			return nil, fmt.Errorf("unmarshal pr row for project %d: %w", projectID, err)
		}
		for _, c := range pr.Commits {
			c["project_id"] = projectID
			out = append(out, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pr rows for project %d: %w", projectID, err)
	}
	return out, nil
}

func maxGitLabIID(ctx context.Context, tx pgx.Tx, vendor, org string, projectID int64) int {
	return maxGitLabEntityIID(ctx, tx, vendor, org, "pr", projectID)
}

func maxGitLabIssueIID(ctx context.Context, tx pgx.Tx, vendor, org string, projectID int64) int {
	return maxGitLabEntityIID(ctx, tx, vendor, org, entityTypeIssue, projectID)
}

func maxGitLabEntityIID(ctx context.Context, tx pgx.Tx, vendor, org, entityType string, projectID int64) int {
	var maxNum int
	_ = tx.QueryRow(ctx, //nolint:errcheck,gosec // COALESCE guarantees a zero-value row
		`SELECT COALESCE(MAX((payload->>'iid')::int), 0)
		 FROM sim_records
		 WHERE vendor=$1 AND org_name=$2 AND entity_type=$3 AND (payload->>'project_id')::bigint=$4`,
		vendor, org, entityType, projectID).Scan(&maxNum)
	return maxNum
}

func gitLabCommitCount(ctx context.Context, tx pgx.Tx, vendor, org string, projectID int64) int {
	var n int
	_ = tx.QueryRow(ctx, //nolint:errcheck,gosec // COALESCE guarantees a zero-value row
		`SELECT COUNT(*) FROM sim_records
		 WHERE vendor=$1 AND org_name=$2 AND entity_type='commit' AND (payload->>'project_id')::bigint=$3`,
		vendor, org, projectID).Scan(&n)
	return n
}
