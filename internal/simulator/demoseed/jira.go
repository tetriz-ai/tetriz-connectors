// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package demoseed

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// Jira payloads are seeded under vendor="jira" (a PM-tool overlay on a VCS demo
// org whose OrgCatalog.Vendor is github/bitbucket), keyed by the same org_name.
// One Jira project per repo; the project key and its issue count are pure
// functions of (org, repo), so the VCS PR builder and the Jira issue builder
// derive the SAME key set with no cross-builder state — the correlation invariant
// the prissue linkage engine depends on.

const (
	// jiraTimestampLayout is Jira's millisecond-plus-offset form (no colon in the
	// offset); the connector's jiraTime decoder's primary layout.
	jiraTimestampLayout = "2006-01-02T15:04:05.000-0700"
	// jiraDateLayout is the date-only form used by duedate / start-date custom field.
	jiraDateLayout = "2006-01-02"

	// Jira custom field IDs the consumer's parser reads (instance-specific in real
	// Jira, fixed here so the demo is self-consistent).
	jiraFieldStartDate   = "customfield_10015"
	jiraFieldStoryPoints = "customfield_10016"
	jiraFieldSprint      = "customfield_10020"

	// A PR number's single draw is partitioned into three roles by these two cut
	// points: below carry the PR stamps its issue key, between carry and coverage
	// the issue exists with no PR referencing it, above coverage there is no issue.
	// The middle band keeps some issues that no pull request references;
	// without it every issue has a PR and the coverage figure is a constant
	// 100%.
	jiraPRKeyCarryPercent    = 0.70
	jiraIssueCoveragePercent = 0.85

	// jiraEpicsPerProject is how many of a project's lowest-numbered issues become
	// epics. They come from the earliest PRs, which never change once seeded, so
	// the epic set is stable as later runs append issues.
	jiraEpicsPerProject = 3
)

// jiraStatuses cycles the three workflow states the parser maps
// (status.statusCategory.key ∈ {new, indeterminate, done}).
var jiraStatuses = []struct {
	name, category string
}{
	{"To Do", "new"},
	{"In Progress", "indeterminate"},
	{"Done", "done"},
}

// jiraProjectKey derives a repo's Jira project key — the uppercase alpha prefix,
// capped at 4 chars (e.g. "platform-monorepo" → "PLAT"). Reuses the Bitbucket
// project-key convention so a repo maps to one stable key across vendors.
func jiraProjectKey(repo string) string { return bbProjectKey(repo) }

// jiraProjectID is the synthetic project id for the i-th repo of an org. Shared by
// BuildJiraProjects and BuildJiraIssues so an issue's fields.project.id matches its
// project row's id.
func jiraProjectID(i int) string { return fmt.Sprint(10000 + i) }

// DeriveBoardID maps a project id to its synthetic scrum board id (projectID-1000),
// so a seeded sprint's originBoardId matches the board the simulator's
// board-listing handler derives from the same project. The internal/simulator/jira package
// carries an independent copy of this formula (the two packages don't import each
// other); internal/simulator/jira's TestBoardIDFormulaMatchesSeed pins the two copies equal,
// so a drift in either fails a test rather than silently ingesting zero sprints.
// Exported for that cross-package pin.
func DeriveBoardID(projectID string) int64 {
	n, _ := strconv.ParseInt(projectID, 10, 64) //nolint:errcheck // non-numeric id → 0; seeded ids are always numeric
	return n - 1000
}

// jiraAccountID is the stable Atlassian accountId for a member login. It is the
// identity anchor: an issue's assignee and reporter carry it, so every Jira
// record for one member names the same account.
func jiraAccountID(login string) string { return "acct-" + login }

// jiraPRRole is what a PR number contributes to the Jira overlay: an issue the PR
// references, an issue nothing references, or no issue at all.
type jiraPRRole uint8

const (
	jiraRoleNoIssue jiraPRRole = iota
	jiraRoleUnlinked
	jiraRoleLinked
)

// jiraRoleForPR classifies a PR number from one PR-key-dedicated draw. Both the
// issue builder and the VCS PR builders call it, so "the keys stamped into PR text
// are exactly the linked subset of the seeded issue keys" is an identity rather
// than two formulas that have to agree.
//
// The draw is dedicated so it never advances the PR builder's own rng — that would
// shift the deterministic history of every non-Jira org.
func jiraRoleForPR(org OrgCatalog, repo RepoSpec, prNum int) jiraPRRole {
	if org.PMTool != pmToolJira {
		return jiraRoleNoIssue
	}
	switch r := fnvRand(org.OrgName, repo.Name, "jira-pr-key", fmt.Sprint(prNum)).Float64(); {
	case r < jiraPRKeyCarryPercent:
		return jiraRoleLinked
	case r < jiraIssueCoveragePercent:
		return jiraRoleUnlinked
	default:
		return jiraRoleNoIssue
	}
}

// jiraIssueKeyFor is the sole issue-key formula: PR n owns {PROJECT}-{n}. Numbering
// is therefore gappy (only PRs with an issue role produce a key), which real Jira
// would not do but nothing downstream reads. What it buys is that an issue key
// names exactly one PR, so no issue can accumulate a second linked PR.
func jiraIssueKeyFor(repo RepoSpec, prNum int) string {
	return fmt.Sprintf("%s-%d", jiraProjectKey(repo.Name), prNum)
}

// jiraLinkKeyForPR returns the Jira issue key a PR should reference, and whether it
// references one at all.
func jiraLinkKeyForPR(org OrgCatalog, repo RepoSpec, prNum int) (string, bool) {
	if jiraRoleForPR(org, repo, prNum) != jiraRoleLinked {
		return "", false
	}
	return jiraIssueKeyFor(repo, prNum), true
}

// BuildJiraProjects returns one Jira project payload per repo. Exported for the
// seed-connector contract test.
func BuildJiraProjects(org OrgCatalog) []map[string]any {
	projects := make([]map[string]any, 0, len(org.Repos))
	for i, repo := range org.Repos {
		projects = append(projects, map[string]any{
			"id":             jiraProjectID(i),
			keyKey:           jiraProjectKey(repo.Name),
			keyName:          capitalize(repo.Name),
			"projectTypeKey": "software",
		})
	}
	return projects
}

// JiraPRRef is the projection of one seeded VCS pull request that the Jira issue
// builder needs: the number that names the issue key, and the timestamps that
// anchor the issue's dates. Vendor-neutral — JiraPRRefs does the per-vendor field
// mapping. MergedAt is zero when the PR never merged.
type JiraPRRef struct {
	Number    int
	CreatedAt time.Time
	UpdatedAt time.Time
	MergedAt  time.Time
}

// JiraPRRefs projects raw seeded pull-request payloads into refs, ascending by
// number. Takes raw JSON rather than the builders' map so the seed path (rows read
// back from sim_records) and the test path share one decode — a map that has
// been through JSON carries float64 where the builder wrote int, and only a typed
// unmarshal is safe for both.
func JiraPRRefs(org OrgCatalog, payloads []json.RawMessage) ([]JiraPRRef, error) {
	refs := make([]JiraPRRef, 0, len(payloads))
	for _, raw := range payloads {
		ref, err := jiraPRRef(org, raw)
		if err != nil {
			return nil, fmt.Errorf("decode pr ref: %w", err)
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(a, b int) bool { return refs[a].Number < refs[b].Number })
	return refs, nil
}

// BuildJiraPRRefs projects freshly built pull-request payloads without touching a
// database. Exported so the seed–connector contract test and the correlation test
// construct issues from the same refs seedJiraIssues reads back.
func BuildJiraPRRefs(org OrgCatalog, prs []map[string]any) ([]JiraPRRef, error) {
	payloads := make([]json.RawMessage, 0, len(prs))
	for _, pr := range prs {
		raw, err := json.Marshal(pr)
		if err != nil {
			return nil, fmt.Errorf("marshal pr payload: %w", err)
		}
		payloads = append(payloads, raw)
	}
	refs, err := JiraPRRefs(org, payloads)
	if err != nil {
		return nil, fmt.Errorf("project pr refs: %w", err)
	}
	return refs, nil
}

// jiraPRRef decodes one PR payload. GitHub carries mergedAt directly; Bitbucket has
// no such field, so a merged PR's updated_on stands in as its merge time.
func jiraPRRef(org OrgCatalog, raw json.RawMessage) (JiraPRRef, error) {
	parse := func(v string) (time.Time, error) {
		if v == "" {
			return time.Time{}, nil
		}
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse pr timestamp %q: %w", v, err)
		}
		return t.UTC(), nil
	}

	var (
		ref                              JiraPRRef
		createdRaw, updatedRaw, mergeRaw string
	)
	switch org.Vendor {
	case vendorBitbucket:
		var bb struct {
			ID        int    `json:"id"`
			CreatedOn string `json:"created_on"`
			UpdatedOn string `json:"updated_on"`
			State     string `json:"state"`
		}
		if err := json.Unmarshal(raw, &bb); err != nil {
			return ref, fmt.Errorf("unmarshal bitbucket pr payload: %w", err)
		}
		ref.Number, createdRaw, updatedRaw = bb.ID, bb.CreatedOn, bb.UpdatedOn
		if bb.State == bbStateMerged {
			mergeRaw = bb.UpdatedOn
		}
	case vendorGitLab:
		var gl struct {
			IID       int     `json:"iid"`
			CreatedAt string  `json:"created_at"`
			UpdatedAt string  `json:"updated_at"`
			MergedAt  *string `json:"merged_at"`
		}
		if err := json.Unmarshal(raw, &gl); err != nil {
			return ref, fmt.Errorf("unmarshal gitlab mr payload: %w", err)
		}
		ref.Number, createdRaw, updatedRaw = gl.IID, gl.CreatedAt, gl.UpdatedAt
		if gl.MergedAt != nil {
			mergeRaw = *gl.MergedAt
		}
	default:
		var gh struct {
			Number    int     `json:"number"`
			CreatedAt string  `json:"createdAt"`
			UpdatedAt string  `json:"updatedAt"`
			MergedAt  *string `json:"mergedAt"`
		}
		if err := json.Unmarshal(raw, &gh); err != nil {
			return ref, fmt.Errorf("unmarshal github pr payload: %w", err)
		}
		ref.Number, createdRaw, updatedRaw = gh.Number, gh.CreatedAt, gh.UpdatedAt
		if gh.MergedAt != nil {
			mergeRaw = *gh.MergedAt
		}
	}

	var err error
	if ref.CreatedAt, err = parse(createdRaw); err != nil {
		return ref, fmt.Errorf("pr created: %w", err)
	}
	if ref.UpdatedAt, err = parse(updatedRaw); err != nil {
		return ref, fmt.Errorf("pr updated: %w", err)
	}
	if ref.MergedAt, err = parse(mergeRaw); err != nil {
		return ref, fmt.Errorf("pr merged: %w", err)
	}
	return ref, nil
}

// jiraStatusByCategory returns the workflow state for a statusCategory key, so the
// linked path can pin a status without depending on jiraStatuses' ordering.
func jiraStatusByCategory(category string) (name, cat string) {
	for _, s := range jiraStatuses {
		if s.category == category {
			return s.name, s.category
		}
	}
	return jiraStatuses[0].name, jiraStatuses[0].category
}

// BuildJiraIssues returns the issue payloads for a Jira-PM org, in the full REST v3
// shape the consumer's parser reads by field key. One issue per pull request whose
// jiraRoleForPR is linked or unlinked, so an issue key names exactly one PR and no
// issue can accumulate a second.
//
// prsByRepo is keyed by RepoSpec.Name and must hold each repo's COMPLETE pull-request
// history, not a delta: the earliest ref is the floor that keeps every issue's
// created inside the seeded window, and the lowest-numbered refs designate the
// project's epics. A repo with no pull requests (direct-push) yields no issues.
//
// Every timestamp derives from its pull request's, and the caller sets cursor_val
// from fields.updated. Because pull-request rows are immutable once seeded, that
// makes re-running this reproduce byte-identical payloads with no wall-clock read —
// which is what lets the issue rows be written non-destructively and still be right.
//
// Exported for the seed-connector contract test.
func BuildJiraIssues(org OrgCatalog, members []map[string]any, prsByRepo map[string][]JiraPRRef) []map[string]any {
	var issues []map[string]any
	for i, repo := range org.Repos {
		refs := prsByRepo[repo.Name]
		if len(refs) == 0 {
			continue
		}
		projectKey := jiraProjectKey(repo.Name)
		projectID := jiraProjectID(i)
		sprints := buildJiraSprintsForRepo(org, repo, i)
		earliest := refs[0].CreatedAt

		// The lowest-numbered issues of the project are its epics. Derived from the
		// earliest pull requests, which never change, so the set is stable as later
		// runs append issues — every parent key emitted below is therefore seeded.
		var epicKeys []string
		for _, ref := range refs {
			if len(epicKeys) == jiraEpicsPerProject {
				break
			}
			if jiraRoleForPR(org, repo, ref.Number) != jiraRoleNoIssue {
				epicKeys = append(epicKeys, jiraIssueKeyFor(repo, ref.Number))
			}
		}
		isEpic := func(key string) bool {
			for _, e := range epicKeys {
				if e == key {
					return true
				}
			}
			return false
		}

		for _, ref := range refs {
			role := jiraRoleForPR(org, repo, ref.Number)
			if role == jiraRoleNoIssue {
				continue
			}
			key := jiraIssueKeyFor(repo, ref.Number)
			rng := fnvRand(org.OrgName, repo.Name, "jira-issue", fmt.Sprint(ref.Number))
			assignee := members[rng.Intn(len(members))]
			reporter := members[rng.Intn(len(members))]

			// An issue is filed before the pull request that resolves it, and never
			// before the oldest seeded pull request — that floor is what keeps it
			// inside the backfill window without reading the clock.
			created := ref.CreatedAt.Add(-time.Duration(1+rng.Intn(72)) * time.Hour)
			if created.Before(earliest) {
				created = earliest
			}

			// A linked issue's state follows its pull request, so a resolved issue
			// always has a merged pull request behind it; an unlinked one is free to
			// sit anywhere in the workflow.
			statusName, statusCat := jiraStatusByCategory(jiraStatuses[rng.Intn(len(jiraStatuses))].category)
			anchor := ref.UpdatedAt
			if role == jiraRoleLinked {
				if ref.MergedAt.IsZero() {
					statusName, statusCat = jiraStatusByCategory("indeterminate")
				} else {
					statusName, statusCat = jiraStatusByCategory("done")
					anchor = ref.MergedAt
				}
			}
			updated := anchor.Add(time.Duration(rng.Intn(6)) * time.Hour)
			if updated.Before(created) {
				updated = created
			}

			issueType := "Story"
			if isEpic(key) {
				issueType = "Epic"
			}

			summary := fmt.Sprintf("%s: %s", projectKey, issueSummary(rng))
			fields := map[string]any{
				"summary":      summary,
				keyDescription: buildADF(summary),
				"created":      created.Format(jiraTimestampLayout),
				"updated":      updated.Format(jiraTimestampLayout),
				"duedate":      created.Add(time.Duration(3+rng.Intn(19)) * 24 * time.Hour).Format(jiraDateLayout),
				"labels":       []string{"backend", repo.Name},
				"issuetype":    map[string]any{keyName: issueType},
				"status": map[string]any{
					keyName:          statusName,
					"statusCategory": map[string]any{keyKey: statusCat},
				},
				"priority":           map[string]any{keyName: jiraPriority(rng)},
				"project":            map[string]any{"id": projectID, keyKey: projectKey},
				"assignee":           jiraPerson(org, assignee),
				"reporter":           jiraPerson(org, reporter),
				jiraFieldStartDate:   created.Format(jiraDateLayout),
				jiraFieldStoryPoints: jiraStoryPoints(rng),
				jiraFieldSprint:      jiraIssueSprintRef(rng, sprints),
				"worklogs":           buildJiraWorklogs(rng, org, assignee, projectKey, ref.Number, created),
			}
			// A resolved issue carries a resolutiondate; an open one omits the field
			// entirely, because an empty-string timestamp fails the connector's
			// *time.Time decode and silently drops the record.
			if statusCat == "done" {
				fields["resolutiondate"] = updated.Format(jiraTimestampLayout)
			}
			// Only a non-epic gets a parent, so the hierarchy stays one level deep.
			if !isEpic(key) && len(epicKeys) > 0 && rng.Float64() < 0.35 {
				fields["parent"] = map[string]any{keyKey: epicKeys[rng.Intn(len(epicKeys))]}
			}

			issues = append(issues, map[string]any{
				"id":     jiraIssueID(i, ref.Number),
				keyKey:   key,
				"self":   fmt.Sprintf("https://%s.atlassian.net/rest/api/3/issue/%s", org.OrgName, key),
				"fields": fields,
			})
		}
	}
	return issues
}

// jiraIssueID is the synthetic numeric issue id, a pure function of the repo index
// and pull-request number so it cannot shift when an earlier repo gains issues.
func jiraIssueID(repoIdx, prNum int) string {
	return fmt.Sprint(100000 + repoIdx*1000000 + prNum)
}

// jiraIssueSprintRef returns the issue's sprint custom field: one of the project's
// real sprints for most issues, empty for the rest (a backlog item). It embeds only
// id/state/name — a sprint's dates are anchored on the current week and would freeze
// stale into an issue row that is never rewritten.
func jiraIssueSprintRef(rng *rand.Rand, sprints []map[string]any) []any {
	if len(sprints) == 0 || rng.Float64() >= 0.85 {
		return []any{}
	}
	s := sprints[rng.Intn(len(sprints))]
	return []any{map[string]any{"id": s["id"], keyState: s[keyState], keyName: s[keyName]}}
}

// jiraIssueUpdated extracts the fields.updated timestamp from a BuildJiraIssues
// payload — the value the caller sets as cursor_val.
func jiraIssueUpdated(issue map[string]any) time.Time {
	fields := issue["fields"].(map[string]any)   //nolint:forcetypeassert // this package built the map
	raw := fields["updated"].(string)            //nolint:forcetypeassert // this package built the map
	t, _ := time.Parse(jiraTimestampLayout, raw) //nolint:errcheck // this package formatted it with the same layout
	return t
}

func jiraPerson(org OrgCatalog, member map[string]any) map[string]any {
	login := memberLogin(org, member)
	return map[string]any{
		"accountId":    jiraAccountID(login),
		"displayName":  capitalize(login) + " Demo",
		"emailAddress": login + "@" + org.OrgName + ".example.com",
	}
}

// memberLogin reads a member's login regardless of vendor payload shape.
func memberLogin(org OrgCatalog, member map[string]any) string {
	switch org.Vendor {
	case vendorBitbucket:
		return bbMemberLogin(member)
	case vendorGitLab:
		return glMemberLogin(member)
	default:
		return asString(member, keyLogin)
	}
}

func issueSummary(rng *rand.Rand) string {
	titles := []string{
		"Add settlement reconciliation flow", "Fix flaky payout webhook", "Refactor ledger writer",
		"Investigate latency spike", "Harden auth token refresh", "Backfill missing statements",
	}
	return titles[rng.Intn(len(titles))]
}

func jiraPriority(rng *rand.Rand) string {
	p := []string{"Highest", "High", "Medium", "Low"}
	return p[rng.Intn(len(p))]
}

func jiraStoryPoints(rng *rand.Rand) int {
	pts := []int{1, 2, 3, 5, 8}
	return pts[rng.Intn(len(pts))]
}

// buildADF wraps text in a minimal-but-non-empty Atlassian Document Format doc —
// the shape fetch_issues_data projects to S3. The connector stores an S3 ref, not
// the extracted text, so a single paragraph is enough for a faithful demo blob.
func buildADF(text string) map[string]any {
	return map[string]any{
		keyType:   "doc",
		"version": 1,
		"content": []any{
			map[string]any{
				keyType:   "paragraph",
				"content": []any{map[string]any{keyType: "text", "text": text}},
			},
		},
	}
}

// buildJiraWorklogs returns 0-2 worklog entries nested on an issue, authored by the
// issue's assignee (so worklog time attributes to a member). The worklog
// route reads these off the parent issue row.
func buildJiraWorklogs(rng *rand.Rand, org OrgCatalog, assignee map[string]any, projectKey string, issueNum int, issueCreated time.Time) []map[string]any {
	n := rng.Intn(3) // 0..2
	logs := make([]map[string]any, 0, n)
	for w := 0; w < n; w++ {
		logs = append(logs, map[string]any{
			"id":               fmt.Sprintf("%s-%d-%d", projectKey, issueNum, w+1),
			"author":           map[string]any{"accountId": jiraAccountID(memberLogin(org, assignee))},
			"created":          issueCreated.Add(time.Duration(w+1) * time.Hour).Format(jiraTimestampLayout),
			"timeSpentSeconds": (rng.Intn(8) + 1) * 1800, // 0.5h..4h
		})
	}
	return logs
}

// BuildJiraSprints returns sprint payloads for a jira-PM org: a few sprints per
// repo project, each bound to the project's synthetic scrum board via originBoardId
// (DeriveBoardID) so the board-listing + per-board-sprint handlers resolve them.
// Exported for the seed-connector contract test.
func BuildJiraSprints(org OrgCatalog) []map[string]any {
	var sprints []map[string]any
	for i, repo := range org.Repos {
		sprints = append(sprints, buildJiraSprintsForRepo(org, repo, i)...)
	}
	return sprints
}

// buildJiraSprintsForRepo returns the sprints of the i-th repo's project. Split out
// so BuildJiraIssues can pick a real sprint for an issue's sprint custom field from
// the same source the sprint rows come from — the two cannot disagree about ids.
func buildJiraSprintsForRepo(org OrgCatalog, repo RepoSpec, i int) []map[string]any {
	var sprints []map[string]any
	projectKey := jiraProjectKey(repo.Name)
	boardID := DeriveBoardID(jiraProjectID(i))
	rng := fnvRand(org.OrgName, repo.Name, "jira-sprint")
	numSprints := 2 + rng.Intn(2) // 2..3
	for sn := 1; sn <= numSprints; sn++ {
		// Sprints march backward from recentWeek in 2-week iterations; the most
		// recent is active, the rest closed.
		end := recentWeek().Add(-time.Duration(numSprints-sn) * 14 * 24 * time.Hour)
		start := end.Add(-14 * 24 * time.Hour)
		state := "closed"
		sprint := map[string]any{
			"id":            int64(boardID*100 + int64(sn)),
			keyState:        state,
			keyName:         fmt.Sprintf("%s Sprint %d", projectKey, sn),
			"startDate":     start.Format(jiraTimestampLayout),
			"endDate":       end.Format(jiraTimestampLayout),
			"completeDate":  end.Format(jiraTimestampLayout),
			"originBoardId": boardID,
			"goal":          fmt.Sprintf("Deliver %s milestone %d", projectKey, sn),
			"boardName":     projectKey + " board",
			"projectKey":    projectKey,
		}
		if sn == numSprints {
			sprint[keyState] = "active"
			delete(sprint, "completeDate")
		}
		sprints = append(sprints, sprint)
	}
	return sprints
}

// seedJiraSprints upserts the org's Jira sprint rows (static — upsert). cursor_val
// is recentWeek so sprints always appear current, matching the other static entities.
func seedJiraSprints(ctx context.Context, tx pgx.Tx, org OrgCatalog) error {
	for _, sprint := range BuildJiraSprints(org) {
		entityID := fmt.Sprint(sprint["id"])
		if err := upsertRow(ctx, tx, "jira", org.OrgName, entityTypeSprint, entityID, sprint, recentWeek(), true); err != nil {
			return fmt.Errorf("upsert jira sprint %s: %w", entityID, err)
		}
	}
	return nil
}

// seedJiraProjects upserts the org's Jira project rows (static — payload refreshed
// in place). Written with the literal "jira" vendor, not org.Vendor.
func seedJiraProjects(ctx context.Context, tx pgx.Tx, org OrgCatalog) error {
	for _, proj := range BuildJiraProjects(org) {
		entityID := proj[keyKey].(string) //nolint:forcetypeassert // built above
		if err := upsertRow(ctx, tx, "jira", org.OrgName, entityTypeProject, entityID, proj, recentWeek(), true); err != nil {
			return fmt.Errorf("upsert jira project %s: %w", entityID, err)
		}
	}
	return nil
}

// seedJiraIssues upserts the org's Jira issue rows, one per pull request that has an
// issue role. Existing rows are never rewritten, so history accumulates: each run's
// new pull requests bring their own new issues and the recent end of the issue
// distribution tracks the present, exactly as the pull-request stream does.
// cursor_val is the issue's fields.updated.
func seedJiraIssues(ctx context.Context, tx pgx.Tx, org OrgCatalog, members []map[string]any) error {
	payloadsByRepo, err := seededPRPayloadsByRepo(ctx, tx, org)
	if err != nil {
		return fmt.Errorf("read seeded prs for %s: %w", org.OrgName, err)
	}

	prsByRepo := make(map[string][]JiraPRRef, len(org.Repos))
	for _, repo := range org.Repos {
		if Profiles[repo.Flavor].PRsPerWeek == 0 {
			continue // direct-push repo: no pull-request sequence, so no issue sequence
		}
		refs, err := JiraPRRefs(org, payloadsByRepo[repo.Name])
		if err != nil {
			return fmt.Errorf("project pr refs for %s: %w", repo.Name, err)
		}
		prsByRepo[repo.Name] = refs
	}

	for _, issue := range BuildJiraIssues(org, members, prsByRepo) {
		entityID := issue[keyKey].(string) //nolint:forcetypeassert // built above
		if err := upsertRow(ctx, tx, "jira", org.OrgName, entityTypeIssue, entityID, issue, jiraIssueUpdated(issue), false); err != nil {
			return fmt.Errorf("upsert jira issue %s: %w", entityID, err)
		}
	}
	return nil
}

// seededPRPayloadsByRepo reads the org's entire seeded pull-request history in one
// pass, grouped by the payload's repo. The row's repo lives inside the JSONB payload
// and no index covers it, so querying per repo would re-scan every one of the org's
// PR rows once per repo; grouping in Go costs one scan regardless of repo count.
// A repo with no seeded PRs is simply absent from the map.
func seededPRPayloadsByRepo(ctx context.Context, tx pgx.Tx, org OrgCatalog) (map[string][]json.RawMessage, error) {
	rows, err := tx.Query(ctx,
		`SELECT COALESCE(payload->>'repo', ''), payload FROM sim_records
		 WHERE vendor=$1 AND org_name=$2 AND entity_type='pr'`,
		org.Vendor, org.OrgName)
	if err != nil {
		return nil, fmt.Errorf("read pr rows: %w", err)
	}
	defer rows.Close()

	out := make(map[string][]json.RawMessage, len(org.Repos))
	for rows.Next() {
		var repo string
		var raw []byte
		if err := rows.Scan(&repo, &raw); err != nil {
			return nil, fmt.Errorf("scan pr row: %w", err)
		}
		// GitLab MR payloads have project_id, not repo. Grouping on the empty
		// COALESCE would dump every MR under "" and each catalog repo would
		// see zero refs. Keep this grouping if a GitLab Jira-PM org is added later.
		if org.Vendor == vendorGitLab {
			repo = gitLabCatalogRepoName(org, projectIDFromPayload(raw))
			if repo == "" {
				continue
			}
		}
		out[repo] = append(out[repo], json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pr rows: %w", err)
	}
	return out, nil
}
