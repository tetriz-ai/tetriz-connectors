# GitHub records

Back to [Records](README.md). The connector id is `github`.

GitHub payloads come in four encodings, and the key style follows the encoding.

| Encoding | Keys | Operations |
|---|---|---|
| GraphQL node, re-marshalled from the connector's struct | camelCase | `fetch_prs`, `fetch_commits`, `fetch_issues`, `fetch_projects` |
| REST body, passed through | snake_case | `fetch_reviews`, `fetch_pr_comments`, `fetch_copilot_usage`, `fetch_branches`, `webhook_events`, `pr_sync_events` |
| JSON the connector builds | snake_case | `fetch_org_members`, `fetch_prs_data`, `fetch_issues_data`, `fetch_config` |
| protojson of a proto message | lowerCamelCase | `fetch_repos`, `fetch_repo_tree`, `fetch_file_content` |

A re-marshalled GraphQL node always has every key its struct declares. A field the struct does not declare is dropped, even when GitHub sends it. An absent list is `null`, not `[]`. A deleted account is `{"login": "", "databaseId": 0}`, not `null`.

protojson omits a field that holds its zero value: an empty string, `false` or `0`. It writes an int64 as a decimal string and an enum as its name.

| Operation | ExternalID | Payload |
|---|---|---|
| [`fetch_repos`](#fetch_repos) | `<repo id>` | RepositoryRecord protojson |
| [`fetch_prs`](#fetch_prs) | `owner/repo#PR-42#opened`, `#merged`, `#closed`, `#snapshot` | GraphQL PullRequest node |
| [`fetch_commits`](#fetch_commits) | `owner/repo#<sha>` | GraphQL Commit node |
| [`fetch_issues`](#fetch_issues) | `owner/repo#ISSUE-7#opened`, `#closed`, `#snapshot` | GraphQL Issue node |
| [`fetch_reviews`](#fetch_reviews) | `owner/repo#PR-42#REVIEW-<id>` | REST review |
| [`fetch_pr_comments`](#fetch_pr_comments) | `owner/repo#PR-42/COMMENT-<id>` | REST review comment |
| [`fetch_copilot_usage`](#fetch_copilot_usage) | `copilot-<org>-<date>`, `copilot-user-<org>-<date>-<user id>` | REST Copilot metrics |
| [`fetch_org_members`](#fetch_org_members) | `member-<user id>` | Built by the connector |
| [`fetch_prs_data`](#fetch_prs_data) | `owner/repo#PR-42` | Pointer to a blob |
| [`fetch_issues_data`](#fetch_issues_data) | `owner/repo#I-7` | Pointer to a blob |
| [`fetch_projects`](#fetch_projects) | `project-<number>` | GraphQL ProjectV2 node |
| [`fetch_repo_tree`](#fetch_repo_tree) | `<repo id>:<sha>` | RepoTreeRecord protojson |
| [`fetch_branches`](#fetch_branches) | `<repo id>:<branch>` | REST branch |
| [`fetch_config`](#fetch_config) | `<repo id>` | Built by the connector |
| [`fetch_file_content`](#fetch_file_content) | `<repository id>#<ide type number>#<source path>#<generation>` | FileContentRecord protojson |
| [`webhook_events`](#webhook_events) | `webhook-<delivery id>` | Webhook body |
| [`pr_sync_events`](#pr_sync_events) | `owner/repo#PR-42#SYNC-<after sha>` | Webhook body |

`fetch_pr_diffs`, `drain_defect_lineage` and `drain_defect_scoring` emit no records. They write to the blob store and the host's services.

`<repo id>` is GitHub's numeric repository id. It survives a rename. `owner/repo` does not: a renamed repository starts new ExternalIDs. Join on the `platform_repo_id` metadata key to follow it.

## fetch_repos

Schema: [`schemas/github/fetch_repos.schema.json`](../../schemas/github/fetch_repos.schema.json).

- **ExternalID.** The numeric repository id, such as `2562639`.
- **DataDate.** Not set. Use `fetchedAt`.
- **Scope.** A GitHub App lists the organization's repositories (`/orgs/{org}/repos`) or, for an individual tenant, the installation's. A personal access token lists `/user/repos`, every repository the token reaches; for an organization tenant with `organization` set, only that organization's repositories are emitted (owner compared case-insensitively), the set `fetch_prs` reads. An individual tenant, or a token config without `organization`, keeps them all.
- **Payload.** A `RepositoryRecord` (`proto/tetriz/connectors/connector/v1/repo_ops.proto`) in protojson.

`vendorBlobJson` is base64 inside the payload, which is itself base64 in the envelope. Decode it twice. Its JSON is a flat object of GitHub-only fields. The proto comment says the vendor id is its only top-level key, but the GitHub blob has no `github` wrapper.

| Field | Type | Meaning |
|---|---|---|
| `platformRepoId` | string | Numeric repository id |
| `name`, `fullName`, `ownerLogin` | string | `widgets`, `acme/widgets`, `acme` |
| `defaultBranch`, `trackedBranch` | string | Default branch, and the branch commit and tree operations follow |
| `visibility` | enum name | `REPOSITORY_VISIBILITY_PUBLIC`, `_PRIVATE` or `_INTERNAL` |
| `archived`, `disabled`, `isFork`, `isTemplate` | boolean | Absent when false |
| `sizeKb` | string | Size in KB, an int64 written as a string |
| `stats` | object | `stars`, `forks`, `watchers`, `subscribers`, `openIssues`, `networkCount`; absent when 0 |
| `vendorCreatedAt`, `vendorUpdatedAt`, `vendorPushedAt` | RFC 3339 | GitHub's timestamps |
| `vendorBlobJson` | base64 JSON | `node_id`, `homepage`, `allow_forking`, `web_commit_signoff_required`, `allow_squash_merge`, `allow_merge_commit`, `allow_rebase_merge` |

The three `allow_*_merge` flags come from a separate GraphQL query. When it fails, they read `false`.

| Metadata key | Meaning |
|---|---|
| `full_name` | `owner/repo` |

Example payload, trimmed, from the simulator:

```json
{"platformRepoId": "2562639", "name": "mobile", "fullName": "demo-gh-startup/mobile",
 "defaultBranch": "main", "trackedBranch": "main", "primaryLanguage": "Ruby",
 "stats": {"openIssues": 17, "stars": 291}, "vendorPushedAt": "2026-09-28T10:04:29Z",
 "vendorBlobJson": "eyJhbGxvd19mb3JraW5nIjpmYWxzZSwi..."}
```

## fetch_prs

Schema: [`schemas/github/fetch_prs.schema.json`](../../schemas/github/fetch_prs.schema.json).

Each pull request gives up to four records, as [ADR-0003](../adr/0003-github-connector-lifecycle-events.md) describes. All four carry the same payload. Read `event_type` in the metadata to tell them apart.

| Record | ExternalID | DataDate |
|---|---|---|
| Opened | `acme/widgets#PR-42#opened` | `createdAt` |
| Merged, when `mergedAt` is set | `acme/widgets#PR-42#merged` | `mergedAt` |
| Closed without a merge | `acme/widgets#PR-42#closed` | `closedAt` |
| Snapshot | `acme/widgets#PR-42#snapshot` | Not set: it is the current state ([ADR-0003 amendment](../adr/0003-github-connector-lifecycle-events.md#amendment-2026-10-the-snapshot-has-no-datadate)) |

Every run re-emits all of them. Deduplicate on the key in the [README](README.md); the snapshot then holds the latest counters.

| Field | Type | Meaning |
|---|---|---|
| `number` | integer | Pull request number |
| `state` | string | `OPEN`, `CLOSED` or `MERGED` |
| `title`, `body`, `url` | string | Body is Markdown |
| `baseRefName`, `headRefName` | string | Target and source branch |
| `createdAt`, `updatedAt` | RFC 3339 | |
| `mergedAt`, `closedAt` | RFC 3339 or null | |
| `additions`, `deletions`, `changedFiles` | integer | Diff size at fetch time |
| `isDraft`, `isCrossRepository` | boolean | Cross-repository means a fork |
| `mergeable` | string | `MERGEABLE`, `CONFLICTING`, `UNKNOWN`, or empty |
| `author` | object | `login`, `databaseId` |
| `mergedBy` | object or null | `login` |
| `reviewDecision` | string or null | `APPROVED`, `CHANGES_REQUESTED`, `REVIEW_REQUIRED` |
| `reviews` | object | `totalCount`, and `nodes` with `author`, `state`, `submittedAt` |
| `reviewRequests` | object | `totalCount`, and `nodes[].requestedReviewer` with `login`, `databaseId` or a team `slug` |
| `comments` | object | `totalCount` |
| `mergeCommit` | object or null | `message`, `parents.totalCount`: 1 for squash or rebase, 2 for a merge commit |
| `closingIssuesReferences` | object | `nodes[]` with `number`, `title`, `state`, `repository.nameWithOwner` |
| `files` | object | Up to 100 `nodes[]` with `path`, `additions`, `deletions`; `pageInfo.hasNextPage` when truncated |
| `commits` | object | `nodes[].commit` with `authoredDate`, `committedDate` |
| `headRepositoryOwner` | object | `login` |

| Metadata key | Meaning |
|---|---|
| `event_type` | `opened`, `merged`, `closed` or `snapshot` |
| `repo`, `owner` | `owner/repo` and `owner` |
| `platform_repo_id` | Numeric repository id; absent when unknown |
| `pr_author_id` | Author's numeric user id; empty for a deleted account |
| `head_branch`, `base_ref_name` | Source and target branch |
| `is_draft` | `true` or `false` |
| `files_changed` | JSON array of paths, as a string, such as `["a.go","b.go"]`. Complete even past 100 files. |
| `files_line_stats` | JSON array, as a string, of `{"path","additions","deletions"}` |

Example payload, trimmed, from the simulator:

```json
{"number": 120, "state": "MERGED", "title": "mobile: fix #120", "baseRefName": "main",
 "headRefName": "release/mobile-120", "createdAt": "2026-10-04T06:00:00Z",
 "mergedAt": "2026-10-05T09:00:00Z", "closedAt": null, "additions": 82, "deletions": 48,
 "changedFiles": 10, "author": {"databaseId": 3953824, "login": "carol"},
 "reviewDecision": null, "reviewRequests": {"nodes": null, "totalCount": 0}}
```

## fetch_commits

Schema: [`schemas/github/fetch_commits.schema.json`](../../schemas/github/fetch_commits.schema.json).

- **ExternalID.** `owner/repo#` and the commit SHA.
- **DataDate.** `committedDate`.
- **Payload.** One commit of the branch history.

A commit reached from several tracked branches is emitted once per branch, with the same ExternalID. The last record holds every branch in `refs`.

| Field | Type | Meaning |
|---|---|---|
| `oid` | string | 40-character SHA |
| `message` | string | Full commit message |
| `authoredDate`, `committedDate` | RFC 3339 | |
| `additions`, `deletions` | integer | |
| `changedFilesIfAvailable` | integer | 0 when GitHub does not report it |
| `author` | object | `name`, `email`, and `user` with `login` and `databaseId`; an author with no account has an empty login and 0 |
| `parents.totalCount` | integer | 2 or more for a merge commit |
| `associatedPullRequests.nodes` | array or null | `number` of each pull request, up to 5 |

| Metadata key | Meaning |
|---|---|
| `repo` | `owner/repo` |
| `platform_repo_id` | Numeric repository id; absent when unknown |
| `refs` | Comma-separated branches the commit was reached from, sorted |

Example payload, from the simulator:

```json
{"oid": "0000000000000000000000000516ddb78238d77b", "message": "chore: commit 4",
 "authoredDate": "2026-10-04T04:24:00Z", "committedDate": "2026-10-04T06:00:00Z",
 "additions": 79, "deletions": 24, "changedFilesIfAvailable": 5,
 "author": {"email": "carol@example.com", "name": "Carol", "user": {"databaseId": 3953824, "login": "carol"}},
 "parents": {"totalCount": 0}, "associatedPullRequests": {"nodes": null}}
```

## fetch_issues

Schema: [`schemas/github/fetch_issues.schema.json`](../../schemas/github/fetch_issues.schema.json).

Each issue gives up to three records, as [ADR-0003](../adr/0003-github-connector-lifecycle-events.md) describes. All three carry the same payload.

| Record | ExternalID | DataDate |
|---|---|---|
| Opened | `acme/widgets#ISSUE-7#opened` | `createdAt` |
| Closed, when closed with `closedAt` set | `acme/widgets#ISSUE-7#closed` | `closedAt` |
| Snapshot | `acme/widgets#ISSUE-7#snapshot` | Not set: it is the current state |

The payload carries every comment: the connector fetches the pages past the first.

| Field | Type | Meaning |
|---|---|---|
| `number` | integer | Issue number |
| `state` | string | `OPEN` or `CLOSED` |
| `stateReason` | string or null | `COMPLETED`, `NOT_PLANNED`, `REOPENED` or `DUPLICATE` |
| `title`, `body`, `url` | string | Body is Markdown |
| `createdAt`, `updatedAt` | RFC 3339 | |
| `closedAt` | RFC 3339 or null | |
| `repository.nameWithOwner` | string | `owner/repo` |
| `author` | object | `login`, `databaseId` |
| `assignees.nodes`, `labels.nodes` | array or null | Assignees with `login` and `databaseId`; labels with `name` |
| `milestone` | object or null | `number`, `title`, `state`, `dueOn` |
| `issueType` | object | `name`; absent when the issue has no native type |
| `parent` | object | `number`, `repository.nameWithOwner`; absent on a top-level issue |
| `subIssues`, `participants` | object | `totalCount` |
| `comments` | object | `totalCount`, `pageInfo`, and `nodes[]` with `id`, `body`, `author`, `createdAt`, `updatedAt` |
| `projectItems.nodes` | array or null | `project` with `title` and `number`, and its `fieldValues` |
| `closedByPullRequestsReferences.nodes` | array or null | `number`, `state`, `repository.nameWithOwner` |

A project field value has no type tag. Which key is present tells its kind: `name` for a single select, `text`, `number`, or `title` with `startDate` and `duration` for an iteration. `field.name` names the field.

| Metadata key | Meaning |
|---|---|
| `event_type` | `opened`, `closed` or `snapshot` |
| `repo` | `owner/repo` |

Example payload, trimmed, from the simulator:

```json
{"number": 115, "state": "CLOSED", "stateReason": null, "title": "mobile: issue #115",
 "createdAt": "2026-09-30T14:00:00Z", "closedAt": null,
 "repository": {"nameWithOwner": "demo-gh-startup/mobile"},
 "author": {"databaseId": 8346149, "login": "dave"}, "labels": {"nodes": []},
 "comments": {"nodes": [], "pageInfo": {"endCursor": "", "hasNextPage": false}, "totalCount": 0}}
```

## fetch_reviews

Schema: [`schemas/github/fetch_reviews.schema.json`](../../schemas/github/fetch_reviews.schema.json).

- **ExternalID.** `owner/repo#PR-42#REVIEW-` and the review id.
- **DataDate.** `submitted_at`. A pending review has none, so its DataDate is not set.
- **Payload.** One element of `GET /repos/{owner}/{repo}/pulls/{number}/reviews`, unchanged.

| Field | Type | Meaning |
|---|---|---|
| `id` | integer | Review id |
| `user` | object or null | `login`, `id`, `type` (`User`, `Bot`, ...); null for a deleted account |
| `state` | string | `APPROVED`, `CHANGES_REQUESTED`, `COMMENTED`, `DISMISSED` or `PENDING` |
| `submitted_at` | RFC 3339 or null | |
| `body`, `commit_id`, `html_url` | string | |

| Metadata key | Meaning |
|---|---|
| `repo` | `owner/repo` |
| `pr_number`, `pr_key` | `42` and `owner/repo#PR-42` |
| `pr_author_login`, `pr_author_id` | The pull request's author; the id is empty when unknown |
| `platform_repo_id` | Numeric repository id; empty when unknown |
| `pr_created_at` | RFC 3339 |
| `reviewer_is_bot` | `true` or `false` |

Example payload, from the simulator:

```json
{"id": 1986045, "state": "CHANGES_REQUESTED", "submitted_at": "2026-10-04T08:00:00Z",
 "user": {"id": 5043885, "login": "eve", "type": "User"}}
```

## fetch_pr_comments

Schema: [`schemas/github/fetch_pr_comments.schema.json`](../../schemas/github/fetch_pr_comments.schema.json).

- **ExternalID.** `owner/repo#PR-42/COMMENT-` and the comment id. The separator before `COMMENT` is a slash, not `#`.
- **DataDate.** `created_at`.
- **Payload.** One review comment from `GET /repos/{owner}/{repo}/pulls/{number}/comments`, unchanged. These are the inline comments on the diff. Conversation comments are not fetched.

| Field | Type | Meaning |
|---|---|---|
| `id` | integer | Comment id |
| `user` | object or null | `login`, `id`, `type` |
| `body`, `path`, `diff_hunk` | string | The comment and where it sits |
| `created_at`, `updated_at` | RFC 3339 | |
| `pull_request_review_id`, `in_reply_to_id` | integer | Review it belongs to, comment it answers |

The metadata keys are those of `fetch_reviews`, plus `owner`. As there, `pr_author_id` is empty when the author is unknown.

Example payload, from the simulator:

```json
{"id": 4360884, "body": "Demo comment 1", "created_at": "2026-10-04T08:00:00Z",
 "user": {"id": 8346149, "login": "dave", "type": "User"}}
```

## fetch_copilot_usage

Schema: [`schemas/github/fetch_copilot_usage.schema.json`](../../schemas/github/fetch_copilot_usage.schema.json).

Two record kinds share this operation. The metadata key `type` tells them apart, and the schema is a `oneOf`.

| `type` | ExternalID | Payload |
|---|---|---|
| `org_metrics` | `copilot-<org>-<date>` | One day of `GET /orgs/{org}/copilot/metrics` |
| `user_metrics` | `copilot-user-<org>-<day>-<user id>` | One element of `GET /orgs/{org}/copilot/usage/user` |

- **DataDate.** The payload's `date` or `day`, at midnight UTC.
- **Re-reads.** Each run re-emits the last days again, because GitHub finalizes the counts late.

| Field | Type | Meaning |
|---|---|---|
| `date` (org) | `YYYY-MM-DD` | The day |
| `total_active_users`, `total_engaged_users` (org) | integer | |
| `copilot_ide_code_completions`, `copilot_ide_chat`, ... (org) | object | Per-feature breakdowns, as GitHub sends them |
| `day` (user) | `YYYY-MM-DD` | The day |
| `github_com_user_id` (user) | string or integer | Numeric user id, passed through as GitHub sent it. The ExternalID writes it in decimal either way. A user row without one has no identity and is skipped. |

| Metadata key | Meaning |
|---|---|
| `org` | Organization login |
| `type` | `org_metrics` or `user_metrics` |

Example payloads, from the simulator:

```json
{"date": "2026-09-28", "total_active_users": 14, "total_engaged_users": 9,
 "copilot_ide_code_completions": {"total_engaged_users": 6}}
{"day": "2026-09-28", "github_com_user_id": "5043885",
 "total_acceptances_count": 10, "total_suggestions_count": 11}
```

## fetch_org_members

Schema: [`schemas/github/fetch_org_members.schema.json`](../../schemas/github/fetch_org_members.schema.json).

- **ExternalID.** `member-` and the numeric user id.
- **DataDate.** Not set.
- **Payload.** Built by the connector. Each run is a full listing.

The connector reads SAML identities first and falls back to the members API. `hint_source` says which one answered.

| Field | Type | Meaning |
|---|---|---|
| `numeric_user_id` | integer | Stable user id |
| `login` | string | Username; it can change |
| `email` | string | SAML nameId, or the public profile email; absent when empty |
| `name`, `avatar_url` | string | Absent when empty |
| `hint_source` | string | `saml_scim` or `org_members_api` |
| `teams` | array of strings | Team slugs; absent when none or when team lookup failed |

| Metadata key | Meaning |
|---|---|
| `hint_source` | As in the payload |

Example payload, from the simulator:

```json
{"numeric_user_id": 9121988, "login": "alice", "name": "Alice Demo",
 "email": "alice@demo-gh-startup.example.com", "hint_source": "org_members_api",
 "avatar_url": "https://avatars.githubusercontent.com/u/7806891"}
```

## fetch_prs_data

Schema: [`schemas/github/fetch_prs_data.schema.json`](../../schemas/github/fetch_prs_data.schema.json).

- **ExternalID.** `owner/repo#PR-42`, without a lifecycle suffix.
- **DataDate.** Not set: the pointer is the description's current state.
- **Payload.** A pointer. The body itself is raw Markdown in the blob store at `s3_key`. A pull request with an empty body emits nothing.

| Field | Type | Meaning |
|---|---|---|
| `s3_key` | string | `descriptions/<organizationId>/github/pull_requests/<repo id>/<number>` |
| `pr_number` | integer | |
| `platform_repo_id` | string | Numeric repository id |
| `repo_full_name` | string | `owner/repo` |

| Metadata key | Meaning |
|---|---|
| `description_sha` | `sha256:` and the hex digest of the body; unchanged means the body is unchanged |
| `repo`, `owner`, `platform_repo_id` | As above |

Example payload, from the simulator:

```json
{"s3_key": "descriptions/00000000-0000-4000-8000-00000000d3e0/github/pull_requests/2739900/1",
 "pr_number": 1, "platform_repo_id": "2739900", "repo_full_name": "demo-gh-startup/web-app"}
```

## fetch_issues_data

Schema: [`schemas/github/fetch_issues_data.schema.json`](../../schemas/github/fetch_issues_data.schema.json).

- **ExternalID.** `owner/repo#I-7`. Note `I-`, where `fetch_issues` uses `ISSUE-`.
- **DataDate.** Not set: the pointer is the description's current state.
- **Payload.** A pointer, in the shape Jira's `fetch_issues_data` shares. The repository id is therefore named `platform_project_id`.

| Field | Type | Meaning |
|---|---|---|
| `s3_key` | string | `descriptions/<organizationId>/github/issues/<repo id>/<number>` |
| `issue_key` | string | `owner/repo#7` |
| `platform_project_id` | string | Numeric repository id |
| `repo_full_name` | string | `owner/repo` |

The metadata keys are `description_sha`, `repo` and `platform_repo_id`, as in `fetch_prs_data`.

The simulator gives its issues empty bodies, so this payload is checked by a fixture test instead (`connectors/github/schema_github_test.go`).

Example payload:

```json
{"s3_key": "descriptions/org-test/github/issues/777/42", "issue_key": "acme/widgets#42",
 "platform_project_id": "777", "repo_full_name": "acme/widgets"}
```

## fetch_projects

Schema: [`schemas/github/fetch_projects.schema.json`](../../schemas/github/fetch_projects.schema.json).

- **ExternalID.** `project-` and the project number.
- **DataDate.** Not set: a project is current state.
- **Payload.** One organization ProjectV2.

| Field | Type | Meaning |
|---|---|---|
| `number` | integer | Project number in the organization |
| `title`, `shortDescription`, `url` | string | |
| `closed` | boolean | |
| `createdAt` | string | RFC 3339, as GitHub sent it |

| Metadata key | Meaning |
|---|---|
| `organization` | Organization login |

Example payload, from the simulator:

```json
{"number": 1, "title": "Sprint 1", "shortDescription": "Demo project: Sprint 1", "closed": false,
 "createdAt": "2026-09-28T10:04:29Z", "url": "https://github.com/orgs/demo-gh-startup/projects/1"}
```

## fetch_repo_tree

Schema: [`schemas/github/fetch_repo_tree.schema.json`](../../schemas/github/fetch_repo_tree.schema.json).

- **ExternalID.** `<repo id>:<commit sha>`.
- **DataDate.** Not set.
- **Payload.** A `RepoTreeRecord` in protojson. The tree is a JSON blob at `treeS3Key`.

| Field | Type | Meaning |
|---|---|---|
| `platformRepoId` | string | Numeric repository id |
| `treeSha`, `treeBranch` | string | Commit and branch the tree was taken at |
| `treeS3Key` | string | Blob key. A `noChange` record names the tree written for the same SHA |
| `treeTruncated` | boolean | GitHub returned a partial tree; absent when false |
| `noChange` | boolean | The SHA matched the last run, so nothing was uploaded; absent when false |
| `fetchedAt` | RFC 3339 | |

| Metadata key | Meaning |
|---|---|
| `repo_full_name` | `owner/repo` |

Example payload, from the simulator:

```json
{"platformRepoId": "2739900", "treeBranch": "main", "treeSha": "000000000000000000000000470b95c0fc70e0de",
 "treeS3Key": "repo-trees/00000000-0000-4000-8000-00000000d3e0/github/2739900/000000000000000000000000470b95c0fc70e0de.json",
 "fetchedAt": "2026-10-05T10:04:45.277387Z"}
```

## fetch_branches

Schema: [`schemas/github/fetch_branches.schema.json`](../../schemas/github/fetch_branches.schema.json).

- **ExternalID.** `<repo id>:<branch name>`. A branch name can contain `/` and `:`.
- **DataDate.** Not set: a branch is current state.
- **Payload.** One element of `GET /repos/{owner}/{repo}/branches`, unchanged.

Each run lists every branch again. A branch missing from a complete listing was deleted; `branch_total` tells a complete listing from a torn one.

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Branch name |
| `commit.sha` | string | Head commit |
| `protected` | boolean | |

| Metadata key | Meaning |
|---|---|
| `repo_full_name`, `platform_repo_id` | The repository |
| `branch_name`, `head_sha`, `protected` | As in the payload |
| `is_default` | `true` when the branch is the default branch |
| `branch_total` | Number of branches in this listing, as a string |

The simulator serves branch listings (the default branch and each open pull request's head branch), and a fixture test also checks this payload.

Example payload:

```json
{"name": "main", "commit": {"sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, "protected": true}
```

## fetch_config

Schema: [`schemas/github/fetch_config.schema.json`](../../schemas/github/fetch_config.schema.json).

- **ExternalID.** The numeric repository id.
- **DataDate.** Not set.
- **Payload.** Built by the connector from the repository and its default branch's rules.

| Field | Type | Meaning |
|---|---|---|
| `repo_settings.default_branch` | string | |
| `branch_rules` | array | `GET /repos/{owner}/{repo}/rules/branches/{branch}`, unchanged; absent when that call failed |

Each rule has a `type`, such as `pull_request`, and its `parameters`, such as `required_approving_review_count` and `allowed_merge_methods`.

| Metadata key | Meaning |
|---|---|
| `scope` | Always `REPO` |
| `full_name` | `owner/repo` |

Example payload:

```json
{"repo_settings": {"default_branch": "main"},
 "branch_rules": [{"type": "pull_request", "ruleset_id": 7,
   "parameters": {"required_approving_review_count": 2, "allowed_merge_methods": ["squash"]}}]}
```

## fetch_file_content

Schema: [`schemas/github/fetch_file_content.schema.json`](../../schemas/github/fetch_file_content.schema.json).

This is a host-only operation. The host's work queue names the AI-tool files to fetch, such as Claude Code skills and Cursor rules. The connector stages each body in the blob store and emits one record per artifact.

- **ExternalID.** `<repository id>#<ide type>#<source path>#<generation>`. The IDE type is the enum's number, such as `1` for Claude Code, while the payload writes its name.
- **DataDate.** Not set.
- **Payload.** A `FileContentRecord` (`proto/tetriz/connectors/workqueue/v1/workqueue.proto`) in protojson.

| Field | Type | Meaning |
|---|---|---|
| `repositoryId`, `repoFullName`, `platformRepoId` | string | The host's id, `owner/repo`, GitHub's id |
| `ideType`, `artifactType` | enum name | Such as `IDE_TYPE_CLAUDE_CODE`, `HARNESS_ARTIFACT_TYPE_SKILL` |
| `sourcePath`, `name` | string | Primary file and display name |
| `branch`, `ref` | string | Where the files were read |
| `generation` | string | int64 freshness token, written as a string |
| `isTombstone` | boolean | The whole artifact was deleted |
| `files` | array | `relativePath`, `contentHash`, `s3Key`, `blobSha`, `isPrimary`, `truncated`, `originalBytes` (a string) |
| `removedPaths` | array of strings | Files removed from a surviving artifact |

| Metadata key | Meaning |
|---|---|
| `repo_full_name` | `owner/repo` |

Example payload:

```json
{"repositoryId": "repo-1", "repoFullName": "acme/widgets", "ideType": "IDE_TYPE_CLAUDE_CODE",
 "sourcePath": ".claude/skills/deploy/SKILL.md", "name": "deploy", "branch": "main",
 "ref": "commit-sha-abc", "generation": "42",
 "files": [{"relativePath": "SKILL.md", "blobSha": "blob-1", "isPrimary": true,
   "s3Key": "harness-staging/org-1/github/repo-1/42/.claude/skills/deploy/SKILL.md",
   "contentHash": "3f1c...", "originalBytes": "9"}]}
```

## webhook_events

Schema: [`schemas/github/webhook_events.schema.json`](../../schemas/github/webhook_events.schema.json).

A host calls this operation with each delivery it receives. The connector checks the signature and emits the body.

- **ExternalID.** `webhook-` and the `X-GitHub-Delivery` id.
- **DataDate.** Not set.
- **Payload.** The delivery body, byte for byte, always a JSON object. Its shape is GitHub's event payload for the event in `event_type`.

Only `push`, `pull_request`, `pull_request_review`, `issues`, `issue_comment`, `create` and `delete` are emitted. `ping` and other events are acknowledged and dropped. A body that is not a JSON object, such as a delivery configured with content type `application/x-www-form-urlencoded`, is rejected with an error and emits nothing; configure the webhook as `application/json`.

| Metadata key | Meaning |
|---|---|
| `event_type` | The `X-GitHub-Event` header |
| `action` | The body's `action`; absent when it has none |

## pr_sync_events

Schema: [`schemas/github/pr_sync_events.schema.json`](../../schemas/github/pr_sync_events.schema.json).

`webhook_events` emits this second record for a `pull_request` delivery with action `synchronize`: a push to the pull request branch. Force pushes count. Its records carry operationId `pr_sync_events`, not `webhook_events`.

- **ExternalID.** `owner/repo#PR-42#SYNC-` and the `after` SHA.
- **DataDate.** Not set.
- **Payload.** The same delivery body as the `webhook_events` record.

| Field | Type | Meaning |
|---|---|---|
| `action` | string | Always `synchronize` |
| `before`, `after` | string | Head SHA before and after the push |
| `pull_request.number` | integer | |
| `pull_request.base.repo.full_name` | string | `owner/repo` |

| Metadata key | Meaning |
|---|---|
| `repo`, `pr_number`, `pr_key` | `owner/repo`, `42`, `owner/repo#PR-42` |
| `after_sha`, `before_sha` | As in the payload |

Example payload, trimmed:

```json
{"action": "synchronize", "before": "1111111111111111111111111111111111111111",
 "after": "2222222222222222222222222222222222222222",
 "pull_request": {"number": 42, "base": {"repo": {"full_name": "acme/widgets"}}}}
```
