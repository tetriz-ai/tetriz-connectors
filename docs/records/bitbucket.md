# Bitbucket records

Back to [Records](README.md). The connector id is `bitbucket`.

Bitbucket payloads come in three encodings. Most are the vendor's JSON with snake_case keys, sometimes with a few integers the connector adds. `fetch_reviews`, `fetch_workspace_members`, `fetch_config` and `fetch_prs_data` are objects the connector builds, also snake_case. `fetch_repos`, `fetch_repo_tree` and `fetch_file_content` are protojson, with lowerCamelCase keys.

Bitbucket timestamps are RFC 3339, such as `2026-01-05T10:00:00.123456+00:00`. Repository, user and workspace UUIDs keep their braces, such as `{ee8feb85-...}`.

| Operation | ExternalID | Payload |
|---|---|---|
| [`fetch_repos`](#fetch_repos) | `<repo uuid>` | RepositoryRecord, protojson |
| [`fetch_prs`](#fetch_prs) | `<ws>/<repo>#PR-<id>#opened`, `#snapshot`, `#merged`, `#closed` | Bitbucket pull request JSON, plus counts |
| [`fetch_commits`](#fetch_commits) | `<ws>/<repo>#<sha>` | Bitbucket commit JSON, plus counts |
| [`fetch_reviews`](#fetch_reviews) | `<ws>/<repo>#PR-<id>/approval-<account>` and others | Built by the connector |
| [`fetch_pr_comments`](#fetch_pr_comments) | `<ws>/<repo>#PR-<id>/COMMENT-<id>` | Bitbucket comment JSON |
| [`fetch_workspace_members`](#fetch_workspace_members) | `member-<account id>` | Built by the connector |
| [`fetch_prs_data`](#fetch_prs_data) | `<ws>/<repo>#PR-<id>` | Pointer to a blob |
| [`fetch_repo_tree`](#fetch_repo_tree) | `<repo uuid>:<sha>` | RepoTreeRecord, protojson |
| [`fetch_branches`](#fetch_branches) | `<repo uuid>:<branch>` | Bitbucket branch JSON |
| [`fetch_branching_model`](#fetch_branching_model) | `<repo uuid>:__branching_model__` | Bitbucket branching model JSON |
| [`fetch_config`](#fetch_config) | `<repo uuid>` | Built by the connector |
| [`fetch_file_content`](#fetch_file_content) | `<repository id>#<ide type>#<path>#<generation>` | FileContentRecord, protojson |
| [`webhook_events`](#webhook_events) | `webhook-<event key>-<request uuid>` | The webhook body |

`fetch_branching_model` is not an operation you schedule. `fetch_branches` emits it beside the branch records.

`fetch_pr_diffs`, `drain_defect_lineage` and `drain_defect_scoring` emit no records. They write to the blob store and the host's services.

Repository names in ExternalIDs (`<ws>/<repo>`) change when a repository is renamed or moved. Join on `platform_repo_id` in the metadata to follow a repository across a rename.

DataDate follows the [records rule](README.md#datadate): events are dated by the event, and current-state records (the `#snapshot`, branches, the branching model, members, description pointers, repositories, configs, trees, file content) have none.

## fetch_repos

Schema: [`schemas/bitbucket/fetch_repos.schema.json`](../../schemas/bitbucket/fetch_repos.schema.json).

- **ExternalID.** The repository UUID, with braces.
- **DataDate.** Not set. Use `fetchedAt`.
- **Payload.** A `RepositoryRecord` in protojson. Zero values are left out, and `sizeKb` is a decimal string.

| Field | Type | Meaning |
|---|---|---|
| `platformRepoId` | string | Repository UUID. It survives renames. |
| `name` | string | Repository slug |
| `fullName` | string | `workspace/slug` |
| `ownerLogin` | string | Workspace slug |
| `defaultBranch` | string | Main branch |
| `trackedBranch` | string | Branch the commit and tree operations read |
| `visibility` | enum string | `REPOSITORY_VISIBILITY_PUBLIC` or `REPOSITORY_VISIBILITY_PRIVATE` only |
| `isFork`, `parentPlatformRepoId` | bool, string | Set for a fork |
| `primaryLanguage`, `sizeKb` | string, string | Language and size on disk in KiB |
| `webUrl`, `sshUrl`, `httpCloneUrl` | string | Links |
| `vendorCreatedAt`, `vendorUpdatedAt` | RFC 3339 | Bitbucket's timestamps. There is no pushed time. |
| `stats` | object | Always `{}`. Bitbucket lists no star or fork counts. |
| `vendorBlobJson` | base64 string | JSON of Bitbucket-only fields, below |

`vendorBlobJson` decodes to a flat object. It is not wrapped in a `bitbucket` key, although the proto comment says so.

| Blob key | Type | Meaning |
|---|---|---|
| `workspace_uuid` | string | Workspace UUID |
| `mainbranch` | object | `name` and `type`, or `{}` |
| `project_key`, `project_uuid` | string | Bitbucket project |
| `scm` | string | `git` |

| Metadata key | Meaning |
|---|---|
| `full_name` | `workspace/slug` |

Example payload, from the simulator:

```json
{"defaultBranch": "main", "fullName": "demo-bb-startup/worker", "name": "worker",
 "ownerLogin": "demo-bb-startup", "platformRepoId": "{ee8feb85c3d01211}", "stats": {},
 "trackedBranch": "main", "vendorBlobJson": "eyJtYWluYnJhbmNoIjp7Im5hbWUiOiJtYWluIiwidHlwZSI6ImJyYW5jaCJ9LCJwcm9qZWN0X2tleSI6IldPUksiLCJwcm9qZWN0X3V1aWQiOiJ7M2EyMTAwYTA2YjA3NjQ1Y30iLCJzY20iOiJnaXQiLCJ3b3Jrc3BhY2VfdXVpZCI6IntmYTE2NTU4N2ZlNDM3NTY4fSJ9",
 "vendorCreatedAt": "2026-04-17T10:04:31Z", "vendorUpdatedAt": "2026-09-28T10:04:31Z",
 "visibility": "REPOSITORY_VISIBILITY_PRIVATE"}
```

The blob decodes to `{"mainbranch":{"name":"main","type":"branch"},"project_key":"WORK","project_uuid":"{3a2100a06b07645c}","scm":"git","workspace_uuid":"{fa165587fe437568}"}`.

## fetch_prs

Schema: [`schemas/bitbucket/fetch_prs.schema.json`](../../schemas/bitbucket/fetch_prs.schema.json).

Each pull request gives up to three records, as [ADR-0003](../adr/0003-github-connector-lifecycle-events.md) describes. All carry the same payload. Read `event_type` in the metadata to tell them apart.

| Record | ExternalID | DataDate |
|---|---|---|
| Opened | `ws/repo#PR-42#opened` | `created_on` |
| Snapshot | `ws/repo#PR-42#snapshot` | Not set |
| Merged, when `state` is `MERGED` | `ws/repo#PR-42#merged` | `updated_on` |
| Closed, when `state` is `DECLINED` or `SUPERSEDED` | `ws/repo#PR-42#closed` | `updated_on` |

- **Merge time.** Bitbucket's pull request has no merged-at or closed-at field, and the activity feed that dates the transition is read only by `fetch_reviews`. So the merged and closed records are dated by `updated_on`, the best time the listing has. A later edit to a merged pull request (a comment, say) moves that date, and a sink keyed on DataDate gets a second `#merged` row; keep the one with the earliest DataDate.
- **Payload.** One pull request from the list endpoint, with participants and reviewers, as Bitbucket sent it. The connector adds four top-level integers.

| Field | Type | Meaning |
|---|---|---|
| `id` | integer | Pull request id, unique in the repository |
| `title`, `description` | string | Title and body |
| `state` | string | `OPEN`, `MERGED`, `DECLINED` or `SUPERSEDED` |
| `created_on`, `updated_on` | RFC 3339 | Creation and last change |
| `author` | user | `uuid`, `account_id`, `display_name`, `nickname` |
| `source.branch.name` | string | Head branch |
| `destination.branch.name` | string | Base branch |
| `destination.repository.uuid` | string | Repository UUID |
| `reviewers` | array of users | Requested reviewers |
| `participants` | array | `user`, `role`, `approved`, `state` (`approved`, `changes_requested` or null) |
| `additions`, `deletions` | integer | Added by the connector: diffstat line totals. Absent when the diffstat call failed. |
| `files_changed` | integer | Added by the connector: distinct file paths in the diffstat. A rename counts once, under its new path; a deletion under its old path. |
| `reviews_requested` | integer | Added by the connector: `reviewers`, or participants with role `REVIEWER` |

| Metadata key | Meaning |
|---|---|
| `event_type` | `opened`, `snapshot`, `merged` or `closed` |
| `repo` | `workspace/slug` |
| `platform_repo_id` | Repository UUID |
| `pr_author_id` | Author account id |
| `head_branch`, `destination_branch` | Branch names |
| `files_changed` | JSON array of distinct file paths, as a string |
| `files_changed_count` | Its length |
| `files_line_stats` | JSON array of `{path, additions, deletions}`, one entry per path. Absent when the diffstat was incomplete. |
| `reviews_requested` | Same as the payload field |
| `first_commit_at` | Earliest commit date, RFC 3339 |
| `pr_commit_shas` | Comma-separated commit SHAs, twelve characters each |
| `is_cross_repository`, `head_repo_owner` | Set for a pull request from a fork |

Example payload, from the simulator (trimmed):

```json
{"id": 300, "title": "worker: docs #300", "state": "MERGED",
 "created_on": "2026-10-04T16:48:00Z", "updated_on": "2026-10-04T18:48:00Z",
 "author": {"account_id": "bob-acct", "display_name": "Bob Demo", "nickname": "bob", "uuid": "{92b0e37921031240}"},
 "source": {"branch": {"name": "feature/worker-300"}},
 "destination": {"branch": {"name": "main"}, "repository": {"full_name": "demo-bb-startup/worker", "uuid": "{ee8feb85c3d01211}"}},
 "additions": 110, "deletions": 26, "files_changed": 2, "reviews_requested": 2}
```

The simulator's pull requests also carry `activity`, `comments`, `commits`, `diff`, `diffstat` and `repo`. Bitbucket's list endpoint does not send them, so do not rely on them.

## fetch_commits

Schema: [`schemas/bitbucket/fetch_commits.schema.json`](../../schemas/bitbucket/fetch_commits.schema.json).

- **ExternalID.** `workspace/slug#` and the full commit SHA.
- **DataDate.** The commit's `date`.
- **Branches.** A commit on several tracked branches is emitted once per branch in a run, with the same ExternalID. `refs` in the metadata lists the branches seen so far.
- **Payload.** One commit from the commits listing, as Bitbucket sent it. The connector adds three integers from the commit's diffstat.

| Field | Type | Meaning |
|---|---|---|
| `hash` | string | Commit SHA |
| `date` | RFC 3339 | Commit date |
| `message` | string | Commit message |
| `author.raw` | string | `Name <email>` as git recorded it |
| `author.user` | user or absent | The Bitbucket user, when the email maps to one |
| `parents` | array | Parent `hash` values |
| `additions`, `deletions`, `files_changed` | integer | Added by the connector; `files_changed` counts distinct paths. Absent when enrichment is off or failed. |

| Metadata key | Meaning |
|---|---|
| `repo` | `workspace/slug` |
| `refs` | Comma-separated branch names |
| `additions`, `deletions`, `files_changed` | Same as the payload fields, when present |

Example payload, from the simulator (trimmed):

```json
{"hash": "00000000000000000000000044990aa12161fd67", "date": "2026-10-04T14:42:00Z",
 "message": "perf: commit 2 for #300",
 "author": {"raw": "Bob Demo <bob@demo-bb-startup.example.com>", "user": {"account_id": "bob-acct", "display_name": "Bob Demo"}},
 "additions": 54, "deletions": 34, "files_changed": 1}
```

## fetch_reviews

Schema: [`schemas/bitbucket/fetch_reviews.schema.json`](../../schemas/bitbucket/fetch_reviews.schema.json).

Bitbucket has no review objects. The connector builds one record per approval, per activity comment and per participant who requested changes.

| Kind | ExternalID | DataDate |
|---|---|---|
| Approval | `ws/repo#PR-42/approval-<account id>` | Approval date |
| Activity comment | `ws/repo#PR-42/comment-<comment id>` | Comment date |
| Changes requested | `ws/repo#PR-42/changes-requested-<account id>` | The pull request's `created_on` |

- **Changes requested.** Bitbucket shows it only as a participant's current state, with no date. The record is dated at the pull request's creation, a fixed time, so it stays one row on every re-fetch.
- **Reviewer identity.** When the reviewer of an approval or a changes-requested record has no account id, the user UUID takes its place in the ExternalID. A reviewer with neither is skipped.
- **Undated activity.** An approval or activity comment without a date is skipped, as `fetch_pr_comments` skips an undated comment.

| Field | Type | Meaning |
|---|---|---|
| `user.account_id`, `user.display_name`, `user.nickname` | string | The reviewer. Empty strings when Bitbucket sent no user. |
| `state` | string | `APPROVED`, `COMMENTED` or `CHANGES_REQUESTED` |
| `pr_id` | string | Pull request id, as a decimal string |
| `created_on` | RFC 3339 | Same as DataDate, in UTC to the second |

| Metadata key | Meaning |
|---|---|
| `review_type` | `approval`, `comment` or `changes_requested` |
| `repo`, `platform_repo_id` | Repository |
| `pr_id`, `pr_key` | `42` and `ws/repo#PR-42` |
| `pr_author_id`, `pr_author_login` | Author account id and display name |
| `pr_created_at` | Pull request creation, RFC 3339 |
| `reviewer_is_bot` | Always `false` today |

Example payload, from the simulator:

```json
{"created_on": "2026-10-04T18:48:00Z", "pr_id": "300", "state": "APPROVED",
 "user": {"account_id": "eve-acct", "display_name": "Eve Demo", "nickname": "eve"}}
```

## fetch_pr_comments

Schema: [`schemas/bitbucket/fetch_pr_comments.schema.json`](../../schemas/bitbucket/fetch_pr_comments.schema.json).

- **ExternalID.** `workspace/slug#PR-<id>/COMMENT-<comment id>`. Note the upper case, unlike the `comment-` records of `fetch_reviews`.
- **DataDate.** The comment's `created_on`. A comment without one is not emitted.
- **Payload.** One pull request comment, as Bitbucket sent it.

| Field | Type | Meaning |
|---|---|---|
| `id` | integer | Comment id |
| `created_on`, `updated_on` | RFC 3339 | Creation and last edit |
| `user` | user | The commenter |
| `content.raw` | string | Comment text, in Markdown |
| `inline` | object or absent | `path`, `from`, `to` for a comment on a line |
| `parent.id` | integer or absent | The comment this one replies to |
| `deleted` | boolean | True for a deleted comment |

| Metadata key | Meaning |
|---|---|
| `repo`, `platform_repo_id` | Repository |
| `pr_id` | Pull request id |
| `pr_author_id`, `pr_author_account_id` | Author account id, under two names |
| `pr_author_login`, `pr_author_display_name` | Author display name, under two names |
| `pr_created_at` | Pull request creation, RFC 3339 |
| `reviewer_is_bot` | Always `false` today |

Example payload, from the simulator:

```json
{"id": 4279, "created_on": "2026-10-04T18:48:00Z",
 "user": {"account_id": "alice-acct", "display_name": "Alice Demo", "nickname": "alice", "uuid": "{e969d0f6c0201b61}"}}
```

## fetch_workspace_members

Schema: [`schemas/bitbucket/fetch_workspace_members.schema.json`](../../schemas/bitbucket/fetch_workspace_members.schema.json).

- **ExternalID.** `member-` and the Atlassian account id.
- **DataDate.** Not set: a member is current state. Use `fetchedAt`.
- **Payload.** A flat object the connector builds. A member without an account id is skipped.

| Field | Type | Meaning |
|---|---|---|
| `account_id` | string | Atlassian account id |
| `display_name`, `nickname` | string | Left out when empty |
| `hint_source` | string | Always `workspace_members_api` |
| `groups` | array of strings | Workspace group names. Left out when the groups call failed. |

| Metadata key | Meaning |
|---|---|
| `workspace` | Workspace slug |

Example payload, from the simulator:

```json
{"account_id": "eve-acct", "display_name": "Eve Demo", "hint_source": "workspace_members_api", "nickname": "eve"}
```

## fetch_prs_data

Schema: [`schemas/bitbucket/fetch_prs_data.schema.json`](../../schemas/bitbucket/fetch_prs_data.schema.json).

- **ExternalID.** `workspace/slug#PR-<id>`, with no event suffix.
- **DataDate.** Not set: the pointer is the description's current state. Use `fetchedAt`.
- **Payload.** A pointer. The description text is in the blob store under `s3_key`. A pull request with an empty description gives no record.

| Field | Type | Meaning |
|---|---|---|
| `s3_key` | string | `descriptions/<organizationId>/bitbucket/pull_requests/<repo uuid>/<id>` |
| `pr_number` | integer | Pull request id |
| `platform_repo_id` | string | Repository UUID |
| `repo_full_name` | string | `workspace/slug` |

| Metadata key | Meaning |
|---|---|
| `repo`, `workspace`, `platform_repo_id` | Repository |
| `description_sha` | `sha256:` and the hex digest of the stored text |

Example payload, from the simulator:

```json
{"platform_repo_id": "{ee8feb85c3d01211}", "pr_number": 300, "repo_full_name": "demo-bb-startup/worker",
 "s3_key": "descriptions/00000000-0000-4000-8000-00000000d3e0/bitbucket/pull_requests/{ee8feb85c3d01211}/300"}
```

## fetch_repo_tree

Schema: [`schemas/bitbucket/fetch_repo_tree.schema.json`](../../schemas/bitbucket/fetch_repo_tree.schema.json).

- **ExternalID.** The repository UUID, `:` and the head commit SHA. Without a known UUID it is the full name instead.
- **DataDate.** Not set. Use `fetchedAt`.
- **No change.** When the head SHA matches the last run, the record has `noChange` true and nothing is uploaded. It has the same ExternalID as the record that uploaded the tree, and the same `treeS3Key`, so a sink that overwrites on ExternalID keeps the key.
- **Payload.** A `RepoTreeRecord` in protojson. The tree itself is JSON in the blob store under `treeS3Key`.

| Field | Type | Meaning |
|---|---|---|
| `platformRepoId` | string | Repository UUID |
| `treeSha` | string | Head commit of `treeBranch` |
| `treeBranch` | string | Branch the tree was read from |
| `treeS3Key` | string | `repo-trees/<organizationId>/bitbucket/<escaped uuid>/<sha>.json`. Always set. |
| `noChange` | boolean | True when nothing was uploaded this run; `treeS3Key` names the earlier upload |
| `fetchedAt` | RFC 3339 | Capture time |

Bitbucket never truncates a tree, so `treeTruncated` is always absent.

| Metadata key | Meaning |
|---|---|
| `repo_full_name` | `workspace/slug` |

Example payload, from the simulator:

```json
{"fetchedAt": "2026-10-05T10:05:34.349369Z", "platformRepoId": "{ee8feb85c3d01211}", "treeBranch": "main",
 "treeS3Key": "repo-trees/00000000-0000-4000-8000-00000000d3e0/bitbucket/%7Bee8feb85c3d01211%7D/000000000000000000000000b9afbc382f6c3236.json",
 "treeSha": "000000000000000000000000b9afbc382f6c3236"}
```

## fetch_branches

Schema: [`schemas/bitbucket/fetch_branches.schema.json`](../../schemas/bitbucket/fetch_branches.schema.json).

- **ExternalID.** The repository UUID, `:` and the branch name. A renamed branch gets a new ExternalID.
- **DataDate.** Not set: a branch is current state. Use `fetchedAt`.
- **Completeness.** Every run lists every branch. A branch missing from a complete listing was deleted. Compare the count with `branch_total` before you conclude that.
- **Payload.** One branch from `refs/branches`, as Bitbucket sent it.

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Branch name |
| `target.hash` | string | Head commit SHA |
| `target.date` | RFC 3339 or absent | Head commit date |
| `merge_strategies`, `default_merge_strategy` | array, string | Allowed merge strategies |

| Metadata key | Meaning |
|---|---|
| `repo_full_name`, `platform_repo_id` | Repository |
| `branch_name`, `head_sha` | Same as the payload |
| `is_default` | `true` for the main branch |
| `branch_total` | Branches in this repository's listing this run |
| `vendor_updated_at` | Head commit date, RFC 3339, when known |

Example payload, from the simulator:

```json
{"name": "main", "target": {"hash": "000000000000000000000000b9afbc382f6c3236"}}
```

## fetch_branching_model

Schema: [`schemas/bitbucket/fetch_branching_model.schema.json`](../../schemas/bitbucket/fetch_branching_model.schema.json).

`fetch_branches` emits this record once per repository that has a branching model. A repository without one, or a failed call, gives no record.

- **ExternalID.** The repository UUID and `:__branching_model__`.
- **DataDate.** Not set. Use `fetchedAt`.
- **Payload.** `GET .../branching-model`, as Bitbucket sent it.

| Field | Type | Meaning |
|---|---|---|
| `development.name` | string | Declared development branch |
| `production.name` | string | Declared production branch, when enabled |
| `branch_types` | array | `kind` (`feature`, `bugfix`, `release`, `hotfix`) and `prefix` |

| Metadata key | Meaning |
|---|---|
| `platform_repo_id` | Repository UUID |
| `development`, `production` | Branch names, empty when unset |

Example payload, from a fixture (the simulator answers 404 for this call):

```json
{"type": "branching_model", "development": {"name": "develop", "use_mainbranch": false},
 "production": {"name": "main", "use_mainbranch": true},
 "branch_types": [{"kind": "feature", "prefix": "feature/"}, {"kind": "release", "prefix": "release/"}]}
```

## fetch_config

Schema: [`schemas/bitbucket/fetch_config.schema.json`](../../schemas/bitbucket/fetch_config.schema.json).

- **ExternalID.** The repository UUID. It equals the `fetch_repos` ExternalID; the operationId keeps them apart.
- **DataDate.** Not set. Use `fetchedAt`.
- **Payload.** An object the connector builds for each discovered repository.

| Field | Type | Meaning |
|---|---|---|
| `repo_settings.default_branch` | string | Main branch |
| `repo_settings.default_merge_strategy` | string | Absent when the branch call failed |
| `repo_settings.merge_strategies` | array or null | Absent when the branch call failed |
| `branch_restrictions` | array | Bitbucket branch-restriction objects, verbatim. Absent without `repository:admin`. |

| Metadata key | Meaning |
|---|---|
| `scope` | Always `REPO` |
| `full_name` | `workspace/slug` |

Example payload, from the simulator:

```json
{"repo_settings": {"default_branch": "main", "default_merge_strategy": "", "merge_strategies": null}}
```

## fetch_file_content

Schema: [`schemas/bitbucket/fetch_file_content.schema.json`](../../schemas/bitbucket/fetch_file_content.schema.json).

A host-only operation: it reads its work list from the host's work queue. Each record is one AI-tool artifact, such as a Claude Code skill.

- **ExternalID.** Repository id, ide type as a number, source path and generation, joined by `#`, such as `repo-1#1#.claude/skills/deploy/SKILL.md#42`. The payload gives the ide type by name.
- **DataDate.** Not set. Use `fetchedAt`.
- **Payload.** A `FileContentRecord` in protojson. `generation` and `originalBytes` are decimal strings. File bodies are in the blob store under each file's `s3Key`.

| Field | Type | Meaning |
|---|---|---|
| `repositoryId` | string | The host's repository id |
| `ideType` | enum string | Such as `IDE_TYPE_CLAUDE_CODE` |
| `sourcePath` | string | The artifact's primary file |
| `name`, `branch`, `ref` | string | Display name, branch and pinned commit |
| `generation` | string | Freshness token |
| `isTombstone` | boolean | The artifact was deleted. It has no files. |
| `files[]` | array | `relativePath`, `contentHash` (hex SHA-256), `s3Key`, `blobSha`, `isPrimary`, `truncated`, `originalBytes` |
| `removedPaths` | array of strings | Files removed from the artifact |

| Metadata key | Meaning |
|---|---|
| `repo_full_name` | `workspace/slug` |

Example payload, from a fixture:

```json
{"repositoryId": "repo-1", "ideType": "IDE_TYPE_CLAUDE_CODE", "sourcePath": ".claude/skills/deploy/SKILL.md",
 "name": "deploy", "branch": "main", "ref": "commit-sha-abc", "generation": "42",
 "platformRepoId": "{uuid-1}", "repoFullName": "acme/widgets",
 "files": [{"relativePath": "SKILL.md", "contentHash": "<hex sha-256>", "s3Key": "harness-staging/...", "blobSha": "blob-1", "isPrimary": true, "originalBytes": "29"}]}
```

## webhook_events

Schema: [`schemas/bitbucket/webhook_events.schema.json`](../../schemas/bitbucket/webhook_events.schema.json).

A host calls this operation with a webhook it received. `sync` cannot run it.

- **ExternalID.** `webhook-`, the `X-Event-Key` and the `X-Request-UUID`. The request UUID names one delivery, and a redelivery of it repeats it, so a retried delivery deduplicates. A delivery without `X-Request-UUID` is rejected.
- **DataDate.** Not set. Use `fetchedAt`.
- **Payload.** The request body, byte for byte, after the signature check. It is always a JSON object: a body that is not is rejected with an error and never emitted. Its shape depends on the event; see Bitbucket's event payload reference.

The connector emits these event keys: `repo:push`, `pullrequest:created`, `pullrequest:updated`, `pullrequest:approved`, `pullrequest:unapproved`, `pullrequest:fulfilled`, `pullrequest:rejected`, `pullrequest:comment_created`, `issue:created`, `issue:updated` and `issue:comment_created`.

| Metadata key | Meaning |
|---|---|
| `event_type` | The `X-Event-Key`, such as `pullrequest:created` |

Example payload, from a fixture (trimmed):

```json
{"actor": {"type": "user", "uuid": "{u}"}, "repository": {"full_name": "ws/repo", "uuid": "{r}"},
 "pullrequest": {"id": 7, "state": "OPEN"}}
```
