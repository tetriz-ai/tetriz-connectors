# GitLab records

[Back to records](README.md). The connector id is `gitlab`. It reads the GitLab REST API v4 for one group, set by `settings.group_id`.

GitLab calls a pull request a merge request (MR). The operations keep the cross-vendor names: `fetch_prs`, `fetch_reviews`, `fetch_pr_comments` and `fetch_prs_data` read merge requests. A merge request has an instance-wide `id` and a project-scoped `iid`, shown as `!iid`. ExternalIDs and metadata use the `iid`.

Most payloads are a GitLab REST object passed through unchanged. Their keys are snake_case, and they carry every key GitLab sends, not only the ones listed here. `fetch_repos`, `fetch_repo_tree` and `fetch_file_content` payloads are protojson, with lowerCamelCase keys. The rest are small objects the connector builds.

Every metadata value is a string. A boolean is `"true"` or `"false"`, and a list is JSON text inside the string.

`webhook_events` emits every record under its own id, never under a polling operation's, so a hook body never overwrites the richer polled record. Its records reuse the polled ExternalID format; see [webhook_events](#webhook_events).

A record that describes current state (a `#snapshot`, a project, a member, a branch, a config, a tree, a description pointer) has no DataDate. An event record is dated by its event.

| Operation | ExternalID | Payload |
|---|---|---|
| `fetch_projects` | `project-<path_with_namespace>` | REST project |
| `fetch_repos` | `<project id>` | RepositoryRecord, protojson |
| `fetch_prs` | `mr-<project id>!<iid>#<event>` | REST merge request |
| `fetch_commits` | `<path_with_namespace>#<sha>` | REST commit |
| `fetch_issues` | `issue-<project id>!<iid>#<event>` | REST issue |
| `fetch_reviews` | `note-<project id>-<note id>` or `approval-<project id>!<iid>-<user id>` | REST note, or a built approval |
| `fetch_pr_comments` | `<path_with_namespace>#MR-<iid>/COMMENT-<note id>` | REST note |
| `fetch_org_members` | `member-<user id>` | Built member object |
| `fetch_prs_data` | `<path_with_namespace>#PR-<iid>` | Built pointer to a blob |
| `fetch_repo_tree` | `<project id>:<tree sha>` | RepoTreeRecord, protojson |
| `fetch_branches` | `<project id>:<branch name>` | REST branch |
| `fetch_config` | `group-<group id>` or `<project id>` | Built settings object |
| `fetch_file_content` | `<repository id>#<ide type number>#<source path>#<generation>` | FileContentRecord, protojson |
| `webhook_events` | As the polled record it mirrors, or `<path_with_namespace>@<ref>` for a push without commits | Hook body, raw or reduced |

`fetch_mr_diffs`, `drain_defect_lineage` and `drain_defect_scoring` emit no records. They write to the blob store and to host services.

## fetch_projects

One record per project in the group, every run.

`fetch_projects` and `fetch_repos` read the same listing. `fetch_projects` passes the GitLab object through and keys it by path. `fetch_repos` maps it to the cross-vendor RepositoryRecord and keys it by the numeric id, which survives a rename. Prefer `fetch_repos` for a repository table.

| Property | Value |
|---|---|
| ExternalID | `project-` plus `path_with_namespace`, such as `project-acme/widgets`. A rename forks the key |
| Encoding | The REST project object, unchanged |
| DataDate | Not set: current state |

| Field | Type | Meaning |
|---|---|---|
| `id` | integer | Project id |
| `path_with_namespace` | string | `group/project` |
| `name`, `path` | string | Display name and short path |
| `default_branch` | string or null | Null for an empty repository |
| `visibility` | string | `public`, `internal` or `private` |
| `created_at` | date-time | When the project was created |
| `last_activity_at` | date-time | Last activity of any kind |
| `namespace` | object | `id`, `path` and `kind` (`group` or `user`) of the owner |

| Metadata key | Meaning |
|---|---|
| `group` | `settings.group_name`, `""` when unset |

Example (trimmed, from the simulator):

```json
{"id": 9100203, "name": "search", "path": "search", "path_with_namespace": "demo-gl-scaleup/search",
 "default_branch": "main", "visibility": "private", "created_at": "2026-05-08T10:04:29Z",
 "last_activity_at": "2026-09-28T10:04:29Z", "namespace": {"id": 91002, "kind": "group", "path": "demo-gl-scaleup"}}
```

Schema: [schemas/gitlab/fetch_projects.schema.json](../../schemas/gitlab/fetch_projects.schema.json)

## fetch_repos

One record per project, as a `RepositoryRecord` ([proto](../../proto/tetriz/connectors/connector/v1/repo_ops.proto)).

| Property | Value |
|---|---|
| ExternalID | `platformRepoId`, the numeric project id |
| Encoding | protojson: lowerCamelCase keys, zero values left out, enums by name |
| DataDate | Not set |

| Field | Type | Meaning |
|---|---|---|
| `platformRepoId` | string | Project id, digits |
| `name` | string | Short path, not the display name |
| `fullName` | string | `group/project` |
| `ownerLogin` | string | Namespace path. Absent when GitLab sent no namespace |
| `visibility` | string | `REPOSITORY_VISIBILITY_PUBLIC`, `_PRIVATE` or `_INTERNAL` |
| `isFork`, `parentPlatformRepoId` | boolean, string | Set from `forked_from_project` |
| `vendorCreatedAt` | date-time | `created_at` |
| `vendorPushedAt` | date-time | `last_activity_at`, which moves on any activity |
| `stats` | object | `stars`, `forks`, `openIssues`. Always present, often `{}` |
| `trackedBranch` | string | Per-project override, else `settings.tracked_branch`, else the default branch |
| `vendorBlobJson` | string | Base64 of a JSON object, below |

GitLab never sets `sizeKb`, `disabled`, `isTemplate`, `licenseKey`, `hasPages`, `hasProjects` or `vendorUpdatedAt`.

`vendorBlobJson` decodes to a flat object. It is not wrapped in a `gitlab` key, although the proto comment says so.

| Blob key | Type | Meaning |
|---|---|---|
| `namespace_id` | integer | Namespace id, 0 when absent |
| `namespace_path`, `namespace_kind` | string | Namespace path and kind |
| `import_status`, `empty_repo` | string, boolean | As GitLab reports them |

| Metadata key | Meaning |
|---|---|
| `full_name` | `group/project` |

Example (from the simulator):

```json
{"platformRepoId": "9100203", "name": "search", "fullName": "demo-gl-scaleup/search", "ownerLogin": "demo-gl-scaleup",
 "defaultBranch": "main", "visibility": "REPOSITORY_VISIBILITY_PRIVATE", "stats": {}, "trackedBranch": "main",
 "vendorCreatedAt": "2026-05-08T10:04:29Z", "vendorPushedAt": "2026-09-28T10:04:29Z",
 "vendorBlobJson": "eyJlbXB0eV9yZXBvIjpmYWxzZSwiaW1wb3J0X3N0YXR1cyI6IiIsIm5hbWVzcGFjZV9pZCI6OTEwMDIsIm5hbWVzcGFjZV9raW5kIjoiZ3JvdXAiLCJuYW1lc3BhY2VfcGF0aCI6ImRlbW8tZ2wtc2NhbGV1cCJ9"}
```

The blob decodes to `{"empty_repo":false,"import_status":"","namespace_id":91002,"namespace_kind":"group","namespace_path":"demo-gl-scaleup"}`.

Schema: [schemas/gitlab/fetch_repos.schema.json](../../schemas/gitlab/fetch_repos.schema.json)

## fetch_prs

Up to four records per merge request, one per lifecycle event, as [ADR-0003](../adr/0003-github-connector-lifecycle-events.md) describes. Every kind carries the same payload. Dispatch on the `event_type` metadata key, not on the payload's `state`.

| Event | ExternalID suffix | DataDate | Emitted when |
|---|---|---|---|
| `opened` | `#opened` | `created_at` | Always |
| `snapshot` | `#snapshot` | Not set: current state | Always |
| `merged` | `#merged` | `merged_at` | `merged_at` is set |
| `closed` | `#closed` | `closed_at`, else `created_at` | `state` is `closed` with no merge |

| Property | Value |
|---|---|
| ExternalID | `mr-<project id>!<iid>` plus the suffix, such as `mr-9100203!1#merged` |
| Encoding | The REST merge request object (`GET /projects/:id/merge_requests`), unchanged |
| DataDate | Per event, above |

| Field | Type | Meaning |
|---|---|---|
| `id`, `iid`, `project_id` | integer | Instance id, project-scoped number, project |
| `title`, `description` | string | Description can be null |
| `state` | string | `opened`, `closed`, `locked` or `merged` |
| `created_at`, `updated_at` | date-time | Set once, and last change |
| `merged_at`, `closed_at` | date-time or null | Lifecycle timestamps |
| `source_branch`, `target_branch` | string | Head and base branch |
| `source_project_id`, `target_project_id` | integer | Differ for a merge request from a fork |
| `author`, `reviewers` | object, array | Users with `id`, `username`, `bot` |
| `user_notes_count` | integer | Comment count |

| Metadata key | Meaning |
|---|---|
| `event_type` | `opened`, `snapshot`, `merged` or `closed` |
| `repo` | `group/project` |
| `group` | `settings.group_name` |
| `pr_author_id` | Author user id, `""` when unknown |
| `head_branch`, `target_branch` | Source and target branch |
| `is_cross_repository` | Source and target projects differ |
| `reviews_requested` | Count of assigned reviewers |
| `files_changed` | JSON array of changed paths, `"[]"` when the diff read failed |
| `files_line_stats` | JSON array of `{path, additions, deletions}`. Absent when the diff read failed or was truncated |
| `first_commit_at` | Earliest commit `authored_date`, RFC 3339. Absent when unknown |
| `pr_commit_shas` | Comma-separated commit SHAs, 12 characters each. Absent when the read failed |
| `closes_issues` | Raw JSON array of issues the merge request closes. Only with `settings.fetch_native_issue_links` |

Example (trimmed, from the simulator):

```json
{"id": 91002030001, "iid": 1, "project_id": 9100203, "title": "search: perf !1", "state": "merged",
 "created_at": "2026-07-07T00:00:00Z", "updated_at": "2026-07-07T02:00:00Z", "merged_at": "2026-07-07T23:00:00Z", "closed_at": null,
 "source_branch": "release/search-1", "target_branch": "main", "additions": 108, "deletions": 39,
 "author": {"id": 91002006, "username": "frank", "bot": false}, "reviewers": [{"id": 91002002, "username": "bob"}], "user_notes_count": 0}
```

Schema: [schemas/gitlab/fetch_prs.schema.json](../../schemas/gitlab/fetch_prs.schema.json)

## fetch_commits

One record per commit on each tracked ref, first-parent history only.

| Property | Value |
|---|---|
| ExternalID | `<path_with_namespace>#<sha>`. A project rename forks the key |
| Encoding | The REST commit detail, unchanged. When the detail read fails with an error a retry cannot fix, the list entry, which has no `stats` |
| DataDate | `committed_date` |

A commit reached from several refs is emitted once per ref. The last emission carries every ref in `refs`.

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Full SHA |
| `message`, `title` | string | Commit message and its first line |
| `author_name`, `author_email` | string | Author identity |
| `authored_date`, `committed_date` | date-time | Author and committer times |
| `parent_ids` | array | Parent SHAs |
| `stats` | object | `additions`, `deletions`, `total` |

| Metadata key | Meaning |
|---|---|
| `repo` | `group/project` |
| `refs` | Comma-separated refs the commit was reached from, sorted |

Example (from the simulator):

```json
{"id": "0000000000000000000000002c30c4b6fdc49ca0", "message": "refactor: commit 2 for !120", "project_id": 9100203,
 "author_name": "Alice Demo", "author_email": "alice@demo-gl-scaleup.example.com",
 "authored_date": "2026-10-04T03:07:00Z", "committed_date": "2026-10-04T03:07:00Z",
 "stats": {"additions": 22, "deletions": 8, "total": 30}}
```

Schema: [schemas/gitlab/fetch_commits.schema.json](../../schemas/gitlab/fetch_commits.schema.json)

## fetch_issues

Up to three records per issue in the group: `#opened` (DataDate `created_at`), `#snapshot` (no DataDate: current state) and `#closed` (`closed_at`, when the issue is closed). Every kind carries the same payload.

| Property | Value |
|---|---|
| ExternalID | `issue-<project id>!<iid>` plus the suffix |
| Encoding | The REST issue object (`GET /groups/:id/issues`), unchanged |
| DataDate | Per event, above |

| Field | Type | Meaning |
|---|---|---|
| `id`, `iid`, `project_id` | integer | Instance id, project-scoped number, project |
| `title`, `state` | string | `state` is `opened` or `closed` |
| `issue_type` | string | `issue`, `incident`, `test_case` or `task` |
| `created_at`, `updated_at` | date-time | Set once, and last change |
| `closed_at` | date-time or null | When closed |
| `labels` | array | Label names |
| `author`, `assignees` | object, array | Users |
| `references.full` | string | `group/project#iid` |

| Metadata key | Meaning |
|---|---|
| `event_type` | `opened`, `snapshot` or `closed` |
| `group` | `settings.group_name` |

Example (trimmed, from the simulator):

```json
{"id": 91002010120, "iid": 120, "project_id": 9100201, "title": "checkout issue 120", "state": "opened", "issue_type": "issue",
 "created_at": "2026-10-04T06:00:00Z", "updated_at": "2026-10-04T08:00:00Z", "closed_at": null, "labels": [], "assignees": [],
 "author": {"id": 91002001, "username": "alice", "bot": false}, "references": {"full": "demo-gl-scaleup/checkout#120"}}
```

Schema: [schemas/gitlab/fetch_issues.schema.json](../../schemas/gitlab/fetch_issues.schema.json)

## fetch_reviews

Review signals on merge requests updated since the cursor: each non-system note, and each approval. The two have different payloads.

| Kind | ExternalID | Payload |
|---|---|---|
| Note | `note-<project id>-<note id>` | REST note, unchanged |
| Approval | `approval-<project id>!<iid>-<user id>` | `{user, state, created_at}`, built |

| Property | Value |
|---|---|
| Encoding | One JSON object, snake_case |
| DataDate | The note's or approval's `created_at` |

Approval fields:

| Field | Type | Meaning |
|---|---|---|
| `user` | object | `id`, `username`, `bot` of the approver |
| `state` | string | Always `APPROVED` |
| `created_at` | date-time | RFC 3339 in UTC, to the second |

Note fields are those of [fetch_pr_comments](#fetch_pr_comments).

| Metadata key | Meaning |
|---|---|
| `state` | `COMMENTED`. Notes only |
| `review_type` | `approval`. Approvals only |
| `repo`, `platform_repo_id` | Project path and id |
| `pr_key` | `mr-<project id>!<iid>`, the fetch_prs ExternalID without its suffix |
| `pr_number` | Merge request iid |
| `pr_author_login`, `pr_author_id` | Merge request author |
| `pr_created_at` | Merge request `created_at`, RFC 3339 |
| `reviewer_is_bot` | The note author or approver is a bot |

Example approval (from the simulator):

```json
{"created_at": "2026-07-07T02:00:00Z", "state": "APPROVED", "user": {"bot": false, "id": 91002001, "username": "alice"}}
```

Schema: [schemas/gitlab/fetch_reviews.schema.json](../../schemas/gitlab/fetch_reviews.schema.json)

## fetch_pr_comments

One record per non-system note on a merge request, created since the cursor. The same notes appear in `fetch_reviews` under another ExternalID.

| Property | Value |
|---|---|
| ExternalID | `<path_with_namespace>#MR-<iid>/COMMENT-<note id>`. A project rename forks the key |
| Encoding | The REST note object, unchanged |
| DataDate | `created_at` |

| Field | Type | Meaning |
|---|---|---|
| `id` | integer | Note id |
| `body` | string | Markdown text |
| `author` | object | `id`, `username`, `bot` |
| `created_at`, `updated_at` | date-time | Written and last edited |
| `system` | boolean | Always `false`: system notes are dropped |

| Metadata key | Meaning |
|---|---|
| `repo`, `platform_repo_id` | Project path and id |
| `mr_iid` | Merge request iid |
| `mr_author_username`, `mr_author_id` | Merge request author |
| `pr_author_login`, `pr_author_id` | The same values, under cross-vendor names |
| `pr_created_at` | Merge request `created_at` |
| `reviewer_is_bot` | The note author is a bot |

Example (from the simulator):

```json
{"id": 3260, "body": "lgtm", "project_id": 9100203, "system": false,
 "author": {"bot": false, "email": "eve@demo-gl-scaleup.example.com", "id": 91002005, "username": "eve"},
 "created_at": "2026-07-07T20:00:00Z", "updated_at": "2026-07-07T20:00:00Z"}
```

Schema: [schemas/gitlab/fetch_pr_comments.schema.json](../../schemas/gitlab/fetch_pr_comments.schema.json)

## fetch_org_members

One record per member of the group, every run.

| Property | Value |
|---|---|
| ExternalID | `member-<user id>` |
| Encoding | The connector's `GroupMemberRecord`. Empty optional keys are left out |
| DataDate | Not set: current state, so every run overwrites one row per member |

| Field | Type | Meaning |
|---|---|---|
| `numeric_user_id` | integer | GitLab user id |
| `login` | string | Username |
| `email` | string | Member email, else the profile's public email. Absent when none is visible |
| `name`, `avatar_url` | string | Profile name and picture |
| `hint_source` | string | Always `group_members_api` |
| `groups` | array | Subgroup names, or the root group name when there are no subgroups |

| Metadata key | Meaning |
|---|---|
| `hint_source` | Same as the payload |

Example (from the simulator):

```json
{"numeric_user_id": 91002006, "login": "frank", "email": "frank@demo-gl-scaleup.example.com", "name": "Frank Demo", "hint_source": "group_members_api"}
```

Schema: [schemas/gitlab/fetch_org_members.schema.json](../../schemas/gitlab/fetch_org_members.schema.json)

## fetch_prs_data

One record per merge request with a non-empty description. The description text is in the blob store, not in the record.

| Property | Value |
|---|---|
| ExternalID | `<path_with_namespace>#PR-<iid>`. Not the fetch_prs key |
| Encoding | One JSON object built by the connector |
| DataDate | Not set: a pointer to the current description |

| Field | Type | Meaning |
|---|---|---|
| `s3_key` | string | `descriptions/<org>/gitlab/pull_requests/<project id>/<iid>` |
| `pr_number` | integer | Merge request iid |
| `platform_repo_id` | string | Project id |
| `repo_full_name` | string | `group/project` |

| Metadata key | Meaning |
|---|---|
| `repo`, `platform_repo_id`, `group` | Project path, id and group name |
| `description_sha` | `sha256:` plus the hex digest of the description |

Example (from the simulator):

```json
{"platform_repo_id": "9100203", "pr_number": 1, "repo_full_name": "demo-gl-scaleup/search",
 "s3_key": "descriptions/00000000-0000-4000-8000-00000000d3e0/gitlab/pull_requests/9100203/1"}
```

Schema: [schemas/gitlab/fetch_prs_data.schema.json](../../schemas/gitlab/fetch_prs_data.schema.json)

## fetch_repo_tree

One record per project, for the head of its default branch, as a `RepoTreeRecord`. The tree is a JSON blob under `treeS3Key`.

| Property | Value |
|---|---|
| ExternalID | `<project id>:<tree sha>` |
| Encoding | protojson |
| DataDate | Not set |

| Field | Type | Meaning |
|---|---|---|
| `platformRepoId` | string | Project id |
| `treeSha`, `treeBranch` | string | Head commit and branch |
| `treeS3Key` | string | `repo-trees/<org>/gitlab/<project id>/<sha>.json`. A `noChange` record names the tree written for the same SHA |
| `treeTruncated` | boolean | The walk stopped early |
| `noChange` | boolean | The SHA matched the cursor, and nothing was uploaded |
| `fetchedAt` | date-time | When the tree was read |

| Metadata key | Meaning |
|---|---|
| `platform_repo_id` | Project id |

Example (from the simulator):

```json
{"platformRepoId": "9100203", "treeSha": "00000000000000000000000000000000008adbab", "treeBranch": "main",
 "treeS3Key": "repo-trees/00000000-0000-4000-8000-00000000d3e0/gitlab/9100203/00000000000000000000000000000000008adbab.json",
 "fetchedAt": "2026-10-05T10:05:15.434844Z"}
```

Schema: [schemas/gitlab/fetch_repo_tree.schema.json](../../schemas/gitlab/fetch_repo_tree.schema.json)

## fetch_branches

One record per branch of every project, every run.

| Property | Value |
|---|---|
| ExternalID | `<project id>:<branch name>` |
| Encoding | The REST branch object, unchanged |
| DataDate | Not set: current state |

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Branch name |
| `commit` | object | Head commit, with `id` and `committed_date` |
| `default`, `protected`, `merged` | boolean | GitLab's branch flags |

| Metadata key | Meaning |
|---|---|
| `repo`, `platform_repo_id` | Project path and id |
| `branch_name`, `head_sha` | Branch and head commit |
| `is_default`, `protected`, `merged` | Branch flags |
| `branch_total` | Branches in the project |
| `vendor_updated_at` | Head commit `committed_date`. Absent when GitLab sent none |

Example (from the simulator):

```json
{"name": "main", "default": true, "commit": {"id": "00000000000000000000000000000000008adbab"}}
```

Schema: [schemas/gitlab/fetch_branches.schema.json](../../schemas/gitlab/fetch_branches.schema.json)

## fetch_config

Merge and review settings: one ACCOUNT record for the group, and one REPO record per project `fetch_projects` or `fetch_repos` discovered.

| Property | Value |
|---|---|
| ExternalID | `group-<group id>` for ACCOUNT, the project id for REPO. Group and project ids are separate sequences, so the prefix keeps a group from taking a project's key |
| Encoding | One JSON object built by the connector, wrapping REST bodies unchanged |
| DataDate | Not set |

| Field | Type | Meaning |
|---|---|---|
| `group_settings` | object | ACCOUNT only. The REST group object |
| `repo_settings` | object | REPO only. Six project settings, below |
| `approvals` | object | REPO only. The REST approvals body. Absent when the read failed |
| `protected_branches` | array | REPO only. REST protected branch objects. Absent when the read failed or found none |

`repo_settings` holds `default_branch`, `merge_method`, `squash_option`, `only_allow_merge_if_pipeline_succeeds`, `only_allow_merge_if_all_discussions_resolved` and `remove_source_branch_after_merge`. The fifth is GitLab's `only_allow_merge_if_all_discussions_are_resolved`, renamed. A setting GitLab did not send is `""` or `false`.

| Metadata key | Meaning |
|---|---|
| `scope` | `ACCOUNT` or `REPO` |
| `full_name` | `group/project`. REPO only |

Example REPO record (from the simulator):

```json
{"repo_settings": {"default_branch": "main", "merge_method": "", "squash_option": "",
 "only_allow_merge_if_all_discussions_resolved": false, "only_allow_merge_if_pipeline_succeeds": false,
 "remove_source_branch_after_merge": false}}
```

Schema: [schemas/gitlab/fetch_config.schema.json](../../schemas/gitlab/fetch_config.schema.json)

## fetch_file_content

One record per AI-tool artifact the host's work queue asks for, such as a skill or a rule file. It runs only with host services, so `serve` never schedules it.

| Property | Value |
|---|---|
| ExternalID | `<repositoryId>#<ideType number>#<sourcePath>#<generation>`. The IDE type is its enum number, such as `1` for `IDE_TYPE_CLAUDE_CODE` |
| Encoding | A `FileContentRecord` ([proto](../../proto/tetriz/connectors/workqueue/v1/workqueue.proto)) in protojson. `generation` and `originalBytes` are int64, so they are strings |
| DataDate | Not set |

| Field | Type | Meaning |
|---|---|---|
| `repositoryId`, `sourcePath` | string | Artifact identity |
| `ideType`, `artifactType` | string | Enum names |
| `branch`, `ref` | string | Where the files were read |
| `generation` | string | Freshness token, int64 as a string |
| `isTombstone` | boolean | The whole artifact was deleted |
| `files` | array | `relativePath`, `contentHash` (hex SHA-256), `s3Key`, `blobSha`, `isPrimary`, `truncated`, `originalBytes` |
| `removedPaths` | array | Files removed from the artifact |

| Metadata key | Meaning |
|---|---|
| `repo_full_name` | `group/project`, `""` on a tombstone |

Example (from the package fixture):

```json
{"repositoryId": "repo-1", "ideType": "IDE_TYPE_CLAUDE_CODE", "sourcePath": ".claude/skills/deploy/SKILL.md", "name": "deploy",
 "branch": "main", "ref": "commit-sha-abc", "generation": "42", "platformRepoId": "12345", "repoFullName": "acme/widgets",
 "files": [{"relativePath": "SKILL.md", "contentHash": "…", "s3Key": "harness-staging/…", "blobSha": "blob-1", "isPrimary": true, "originalBytes": "9"}]}
```

Schema: [schemas/gitlab/fetch_file_content.schema.json](../../schemas/gitlab/fetch_file_content.schema.json)

## webhook_events

What a host passes to the webhook RPCs. Every record carries operationId `webhook_events`, never a polling operation's, so it never overwrites the polled record that shares its ExternalID. Read the polled operation for the full object, and this one for what a hook said between polls. Dispatch on the `record_kind` metadata key.

| `record_kind` | Hook | ExternalID | DataDate | Payload |
|---|---|---|---|---|
| `commit` | Push Hook, one per pushed commit | `<path_with_namespace>#<sha>`, as `fetch_commits` | The commit's `timestamp` | `{"id", "timestamp"}` |
| `push` | Push or Tag Push Hook with no commits: a tag push or a ref deletion | `<path_with_namespace>@<ref>`, such as `acme/widgets@refs/tags/v1.0` | Not set: the ref's latest push | The raw hook body |
| `merge_request` | Merge Request Hook | `mr-<project id>!<iid>#<event>`, as `fetch_prs` | Per event, as `fetch_prs` | The raw hook body, the merge request under `object_attributes` |
| `issue` | Issue Hook | `issue-<project id>!<iid>#<event>`, as `fetch_issues` | Per event, as `fetch_issues` | The hook reshaped into REST keys |
| `note` | Note Hook, non-system notes only | `note-<project id>-<note id>`, as `fetch_reviews` | The note's `created_at` | `{"id", "author": {"id", "username"}, "created_at"}` |

A Merge Request Hook emits `#opened` when its `action` is `open`, `#snapshot` always, `#merged` when `merged_at` is set, and `#closed` when closed without a merge. A reopen emits no `#opened`. An Issue Hook emits `#snapshot`, and `#closed` when closed; the poll emits the issue's `#opened`.

The issue reshape fills only the REST keys the hook has: `iid`, `title`, `state`, `issue_type`, `created_at`, `closed_at`, `labels`, `assignees`, `author`, `references` and `due_date`. It has no `id`, `project_id` or `updated_at`. Assignee and author `username` are `""`.

| Metadata key | Meaning |
|---|---|
| `record_kind` | `commit`, `push`, `merge_request`, `issue` or `note` |
| `event_type` | `merge_request` and `issue`: the lifecycle event. `push`: the `X-Gitlab-Event` header, such as `Tag Push Hook` |
| `repo` | `commit` only: `group/project` |
| `review_type`, `pr_key`, `mr_iid`, `pr_number`, `pr_author_login`, `pr_author_id`, `platform_repo_id`, `reviewer_is_bot` | `note` only, as `fetch_reviews` |

Schema: [schemas/gitlab/webhook_events.schema.json](../../schemas/gitlab/webhook_events.schema.json)

## How these schemas are checked

`TestSimulatedPayloadsMatchSchemas` in `schemas/simulator_test.go` runs every operation against the seeded simulator and validates each payload. `connectors/gitlab/schema_gitlab_test.go` covers what the simulator cannot run: every webhook hook, `fetch_file_content`, and fully populated `fetch_repos` and `fetch_org_members` payloads.
