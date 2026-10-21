# Jira records

Back to [Records](README.md). The connector id is `jira`.

Every Jira payload except `fetch_issues_data` is the vendor's JSON, byte for byte. Its keys are camelCase, as Jira sends them. Jira timestamps carry an offset without a colon, such as `2026-01-16T15:03:31.606+0200`, so they are not RFC 3339.

| Operation | ExternalID | Payload |
|---|---|---|
| [`fetch_projects`](#fetch_projects) | `project-<id>` | Jira project JSON |
| [`fetch_issues`](#fetch_issues) | `<issue id>#created`, `#snapshot`, `#resolved` | Jira issue JSON |
| [`fetch_issues_data`](#fetch_issues_data) | `<issue id>` | Pointer to a blob, snake_case |
| [`fetch_comments`](#fetch_comments) | `comment-<id>` | Jira comment JSON |
| [`fetch_worklogs`](#fetch_worklogs) | `worklog-<id>` | Jira worklog JSON |
| [`fetch_sprints`](#fetch_sprints) | `sprint-<id>` | Jira Agile sprint JSON |

Every record carries `cloud_id` in its metadata: the Atlassian site it came from.

ExternalIDs use Jira's numeric ids, which never change. Issue and project keys change when an issue moves or a project is renamed, so they are in the payload and metadata only. An issue or project without an id is skipped.

`dataDate` follows the [record rule](README.md#datadate): event records (`#created`, `#resolved`, comments, worklogs) are dated by the event; current-state records (projects, sprints, `#snapshot`, `fetch_issues_data`) have none.

## fetch_projects

Schema: [`schemas/jira/fetch_projects.schema.json`](../../schemas/jira/fetch_projects.schema.json).

- **ExternalID.** `project-` and the numeric project id, such as `project-10001`.
- **DataDate.** None: a project is current state.
- **Payload.** One project from `GET /rest/api/3/project/search`.

A Jira admin can change a project key. The ExternalID is the id, so a rename overwrites the same record; `project_key` in the metadata and `key` in the payload carry the new key.

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Numeric project id. It never changes. |
| `key` | string | Project key, the prefix of its issue keys |
| `name` | string | Display name |
| `projectTypeKey` | string | `software`, `business` or `service_desk` |

| Metadata key | Meaning |
|---|---|
| `project_key` | Project key at fetch time |
| `cloud_id` | Atlassian site id |

Example payload, from the simulator:

```json
{"id": "10001", "key": "WORK", "name": "Worker", "projectTypeKey": "software"}
```

## fetch_issues

Schema: [`schemas/jira/fetch_issues.schema.json`](../../schemas/jira/fetch_issues.schema.json).

Each issue gives up to three records, as [ADR-0003](../adr/0003-github-connector-lifecycle-events.md) describes. All three carry the same payload. Read `event_type` in the metadata to tell them apart.

| Record | ExternalID | DataDate |
|---|---|---|
| Created | `1100042#created` | `fields.created` |
| Snapshot | `1100042#snapshot` | None |
| Resolved, when `fields.resolutiondate` is set | `1100042#resolved` | `fields.resolutiondate` |

The ExternalID starts with the numeric issue `id`, not the key.

The event names differ from GitHub's: Jira has `created` and `resolved`, not `opened`, `merged` and `closed`.

- **Payload.** One issue from `POST /rest/api/3/search/jql`, with `fields=*all` and `expand=changelog,renderedFields`.
- **Re-reads.** Each run re-reads five minutes before its cursor, so a record arrives again with the same ExternalID.
- **Moves.** An issue moved to another project gets a new key but keeps its `id`, so its records keep their ExternalIDs and are overwritten. `issue_key` in the metadata carries the new key.
- **Reopening.** A reopened issue clears `resolutiondate`. The `#resolved` record already emitted stays in a sink; the snapshot shows the current state.

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Numeric issue id |
| `key` | string | Issue key, such as `ENG-42` |
| `fields.summary` | string | Title |
| `fields.created` | Jira timestamp | When the issue was created |
| `fields.updated` | Jira timestamp | Last change |
| `fields.resolutiondate` | Jira timestamp or null | When it was resolved |
| `fields.status.name` | string | Workflow status |
| `fields.status.statusCategory.key` | string | `new`, `indeterminate` or `done` |
| `fields.issuetype.name` | string | Bug, Story, Epic and so on |
| `fields.project` | object | `id` and `key` of the project |
| `fields.assignee`, `fields.reporter` | user or null | `accountId`, `displayName`, `emailAddress` when visible |
| `fields.priority.name` | string | Priority |
| `fields.labels` | array of strings | Labels |
| `fields.description` | ADF or null | Atlassian Document Format tree |
| `fields.customfield_<n>` | any | Site-specific custom fields, such as sprint or story points |
| `changelog.histories` | array | Field changes, each with `author`, `created` and `items` |
| `renderedFields` | object | Fields rendered to HTML |

Custom field numbers differ per site. Story points and sprint are custom fields, so look their numbers up on the site.

| Metadata key | Meaning |
|---|---|
| `event_type` | `created`, `snapshot` or `resolved` |
| `issue_key` | Issue key at fetch time |
| `project` | Project key the run read |
| `cloud_id` | Atlassian site id |

Example payload, from the simulator and trimmed:

```json
{
  "id": "1100001",
  "key": "WORK-1",
  "self": "https://demo-bb-startup.atlassian.net/rest/api/3/issue/WORK-1",
  "fields": {
    "summary": "WORK: Backfill missing statements",
    "created": "2026-07-07T00:00:00.000+0000",
    "updated": "2026-07-07T02:00:00.000+0000",
    "status": {"name": "In Progress", "statusCategory": {"key": "indeterminate"}},
    "issuetype": {"name": "Epic"},
    "project": {"id": "10001", "key": "WORK"},
    "assignee": {"accountId": "acct-carol", "displayName": "Carol Demo"},
    "customfield_10016": 2,
    "labels": ["backend", "worker"]
  }
}
```

## fetch_issues_data

Schema: [`schemas/jira/fetch_issues_data.schema.json`](../../schemas/jira/fetch_issues_data.schema.json).

The payload does not hold the description. It points at the description, which the operation writes to the blob store first.

- **ExternalID.** The numeric issue id, such as `1100042`. The key is `issue_key` in the payload.
- **DataDate.** None: the pointer is the description's current state.
- **Payload.** JSON the connector builds, with snake_case keys.
- **Skipped.** An issue with no description, or no project id, emits no record.

| Field | Type | Meaning |
|---|---|---|
| `s3_key` | string | Blob key: `descriptions/<organization>/jira/issues/<project id>/<issue key>`. A moved issue's next record points at a new blob. |
| `issue_key` | string | Issue key |
| `platform_project_id` | string | Numeric project id |

The blob holds the `fields.description` ADF JSON as Jira returned it.

| Metadata key | Meaning |
|---|---|
| `description_sha` | `sha256:` and the hex digest of the blob, to skip unchanged descriptions |
| `project` | Project key |
| `project_id` | Numeric project id |
| `cloud_id` | Atlassian site id |

Example payload, from the simulator:

```json
{
  "s3_key": "descriptions/00000000-0000-4000-8000-00000000d3e0/jira/issues/10001/WORK-1",
  "issue_key": "WORK-1",
  "platform_project_id": "10001"
}
```

## fetch_comments

Schema: [`schemas/jira/fetch_comments.schema.json`](../../schemas/jira/fetch_comments.schema.json).

`fetch_comments` has no default schedule. The simulator serves comments derived from each issue, and a fixture test in `connectors/jira/schema_jira_test.go` also checks this schema.

- **ExternalID.** `comment-` and the comment id.
- **DataDate.** `created`, or none when it is missing.
- **Payload.** One comment from `GET /rest/api/3/issue/{key}/comment`.

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Comment id |
| `author`, `updateAuthor` | user | Who wrote and who last edited it |
| `body` | ADF | The comment text |
| `created`, `updated` | Jira timestamp | When it was written and last edited |
| `jsdPublic` | boolean | Visible to service desk customers |
| `visibility` | object | Role or group restriction, when set |

| Metadata key | Meaning |
|---|---|
| `issue_key` | Key of the issue |
| `project` | Project key |
| `cloud_id` | Atlassian site id |

Example payload, from the fixture and trimmed:

```json
{
  "id": "10000",
  "author": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Mia Krystof"},
  "body": {"type": "doc", "version": 1, "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Lorem ipsum dolor sit amet."}]}]},
  "created": "2021-01-17T12:34:00.000+0000",
  "updated": "2021-01-18T23:45:00.000+0000"
}
```

## fetch_worklogs

Schema: [`schemas/jira/fetch_worklogs.schema.json`](../../schemas/jira/fetch_worklogs.schema.json).

- **ExternalID.** `worklog-` and the worklog id.
- **DataDate.** `created`, or none when it is missing. It is not `started`, the time the work began.
- **Payload.** One worklog from `GET /rest/api/3/issue/{key}/worklog`.

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Worklog id |
| `issueId` | string | Numeric id of the issue |
| `author` | user | Who logged the time |
| `started` | Jira timestamp | When the work began |
| `timeSpentSeconds` | integer | Time logged |
| `timeSpent` | string | The same, as Jira shows it, such as `3h 20m` |
| `comment` | ADF | Optional note |
| `created`, `updated` | Jira timestamp | When it was logged and last edited |

| Metadata key | Meaning |
|---|---|
| `issue_key` | Key of the issue |
| `project` | Project key |
| `cloud_id` | Atlassian site id |

Example payload, from the simulator:

```json
{"id": "WORK-2-1", "author": {"accountId": "acct-carol"}, "created": "2026-07-07T01:00:00.000+0000", "timeSpentSeconds": 1800}
```

## fetch_sprints

Schema: [`schemas/jira/fetch_sprints.schema.json`](../../schemas/jira/fetch_sprints.schema.json).

- **ExternalID.** `sprint-` and the numeric sprint id.
- **DataDate.** None: a sprint is current state.
- **Payload.** One sprint from `GET /rest/agile/1.0/board/{id}/sprint`.
- **Boards.** A sprint shown on two boards is emitted once per board, with the same ExternalID and each board's metadata.

The Agile API sends numeric ids, unlike the string ids of the REST API.

| Field | Type | Meaning |
|---|---|---|
| `id` | integer | Sprint id |
| `name` | string | Sprint name |
| `state` | string | `future`, `active` or `closed` |
| `goal` | string | Sprint goal |
| `startDate`, `endDate` | Jira timestamp | Planned start and end |
| `completeDate` | Jira timestamp | When it was closed |
| `originBoardId` | integer | The board that created it |

| Metadata key | Meaning |
|---|---|
| `board_id` | Board the sprint was read from, as a decimal string |
| `board_name` | Board name |
| `board_type` | Board type, such as `scrum`. A board without sprints emits none. |
| `project_id` | Project the board belongs to, when it has one |
| `project_key` | Its key, when it has one |
| `cloud_id` | Atlassian site id |

Example payload, from the simulator and trimmed:

```json
{
  "id": 900103,
  "name": "WORK Sprint 3",
  "state": "active",
  "goal": "Deliver WORK milestone 3",
  "startDate": "2026-09-14T10:04:31.175+0000",
  "endDate": "2026-09-28T10:04:31.175+0000",
  "originBoardId": 9001
}
```
