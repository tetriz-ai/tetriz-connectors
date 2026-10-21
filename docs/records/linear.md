# Linear records

The connector id is `linear`. It reads the Linear GraphQL API with an OAuth token.

Every payload is one GraphQL node, decoded into a Go struct and encoded again. So a payload holds exactly the keys this page lists, and never a key Linear adds later. Every key is always present, even when Linear returned it as null or left it out.

The re-encoding changes some values:

- A string Linear returned as null, such as an unset `description`, becomes `""`.
- A timestamp Linear returned as null becomes `0001-01-01T00:00:00Z` when its Go field is not a pointer. The fields that can be null are typed as nullable below.
- Timestamps are RFC 3339 in UTC. Linear sends milliseconds, and a timestamp whose milliseconds are zero loses them, so `2026-06-10T01:00:00.000Z` becomes `2026-06-10T01:00:00Z`.

No operation sets metadata keys or a DataDate. Every record is the node's current state, upserted on its ExternalID; read `updatedAt` from the payload (`connectors/linear/schema_test.go`).

`fetch_teams`, `fetch_users`, `fetch_projects` and `fetch_labels` read the whole workspace on every run. `fetch_sprints` and `fetch_issues` read team by team, archived teams included.

## fetch_teams

One record per team.

| Property | Value |
|---|---|
| ExternalID | The team `id` (UUID) |
| Encoding | One JSON object, the connector's `TeamNode` |
| DataDate | None (current state) |
| Metadata keys | None |

### Fields

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Team id (UUID) |
| `name` | string | Team name |
| `key` | string | Team key, the prefix of issue identifiers, such as `ENG`. Renameable |
| `description` | string | Team description, `""` when unset |
| `timezone` | string | The team's IANA timezone |
| `createdAt` | date-time | When the team was created |
| `updatedAt` | date-time | When the team last changed |
| `archivedAt` | date-time or null | When the team was archived |

### Example

```json
{"id": "team-1", "name": "Engineering", "key": "ENG", "description": "Core", "timezone": "Europe/London",
 "createdAt": "2024-01-02T03:04:05.678Z", "updatedAt": "2026-06-10T01:00:00Z", "archivedAt": null}
```

Verified by running the operation against the package's fake gateway, on a full node and on a node with only `id` and `updatedAt` (`connectors/linear/schema_test.go`).

Schema: [schemas/linear/fetch_teams.schema.json](../../schemas/linear/fetch_teams.schema.json)

## fetch_users

One record per workspace member, disabled members included.

| Property | Value |
|---|---|
| ExternalID | The user `id` (UUID) |
| Encoding | One JSON object, the connector's `UserNode` |
| DataDate | None (current state) |
| Metadata keys | None |

### Fields

| Field | Type | Meaning |
|---|---|---|
| `id` | string | User id (UUID) |
| `name` | string | Full name |
| `displayName` | string | Display name |
| `email` | string | Email address |
| `active` | boolean | `false` for a disabled member |
| `admin` | boolean | `true` for a workspace admin |
| `createdAt` | date-time | When the user was created |
| `updatedAt` | date-time | When the user last changed |
| `archivedAt` | date-time or null | When the user was archived |

### Example

```json
{"id": "user-1", "name": "Ada Lovelace", "displayName": "ada", "email": "ada@example.com", "active": true, "admin": true,
 "createdAt": "2024-01-02T03:04:05Z", "updatedAt": "2026-06-10T01:00:00Z", "archivedAt": null}
```

Verified by running the operation against the package's fake gateway, on full nodes and on a node with only `id` and `updatedAt` (`connectors/linear/schema_test.go`).

Schema: [schemas/linear/fetch_users.schema.json](../../schemas/linear/fetch_users.schema.json)

## fetch_projects

One record per project.

| Property | Value |
|---|---|
| ExternalID | The project `id` (UUID) |
| Encoding | One JSON object, the connector's `ProjectNode` |
| DataDate | None (current state) |
| Metadata keys | None |

### Fields

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Project id (UUID) |
| `name` | string | Project name |
| `description` | string | Project description, `""` when unset |
| `state` | string | `backlog`, `planned`, `started`, `paused`, `completed` or `canceled`, as Linear reports it |
| `url` | string | The project's page in the Linear app |
| `progress` | number | Completion from 0 to 1, computed by Linear |
| `targetDate` | string | Target date as `YYYY-MM-DD`, `""` when unset |
| `createdAt` | date-time | When the project was created |
| `updatedAt` | date-time | When the project last changed |
| `archivedAt` | date-time or null | When the project was archived |

### Example

```json
{"id": "proj-1", "name": "Launch", "description": "Ship it", "state": "started",
 "url": "https://linear.app/acme/project/launch-1", "progress": 0.42, "targetDate": "2026-09-30",
 "createdAt": "2024-01-02T03:04:05Z", "updatedAt": "2026-06-10T01:00:00Z", "archivedAt": null}
```

Verified by running the operation against the package's fake gateway, on full nodes and on a node with only `id` and `updatedAt` (`connectors/linear/schema_test.go`).

Schema: [schemas/linear/fetch_projects.schema.json](../../schemas/linear/fetch_projects.schema.json)

## fetch_labels

One record per issue label, workspace labels and team labels alike.

| Property | Value |
|---|---|
| ExternalID | The label `id` (UUID) |
| Encoding | One JSON object, the connector's `LabelNode` |
| DataDate | None (current state) |
| Metadata keys | None |

### Fields

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Label id (UUID) |
| `name` | string | Label name |
| `color` | string | Hex colour, such as `#eb5757` |
| `description` | string | Label description, `""` when unset |
| `createdAt` | date-time | When the label was created |
| `updatedAt` | date-time | When the label last changed |
| `archivedAt` | date-time or null | When the label was archived |
| `team` | object or null | `{id, key}` of the owning team, `null` for a workspace label |

### Example

```json
{"id": "label-1", "name": "bug", "color": "#eb5757", "description": "Something broke",
 "createdAt": "2024-01-02T03:04:05Z", "updatedAt": "2026-06-10T01:00:00Z", "archivedAt": null,
 "team": {"id": "team-1", "key": "ENG"}}
```

Verified by running the operation against the package's fake gateway, on full nodes and on a node with only `id` and `updatedAt` (`connectors/linear/schema_test.go`).

Schema: [schemas/linear/fetch_labels.schema.json](../../schemas/linear/fetch_labels.schema.json)

## fetch_sprints

One record per cycle, which is Linear's sprint, for every team. Archived cycles are included.

| Property | Value |
|---|---|
| ExternalID | The cycle `id` (UUID) |
| Encoding | One JSON object, the connector's `CycleNode` |
| DataDate | None (current state) |
| Metadata keys | None |

Every run re-emits every cycle. The cycles query is not filtered by the team's checkpoint.

### Fields

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Cycle id (UUID) |
| `number` | integer | Cycle number within its team |
| `name` | string | Cycle name, `""` when unnamed, which is common |
| `startsAt` | date-time | Cycle start |
| `endsAt` | date-time | Cycle end |
| `completedAt` | date-time or null | When the cycle completed |
| `archivedAt` | date-time or null | When the cycle was archived |
| `updatedAt` | date-time | When the cycle last changed |
| `progress` | number | Completion from 0 to 1, computed by Linear |
| `team` | object | `{id, key}` of the cycle's team |

### Example

```json
{"id": "cycle-1", "number": 7, "name": "Sprint 7", "startsAt": "2026-06-01T00:00:00Z", "endsAt": "2026-06-15T00:00:00Z",
 "completedAt": "2026-06-15T00:00:00Z", "archivedAt": null, "updatedAt": "2026-06-15T00:00:01Z", "progress": 1,
 "team": {"id": "team-1", "key": "ENG"}}
```

Verified by running the operation against the package's fake gateway, on full nodes and on a node with only `id` and `updatedAt` (`connectors/linear/schema_test.go`).

Schema: [schemas/linear/fetch_sprints.schema.json](../../schemas/linear/fetch_sprints.schema.json)

## fetch_issues

One record per issue updated since the team's checkpoint, archived issues included.

| Property | Value |
|---|---|
| ExternalID | The issue `id` (UUID) |
| Encoding | One JSON object, the connector's `IssueNode` |
| DataDate | None (current state) |
| Metadata keys | None |

The first run reads every issue. Later runs read issues updated after the team's last `updatedAt`, less five minutes, so a run re-emits a few issues the previous run already sent.

The `identifier`, such as `ENG-42`, changes when an issue moves to another team. It is in the payload but is not the ExternalID, so a moved issue keeps its record.

### Fields

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Issue id (UUID), stable across team moves |
| `identifier` | string | Team key and number, such as `ENG-42` |
| `title` | string | Issue title |
| `description` | string | Markdown description, `""` when unset |
| `priority` | integer | 0 none, 1 urgent, 2 high, 3 medium, 4 low |
| `estimate` | number or null | Estimate in the team's points scale, `null` when unestimated |
| `dueDate` | string | Due date as `YYYY-MM-DD`, `""` when unset |
| `createdAt` | date-time | When the issue was created |
| `updatedAt` | date-time | When the issue last changed |
| `completedAt` | date-time or null | When the issue was completed |
| `canceledAt` | date-time or null | When the issue was canceled |
| `archivedAt` | date-time or null | When the issue was archived |
| `state` | object | `{id, name, type}`. `type` is `triage`, `backlog`, `unstarted`, `started`, `completed`, `canceled` or `duplicate` |
| `team` | object | `{id, key}` of the issue's team |
| `assignee` | object or null | `{id, name, email}`, `null` when unassigned |
| `creator` | object or null | `{id, name, email}`, `null` when Linear returns none |
| `project` | object or null | `{id, name}` |
| `cycle` | object or null | `{id, number, name}` |
| `parent` | object or null | `{id, identifier, team: {id, key}}` for a sub-issue |
| `labels.nodes` | array or null | `{id, name}` per label |
| `attachments.nodes` | array or null | `{url, sourceType}` per GitHub or GitLab attachment, at most 10 |
| `attachments.pageInfo.hasNextPage` | boolean | `true` when the issue had more than 10 such attachments and the rest were dropped |
| `attachments.pageInfo.endCursor` | string | Always `""`, because the query does not select it |

`labels` and `attachments` are always objects. Their `nodes` is `null` only when Linear returned no connection, which a normal response does not do.

### Example

```json
{
  "id": "issue-1", "identifier": "ENG-42", "title": "Fix login", "description": "Steps:\n1. log in",
  "priority": 2, "estimate": 3, "dueDate": "2026-07-01",
  "createdAt": "2026-06-01T00:00:00Z", "updatedAt": "2026-06-10T01:00:00Z",
  "completedAt": "2026-06-10T01:00:00Z", "canceledAt": null, "archivedAt": null,
  "state": {"id": "state-1", "name": "Done", "type": "completed"},
  "team": {"id": "team-1", "key": "ENG"},
  "assignee": {"id": "user-1", "name": "Ada Lovelace", "email": "ada@example.com"},
  "creator": null, "project": {"id": "proj-1", "name": "Launch"},
  "cycle": {"id": "cycle-1", "number": 7, "name": "Sprint 7"}, "parent": null,
  "labels": {"nodes": [{"id": "label-1", "name": "bug"}]},
  "attachments": {"nodes": [{"url": "https://github.com/acme/api/pull/12", "sourceType": "github"}],
                  "pageInfo": {"hasNextPage": false, "endCursor": ""}}
}
```

Verified by running the operation against the package's fake gateway, on a full issue, an issue with every nullable field null, and an issue with only `id`, `identifier` and `updatedAt` (`connectors/linear/schema_test.go`).

Schema: [schemas/linear/fetch_issues.schema.json](../../schemas/linear/fetch_issues.schema.json)
