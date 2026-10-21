# Records

A record is one item a connector read from a vendor: a pull request, an issue, a usage row. This page says how to read one. Each connector's page lists its operations and the exact payload of each.

A JSON Schema in [`schemas/`](../../schemas) describes every payload. A test runs the GitHub, GitLab, Bitbucket and Jira connectors against the simulator and validates every record they emit, so a payload change fails `go test`.

## Connectors

| Connector id | Records page |
|---|---|
| `github` | [GitHub](github.md) |
| `gitlab` | [GitLab](gitlab.md) |
| `bitbucket` | [Bitbucket](bitbucket.md) |
| `jira` | [Jira](jira.md) |
| `linear` | [Linear](linear.md) |
| `cursor` | [Cursor](cursor.md) |
| `claude-code` | [Claude Code](claude-code.md) |
| `codex` | [Codex](codex.md) |
| `kiro` | [Kiro](kiro.md) |

## Reading an envelope

The sink publishes each record as a `ConnectorDataEnvelope` ([`proto/tetriz/connectors/connector/v1/connector_data_event.proto`](../../proto/tetriz/connectors/connector/v1/connector_data_event.proto)). The JSON-lines sink writes one envelope per line. The SQS publisher sends the same JSON as the message body.

The JSON is protojson. Keys are camelCase, empty fields are left out, timestamps are RFC 3339 and bytes are base64.

```json
{"events":[{"connectorId":"jira","operationId":"fetch_worklogs","organizationId":"00000000-0000-4000-8000-00000000d3e0","externalId":"worklog-WORK-2-1","payload":"eyJhdXRob3IiOnsiYWNjb3VudElkIjoiYWNjdC1jYXJvbCJ9LCJjcmVhdGVkIjoiMjAyNi0wNy0wN1QwMTowMDowMC4wMDArMDAwMCIsImlkIjoiV09SSy0yLTEiLCJ0aW1lU3BlbnRTZWNvbmRzIjoxODAwfQ==","fetchedAt":"2026-10-05T10:05:37.101Z","metadata":{"cloud_id":"demo-bb-startup","issue_key":"WORK-2","project":"WORK"},"dataDate":"2026-07-07T01:00:00Z","syncRunId":"5a4f8509-74fa-44da-8a06-0f625119ecb7"}],"organizationId":"00000000-0000-4000-8000-00000000d3e0"}
```

To read the record, base64-decode `payload`. The result is the record's JSON:

```json
{"author":{"accountId":"acct-carol"},"created":"2026-07-07T01:00:00.000+0000","id":"WORK-2-1","timeSpentSeconds":1800}
```

The sink sends one event per envelope today. Read `events` as a list anyway, as the proto allows several.

An SQS body over 225 KiB is written to the blob store, and the message body is the blob key instead. The message attribute `USE_S3` is `"true"` then.

## Event fields

| Field | Type | Meaning |
|---|---|---|
| `connectorId` | string | Registry id, such as `github` or `claude-code` |
| `operationId` | string | The kind of record. It picks the schema. |
| `organizationId` | string | The tenant, a UUID |
| `externalId` | string | The record's id within its connector and operation |
| `payload` | base64 | The record's JSON. Its shape depends on `connectorId` and `operationId`. |
| `fetchedAt` | timestamp | When the connector read it |
| `dataDate` | timestamp | When the activity happened. Absent when the operation sets none. |
| `metadata` | map of string to string | Routing and filtering keys |
| `syncRunId` | string | The sync run that emitted it. Absent for webhook records. |

`syncRunId` groups records by run. It does not mean the run saw everything at the vendor: a run can skip a repository whose fetch failed.

## Identity and duplicates

Delivery is at least once. The same record arrives again after a retry, a re-read overlap window or a run that resumes from its cursor boundary.

Deduplicate on `(organizationId, connectorId, operationId, externalId, dataDate)`, with an absent `dataDate` as one value. When two copies share that key, keep the one with the later `fetchedAt`. The Postgres sink (`sink/postgres`) keys its rows this way.

- **ExternalID.** It is unique within one organization, connector and operation. The same string can mean different things in two operations.
- **Built from stable ids.** Most ExternalIDs come from vendor ids or numbers that never change. Some still include a name the vendor lets users change, such as a repository's `owner/repo` path on GitHub, GitLab and Bitbucket; the connector page says which, and the metadata carries the stable id to join on.
- **Lifecycle records.** A pull request or an issue gives several records, such as `#opened`, `#merged` and `#snapshot` ([ADR-0003](../adr/0003-github-connector-lifecycle-events.md)). Each has its own ExternalID and `event_type` metadata. The event records never change. The snapshot is overwritten with the latest state.
- **DataDate is in the key.** A few operations reuse an ExternalID per day and tell the days apart by `dataDate` (Kiro's per-client-type rows). The rule below keeps `dataDate` fixed for every other ExternalID, so the key never forks one item into a row per run.

## DataDate

`dataDate` is the day the record's data describes. One rule covers every connector:

- **Day-partitioned data sets it to the day.** A usage or analytics row for one day (Copilot metrics, Cursor, Claude Code, Codex and Kiro usage) is dated by that day.
- **Event records set it to when the event happened.** A commit is dated by its committed date; a comment, review, approval or worklog by its creation; a lifecycle record (`#opened`, `#merged`, `#closed`, `#created`, `#resolved`) by its transition.
- **Current-state snapshots leave it empty.** A `#snapshot` record, a branch, a member, a project, a sprint, a repository, a config, a tree, a description pointer, a file's content: each is the item's latest state, overwritten on every run.

A connector never sets `dataDate` to the fetch time or to the vendor's last-updated time. Both change between runs, so a sink keyed on `dataDate` would hold a new row for the same item every run. The connector pages say which time each dated operation uses.

When `dataDate` is absent, use `fetchedAt`.

## Metadata

Metadata holds short keys for routing, such as `repo`, `project` or `event_type`. Keys are snake_case.

Every value is a string. A number is written in decimal (`"9001"`), and a boolean is `"true"` or `"false"`. Parse a value before you compare it as a number.

Metadata is not a copy of the payload. Read fields from the payload, and use metadata to route and filter.

## Payload encodings

Payloads come in three kinds. The connector pages say which kind each operation uses.

| Kind | Keys | Example |
|---|---|---|
| Vendor JSON, passed through | As the vendor sends them | Jira issues, GitHub GraphQL pull requests |
| JSON the connector builds | snake_case, mostly | Description pointers, branch records |
| A proto message in protojson | camelCase | `RepositoryRecord`, `RepoTreeRecord` |

Watch for these quirks:

- **Mixed naming.** GitHub GraphQL payloads use camelCase (`createdAt`). GitHub, GitLab and Bitbucket REST payloads use snake_case (`created_at`). Nothing renames them.
- **Base64 inside base64.** `RepositoryRecord.vendorBlobJson` is bytes, so protojson writes it as base64. Inside the envelope the whole payload is base64 again. Decode the payload, parse it, then decode `vendorBlobJson` to get its JSON object.
- **int64 as strings.** protojson writes an int64 as a string, such as `"sizeKb": "1024"`. int32 fields, such as the repository stats, stay numbers.
- **Missing zero values.** protojson leaves out `false`, `0` and `""`. A missing `archived` means false.
- **Enums as names.** `visibility` is `"REPOSITORY_VISIBILITY_PRIVATE"`, not a number.
- **Jira timestamps.** Jira writes `2026-01-16T15:03:31.606+0200`, with no colon in the offset. Many RFC 3339 parsers reject it.
- **Webhook bodies.** `webhook_events` records carry the vendor's webhook body unchanged, whatever its event type. The body is always a JSON object: GitHub and Bitbucket refuse a delivery whose body is not JSON (such as a hook set to `application/x-www-form-urlencoded`), so set the webhook's content type to JSON.

## Validating a payload

The schema for a record is `schemas/<connectorId>/<operationId>.schema.json`. It describes the decoded payload, not the envelope.

In Go, the `schemas` package embeds every schema:

```go
import "github.com/tetriz-ai/tetriz-connectors/schemas"

// payload is the base64-decoded event payload.
if err := schemas.Validate(event.GetConnectorId(), event.GetOperationId(), payload); err != nil {
	return err // names the schema and the failing field
}
```

`schemas.Schema` returns one schema file, and `schemas.List` names them all. `Validate` returns an error wrapping `schemas.ErrNoSchema` for an operation it does not know.

Any JSON Schema draft 2020-12 tool works too. Some schemas point at shared shapes in `schemas/common`, so load those as well. With [ajv-cli](https://github.com/ajv-validator/ajv-cli):

```sh
npx -p ajv-cli@5 -p ajv-formats@2 ajv validate --spec=draft2020 -c ajv-formats \
  -s schemas/jira/fetch_sprints.schema.json -r "schemas/common/*.json" -d payload.json
```

The Go package asserts `format`, such as `date-time`. It also decodes base64 fields marked `contentEncoding` and checks their JSON against `contentSchema`. Most other tools treat both as notes unless told otherwise.

## How the schemas are laid out

- **One file per record operationId.** The file is named after the `operationId` a record carries, not after the operation that ran. GitHub's `webhook_events` also emits `pr_sync_events` records, which get their own file.
- **Webhooks emit under their own id.** A webhook's records carry operationId `webhook_events` (GitHub also emits `pr_sync_events`), never a polling operation's id, so a hook body never overwrites the richer polled record. GitLab's `webhook_events` reuses the polled ExternalID formats and names each record's shape in the `record_kind` metadata key; its schema holds a `oneOf` with one branch per kind.
- **Several kinds in one operation.** When one operation emits records of different shapes under its own `operationId`, its schema is a `oneOf` with one branch per kind. Lifecycle records share one shape, so they need no `oneOf`.
- **Strict where the code is.** Keys the connector always writes are required. Objects the connector builds forbid other keys. Vendor objects allow other keys, because vendors add fields.
- **Shared shapes.** `schemas/common` holds shapes several schemas use, such as `RepositoryRecord`, `RepoTreeRecord` and `FileContentRecord`. They are not record schemas.
- **Complete.** A test checks that every registered operation that emits records has a schema, and that every schema belongs to one. An operation that emits nothing is listed in that test with the reason.
