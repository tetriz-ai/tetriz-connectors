# Jira connector

Reads Jira Cloud sites through Atlassian's OAuth 2.0 (3LO) API.

| Operation | What it reads |
|---|---|
| `fetch_projects` | Projects |
| `fetch_issues` | Issues, as lifecycle records (`#created`, `#resolved`, `#snapshot`; see [ADR-0003](../../docs/adr/0003-github-connector-lifecycle-events.md)) |
| `fetch_issues_data` | Issue descriptions, written to the blob store |
| `fetch_comments` | Issue comments (no default schedule) |
| `fetch_worklogs` | Worklogs |
| `fetch_sprints` | Boards and sprints |

A Jira "site" is one Atlassian cloud instance, identified by a cloud id. One
OAuth grant can reach several sites; the connect flow asks the admin to pick
one when it sees more than one.

## Configuration

Credentials: `{"oauth_refresh_token": "..."}`. The access token is never
stored; the gateway's refresher (`internal/gateway/jira/gateway.go`) exchanges the
refresh token, caches the access token under one refresh lock per
organization, and persists the rotated refresh token that Atlassian returns
on every exchange.

Settings:

```json
{"cloud_id": "...", "cloud_name": "acme", "site_url": "https://acme.atlassian.net", "project_keys": ["ENG"]}
```

`cloud_id` is required: Jira URLs carry no organization slug, and an empty
cloud id syncs nothing without an error. `project_keys` limits the sync;
empty means every project.

The runner reads the OAuth client from `TETRIZ_JIRA_OAUTH_CLIENT_ID` and
`TETRIZ_JIRA_OAUTH_CLIENT_SECRET`. The rate of the `jira-rest` group comes
from the gateway configuration.

## Setting up the OAuth app

1. In the [Atlassian developer console](https://developer.atlassian.com/console/myapps/),
   create an **OAuth 2.0 integration**.
2. Under **Permissions**, add the Jira API scopes the connect flow requests
   (`jiraOAuthScopes` in `auth/integrations/jira_setup.go`): `read:jira-work`,
   `read:jira-user`, `read:sprint:jira-software`,
   `read:board-scope:jira-software`, `read:project:jira` and
   `offline_access`. `offline_access` must also appear in the authorize URL;
   Atlassian does not add it from the console settings.
3. Under **Authorization**, add your host's Jira callback URL. It must match
   the configured callback exactly, including any trailing slash.
4. Copy the client id and secret into the environment variables above.

## Cursors

`fetch_issues`, `fetch_comments` and `fetch_worklogs` keep an `updated`
watermark per project. Each incremental run reads from 14 hours plus an
overlap window before the cursor. The overlap defaults to five minutes
(`CursorOverlapWindow` in `config.JiraConfig`) because Jira's JQL
`updated >=` has minute precision. The 14 hours are there because Jira reads
a JQL date in the authorizing user's time zone, which the connector does not
know (`effectiveSince` in `operations.go`). `fetch_issues_data` applies the
14 hours without the overlap. A run therefore re-reads up to
about 14 hours of updates. Records are keyed so the re-read produces no
duplicates in an idempotent sink. Pages are checkpointed as they complete.

## Tests

```bash
go test -race ./connectors/jira/... ./gateway/jira/... ./auth/integrations/...
```
