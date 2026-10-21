# ADR-0003: Lifecycle event records for pull requests and issues

## Status

Accepted. The GitHub connector follows it;
the GitLab, Bitbucket and Jira connectors apply the same pattern.

## Context

Pull requests and issues change state over days (opened, then merged or
closed). Commits and reviews happen once. Emitting one record per pull
request, dated by its creation time, breaks a downstream consumer that
counts activity per day:

- **Wrong date.** A pull request opened on 10 January and merged on
  15 January is re-fetched after the merge. The record keeps its key and its
  creation date, so the merge is counted on 10 January and 15 January has
  none.
- **Lost signal.** The consumer derives the event from the current state.
  Once the payload says merged, the opened event is gone.
- **Misattributed size.** Additions and deletions grow with new commits but
  stay dated at creation.

The vendor returns the current snapshot with fixed timestamps: `createdAt`,
`mergedAt` and `closedAt` are set once and never change. The cursor already
tracks `updatedAt`, so a state change is re-fetched; the problem is only that
the re-fetch overwrites a single record.

## Decision

Emit a separate immutable record for each lifecycle event, derived from the
timestamps, plus one snapshot record that is overwritten on every fetch.

Pull requests, up to four records:

| Record | ExternalID | DataDate | Emitted when |
|---|---|---|---|
| Opened | `owner/repo#PR-42#opened` | `createdAt` | Always |
| Merged | `owner/repo#PR-42#merged` | `mergedAt` | `mergedAt` is set |
| Closed | `owner/repo#PR-42#closed` | `closedAt` | Closed without a merge |
| Snapshot | `owner/repo#PR-42#snapshot` | empty | Always |

Issues, up to three: `#opened`, `#closed` (when closed) and `#snapshot`.

Each record carries `event_type` in its metadata (`opened`, `merged`,
`closed`, `snapshot`), so a consumer dispatches on it rather than on the
payload's state.

The snapshot exists for counters that belong to no single event (comment
count, review requests). It keeps the same key on every fetch and has no
DataDate, so an idempotent sink holds one row with the latest totals. DataDate
is part of the dedup key, and the record contract
([`sdk/record.go`](../../sdk/record.go),
[`docs/records/README.md`](../records/README.md#datadate)) sets it only for
day-partitioned data and for event records dated by the event. A sink keys an
empty DataDate as one value.

Example, pull request 42 opened on 10 January and merged on 15 January:

```
Fetch on 10 Jan: #opened (10 Jan), #snapshot (comments=0)
Fetch on 12 Jan: #opened unchanged, #snapshot overwritten (comments=8)
Fetch on 15 Jan: #opened unchanged, #snapshot overwritten (comments=27),
                 #merged (15 Jan, carries the final diff size)
```

A sink keyed on (organization, connector, operation, ExternalID, DataDate)
keeps the records apart: the keys differ, and the event dates usually differ
too.

## Consequences

- A consumer counts each event on the day it happened, and the opened event
  survives the merge.
- Time to merge, time to close and stale pull requests become computable
  from the records.
- A pull request produces up to four records, each with an `event_type`.
- Bitbucket's pull request listing has no merge or close time, so its
  `#merged` and `#closed` records are dated by `updated_on`, which moves when
  the pull request is commented on after the transition. The Bitbucket
  records page says so; keep the earliest of those rows.
