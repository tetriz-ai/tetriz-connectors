# Connector sync walkthrough

The GIF illustrates actual CLI and Postgres output captured on 2026-10-16 from the bundled
GitHub simulator. It is not a shipped UI. No vendor account or real credential was used.
The capture ran the source-built CLI, simulator, Postgres and Redis as local processes, with
only the demo organization routed to the simulator. The root quick start provides the same
components through Compose.

| Step | Captured evidence |
|---|---|
| Configure the simulator organization | `config.txt` |
| Sync three repositories | `repos.txt`: completed, three emitted records |
| Inspect progress before the first PR sync | `cursor-before.txt`: no cursor |
| Sync pull requests to the Postgres sink | `first.txt`: completed, 2,634 emitted event records |
| Read the saved per-repository progress | `cursor-after.txt`: three repository checkpoints |
| Query merged events in Postgres | `records.txt`: 238 + 94 + 440 = 772 rows |
| Start a new CLI process and sync again | `second.txt`: completed, eight emitted records, unchanged cursor |
| Check whether those eight rows already existed | `replay.txt`: eight rows, all first seen before the second sync |

Emitted records are not a unique-PR count. The GitHub operation emits multiple event types.
The SQL result selects merged events. The second run deliberately shows boundary replay:
delivery is at least once and the Postgres sink upserts by its record key. JSON-lines consumers
must handle duplicates themselves. This demonstrates a completed run followed by a new CLI
process; it does not simulate a crash halfway through a run.

Nine connectors are available, but this run exercises GitHub only. The bundled simulator
supports GitHub, GitLab, Bitbucket and Jira. Each vendor keeps its own payload shape within the
shared record envelope; the runner does not compute engineering metrics or ship a dashboard.

## Reproduce the sync

Start a **fresh disposable** Compose stack using the root
[quick start](../../../README.md#quick-start). Use a fresh store so the first run starts with
no cursor. From the repo root:

```sh
ORG=00000000-0000-4000-8000-00000000d3e0
docker compose exec -T runner tetriz-connectors config put --org "$ORG" --connector github \
  --settings '{"organization":"demo-gh-startup"}' --credentials '{}'
docker compose exec -T runner tetriz-connectors sync --org "$ORG" --connector github --op fetch_repos
docker compose exec -T runner tetriz-connectors cursor show --org "$ORG" --connector github --op fetch_prs
docker compose exec -T -e TETRIZ_SINK=postgres runner tetriz-connectors sync \
  --org "$ORG" --connector github --op fetch_prs
docker compose exec -T runner tetriz-connectors cursor show --org "$ORG" --connector github --op fetch_prs
docker compose exec -T -e TETRIZ_SINK=postgres runner tetriz-connectors sync \
  --org "$ORG" --connector github --op fetch_prs
```

Each invocation is a separate CLI process. Compose's serving runner may also sync on its
schedule; for an isolated capture, stop that serving process and run equivalent commands
against just the local simulator, Postgres and Redis, as this capture did. Simulator data is
seeded relative to the day, so counts and dates can change.

Query the rows:

```sql
SELECT metadata->>'repo' AS repository, count(*) AS merged
FROM connector_records
WHERE organization_id = '00000000-0000-4000-8000-00000000d3e0'
  AND connector_id = 'github' AND operation_id = 'fetch_prs'
  AND metadata->>'event_type' = 'merged'
GROUP BY 1 ORDER BY 1;
```

For the replay check, select rows whose `sync_run_id` equals the second run's `run_id`. Verify
their `first_seen_at` values precede the `updated_at` of the cursor captured after the first
run. Also compare the two result cursors. A smaller record count alone does not prove replay.

## Refresh the artwork

Save fresh stdout into the corresponding transcript files above, update the narration and
evidence excerpts in `scenes.json` together, then render with Pillow:

```sh
python3 scripts/render-readme-demo.py --preview-dir /tmp/connectors-demo
```

The renderer supports Arial/Menlo on macOS and DejaVu on Linux. It rejects missing evidence
excerpts and text that exceeds the layout. Visually inspect every scene before committing.
