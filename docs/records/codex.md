# Codex records

The connector id is `codex`. It has one operation, and each payload is a raw row from the Codex Enterprise Analytics API, stored as the vendor sent it.

A payload is JSON bytes. In a JSON-encoded envelope the `payload` field is that JSON, base64-encoded.

## fetch_codex_usage

One record per developer per UTC day, from `GET /v1/analytics/codex/workspaces/{workspace_id}/usage`.

| Property | Value |
|---|---|
| ExternalID | `<workspace_id>\|<user_id>\|<YYYY-MM-DD>`, for example `00000000-0000-4000-8000-00000000c0de\|u_1\|2026-10-04` |
| Encoding | The vendor's row object, verbatim. The connector parses only three fields. |
| DataDate | Midnight UTC of the day the row covers. It equals the date in the ExternalID. |
| Metadata keys | `workspace_id`, `developer_id`, `developer_email` (empty when null), `data_date` (`YYYY-MM-DD`) |

OpenAI publishes no response schema. The connector never flattens a counter, so a `null` counter stays `null` and means "not applicable", not zero.

### Fields

| Field | Type | Meaning |
|---|---|---|
| `user_id` | string | The developer's OpenAI account id. Never empty and never contains `\|`. |
| `bucket_start_time` | integer | Start of the day the vendor filed the row under, in Unix seconds. Optional. When present it falls on the requested day. |
| `actor` | object or null | Who the row is for. Optional. |
| `actor.email` | string or null | The developer's email. |
| any other key | any | Vendor counters, kept verbatim, nullable. |

A row that breaks these rules fails the run, so it is never emitted.

### Example

```json
{
  "user_id": "u_1",
  "bucket_start_time": 1791072000,
  "actor": {"email": "dev@example.com"},
  "credits": 12,
  "tokens": null
}
```

Verified by running the operation against a scripted gateway, with rows that have every optional field, a null actor and only `user_id` (`connectors/codex/schema_test.go`).

Schema: [schemas/codex/fetch_codex_usage.schema.json](../../schemas/codex/fetch_codex_usage.schema.json)
