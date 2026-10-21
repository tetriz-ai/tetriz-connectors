# Claude Code records

The connector id is `claude-code`. It reads the Claude Enterprise Analytics API and the organization member roster.

Every payload is the vendor's JSON, stored byte for byte. The connector parses only the keys it builds the ExternalID from. Everything else is passed through untouched, including fields the vendor adds later.

No captured vendor response exists for these endpoints. The schemas type the fields the vendor documents, and require only the keys the connector checks. A field marked "vendor-documented" below is not checked by the connector.

Two encoding rules apply to every operation:

- Money is a decimal string in fractional cents. `"41280.000000"` means $412.80. Parse it as a decimal, not a float.
- A nullable metric separates zero from not applicable. Do not read `null` as `0`.

No operation sets metadata keys. A consumer upserts on the ExternalID, because a later run can re-emit an identity with revised figures.

## fetch_analytics_users

One record per user per calendar day of activity.

| Property | Value |
|---|---|
| ExternalID | `<user.id>\|<YYYY-MM-DD>`, the user and the day read |
| Encoding | One JSON object: the vendor's row from `/v1/organizations/analytics/users` |
| DataDate | 00:00 UTC on the day read |
| Metadata keys | None |

### Fields

| Field | Type | Meaning |
|---|---|---|
| `user.id` | string, required | Anthropic user id |
| `user.email_address` | string | The user's email (vendor-documented) |
| `last_activity_date` | string | Last active day, `YYYY-MM-DD` (vendor-documented) |
| `rbac_group_id`, `rbac_group_name` | string | The user's group (vendor-documented) |
| `claude_code_metrics.core_metrics` | object | `commit_count`, `pull_request_count`, `distinct_session_count`, `lines_of_code.added_count` and `removed_count` (vendor-documented) |
| `claude_code_metrics.tool_actions` | object | `accepted_count` and `rejected_count` per edit tool (vendor-documented) |
| `chat_metrics` | object | Conversation and message counts (vendor-documented) |
| `office_metrics` | object | Office add-in activity per app (vendor-documented) |
| `web_search_count` | integer | Web searches that day (vendor-documented) |

### Example

```json
{
  "user": {"id": "user_A", "email_address": "a@example.com", "type": "user"},
  "last_activity_date": "2026-08-25",
  "claude_code_metrics": {
    "core_metrics": {"commit_count": 3, "pull_request_count": 1, "distinct_session_count": 4,
      "lines_of_code": {"added_count": 120, "removed_count": 30}}
  },
  "web_search_count": 11
}
```

Verified by running the operation against the package's mocked vendor gateway, on the documented row and on a minimal row (`connectors/anthropic/schema_test.go`).

Schema: [schemas/claude-code/fetch_analytics_users.schema.json](../../schemas/claude-code/fetch_analytics_users.schema.json)

## fetch_analytics_skills

One record per user per calendar day, holding all of that user's skill rows.

| Property | Value |
|---|---|
| ExternalID | `<user.id>\|<YYYY-MM-DD>`, the user and the day read |
| Encoding | A JSON array of the vendor's rows from `/v1/organizations/analytics/skills`, grouped by user |
| DataDate | 00:00 UTC on the day read |
| Metadata keys | None |

The skill name is renameable, so it is not part of the ExternalID. A user's skills for a day sit in one array instead.

### Fields

Each array element has these fields.

| Field | Type | Meaning |
|---|---|---|
| `user.id` | string, required | Anthropic user id, the same in every element |
| `skill_name` | string | The skill (vendor-documented) |
| `distinct_user_count` | integer | Distinct users in the row (vendor-documented) |
| `attributed_list_price` | string | Decimal string in fractional cents (vendor-documented) |
| `claude_code_metrics` | object | Such as `distinct_session_skill_used_count` (vendor-documented) |
| `chat_metrics`, `office_metrics` | object | Skill use in chat and Office apps (vendor-documented) |

### Example

```json
[
  {"user": {"id": "user_A"}, "skill_name": "code-review", "distinct_user_count": 1,
   "attributed_list_price": "1200.000000",
   "claude_code_metrics": {"distinct_session_skill_used_count": 3}}
]
```

Verified by running the operation against the mocked vendor gateway, with one row and with two rows for one user-day (`connectors/anthropic/schema_test.go`).

Schema: [schemas/claude-code/fetch_analytics_skills.schema.json](../../schemas/claude-code/fetch_analytics_skills.schema.json)

## fetch_analytics_user_cost

One record per user, per daily bucket, per product.

| Property | Value |
|---|---|
| ExternalID | `<actor.user_id>\|<YYYY-MM-DD>\|<product>`, where the date is `starting_at` in UTC. A row with no product, or a null or empty one, has `unknown` as the last part |
| Encoding | A JSON array of the vendor's rows from `/v1/organizations/analytics/user_cost_report` |
| DataDate | 00:00 UTC on the ExternalID's date |
| Metadata keys | None |

The vendor fans a bucket out by model, cost type and token type. The model is renameable, so those rows share one record.

Rows without a product share the `unknown` record with any row whose product is `unknown`. No row is dropped: each is an element of that record's array.

### Fields

Each array element has these fields.

| Field | Type | Meaning |
|---|---|---|
| `actor.user_id` | string, required | Anthropic user id |
| `starting_at` | string (RFC 3339), required | Bucket start |
| `product` | string | Such as `claude_code` or `chat` |
| `ending_at` | string | Bucket end, exclusive (vendor-documented) |
| `actor.email`, `actor.name`, `actor.deleted` | string, string, boolean | The user (vendor-documented) |
| `model` | string | Model id or display name (vendor-documented) |
| `amount`, `list_amount` | string | Net and list cost, decimal string in fractional cents (vendor-documented) |
| `currency` | string | Such as `USD` (vendor-documented) |
| `cost_type`, `token_type` | string or null | What the cost is for (vendor-documented) |
| `context_window`, `inference_geo`, `speed` | string | Pricing dimensions (vendor-documented) |
| `rbac_group_id`, `slack_channel_id` | string or null | Group and Slack channel (vendor-documented) |

### Example

```json
[
  {"actor": {"type": "user_actor", "user_id": "user_A", "email": "a@example.com"},
   "amount": "41280.000000", "currency": "USD", "cost_type": "tokens",
   "token_type": "output_tokens", "model": "claude-opus-4", "product": "claude_code",
   "starting_at": "2026-08-25T00:00:00Z", "ending_at": "2026-08-26T00:00:00Z"}
]
```

Verified by running the operation against the mocked vendor gateway, with one row, a minimal row, and two models for one product (`connectors/anthropic/schema_test.go`).

Schema: [schemas/claude-code/fetch_analytics_user_cost.schema.json](../../schemas/claude-code/fetch_analytics_user_cost.schema.json)

## fetch_analytics_user_usage

One record per user, per daily bucket, per product.

| Property | Value |
|---|---|
| ExternalID | `<actor.user_id>\|<YYYY-MM-DD>\|<product>`, as for `fetch_analytics_user_cost` |
| Encoding | A JSON array of the vendor's rows from `/v1/organizations/analytics/user_usage_report` |
| DataDate | 00:00 UTC on the ExternalID's date |
| Metadata keys | None |

### Fields

Each array element has these fields.

| Field | Type | Meaning |
|---|---|---|
| `actor.user_id` | string, required | Anthropic user id |
| `starting_at` | string (RFC 3339), required | Bucket start |
| `product` | string | Such as `claude_code` |
| `ending_at` | string | Bucket end, exclusive (vendor-documented) |
| `model` | string | Model id or display name (vendor-documented) |
| `total_tokens`, `uncached_input_tokens`, `output_tokens`, `cache_read_input_tokens` | integer | Token counts (vendor-documented) |
| `cache_creation` | object | Cache-write tokens by TTL (vendor-documented) |
| `server_tool_use` | object | Such as `web_search_requests` (vendor-documented) |
| `requests` | integer | API requests (vendor-documented) |

### Example

```json
[
  {"actor": {"type": "user_actor", "user_id": "user_A"},
   "total_tokens": 1000, "uncached_input_tokens": 400, "output_tokens": 500,
   "cache_read_input_tokens": 80, "requests": 17, "model": "claude-opus-4",
   "product": "claude_code", "starting_at": "2026-08-25T00:00:00Z"}
]
```

Verified by running the operation against the mocked vendor gateway, on the documented row and a minimal row (`connectors/anthropic/schema_test.go`).

Schema: [schemas/claude-code/fetch_analytics_user_usage.schema.json](../../schemas/claude-code/fetch_analytics_user_usage.schema.json)

## fetch_organization_members

One record per member per snapshot day. Each run re-reads the whole roster.

| Property | Value |
|---|---|
| ExternalID | `<id>\|<YYYY-MM-DD>`, the member and the UTC day of the run |
| Encoding | One JSON object: the vendor's member from `/v1/organizations/users` |
| DataDate | 00:00 UTC on the day the snapshot was taken |
| Metadata keys | None |

A member who leaves is absent from the next snapshot. No tombstone is emitted. Records are emitted only once the whole roster has been read.

### Fields

| Field | Type | Meaning |
|---|---|---|
| `id` | string, required | Anthropic user id |
| `email`, `name` | string | The member (vendor-documented) |
| `role` | string | Such as `admin`, `user` or `claude_code_user`. The only licence signal (vendor-documented) |
| `added_at` | string | When the member joined, RFC 3339 (vendor-documented) |
| `type` | string | `user` (vendor-documented) |

### Example

```json
{"id": "user_A", "added_at": "2026-03-01T10:00:00Z", "email": "a@example.com",
 "name": "A", "role": "claude_code_user", "type": "user"}
```

Verified by running the operation against the mocked vendor gateway, on the documented member and a minimal one (`connectors/anthropic/schema_test.go`).

Schema: [schemas/claude-code/fetch_organization_members.schema.json](../../schemas/claude-code/fetch_organization_members.schema.json)
