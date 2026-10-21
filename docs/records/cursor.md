# Cursor records

The connector id is `cursor`. It has one operation, and each payload is a raw Cursor event, stored as the vendor sent it.

A payload is JSON bytes. In a JSON-encoded envelope the `payload` field is that JSON, base64-encoded.

## fetch_usage_events

One record per billed model call, from `POST /teams/filtered-usage-events`.

| Property | Value |
|---|---|
| ExternalID | `<conversationId>#<timestamp>`, for example `conv-a#1787184000000`. The vendor gives an event no id of its own. |
| Encoding | The vendor's event object, verbatim. The connector parses only three fields. |
| DataDate | Midnight UTC of the day the call was billed. |
| Metadata keys | `conversation_id`, `user_email` (empty when the event has none) |

An event without a `conversationId`, or with a `timestamp` that is not a positive integer, is skipped and not emitted.

### Fields

The connector guarantees the first two fields. The rest are the vendor's, typed as seen in a live capture.

| Field | Type | Meaning |
|---|---|---|
| `timestamp` | string | Billed time in epoch milliseconds, as a decimal string. Not a number. |
| `conversationId` | string | The conversation the call belonged to. Never empty. |
| `userEmail` | string | The developer's email. Can be absent. |
| `model` | string | Model used. Mixes internal ids with display names. |
| `kind` | string | A label such as `Example plan`. Not a stable code. |
| `maxMode` | boolean | Whether Max mode was on. |
| `requestsCosts` | number | Request units charged. |
| `isTokenBasedCall` | boolean | Whether the call was billed by tokens. |
| `isChargeable` | boolean | Whether the call is chargeable. |
| `isHeadless` | boolean | Whether the call came from a headless client. |
| `tokenUsage` | object | `inputTokens`, `outputTokens`, `cacheWriteTokens`, `cacheReadTokens` and `totalCents`. Absent on some events. |
| `chargedCents` | number | Amount charged in US cents, fractional. |
| `cursorTokenFee` | number | Cursor's token fee. |
| `serviceAccountId` | string | The literal string `"null"` when there is no service account, not JSON null. |

The vendor can add fields. They are kept.

### Example

```json
{
  "timestamp": "1787184000000",
  "userEmail": "dev@example.com",
  "model": "gpt-5",
  "kind": "Example plan",
  "maxMode": false,
  "requestsCosts": 1,
  "isTokenBasedCall": true,
  "isChargeable": true,
  "isHeadless": false,
  "tokenUsage": {"inputTokens": 1000, "outputTokens": 100, "cacheWriteTokens": 0, "cacheReadTokens": 200, "totalCents": 2.5},
  "chargedCents": 2.5,
  "cursorTokenFee": 0,
  "serviceAccountId": "null",
  "conversationId": "00000000-0000-4000-8000-000000000000"
}
```

Verified by running the operation against a fake gateway that returns the live-captured event and two sparser ones (`connectors/cursor/schema_test.go`).

Schema: [schemas/cursor/fetch_usage_events.schema.json](../../schemas/cursor/fetch_usage_events.schema.json)
