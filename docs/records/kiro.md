# Kiro records

The connector id is `kiro`. It has one operation, and each payload is a pointer to a CSV report the connector copied into the runner's blob store.

A payload is JSON bytes. In a JSON-encoded envelope the `payload` field is that JSON, base64-encoded.

## fetch_user_activity_report

One record per client type per UTC day. Kiro writes one CSV file per client type per day into the customer's bucket. The connector copies the latest file for each client type and emits a record that points at the copy.

| Property | Value |
|---|---|
| ExternalID | The client type from the file name, for example `KIRO_IDE` |
| Encoding | A JSON object written by the connector (`kiro.PointerPayload`). It never holds the CSV bytes. |
| DataDate | Midnight UTC of the report's day. |
| Metadata keys | `client_type` (same as the ExternalID), `day` (`YYYY-MM-DD`) |

The ExternalID repeats across days. A record is unique on the ExternalID with the DataDate. A republished day overwrites the same copy and the same record.

The connector does not parse the CSV. Read it from the blob store at `s3_key`.

### Fields

| Field | Type | Meaning |
|---|---|---|
| `s3_bucket` | string, optional | Blob store bucket holding the copy. Absent when the store cannot name itself, such as a memory or directory store. `s3_key` then locates the copy in the store the runner is configured with. |
| `s3_key` | string | Key of the copy: `kiro/<organization>/<yyyy>/<mm>/<dd>/<client type>_user_report.csv` |
| `source_bucket` | string | The customer's bucket the vendor wrote to. |
| `source_key` | string | The vendor's key, ending `<client type>_<account>_user_report_<timestamp>.csv` |
| `content_sha256` | string | Lowercase hex SHA-256 of the stored bytes. |
| `source_last_modified` | string | The source object's LastModified, RFC 3339 in UTC, to the second. |

Every other field is always present. The object has no other keys.

### Example

```json
{
  "s3_bucket": "tetriz-blobs",
  "s3_key": "kiro/11111111-2222-3333-4444-555555555555/2026/10/04/KIRO_IDE_user_report.csv",
  "source_bucket": "acme-kiro-reports",
  "source_key": "kiro/AWSLogs/123456789012/KiroLogs/user_report/us-east-1/2026/10/04/00/KIRO_IDE_123456789012_user_report_1717171717.csv",
  "content_sha256": "9f2c0e3f8a1b4d5c6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5",
  "source_last_modified": "2026-10-05T01:12:09Z"
}
```

Verified by running the operation against a fake bucket, with a blob store that names its bucket and one that does not (`connectors/kiro/schema_test.go`).

Schema: [schemas/kiro/fetch_user_activity_report.schema.json](../../schemas/kiro/fetch_user_activity_report.schema.json)
