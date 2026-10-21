// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/blobstore"
	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// namedStore is a blob store that names its bucket, as the production client does.
type namedStore struct{ blobstore.Memory }

func (*namedStore) Bucket() string { return "tetriz-blobs" }

// TestSchema_FetchUserActivityReport runs the operation against the fake bucket and
// validates every emitted payload against
// schemas/kiro/fetch_user_activity_report.schema.json, once with a store that names its
// bucket and once with one that cannot, which omits s3_bucket rather than writing "".
func TestSchema_FetchUserActivityReport(t *testing.T) {
	t.Parallel()

	for name, store := range map[string]sdk.BlobStore{"named": &namedStore{}, "unnamed": &blobstore.Memory{}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			prefix, _ := yesterdaysPrefix(t)
			bucket := &fakeBucket{objects: map[string][]SourceObject{prefix: {
				{Key: prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv", LastModified: time.Now().UTC(), Size: 10},
				{Key: prefix + "KIRO_CLI_" + testAccount + "_user_report_1717171718.csv", LastModified: time.Now().UTC(), Size: 10},
			}}}
			sink := testkit.NewChannelSink()
			params := &sdk.ExecutionParams{
				Config:        &sdk.ConnectorConfig{ConnectorID: connectorID, OrganizationID: execTestOrg, Settings: execSettings(t)},
				Sink:          sink,
				VendorGateway: bucket,
				BlobStore:     store,
			}
			_, err := (&FetchUserActivityReportOp{}).Execute(context.Background(), params)
			require.NoError(t, err)

			records := sink.Records()
			require.Len(t, records, 2)
			for _, r := range records {
				assert.NoError(t, schemas.Validate(r.ConnectorID, r.OperationID, r.Payload), r.ExternalID)
				var fields map[string]any
				require.NoError(t, json.Unmarshal(r.Payload, &fields))
				if name == "named" {
					assert.Equal(t, "tetriz-blobs", fields["s3_bucket"])
				} else {
					assert.NotContains(t, fields, "s3_bucket", "an unknown bucket is absent, not an empty string")
				}
			}
		})
	}

	// The schema is closed: PointerPayload is the connector's own type, so a new field
	// must be added to the schema and the docs.
	b, err := json.Marshal(PointerPayload{
		Key: "kiro/o/2026/01/02/KIRO_IDE_user_report.csv", SourceBucket: "b", SourceKey: "k",
		ContentSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", SourceLastModified: "2026-01-02T00:00:00Z",
	})
	require.NoError(t, err)
	require.NoError(t, schemas.Validate("kiro", "fetch_user_activity_report", b))
	assert.Error(t, schemas.Validate("kiro", "fetch_user_activity_report",
		[]byte(strings.Replace(string(b), "{", `{"s3_bucket":"",`, 1))), "an empty bucket name is not a bucket")
	var withExtra map[string]any
	require.NoError(t, json.Unmarshal(b, &withExtra))
	withExtra["extra"] = 1
	b, err = json.Marshal(withExtra)
	require.NoError(t, err)
	assert.Error(t, schemas.Validate("kiro", "fetch_user_activity_report", b))
}
