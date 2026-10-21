// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

func init() {
	const c = "gitlab"

	declareNoRecords(c, "fetch_mr_diffs", "writes diffs to the blob store and reports status to the host work queue; emits no record")
	declareNoRecords(c, "drain_defect_lineage", "writes blame results to the host's BlameSink; emits no record")
	declareNoRecords(c, "drain_defect_scoring", "writes scores to the host's BlameSink; emits no record")

	declareSimGap(c, "webhook_events", "webhook-only: sync cannot run it; connectors/gitlab/schema_gitlab_test.go validates its records")
	declareSimGap(c, "fetch_file_content", "host-only: needs the host work queue; connectors/gitlab/schema_gitlab_test.go validates its records")
	declareSimGap(c, "fetch_mr_diffs", "host-only: needs the host work queue")
	declareSimGap(c, "drain_defect_lineage", "host-only: needs the host BlameSink and lineage walker")
	declareSimGap(c, "drain_defect_scoring", "host-only: needs the host BlameSink and defect scorer")
}
