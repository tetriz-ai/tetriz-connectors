// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

func init() {
	const c = "github"
	declareNoRecords(c, "fetch_pr_diffs", "writes diffs to the blob store and reports them to the host work queue; emits no record")
	declareNoRecords(c, "drain_defect_lineage", "writes blame to the host blame sink; emits no record")
	declareNoRecords(c, "drain_defect_scoring", "writes scores to the host blame sink; emits no record")
	declareEmits(c, "webhook_events", "webhook_events", "pr_sync_events")

	const fixture = "checked by connectors/github/schema_github_test.go"
	declareSimGap(c, "webhook_events", "a webhook operation, not a pull operation; "+fixture)
	declareSimGap(c, "fetch_file_content", "needs the host work_queue; "+fixture)
	declareSimGap(c, "fetch_pr_diffs", "needs the host work_queue")
	declareSimGap(c, "drain_defect_lineage", "needs the host blame_sink and lineage_walker")
	declareSimGap(c, "drain_defect_scoring", "needs the host blame_sink and defect_scorer")
	declareSimEmpty(c, "fetch_issues_data", "the simulator synthesizes issues with an empty body, and an empty body emits nothing; "+fixture)
}
