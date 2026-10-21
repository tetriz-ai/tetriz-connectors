// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

func init() {
	const c = "bitbucket"

	declareNoRecords(c, "fetch_pr_diffs", "writes diffs to the blob store and reports them to the host work queue; emits no record")
	declareNoRecords(c, "drain_defect_lineage", "writes blame results to the host's blame sink; emits no record")
	declareNoRecords(c, "drain_defect_scoring", "writes defect scores to the host's blame sink; emits no record")

	// fetch_branches also emits one branching-model companion record per
	// repository, under its own operationId.
	declareEmits(c, "fetch_branches", "fetch_branches", "fetch_branching_model")

	declareSimGap(c, "webhook_events", "webhook-only: a host calls HandleWebhook; sync cannot run it (connectors/bitbucket/schema_bitbucket_test.go)")
	declareSimGap(c, "fetch_file_content", "needs the host work queue (connectors/bitbucket/schema_bitbucket_test.go)")
	declareSimGap(c, "fetch_pr_diffs", "needs the host work queue")
	declareSimGap(c, "drain_defect_lineage", "needs the host blame sink and lineage walker")
	declareSimGap(c, "drain_defect_scoring", "needs the host blame sink and defect scorer")
}
