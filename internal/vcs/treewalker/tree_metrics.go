// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package treewalker

import (
	"github.com/prometheus/client_golang/prometheus"
)

// reason label values for connector_repo_tree_fetch_errors_total.
// Unexported per project metrics convention — callers use the exported
// RecordTreeFetch* helpers below instead of passing label strings directly.
const (
	reasonNetwork  = "network"
	reasonS3Upload = "s3_upload"
	reasonInternal = "internal"
)

var (
	// repoTreeFetchErrorsTotal counts errors encountered while fetching a repo
	// tree (branch SHA resolution, tree walk, or S3 upload). One label set per
	// (connector, reason); callers classify the error before incrementing.
	repoTreeFetchErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_repo_tree_fetch_errors_total",
			Help: "Total errors encountered while fetching a repository tree, classified by connector and reason.",
		},
		[]string{"connector", "reason"},
	)

	// repoTreeFetchedTotal counts successful repo tree uploads to S3. The
	// truncated label is "true" when the vendor's recursive result was
	// incomplete and the BFS fallback was used.
	repoTreeFetchedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_vcs_repositories_tree_fetched_total",
			Help: "Total repository trees successfully fetched and uploaded to S3.",
		},
		[]string{"connector", "truncated"},
	)
)

// RecordTreeFetchNetworkError increments the error counter for a network-layer failure.
func RecordTreeFetchNetworkError(connector string) {
	repoTreeFetchErrorsTotal.WithLabelValues(connector, reasonNetwork).Inc()
}

// RecordTreeFetchS3UploadError increments the error counter for an S3 upload failure.
func RecordTreeFetchS3UploadError(connector string) {
	repoTreeFetchErrorsTotal.WithLabelValues(connector, reasonS3Upload).Inc()
}

// RecordTreeFetchInternalError increments the error counter for unexpected internal errors.
func RecordTreeFetchInternalError(connector string) {
	repoTreeFetchErrorsTotal.WithLabelValues(connector, reasonInternal).Inc()
}

// RecordTreeFetched increments the success counter for the given connector.
// truncated should be "true" if the tree was only partially fetched.
func RecordTreeFetched(connector string, truncated bool) {
	tv := "false"
	if truncated {
		tv = "true"
	}
	repoTreeFetchedTotal.WithLabelValues(connector, tv).Inc()
}
