// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package vendorids holds the canonical connector/vendor identifiers shared by
// the connectors and their downstream consumers. Each value is the connector_id that is
// persisted on every record, and keys per-vendor behaviour, so it must stay
// the single source of truth rather than being re-typed as a literal at each use.
package vendorids

// VCS connector IDs. Untyped string constants so callers can assign them to a
// typed alias (e.g. sdk.ConnectorID) at compile time without conversion.
const (
	GitHub    = "github"
	GitLab    = "gitlab"
	Bitbucket = "bitbucket"
)

// VCSConnectorIDs is the canonical list of VCS connector IDs — the single source
// of truth for callers that must iterate every VCS platform (e.g. resolving a
// person's author identities across platforms, or validating a connector_id).
var VCSConnectorIDs = []string{GitHub, GitLab, Bitbucket}

// Connector operation IDs that are load-bearing ACROSS the connector↔consumer boundary and
// must stay one source of truth (a rename on one side would otherwise silently break the
// contract with no compile error). OpFetchFileContent threads the VCS content pipeline (AI coding-agent configuration files):
// the connector's emit → the record's operation_id → the consumer's work-list's in-flight guard
// → a consumer's pre-pass scope. OpFetchBranchingModel tags the Bitbucket
// per-repo branching-model companion record: the
// connector's fetch_branches emits it under this id so a vendor-agnostic consumer,
// which lists only fetch_branches, never sees it, and the merge-flow detector reads it back by
// this id. Each side aliases these to its own local name rather than re-typing the literal.
const (
	OpFetchFileContent    = "fetch_file_content"
	OpFetchBranchingModel = "fetch_branching_model"
)

// BranchingModelRecordSuffix is appended to a repo's platform_repo_id to form the record_key of
// its OpFetchBranchingModel companion record. Shared across the connector↔consumer boundary so
// the emit and the read agree on one key; a drift on either side would silently drop the prior.
const BranchingModelRecordSuffix = ":__branching_model__"
