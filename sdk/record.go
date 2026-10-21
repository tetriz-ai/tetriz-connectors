// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"encoding/json"
	"time"
)

// RawRecord is the universal envelope for all fetched data.
// The framework never inspects Payload — it is opaque bytes.
type RawRecord struct {
	// ConnectorID identifies the source connector (e.g., "github", "jira").
	ConnectorID string `json:"connector_id"`

	// OperationID identifies which operation produced this record (e.g., "fetch_prs").
	OperationID string `json:"operation_id"`

	// OrganizationID scopes this record to a tenant.
	OrganizationID string `json:"organization_id"`

	// ExternalID is the record's identity in the source system. A sink should
	// deduplicate on (organization, connector, operation, ExternalID, DataDate).
	//
	// Compose it only from values the vendor cannot rename — a stable id, a period, or a
	// documented closed set (e.g. a PR number, a Slack message ts, an actor id plus a day).
	// A renameable component does not error when it moves; it changes the tuple, so the
	// entity forks into a second row and double-counts.
	ExternalID string `json:"external_id"`

	// Payload is the connector's record as JSON: normalized fields for some
	// operations, with the vendor body in vendorBlobJson where kept. The
	// connector defines each operation's shape; the normalized ones are the
	// messages in proto/tetriz/connectors/connector/v1. The runner never
	// deserializes it.
	Payload json.RawMessage `json:"payload"`

	// FetchedAt is when this record was retrieved from the vendor API.
	FetchedAt time.Time `json:"fetched_at"`

	// DataDate is the day the record's data describes. It is part of the dedup key,
	// so it must be fixed for an ExternalID: every fetch of the same item sets the
	// same value.
	//
	// Set it only for:
	//   - day-partitioned data: a usage or analytics row for one day, dated by that day;
	//   - an event record, dated by when the event happened: a commit's committed date,
	//     a comment's or a review's creation, a lifecycle transition (#opened, #merged).
	//
	// Leave it zero for a current-state snapshot: an entity's latest state (a #snapshot
	// record, a branch, a member, a project, a sprint, a config, a repository tree, a
	// description pointer). Never set it to the fetch time or to a vendor
	// last-updated time: either changes between runs, so a date-keyed sink gets a new
	// row for the same item on every run. With DataDate zero a consumer uses FetchedAt.
	DataDate time.Time `json:"data_date,omitempty"`

	// Metadata is optional key-value pairs for routing/filtering downstream
	// (e.g., "repo"="acme/widgets", "channel"="engineering").
	Metadata map[string]string `json:"metadata,omitempty"`
}
