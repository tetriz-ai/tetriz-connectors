// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package logger

// Field keys shared across the repository's log entries.
const (
	RedisKey      = "Redis Key"
	ArrayIndex    = "arrIndex"
	RequestType   = "request_type"
	Group         = "group"
	URL           = "url"
	Method        = "method"
	Status        = "status"
	OperationID   = "operation_id"
	ExternalID    = "external_id"
	UserEmail     = "user_email"
	OrgID         = "org_id"
	ConnectorID   = "connector_id"
	Trigger       = "trigger"
	SyncRunID     = "sync_run_id"
	TraceID       = "trace_id"
	Domain        = "domain"
	TeamID        = "team_id"
	PersonID      = "person_id"
	UserID        = "user_id"
	PullRequestID = "pull_request_id"
	IssueKey      = "issue_key"
	RepoFullName  = "repo_full_name"
	Type          = "type"
	State         = "state"
	Login         = "login"
	ErrorKind     = "error_kind"
	ErrorMessage  = "error_message"
	ErrorType     = "error_type"
)
