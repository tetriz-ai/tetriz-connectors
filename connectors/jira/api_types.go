// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"fmt"
	"strings"
	"time"
)

// Lightweight Jira Cloud API response types used only for JSON unmarshaling.
// These mirror the Jira REST API v3 shapes — NOT a downstream domain model.
// The full JSON payload goes into RawRecord.Payload as opaque bytes.
// We only extract enough fields for ExternalID, cursor advancement, and pagination.

// jiraTime handles Jira's non-RFC3339 timestamp format (e.g. "2026-01-16T15:03:31.606+0200").
// Go's time.Time expects "+02:00" but Jira sends "+0200".
type jiraTime struct {
	time.Time
}

func (jt *jiraTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "" || s == "null" {
		return nil
	}
	// Try RFC3339 first, then Jira's format without colon in timezone.
	t, err := time.Parse("2006-01-02T15:04:05.000Z0700", s)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05Z0700", s)
	}
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
	}
	if err != nil {
		return fmt.Errorf("parsing jira timestamp %q: %w", s, err)
	}
	jt.Time = t
	return nil
}

// --- Project types ---

type jiraProject struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Key is the short alphanumeric project identifier used in issue keys (e.g. "PROJ" in "PROJ-123").
	Key string `json:"key"`
	// ProjectTypeKey categorises the project by workflow template (e.g. "software", "business", "service_desk").
	// It is distinct from Key — Key identifies the project, ProjectTypeKey describes its type.
	ProjectTypeKey string `json:"projectTypeKey"`
}

// --- Issue types ---

type jiraIssue struct {
	ID     string          `json:"id"`
	Key    string          `json:"key"`
	Self   string          `json:"self"`
	Fields jiraIssueFields `json:"fields"`
}

type jiraIssueFields struct {
	Updated        jiraTime  `json:"updated"`
	Created        jiraTime  `json:"created"`
	Summary        string    `json:"summary"`
	ResolutionDate *jiraTime `json:"resolutiondate,omitempty"`
	Status         *struct {
		Name           string `json:"name"`
		StatusCategory struct {
			Key string `json:"key"`
		} `json:"statusCategory"`
	} `json:"status,omitempty"`
	Project *struct {
		Key string `json:"key"`
	} `json:"project,omitempty"`
}

// --- Comment types ---

type jiraComment struct {
	ID      string   `json:"id"`
	Created jiraTime `json:"created"`
}

// --- Worklog types ---

type jiraWorklog struct {
	ID      string   `json:"id"`
	Created jiraTime `json:"created"`
}

// --- Agile: Board and Sprint types ---

// jiraBoard is a lightweight view of a Jira Agile board. Only the fields used
// to iterate sprints and resolve the owning project are extracted — the full
// raw board JSON is not emitted as a record.
type jiraBoard struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Location *struct {
		// ProjectID is the numeric project id; ProjectKey is the short alphanumeric
		// project identifier. Either may be absent on boards without a project binding.
		ProjectID  int64  `json:"projectId,omitempty"`
		ProjectKey string `json:"projectKey,omitempty"`
	} `json:"location,omitempty"`
}

// jiraSprint is a lightweight view of a Jira sprint. Only the identifier is
// extracted here — the full sprint payload (name, state, startDate, endDate,
// completeDate, originBoardId, goal) is passed through as RawRecord.Payload.
type jiraSprint struct {
	ID int64 `json:"id"`
}
