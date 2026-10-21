// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// PageInfo is the standard GraphQL cursor-based pagination envelope.
type PageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

// graphqlResponse is the top-level wrapper for all Linear GraphQL responses.
type graphqlResponse[T any] struct {
	Data   T              `json:"data"`
	Errors []graphqlError `json:"errors"`
}

type graphqlError struct {
	Message    string `json:"message"`
	Extensions struct {
		Code string `json:"code"`
	} `json:"extensions"`
}

// graphQLErr turns a GraphQL errors array into the operation's error: a
// rate-limit SyncError when any error carries Linear's RATELIMITED code
// (https://linear.app/developers/rate-limiting), and a plain error naming the
// first message otherwise.
func graphQLErr(prefix string, errs []graphqlError) error {
	for _, e := range errs {
		if e.Extensions.Code == "RATELIMITED" {
			return &sdk.SyncError{ConnectorID: connectorID, Kind: sdk.ErrKindRateLimit, Retryable: true,
				Cause: fmt.Errorf("%s: %s", prefix, e.Message)}
		}
	}
	return fmt.Errorf("%s: %s", prefix, errs[0].Message)
}

// --- Team ---

// TeamNode is a single Linear team returned by the teams query.
type TeamNode struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Key         string     `json:"key"`
	Description string     `json:"description"`
	Timezone    string     `json:"timezone"`
	CreatedAt   time.Time  `json:"createdAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	ArchivedAt  *time.Time `json:"archivedAt"`
}

type teamConnection struct {
	Nodes    []TeamNode `json:"nodes"`
	PageInfo PageInfo   `json:"pageInfo"`
}

type teamsData struct {
	Teams teamConnection `json:"teams"`
}

// --- User ---

// UserNode is a single Linear workspace member returned by the users query.
type UserNode struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	DisplayName string     `json:"displayName"`
	Email       string     `json:"email"`
	Active      bool       `json:"active"`
	Admin       bool       `json:"admin"`
	CreatedAt   time.Time  `json:"createdAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	ArchivedAt  *time.Time `json:"archivedAt"`
}

type userConnection struct {
	Nodes    []UserNode `json:"nodes"`
	PageInfo PageInfo   `json:"pageInfo"`
}

type usersData struct {
	Users userConnection `json:"users"`
}

// --- Project ---

// ProjectNode is a single Linear project (cross-team initiative) returned by the projects query.
type ProjectNode struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	State       string     `json:"state"` // backlog | planned | started | paused | completed | canceled
	URL         string     `json:"url"`
	Progress    float64    `json:"progress"`   // 0.0–1.0, auto-computed
	TargetDate  string     `json:"targetDate"` // YYYY-MM-DD or empty
	CreatedAt   time.Time  `json:"createdAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	ArchivedAt  *time.Time `json:"archivedAt"`
}

type projectConnection struct {
	Nodes    []ProjectNode `json:"nodes"`
	PageInfo PageInfo      `json:"pageInfo"`
}

type projectsData struct {
	Projects projectConnection `json:"projects"`
}

// --- Cycle (sprint) ---

// CycleNode is a single Linear cycle (sprint equivalent) returned by the cycles query.
type CycleNode struct {
	ID          string     `json:"id"`
	Number      int        `json:"number"`
	Name        string     `json:"name"`
	StartsAt    time.Time  `json:"startsAt"`
	EndsAt      time.Time  `json:"endsAt"`
	CompletedAt *time.Time `json:"completedAt"`
	ArchivedAt  *time.Time `json:"archivedAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	Progress    float64    `json:"progress"`
	Team        TeamRef    `json:"team"`
}

type cycleConnection struct {
	Nodes    []CycleNode `json:"nodes"`
	PageInfo PageInfo    `json:"pageInfo"`
}

type cyclesData struct {
	Cycles cycleConnection `json:"cycles"`
}

// --- Issue ---

// IssueNode is a single Linear issue returned by the issues query.
type IssueNode struct {
	ID          string      `json:"id"`         // immutable; the ExternalID
	Identifier  string      `json:"identifier"` // e.g. "ENG-42"; changes when the issue moves team
	Title       string      `json:"title"`
	Description string      `json:"description"`
	Priority    int         `json:"priority"` // 0=none,1=urgent,2=high,3=medium,4=low
	Estimate    *float64    `json:"estimate"`
	DueDate     string      `json:"dueDate"` // YYYY-MM-DD or empty
	CreatedAt   time.Time   `json:"createdAt"`
	UpdatedAt   time.Time   `json:"updatedAt"`
	CompletedAt *time.Time  `json:"completedAt"`
	CanceledAt  *time.Time  `json:"canceledAt"`
	ArchivedAt  *time.Time  `json:"archivedAt"`
	State       StateRef    `json:"state"`
	Team        TeamRef     `json:"team"`
	Assignee    *UserRef    `json:"assignee"`
	Creator     *UserRef    `json:"creator"`
	Project     *ProjectRef `json:"project"`
	Cycle       *CycleRef   `json:"cycle"`
	Parent      *ParentRef  `json:"parent"`
	Labels      LabelNodes  `json:"labels"`
	// Attachments carries the issue's VCS attachment links (Linear's "link PR"
	// UI action / branch-name auto-link). The fetch_issues query filters the
	// connection to GitHub-sourced attachments and selects only url + sourceType.
	Attachments AttachmentNodes `json:"attachments"`
}

// AttachmentNodes is the (capped, non-sub-paginated) attachments connection on
// an issue. PageInfo.HasNextPage is true when the issue has more attachments
// than the query's first: cap, so a consumer can detect the dropped tail.
type AttachmentNodes struct {
	Nodes    []AttachmentRef `json:"nodes"`
	PageInfo PageInfo        `json:"pageInfo"`
}

// AttachmentRef is one issue attachment. URL is the linked external resource
// (for a GitHub PR, "https://github.com/{owner}/{repo}/pull/{n}"); SourceType
// is the integration family (e.g. "github"), used as a cheap pre-filter — the
// URL shape is the authoritative PR discriminator.
type AttachmentRef struct {
	URL        string `json:"url"`
	SourceType string `json:"sourceType"`
}

type issueConnection struct {
	Nodes    []IssueNode `json:"nodes"`
	PageInfo PageInfo    `json:"pageInfo"`
}

type issuesData struct {
	Issues issueConnection `json:"issues"`
}

// --- Label ---

// LabelNode is a single Linear issue label returned by the issueLabels query.
type LabelNode struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Color       string     `json:"color"`
	Description string     `json:"description"`
	CreatedAt   time.Time  `json:"createdAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	ArchivedAt  *time.Time `json:"archivedAt"`
	// Team is nil for workspace-level labels.
	Team *TeamRef `json:"team"`
}

type labelConnection struct {
	Nodes    []LabelNode `json:"nodes"`
	PageInfo PageInfo    `json:"pageInfo"`
}

type labelsData struct {
	IssueLabels labelConnection `json:"issueLabels"`
}

// --- Nested reference types ---

// TeamRef is a lightweight team reference embedded in other nodes.
type TeamRef struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

// UserRef is a lightweight user reference embedded in other nodes.
type UserRef struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// StateRef carries the workflow state of an issue, including the category type.
type StateRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Type is the Linear state category: triage|backlog|unstarted|started|completed|canceled|duplicate.
	Type string `json:"type"`
}

// ProjectRef is a lightweight project reference embedded in issues.
type ProjectRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// CycleRef is a lightweight cycle reference embedded in issues.
type CycleRef struct {
	ID     string `json:"id"`
	Number int    `json:"number"`
	Name   string `json:"name"`
}

// ParentRef is a lightweight reference to a parent issue (for sub-issues).
type ParentRef struct {
	ID         string  `json:"id"`
	Identifier string  `json:"identifier"`
	Team       TeamRef `json:"team"`
}

// LabelNodes is the labels connection embedded in an issue.
type LabelNodes struct {
	Nodes []LabelRef `json:"nodes"`
}

// LabelRef is a lightweight label reference embedded in an issue's labels list.
type LabelRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
