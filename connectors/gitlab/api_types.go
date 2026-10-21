// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"fmt"
	"time"
)

// Minimal structs for JSON unmarshaling of GitLab API responses.
// Only fields needed for ExternalID, cursor advancement, and pagination.
//
// ExternalID() methods define the canonical record key for each entity type.
// Both poll operations and webhook handlers must use these methods so that
// a webhook re-delivery of the same entity upserts into the same row as the
// poll-fetched record.

type glProject struct {
	ID                int       `json:"id"`
	PathWithNamespace string    `json:"path_with_namespace"`
	Name              string    `json:"name"`
	Path              string    `json:"path"` // short repo name (no namespace)
	DefaultBranch     string    `json:"default_branch"`
	Visibility        string    `json:"visibility"` // "public", "internal", "private"
	CreatedAt         time.Time `json:"created_at"`
	// Extended fields used by FetchReposOp for RepositoryRecord mapping.
	Description       string     `json:"description"`
	Archived          bool       `json:"archived"`
	LastActivityAt    *time.Time `json:"last_activity_at"`
	StarCount         int        `json:"star_count"`
	ForksCount        int        `json:"forks_count"`
	OpenIssuesCount   int        `json:"open_issues_count"`
	IssuesEnabled     bool       `json:"issues_enabled"`
	WikiEnabled       bool       `json:"wiki_enabled"`
	WebURL            string     `json:"web_url"`
	SSHURLToRepo      string     `json:"ssh_url_to_repo"`
	HTTPURLToRepo     string     `json:"http_url_to_repo"`
	Topics            []string   `json:"topics"`
	ImportStatus      string     `json:"import_status"`
	EmptyRepo         bool       `json:"empty_repo"`
	ForkedFromProject *struct {
		ID int `json:"id"`
	} `json:"forked_from_project"`
	Namespace *struct {
		ID   int    `json:"id"`
		Path string `json:"path"`
		Kind string `json:"kind"`
	} `json:"namespace"`
}

func (p glProject) ExternalID() string {
	return fmt.Sprintf("project-%s", p.PathWithNamespace)
}

type glMergeRequest struct {
	ID              int        `json:"id"`
	IID             int        `json:"iid"` // project-scoped MR number
	ProjectID       int        `json:"project_id"`
	SourceProjectID int64      `json:"source_project_id"`
	TargetProjectID int64      `json:"target_project_id"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	MergedAt        *time.Time `json:"merged_at"`
	ClosedAt        *time.Time `json:"closed_at"`
	State           string     `json:"state"`
	TargetBranch    string     `json:"target_branch"`
	SourceBranch    string     `json:"source_branch"`
	// Diff stats — available when fetched with with_stats=true
	Additions    int64  `json:"additions"`
	Deletions    int64  `json:"deletions"`
	ChangesCount string `json:"changes_count"`
	// Author info for identity resolution
	Author struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Email    string `json:"email"`
		Bot      bool   `json:"bot"`
	} `json:"author"`
	// Comment/discussion counts
	UserNotesCount int64 `json:"user_notes_count"`
	// Assigned reviewers (GitLab 13.8+ returns this on the MR list object); the
	// count is stamped as reviews_requested metadata for parity with GitHub's
	// reviewRequests.totalCount. Only the count is
	// used here — a consumer parses the same raw mrRaw payload
	// independently (mrEntityPayload) to resolve each reviewer's identity, so
	// this connector-side struct doesn't need Username too.
	Reviewers []struct {
		ID int64 `json:"id"`
	} `json:"reviewers"`
}

func (mr glMergeRequest) ExternalID() string {
	return fmt.Sprintf("mr-%d!%d", mr.ProjectID, mr.IID)
}

type glCommit struct {
	ID            string    `json:"id"` // SHA
	CommittedDate time.Time `json:"committed_date"`
	AuthorEmail   string    `json:"author_email"`
}

func (c glCommit) ExternalID(projectPath string) string {
	return fmt.Sprintf("%s#%s", projectPath, c.ID)
}

type glIssue struct {
	ID        int        `json:"id"`
	IID       int        `json:"iid"` // project-scoped issue number
	ProjectID int        `json:"project_id"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	ClosedAt  *time.Time `json:"closed_at"`
	State     string     `json:"state"`
}

func (i glIssue) ExternalID() string {
	return fmt.Sprintf("issue-%d!%d", i.ProjectID, i.IID)
}

type glNote struct {
	ID        int       `json:"id"`
	ProjectID int       `json:"project_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	System    bool      `json:"system"`
	// Author of the note. GitLab's notes endpoint returns the author user
	// object inline; the `bot` flag is the authoritative bot signal for
	// reviewer-load filtering downstream.
	Author struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	} `json:"author"`
}

// glMRDiffEntry represents a single file-level diff entry from the
// GET /projects/:id/merge_requests/:iid/diffs endpoint.
type glMRDiffEntry struct {
	OldPath     string `json:"old_path"`
	NewPath     string `json:"new_path"`
	Diff        string `json:"diff"`
	NewFile     bool   `json:"new_file"`
	RenamedFile bool   `json:"renamed_file"`
	DeletedFile bool   `json:"deleted_file"`
}

// glMRFileLineStat stores per-file addition/deletion counts for the
// files_line_stats metadata field.
type glMRFileLineStat struct {
	Path      string `json:"path"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

type glApproval struct {
	User struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	} `json:"user"`
	CreatedAt time.Time `json:"created_at"`
}

type glApprovalsResponse struct {
	Approved   bool         `json:"approved"`
	ApprovedBy []glApproval `json:"approved_by"`
}

func (n glNote) ExternalID() string {
	return fmt.Sprintf("note-%d-%d", n.ProjectID, n.ID)
}

// Webhook-specific payload types — only fields needed for routing and external ID derivation.

type glPushHook struct {
	Project struct {
		PathWithNamespace string `json:"path_with_namespace"`
	} `json:"project"`
	Commits []struct {
		ID        string    `json:"id"` // SHA
		Timestamp time.Time `json:"timestamp"`
	} `json:"commits"`
	Ref string `json:"ref"`
}

func (h glPushHook) TagExternalID() string {
	return fmt.Sprintf("%s@%s", h.Project.PathWithNamespace, h.Ref)
}

type glMRHook struct {
	ObjectAttributes struct {
		IID       int        `json:"iid"`
		ProjectID int        `json:"project_id"`
		CreatedAt time.Time  `json:"created_at"`
		UpdatedAt time.Time  `json:"updated_at"`
		MergedAt  *time.Time `json:"merged_at"`
		ClosedAt  *time.Time `json:"closed_at"`
		State     string     `json:"state"`
		// Action is what the hook reports: open, reopen, update, close, merge,
		// approved and so on. Only "open" is the creation event.
		Action string `json:"action"`
	} `json:"object_attributes"`
}

func (h glMRHook) ExternalID() string {
	return fmt.Sprintf("mr-%d!%d", h.ObjectAttributes.ProjectID, h.ObjectAttributes.IID)
}

type glIssueHook struct {
	ObjectAttributes struct {
		IID         int        `json:"iid"`
		ProjectID   int        `json:"project_id"`
		Title       string     `json:"title"`
		AuthorID    int64      `json:"author_id"`
		AssigneeIDs []int64    `json:"assignee_ids"`
		CreatedAt   time.Time  `json:"created_at"`
		UpdatedAt   time.Time  `json:"updated_at"`
		ClosedAt    *time.Time `json:"closed_at"`
		State       string     `json:"state"`
		// DueDate is a date-only string ("2026-04-30") or empty — carries the
		// same milestone due-date value the REST payload nests under
		// milestone.due_date. Kept so the reshape doesn't NULL out a
		// already-set due date on every webhook delivery.
		DueDate string `json:"due_date"`
		// Type is GitLab's native issue type, Title-cased ("Issue",
		// "Incident", "Test Case", "Task") — the REST fetch_issues shape
		// (the consumer's GitLab issue type)
		// stores it lowercase with underscores, so callers reshaping this
		// hook must normalise it, not just lowercase it.
		Type string `json:"type"`
	} `json:"object_attributes"`
	// Project carries the path the REST issue payload's references.full
	// derives its project-path prefix from; the hook has no references field.
	Project struct {
		PathWithNamespace string `json:"path_with_namespace"`
	} `json:"project"`
	// Labels is the hook's own top-level array of label objects — distinct
	// from the REST fetch_issues payload's flat []string.
	Labels []struct {
		Title string `json:"title"`
	} `json:"labels"`
}

func (h glIssueHook) ExternalID() string {
	return fmt.Sprintf("issue-%d!%d", h.ObjectAttributes.ProjectID, h.ObjectAttributes.IID)
}

type glNoteHook struct {
	ObjectAttributes struct {
		ID        int       `json:"id"`
		ProjectID int       `json:"project_id"`
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
		// System marks a system-generated note (label change, assignment, "merged", etc.).
		System bool `json:"system"`
	} `json:"object_attributes"`
	// User is the author of the note. Top-level on the note hook payload.
	// `bot` is the authoritative reviewer-bot signal for consumer filtering.
	User struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	} `json:"user"`
	// MergeRequest is populated when the note is attached to an MR — the
	// reviewer-load metric only consumes MR notes. Surfaces the MR author
	// and the project ID so a consumer can do self-PR filtering and
	// platform_repo_id lookups without joining records.
	MergeRequest struct {
		IID    int `json:"iid"`
		Author struct {
			ID       int64  `json:"id"`
			Username string `json:"username"`
		} `json:"author"`
	} `json:"merge_request"`
	Project struct {
		ID int64 `json:"id"`
	} `json:"project"`
}

func (h glNoteHook) ExternalID() string {
	return fmt.Sprintf("note-%d-%d", h.ObjectAttributes.ProjectID, h.ObjectAttributes.ID)
}
