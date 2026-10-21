// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import "time"

// Lightweight Bitbucket API response types used only for JSON unmarshaling.
// These mirror the Bitbucket REST API v2.0 shapes — NOT a downstream domain model.
// The full JSON payload goes into RawRecord.Payload as opaque bytes.
// We only extract enough fields for ExternalID, cursor advancement, and pagination.

// --- Repository types ---

type bbRepository struct {
	UUID        string        `json:"uuid"`
	Slug        string        `json:"slug"`
	FullName    string        `json:"full_name"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	IsPrivate   bool          `json:"is_private"`
	Language    string        `json:"language"`
	Size        int64         `json:"size"` // bytes on disk
	SCM         string        `json:"scm"`
	UpdatedOn   time.Time     `json:"updated_on"`
	CreatedOn   time.Time     `json:"created_on"`
	Mainbranch  *bbBranchRef  `json:"mainbranch,omitempty"`
	Workspace   *bbWorkspace  `json:"workspace,omitempty"`
	Project     *bbProject    `json:"project,omitempty"`
	Parent      *bbRepository `json:"parent,omitempty"` // set when repo is a fork
	Links       *bbRepoLinks  `json:"links,omitempty"`
}

// bbBranchRef is the lightweight branch reference on the repository payload.
type bbBranchRef struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// bbWorkspace is the workspace stub embedded in the repository payload.
type bbWorkspace struct {
	UUID string `json:"uuid"`
	Slug string `json:"slug"`
	Name string `json:"name"`
}

// bbRepoLinks holds the self/html/clone link group on a repository.
type bbRepoLinks struct {
	HTML  *bbLink   `json:"html,omitempty"`
	Clone []bbClone `json:"clone,omitempty"`
}

// bbLink is a generic href container used across Bitbucket API link objects.
type bbLink struct {
	Href string `json:"href"`
}

// bbClone is one entry in the repository clone links list (https or ssh).
type bbClone struct {
	Name string `json:"name"`
	Href string `json:"href"`
}

type bbProject struct {
	Key  string `json:"key"`
	Name string `json:"name"`
	UUID string `json:"uuid"`
}

// --- Pull Request types ---

type bbPullRequest struct {
	ID          int       `json:"id"`
	Title       string    `json:"title"`
	State       string    `json:"state"`
	CreatedOn   time.Time `json:"created_on"`
	UpdatedOn   time.Time `json:"updated_on"`
	Author      *bbUser   `json:"author,omitempty"`
	Source      *bbPRRef  `json:"source,omitempty"`
	Destination *bbPRRef  `json:"destination,omitempty"`

	// Reviewers is the list of users explicitly requested as reviewers at PR
	// open time. Surfaced via the `?fields=+values.reviewers` expansion in the
	// PR list URL; absent from the default slim list response.
	Reviewers []bbUser `json:"reviewers,omitempty"`

	// Participants is the broader set of users who have engaged with the PR
	// (author, reviewers, commenters). We use it as a fallback when the
	// explicit Reviewers array is empty — some Bitbucket orgs surface
	// reviewers only through participants entries with role="REVIEWER".
	Participants []bbPRParticipant `json:"participants,omitempty"`
}

// bbPRParticipant mirrors the Bitbucket PR participant object.
type bbPRParticipant struct {
	User     *bbUser `json:"user,omitempty"`
	Role     string  `json:"role,omitempty"`
	Approved bool    `json:"approved,omitempty"`
	State    string  `json:"state,omitempty"`
}

type bbPRRef struct {
	Branch     *bbBranch     `json:"branch,omitempty"`
	Repository *bbRepository `json:"repository,omitempty"`
}

type bbBranch struct {
	Name string `json:"name"`
}

// --- Commit types ---

type bbCommit struct {
	Hash    string    `json:"hash"`
	Date    time.Time `json:"date"`
	Message string    `json:"message"`
	Author  *bbAuthor `json:"author,omitempty"`
}

type bbAuthor struct {
	Raw  string  `json:"raw"`
	User *bbUser `json:"user,omitempty"`
}

// --- User types ---

type bbUser struct {
	UUID        string `json:"uuid"`
	DisplayName string `json:"display_name"`
	Nickname    string `json:"nickname"`
	AccountID   string `json:"account_id"`
}

// --- Diffstat types ---

type bbDiffstatEntry struct {
	LinesAdded   int             `json:"lines_added"`
	LinesRemoved int             `json:"lines_removed"`
	Status       string          `json:"status"`
	New          *bbDiffstatFile `json:"new,omitempty"`
	Old          *bbDiffstatFile `json:"old,omitempty"`
}

type bbDiffstatFile struct {
	Path string `json:"path"`
}

// bbPRFileLineStat is the per-file change record stored in files_line_stats metadata.
type bbPRFileLineStat struct {
	Path      string `json:"path"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

// --- Issue types ---

type bbIssue struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	State     string    `json:"state"` // "new", "open", "resolved", "closed", "on hold", "wontfix", "invalid", "duplicate"
	CreatedOn time.Time `json:"created_on"`
	UpdatedOn time.Time `json:"updated_on"`
	Reporter  *bbUser   `json:"reporter,omitempty"`
	Assignee  *bbUser   `json:"assignee,omitempty"`
}

// --- Workspace member types ---

type bbWorkspaceMember struct {
	User *bbUser `json:"user"`
}

// WorkspaceMemberRecord is the payload emitted per Bitbucket workspace member.
// Downstream: A consumer resolves identities from this.
type WorkspaceMemberRecord struct {
	// AccountID is the stable Bitbucket account ID (e.g. "557058:...").
	AccountID string `json:"account_id"`
	// DisplayName is the human-readable display name.
	DisplayName string `json:"display_name,omitempty"`
	// Nickname is the Bitbucket username/slug (mutable).
	Nickname string `json:"nickname,omitempty"`
	// HintSource identifies where the identity data came from.
	HintSource string `json:"hint_source"`
	// Groups is the list of Bitbucket workspace group names this member belongs to.
	Groups []string `json:"groups,omitempty"`
}

// bbGroup represents a group from the Bitbucket v1.0 groups API.
type bbGroup struct {
	Name    string   `json:"name"`
	Slug    string   `json:"slug"`
	Members []bbUser `json:"members"`
}

// --- PR Activity types ---

type bbActivity struct {
	Approval *bbApproval `json:"approval,omitempty"`
	Comment  *bbComment  `json:"comment,omitempty"`
}

type bbApproval struct {
	Date string  `json:"date"`
	User *bbUser `json:"user,omitempty"`
}

type bbComment struct {
	ID        int64     `json:"id"`
	CreatedOn time.Time `json:"created_on"`
	User      *bbUser   `json:"user,omitempty"`
}
