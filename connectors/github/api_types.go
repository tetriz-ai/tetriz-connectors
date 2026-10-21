// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Lightweight GitHub API response types used only for JSON unmarshaling.
// These mirror the GitHub REST/GraphQL API shapes — NOT a downstream domain model.
// The full JSON payload goes into RawRecord.Payload as opaque bytes.
// We only extract enough fields for ExternalID, cursor advancement, and pagination.

// --- REST API types ---

type ghPullRequest struct {
	Number    int       `json:"number"`
	UpdatedAt time.Time `json:"updated_at"`
	State     string    `json:"state"`
	User      ghUser    `json:"user"`
	Base      ghRef     `json:"base"`
}

type ghIssue struct {
	Number    int       `json:"number"`
	UpdatedAt time.Time `json:"updated_at"`
	State     string    `json:"state"`
	User      ghUser    `json:"user"`
}

type ghReview struct {
	ID          int64     `json:"id"`
	User        ghUser    `json:"user"`
	State       string    `json:"state"`
	SubmittedAt time.Time `json:"submitted_at"`
}

type ghPRComment struct {
	ID        int64     `json:"id"`
	User      ghUser    `json:"user"`
	CreatedAt time.Time `json:"created_at"`
}

type ghRepository struct {
	ID                       int64     `json:"id"`
	NodeID                   string    `json:"node_id"`
	Name                     string    `json:"name"`
	FullName                 string    `json:"full_name"`
	Private                  bool      `json:"private"`
	Archived                 bool      `json:"archived"`
	Disabled                 bool      `json:"disabled"`
	DefaultBranch            string    `json:"default_branch"`
	Visibility               string    `json:"visibility"`
	Fork                     bool      `json:"fork"`
	IsTemplate               bool      `json:"is_template"`
	Language                 string    `json:"language,omitempty"`
	Size                     int64     `json:"size"`
	Topics                   []string  `json:"topics,omitempty"`
	HasIssues                bool      `json:"has_issues"`
	HasWiki                  bool      `json:"has_wiki"`
	HasPages                 bool      `json:"has_pages"`
	HasProjects              bool      `json:"has_projects"`
	AllowForking             bool      `json:"allow_forking"`
	WebCommitSignoffRequired bool      `json:"web_commit_signoff_required"`
	HTMLURL                  string    `json:"html_url"`
	SSHURL                   string    `json:"ssh_url"`
	CloneURL                 string    `json:"clone_url"`
	Homepage                 string    `json:"homepage,omitempty"`
	Description              string    `json:"description,omitempty"`
	StargazersCount          int       `json:"stargazers_count"`
	WatchersCount            int       `json:"watchers_count"`
	ForksCount               int       `json:"forks_count"`
	SubscribersCount         int       `json:"subscribers_count"`
	OpenIssuesCount          int       `json:"open_issues_count"`
	NetworkCount             int       `json:"network_count"`
	CreatedAt                time.Time `json:"created_at"`
	UpdatedAt                time.Time `json:"updated_at"`
	PushedAt                 time.Time `json:"pushed_at"`
	Owner                    struct {
		Login string `json:"login"`
	} `json:"owner"`
	License *struct {
		Key string `json:"key"`
	} `json:"license,omitempty"`
	Parent *struct {
		ID int64 `json:"id"`
	} `json:"parent,omitempty"`
	// Merge-method toggles: available from the individual repo REST endpoint and
	// from a separate GraphQL query (buildRepoMergeMethodsQuery). Not present in
	// the listing response; populated by fetchMergeMethodToggles in fetch_repos.
	AllowSquashMerge bool `json:"allow_squash_merge"`
	AllowMergeCommit bool `json:"allow_merge_commit"`
	AllowRebaseMerge bool `json:"allow_rebase_merge"`
}

type ghUser struct {
	Login string `json:"login"`
	ID    int64  `json:"id"`
	// Type is the GitHub account type: "User", "Bot", "Organization", "Mannequin".
	// Used by reviewer-load filtering to skip bot reviewers/commenters.
	Type string `json:"type,omitempty"`
}

type ghRef struct {
	Repo ghRepoRef `json:"repo"`
}

type ghRepoRef struct {
	FullName string `json:"full_name"`
}

// --- GraphQL response types ---

type ghGraphQLData struct {
	Repository ghGQLRepository `json:"repository"`
}

type ghGQLRepository struct {
	// DatabaseID is GitHub's stable internal repo ID; survives renames and
	// transfers. Surfaced into record metadata platform_repo_id so the
	// consumer can populate a consumer's pull-request repository id without falling
	// back to repo_full_name.
	DatabaseID   int64                            `json:"databaseId"`
	PullRequests *ghGQLConnection[ghGQLPRNode]    `json:"pullRequests,omitempty"`
	Ref          *ghGQLRef                        `json:"defaultBranchRef,omitempty"`
	CustomRef    *ghGQLRef                        `json:"ref,omitempty"`
	Issues       *ghGQLConnection[ghGQLIssueNode] `json:"issues,omitempty"`
	// Merge-method toggles: true when the method is enabled for the repo.
	// Fetched separately by fetchMergeMethodToggles and merged into the raw record.
	SquashMergeAllowed bool `json:"squashMergeAllowed"`
	MergeCommitAllowed bool `json:"mergeCommitAllowed"`
	RebaseMergeAllowed bool `json:"rebaseMergeAllowed"`
}

type ghGQLRef struct {
	Target ghGQLTarget `json:"target"`
}

type ghGQLTarget struct {
	History *ghGQLConnection[ghGQLCommitNode] `json:"history,omitempty"`
}

type ghGQLConnection[T any] struct {
	Nodes    []T        `json:"nodes"`
	PageInfo ghPageInfo `json:"pageInfo"`
}

type ghPageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type ghGQLPRNode struct {
	Number       int        `json:"number"`
	UpdatedAt    time.Time  `json:"updatedAt"`
	Title        string     `json:"title"`
	URL          string     `json:"url"`
	State        string     `json:"state"`
	BaseRefName  string     `json:"baseRefName"`
	HeadRefName  string     `json:"headRefName"`
	CreatedAt    time.Time  `json:"createdAt"`
	MergedAt     *time.Time `json:"mergedAt"`
	ClosedAt     *time.Time `json:"closedAt"`
	Additions    int        `json:"additions"`
	Deletions    int        `json:"deletions"`
	ChangedFiles int        `json:"changedFiles"`
	Mergeable    string     `json:"mergeable"` // MERGEABLE | CONFLICTING | UNKNOWN
	Author       struct {
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	} `json:"author"`
	// MergedBy is selected by buildPRsQuery; null on an unmerged PR or a deleted account.
	// Declared so the re-marshalled payload keeps it.
	MergedBy *struct {
		Login string `json:"login"`
	} `json:"mergedBy"`
	Comments struct {
		TotalCount int `json:"totalCount"`
	} `json:"comments"`
	// Reviews carries each review's submitted time in the emitted payload. Consumers that measure
	// review latency read it, so keep the field.
	Reviews struct {
		TotalCount int `json:"totalCount"`
		Nodes      []struct {
			Author struct {
				Login      string `json:"login"`
				DatabaseID int64  `json:"databaseId"`
			} `json:"author"`
			State       string    `json:"state"`
			SubmittedAt time.Time `json:"submittedAt"`
		} `json:"nodes"`
	} `json:"reviews"`
	ReviewDecision *string `json:"reviewDecision"`
	ReviewRequests struct {
		TotalCount int `json:"totalCount"`
		Nodes      []struct {
			RequestedReviewer struct {
				Login string `json:"login"`
				// DatabaseID is the key the identity layer stores GitHub users under.
				// Absent for a Team reviewer, hence omitempty.
				DatabaseID int64  `json:"databaseId,omitempty"`
				Slug       string `json:"slug"`
			} `json:"requestedReviewer"`
		} `json:"nodes"`
	} `json:"reviewRequests"`
	Body    string `json:"body"`
	IsDraft bool   `json:"isDraft"`
	// MergeCommit drives merge_strategy derivation.
	// Null on un-merged PRs. parents.totalCount==1 ⇒ squash/rebase, ==2 ⇒ merge-commit;
	// message feeds the squash-by-title heuristic in mergestrategy.FromGitHub.
	MergeCommit *struct {
		Message string `json:"message"`
		Parents struct {
			TotalCount int `json:"totalCount"`
		} `json:"parents"`
	} `json:"mergeCommit"`
	ClosingIssuesReferences struct {
		Nodes []struct {
			Number int    `json:"number"`
			Title  string `json:"title"`
			State  string `json:"state"`
			// Repository carries nameWithOwner so cross-repo issue keys
			// resolve correctly. NULL on pre-extension
			// the record store.
			Repository struct {
				NameWithOwner string `json:"nameWithOwner"`
			} `json:"repository"`
		} `json:"nodes"`
	} `json:"closingIssuesReferences"`
	Files struct {
		Nodes []struct {
			Path      string `json:"path"`
			Additions int    `json:"additions"`
			Deletions int    `json:"deletions"`
		} `json:"nodes"`
		PageInfo struct {
			HasNextPage bool `json:"hasNextPage"`
		} `json:"pageInfo"`
	} `json:"files"`
	// Commits selects the earliest commit on the PR's branch.
	// Covers squash-merged PRs whose branch commits never reach default-branch history.
	Commits struct {
		Nodes []struct {
			Commit struct {
				AuthoredDate  time.Time `json:"authoredDate"`
				CommittedDate time.Time `json:"committedDate"`
			} `json:"commit"`
		} `json:"nodes"`
	} `json:"commits"`
	// IsCrossRepository is true when the PR head repository differs from the base
	// repository (fork PRs). GitHub GraphQL field; false for same-repo PRs.
	IsCrossRepository   bool `json:"isCrossRepository"`
	HeadRepositoryOwner struct {
		Login string `json:"login"`
	} `json:"headRepositoryOwner"`
}

type ghGQLCommitNode struct {
	OID                     string    `json:"oid"`
	AuthoredDate            time.Time `json:"authoredDate"`
	CommittedDate           time.Time `json:"committedDate"`
	Message                 string    `json:"message"`
	Additions               int       `json:"additions"`
	Deletions               int       `json:"deletions"`
	ChangedFilesIfAvailable int       `json:"changedFilesIfAvailable"`
	Author                  struct {
		Name  string `json:"name"`
		Email string `json:"email"`
		User  struct {
			Login      string `json:"login"`
			DatabaseID int64  `json:"databaseId"`
		} `json:"user"`
	} `json:"author"`
	Parents struct {
		TotalCount int `json:"totalCount"`
	} `json:"parents"`
	// AssociatedPullRequests links default-branch commits back to their PR(s).
	// first: 5 is generous — most commits belong to one PR.
	AssociatedPullRequests struct {
		Nodes []struct {
			Number int `json:"number"`
		} `json:"nodes"`
	} `json:"associatedPullRequests"`
}

type ghGQLIssueComment struct {
	ID     string `json:"id"`
	Body   string `json:"body"`
	Author struct {
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	} `json:"author"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ghGQLIssueNode is the typed view of one issue from buildIssuesQuery.
// fetchIssuesForRepo unmarshals GitHub's response into this struct and, after
// merging any tail-fetched comment pages, re-marshals it to produce the
// fetch_issues record payload. Invariant: every field buildIssuesQuery
// selects must also be declared here — a field present in the query but absent
// from this struct is silently dropped by the re-marshal and never reaches a
// downstream consumer. The Test*_Preserves* round-trip tests guard this.
type ghGQLIssueNode struct {
	Number      int        `json:"number"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	Title       string     `json:"title"`
	URL         string     `json:"url"`
	State       string     `json:"state"`
	StateReason *string    `json:"stateReason"`
	CreatedAt   time.Time  `json:"createdAt"`
	ClosedAt    *time.Time `json:"closedAt"`
	Body        string     `json:"body"`
	Repository  struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"repository"`
	Author struct {
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	} `json:"author"`
	Assignees struct {
		Nodes []struct {
			Login      string `json:"login"`
			DatabaseID int64  `json:"databaseId"`
		} `json:"nodes"`
	} `json:"assignees"`
	Labels struct {
		Nodes []struct {
			Name string `json:"name"`
		} `json:"nodes"`
	} `json:"labels"`
	Milestone *struct {
		Number int64      `json:"number"`
		Title  string     `json:"title"`
		State  string     `json:"state"`
		DueOn  *time.Time `json:"dueOn"`
	} `json:"milestone"`
	// IssueType is GitHub's native issue type (a 2024 Issues feature); nil when
	// the issue has no native type (org hasn't enabled them, or none assigned).
	// Feeds a consumer's issue type.
	IssueType *struct {
		Name string `json:"name"`
	} `json:"issueType,omitempty"`
	// Parent is the sub-issue parent reference; nil on top-level issues. Feeds
	// a consumer's parent-issue link.
	Parent *struct {
		Number     int64 `json:"number"`
		Repository struct {
			NameWithOwner string `json:"nameWithOwner"`
		} `json:"repository"`
	} `json:"parent,omitempty"`
	SubIssues struct {
		TotalCount int `json:"totalCount"`
	} `json:"subIssues"`
	Participants struct {
		TotalCount int `json:"totalCount"`
	} `json:"participants"`
	Comments struct {
		TotalCount int                 `json:"totalCount"`
		Nodes      []ghGQLIssueComment `json:"nodes"`
		PageInfo   ghPageInfo          `json:"pageInfo"`
	} `json:"comments"`
	ProjectItems struct {
		Nodes []struct {
			Project struct {
				Title  string `json:"title"`
				Number int    `json:"number"`
			} `json:"project"`
			FieldValues struct {
				Nodes []ghProjectFieldValue `json:"nodes"`
			} `json:"fieldValues"`
		} `json:"nodes"`
	} `json:"projectItems"`
	// ClosedByPullRequestsReferences carries the PRs GitHub reports as closing
	// this issue (PR-Issue linkage). The fetch_issues query requests it
	// (graphql.go buildIssuesQuery); this field must exist on the node struct
	// or the re-marshal that produces the record payload drops it before
	// a downstream consumer can read it.
	ClosedByPullRequestsReferences struct {
		Nodes []struct {
			Number     int    `json:"number"`
			State      string `json:"state"`
			Repository struct {
				NameWithOwner string `json:"nameWithOwner"`
			} `json:"repository"`
		} `json:"nodes"`
	} `json:"closedByPullRequestsReferences"`
}

// ghProjectFieldValue handles the GraphQL union type for project field values.
// JSON unmarshaling relies on field presence to determine the variant.
type ghProjectFieldValue struct {
	// SingleSelectValue
	Name string `json:"name,omitempty"`
	// TextValue
	Text string `json:"text,omitempty"`
	// NumberValue
	Number *float64 `json:"number,omitempty"`
	// IterationValue
	Title     string `json:"title,omitempty"`
	StartDate string `json:"startDate,omitempty"` // YYYY-MM-DD, present only on iteration values
	Duration  int    `json:"duration,omitempty"`  // days, iteration-only
	// All variants have a field with a name
	Field struct {
		Name string `json:"name"`
	} `json:"field"`
}

// --- Copilot API types ---

type ghCopilotMetrics struct {
	Date string `json:"date"`
}

type ghCopilotUsageResponse struct {
	// The API returns an array; each element represents one user's daily usage.
	GitHubComUserID copilotUserID `json:"github_com_user_id"`
	Day             string        `json:"day"`
}

// copilotUserID is github_com_user_id, which the API may send as a JSON
// number or a string. Both decode to the decimal string; null decodes to "".
type copilotUserID string

func (id *copilotUserID) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	switch {
	case s == "null":
		*id = ""
		return nil
	case strings.HasPrefix(s, `"`):
		var v string
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*id = copilotUserID(v)
		return nil
	default:
		var n json.Number
		if err := json.Unmarshal(b, &n); err != nil {
			return fmt.Errorf("github_com_user_id: %w", err)
		}
		*id = copilotUserID(n.String())
		return nil
	}
}

// --- Webhook payload types ---

type ghWebhookPayload struct {
	Action      string         `json:"action,omitempty"`
	Before      string         `json:"before,omitempty"` // previous head SHA (pull_request.synchronize)
	After       string         `json:"after,omitempty"`  // new head SHA (pull_request.synchronize)
	PullRequest *ghPullRequest `json:"pull_request,omitempty"`
	Issue       *ghIssue       `json:"issue,omitempty"`
	Review      *ghReview      `json:"review,omitempty"`
	Repository  *ghRepository  `json:"repository,omitempty"`
	Sender      *ghUser        `json:"sender,omitempty"`
}
