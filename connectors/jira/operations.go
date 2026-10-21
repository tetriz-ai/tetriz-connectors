// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"go.uber.org/zap"

	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// safePageLen clamps a page length to the int32 range used by Jira's startAt
// paging counters. Page sizes are bounded by maxResults (hundreds), so the
// clamp only guards against a pathological response rather than real data.
func safePageLen(n int) int32 {
	if n >= 0 && n <= math.MaxInt32 {
		return int32(n)
	}
	return math.MaxInt32
}

// defaultLookback is the maximum history window for first-sync operations.
// When no cursor exists, operations fetch data from this far back instead of
// fetching the entire history.
const defaultLookback = 12 * 30 * 24 * time.Hour // ~12 months

// defaultCursorOverlapWindow is the fallback look-back applied to a non-zero
// cursor.Timestamp when the operator-configurable
// config.JiraConfig.CursorOverlapWindow is unset (nil JiraConfig or zero
// value). Defense-in-depth against boundary misses the
// (Timestamp, LastIssueID) tiebreaker could theoretically leave: JQL
// minute-precision rounding, clock skew between Jira's `updated` write and
// our cursor write, intra-second writes elided by the high-water mark.
// An idempotent upsert in the consumer absorbs the re-emission so
// no duplicate downstream rows result.
//
// Overridable via the Jira connector options (CursorOverlapWindow).
const defaultCursorOverlapWindow = 5 * time.Minute

// resolveCursorOverlap returns the configured overlap window, falling back to
// defaultCursorOverlapWindow when no operator-set value is available. Negative
// or zero configured values are treated as "use the default" so a typo in
// YAML can't silently disable the safety margin.
func resolveCursorOverlap(jiraCfg *connectorconfig.JiraConfig) time.Duration {
	if jiraCfg == nil || jiraCfg.CursorOverlapWindow <= 0 {
		return defaultCursorOverlapWindow
	}
	return jiraCfg.CursorOverlapWindow
}

// firstSyncSince returns a floor timestamp for first-sync runs.
// If since is zero (no prior cursor), it returns now minus defaultLookback.
// Otherwise it returns since unchanged.
func firstSyncSince(since time.Time) time.Time {
	if since.IsZero() {
		return time.Now().UTC().Add(-defaultLookback)
	}
	return since
}

// maxJQLZoneOffset is the widest offset a Jira user's profile time zone can have
// from UTC (UTC+14, Line Islands; the westmost, UTC-12, is narrower).
//
// Jira reads a JQL date literal such as "2026-05-01 12:00" in the searching
// user's profile time zone, not UTC, and the literal cannot carry a zone. Under
// OAuth 3LO the searching user is whoever authorized the connection, whose zone
// the connector does not know. The connector renders the cursor's UTC wall clock,
// so for a user west of UTC the boundary Jira applies lands up to this much after
// the cursor, and every update in that gap would be skipped for good. Widening the
// lower bound by this much keeps the applied boundary at or before the cursor in
// every zone. The cost is re-fetching up to this window each run, which the
// at-least-once delivery at the cursor boundary already absorbs.
const maxJQLZoneOffset = 14 * time.Hour

// effectiveSince returns the JQL `updated >=` lower bound for an incremental
// fetch. For a zero cursor (first sync), falls back to firstSyncSince. For a
// non-zero cursor, subtracts the operator-configured overlap as a safety
// margin against boundary misses (see defaultCursorOverlapWindow doc), plus
// maxJQLZoneOffset so the bound holds whatever time zone Jira reads it in.
func effectiveSince(cursorTime time.Time, overlap time.Duration) time.Time {
	if cursorTime.IsZero() {
		return firstSyncSince(cursorTime)
	}
	return cursorTime.Add(-overlap - maxJQLZoneOffset)
}

const (
	// jiraTimeFormat is the Jira-specific datetime format used in JQL queries.
	jiraTimeFormat = "2006-01-02 15:04"

	// projectsPageSize is the page size for Jira project list requests.
	projectsPageSize = 50

	// maxResultsPerPage is the maximum number of results per Jira issue search page.
	maxResultsPerPage = 100

	// commentsPageSize is the page size for Jira comment list requests.
	commentsPageSize = 50

	// worklogsPageSize is the page size for Jira worklog list requests.
	worklogsPageSize = 50

	// boardsPageSize is the page size for Jira Agile board list requests.
	boardsPageSize = 50

	// sprintsPageSize is the page size for Jira Agile sprint list requests.
	sprintsPageSize = 50

	// issueExpandFields specifies the Jira issue fields to expand in search responses.
	issueExpandFields = "changelog,renderedFields"

	// issueFields specifies which fields to return for each issue.
	issueFields = "*all"
)

// emitRecord emits a single raw JSON record to the sink.
func emitRecord(ctx context.Context, sink sdk.DataSink, opID sdk.OperationID, orgID string, externalID string, payload json.RawMessage, metadata map[string]string, dataDate time.Time) error {
	if err := sink.Emit(ctx, &sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    string(opID),
		OrganizationID: orgID,
		ExternalID:     externalID,
		Payload:        payload,
		FetchedAt:      time.Now().UTC(),
		DataDate:       dataDate,
		Metadata:       metadata,
	}); err != nil {
		return fmt.Errorf("emitting jira record %s: %w", externalID, err)
	}
	return nil
}

// copyMeta returns a shallow copy of metadata so callers can add per-record keys
// (e.g. event_type) without mutating the base metadata map shared across a loop
// iteration.
//
// This is specific to Jira's lifecycle-event model: a single issue emits
// multiple records (created, snapshot, resolved) each with a different
// event_type. All three share a baseMeta, so each must work on its own copy to
// avoid cross-contaminating siblings. Other connectors (GitHub, Bitbucket, etc.)
// emit one record per entity with no per-variant metadata mutations, so they
// never hit this pattern.
func copyMeta(metadata map[string]string) map[string]string {
	copied := make(map[string]string, len(metadata)+1)
	for key, value := range metadata {
		copied[key] = value
	}
	return copied
}

// projectCursorState tracks incremental sync progress for a single Jira
// project. The semantics of the fields differ slightly per operation:
//
//   - fetch_issues: paginates issue pages, may interrupt mid-pagination — uses
//     {Timestamp, PageToken, Since}. LastIssueID is unused (zero).
//   - fetch_worklogs / fetch_comments: iterates one issue at a time — uses
//     {Timestamp, LastIssueID} as a (high-water, tiebreaker) tuple to enable
//     deterministic resume after a crash mid-iteration. PageToken/Since unused.
//
// One struct keeps the cursor schema uniform across ops; per-op semantics are
// documented at the field-comment level. omitempty drops unused fields from the
// emitted JSON so cursor bytes stay bounded by the fields a given op uses.
type projectCursorState struct {
	// Timestamp is the high-water mark — max issue.fields.updated seen across
	// all processed records for this project.
	Timestamp time.Time `json:"timestamp"`

	// PageToken is fetch_issues' next-page token, persisted when pagination is
	// interrupted (ctx cancel, error). Empty on a clean state. Non-empty
	// implies a mid-pagination resume — caller MUST pair with Since to pin the
	// original JQL boundary (page tokens are server-stateful).
	PageToken string `json:"page_token,omitempty"`

	// Since is fetch_issues' pinned JQL `updated >= ?` boundary for the
	// in-flight pagination run. Only populated when PageToken is non-empty.
	// Note: ",omitempty" is a no-op for time.Time (zero is not empty), but the
	// tag is harmless and consistent with the rest of the struct.
	Since time.Time `json:"since,omitempty"`

	// LastIssueID is the numeric Jira issue ID of the last successfully
	// processed issue for fetch_worklogs / fetch_comments. Used as a tiebreaker
	// when multiple issues share the same Timestamp on resume — Jira Cloud
	// assigns IDs globally monotonically, so a newer issue always has a higher
	// ID, ensuring no resume-time miss for new same-Timestamp issues.
	LastIssueID string `json:"last_issue_id,omitempty"`
}

// UnmarshalJSON reads the {timestamp,page_token,since,last_issue_id} layout. An
// entry that does not parse, or parses with a zero Timestamp, is an error, so
// existingCursorStates drops it and that project starts over.
func (s *projectCursorState) UnmarshalJSON(data []byte) error {
	// Aliased type avoids recursion into this UnmarshalJSON.
	type currentFormat projectCursorState
	var current currentFormat
	if err := json.Unmarshal(data, &current); err != nil {
		return fmt.Errorf("jira cursor: entry does not parse (raw=%q): %w", string(data), err)
	}
	if current.Timestamp.IsZero() {
		return fmt.Errorf("jira cursor: entry has no timestamp (raw=%q)", string(data))
	}
	*s = projectCursorState(current)
	return nil
}

// existingCursorStates extracts all per-project cursor states from a composite
// cursor. Parse failures and zero-timestamp entries are logged and dropped so
// the run continues with whatever entries did parse — a single corrupt entry
// must not prevent every other project from advancing.
func existingCursorStates(ctx context.Context, cursor *sdk.Cursor) map[string]projectCursorState {
	states := make(map[string]projectCursorState)
	if cursor == nil {
		return states
	}
	parts, err := sdk.CompositeFromCursor(cursor)
	if err != nil {
		logger.Error(ctx, "jira: failed to parse composite cursor; starting fresh",
			zap.Error(err))
		return states
	}
	for projectKey, raw := range parts {
		var state projectCursorState
		if err := json.Unmarshal(raw, &state); err != nil {
			logger.Error(ctx, "jira: failed to unmarshal per-project cursor state; dropping entry",
				zap.String("project", projectKey),
				zap.ByteString("raw", raw),
				zap.Error(err))
			continue
		}
		if state.Timestamp.IsZero() {
			logger.Error(ctx, "jira: per-project cursor parsed with zero Timestamp; dropping entry",
				zap.String("project", projectKey),
				zap.String("page_token", state.PageToken),
				zap.Time("since", state.Since),
				zap.String("last_issue_id", state.LastIssueID))
			continue
		}
		states[projectKey] = state
	}
	return states
}

// buildStateCursor creates a composite cursor from per-project cursor states.
// Marshal errors are logged but non-fatal: the affected entry is omitted and
// will re-sync from scratch on the next run.
func buildStateCursor(ctx context.Context, states map[string]projectCursorState) *sdk.Cursor {
	parts := make(map[string]json.RawMessage, len(states))
	for projectKey, state := range states {
		// Skip a zero-Timestamp state: an empty project's JQL window yields no
		// issues, so its high-water mark stays zero and it would marshal to a
		// zero-Timestamp entry that existingCursorStates drops on read anyway —
		// an infinite re-write churn every run. Mirrors that read-side drop rule
		// so the write and read sides agree on which entries are usable.
		if state.Timestamp.IsZero() {
			continue
		}
		raw, err := json.Marshal(state)
		if err != nil {
			// projectCursorState only contains time.Time and string fields, so
			// Marshal should not fail. Log if it does so the regression is
			// visible rather than silent.
			logger.Error(ctx, "jira: failed to marshal per-project cursor state; dropping entry",
				zap.String("project", projectKey),
				zap.Error(err))
			continue
		}
		parts[projectKey] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// pageCheckpointFn is called after each fully-processed page with the current
// high-water mark and the nextPageToken for the next page.
type pageCheckpointFn func(ctx context.Context, highWater time.Time, nextPageToken string) error

// makePageCheckpoint builds a pageCheckpointFn that updates the per-project cursor
// state and delegates to the framework checkpoint.
func makePageCheckpoint(
	checkpoint sdk.CheckpointFunc,
	cursorStates map[string]projectCursorState,
	projectKey string,
	since time.Time,
) pageCheckpointFn {
	if checkpoint == nil {
		return nil
	}
	return func(ctx context.Context, highWater time.Time, nextPageToken string) error {
		cursorStates[projectKey] = projectCursorState{
			Timestamp: highWater,
			PageToken: nextPageToken,
			Since:     since,
		}
		return checkpoint(ctx, buildStateCursor(ctx, cursorStates))
	}
}

// issueRef is a lightweight handle to a Jira issue used by fetch_worklogs and
// fetch_comments. Carrying ID alongside Key + UpdatedOn lets the caller advance
// its cursor with (UpdatedOn, ID) as a tiebreaker — see isAlreadyProcessed.
type issueRef struct {
	ID        string
	Key       string
	UpdatedOn time.Time
}

// isAlreadyProcessed implements the cross-run skip predicate.
// An issue with (UpdatedOn, ID) <= (cursorTime, cursorID) was already processed
// in a prior run, so it should be skipped on resume.
//
// IDs are compared numerically because Jira Cloud assigns issue IDs globally
// monotonically — a newer issue always has a higher ID. This avoids the
// alphabetic-key trap where a new issue created between runs at the same
// updated_on with a lexicographically lower key would be wrongly skipped.
func isAlreadyProcessed(ref issueRef, cursorTime time.Time, cursorID string) bool {
	if ref.UpdatedOn.Before(cursorTime) {
		return true
	}
	if ref.UpdatedOn.Equal(cursorTime) && cursorID != "" {
		return compareIssueIDs(ref.ID, cursorID) <= 0
	}
	return false
}

// compareIssueIDs compares two Jira issue ID strings numerically.
// Returns -1 if leftID < rightID, 0 if equal, +1 if leftID > rightID.
// Falls back to string compare when either side fails to parse as an integer —
// defensive, since Jira's monotonic numeric IDs should always parse cleanly.
func compareIssueIDs(leftID, rightID string) int {
	leftNum, leftErr := strconv.ParseInt(leftID, 10, 64)
	rightNum, rightErr := strconv.ParseInt(rightID, 10, 64)
	if leftErr != nil || rightErr != nil {
		// Jira Cloud IDs are monotonic integers and should always parse;
		// a non-numeric ID is unexpected but we degrade to a string compare
		// rather than mis-order the resume tiebreaker. No ctx in scope (called
		// from a sort.Slice less-func and isAlreadyProcessed), so log no-ctx.
		logger.Warn("jira: non-numeric issue ID, falling back to string compare",
			zap.String("left_id", leftID),
			zap.String("right_id", rightID),
			zap.Error(errors.Join(leftErr, rightErr)))
		switch {
		case leftID < rightID:
			return -1
		case leftID > rightID:
			return 1
		default:
			return 0
		}
	}
	switch {
	case leftNum < rightNum:
		return -1
	case leftNum > rightNum:
		return 1
	default:
		return 0
	}
}

// errStuckPageToken reports a JQL search page whose nextPageToken equals the
// token the request sent. Following it would request the same page forever, so
// the search loops stop with this error instead, the way the project and board
// listings stop on a stuck pager.
var errStuckPageToken = errors.New("jira search returned the nextPageToken it was sent; pager is not advancing")

// checkPageTokenAdvances returns errStuckPageToken when a non-empty page hands
// back the token that requested it.
func checkPageTokenAdvances(sent string, resp *connectorpb.JiraSearchIssuesResponse) error {
	if sent != "" && resp.GetNextPageToken() == sent && len(resp.GetIssues()) > 0 {
		return fmt.Errorf("%w (token %q)", errStuckPageToken, sent)
	}
	return nil
}

// fetchAllProjectKeys returns the list of project keys to sync.
// If settings.ProjectKeys is set, those are used; otherwise all projects are fetched via REST.
func fetchAllProjectKeys(ctx context.Context, gw sdk.VendorGateway, settings JiraSettings) ([]string, error) {
	if len(settings.ProjectKeys) > 0 {
		return settings.ProjectKeys, nil
	}

	restGW := gw.ForGroup(constants.GroupJiraREST)
	var projectKeys []string
	startAt := int32(0)

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.JiraListProjectsRequest{
			CloudId:    settings.CloudID,
			StartAt:    startAt,
			MaxResults: projectsPageSize,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch projects: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.JiraListProjectsResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *JiraListProjectsResponse", protoResp)
		}

		for _, projectRaw := range resp.GetProjects() {
			var project jiraProject
			if err := json.Unmarshal(projectRaw, &project); err != nil {
				logger.Error(ctx, "jira: failed to unmarshal project entry in fetchAllProjectKeys; skipping",
					zap.ByteString("raw", projectRaw),
					zap.Error(err))
				continue
			}
			projectKeys = append(projectKeys, project.Key)
		}

		if resp.GetIsLast() {
			break
		}
		if len(resp.GetProjects()) == 0 {
			// Defensive: a stuck pager (isLast=false with an empty page) would
			// otherwise spin forever advancing startAt by zero.
			break
		}
		startAt += safePageLen(len(resp.GetProjects()))
	}

	return projectKeys, nil
}
