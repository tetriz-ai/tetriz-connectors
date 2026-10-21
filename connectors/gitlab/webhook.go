// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// WebhookEventsOp processes inbound GitLab webhook payloads.
// It implements sdk.WebhookOperation. The webhook RPCs in auth/integrations
// call it with the payload a host received.
//
// Every record it emits carries operationId webhook_events, as GitHub's do,
// never a polling operation's id: a hook body is smaller than the polled
// object, so sharing the polled key would overwrite the richer record. The
// metadata key record_kind names the shape:
//
// Push Hook          → one "commit" record per pushed commit.
// Push/Tag Push Hook with no commits (a tag push or a ref deletion) → one "push" record, the raw body.
// Merge Request Hook → "merge_request" lifecycle records (#opened on open, #snapshot, #merged, #closed).
// Issue Hook         → "issue" lifecycle records (#snapshot, #closed).
// Note Hook          → one "note" record.
// All other event types → acknowledged and dropped.
type WebhookEventsOp struct{}

// opWebhookEvents is the operationId of the webhook op and of every record it emits.
const opWebhookEvents sdk.OperationID = "webhook_events"

// Values of the record_kind metadata key on webhook_events records.
const (
	webhookKindCommit       = "commit"
	webhookKindPush         = "push"
	webhookKindMergeRequest = "merge_request"
	webhookKindIssue        = "issue"
	webhookKindNote         = "note"
)

// withRecordKind returns a copy of meta with record_kind set.
func withRecordKind(meta map[string]string, kind string) map[string]string {
	out := make(map[string]string, len(meta)+1)
	for k, v := range meta {
		out[k] = v
	}
	out["record_kind"] = kind
	return out
}

func (op *WebhookEventsOp) OperationID() sdk.OperationID { return opWebhookEvents }

func (op *WebhookEventsOp) HandleWebhook(ctx context.Context, params *sdk.WebhookParams) (*sdk.WebhookResult, error) {
	var creds GitLabCredentials
	if err := json.Unmarshal(params.Config.Credentials, &creds); err != nil {
		return nil, fmt.Errorf("webhook: parse credentials: %w", err)
	}

	// Verify X-Gitlab-Token: always required for GitLab.
	if creds.WebhookSecret == "" {
		return nil, fmt.Errorf("webhook: no webhook secret configured")
	}
	received := params.Headers.Get("X-Gitlab-Token")
	// Constant-time comparison to prevent timing attacks.
	if !hmac.Equal([]byte(creds.WebhookSecret), []byte(received)) {
		return nil, fmt.Errorf("webhook: invalid X-Gitlab-Token")
	}

	eventType := params.Headers.Get("X-Gitlab-Event")

	switch eventType {
	case "":
		// Ping / test delivery — nothing to store.
		return &sdk.WebhookResult{Acknowledged: true}, nil

	case "Push Hook", "Tag Push Hook":
		if err := op.handlePushHook(ctx, params, eventType); err != nil {
			return nil, fmt.Errorf("handling push hook: %w", err)
		}

	case "Merge Request Hook":
		if err := op.handleMRHook(ctx, params); err != nil {
			return nil, fmt.Errorf("handling merge request hook: %w", err)
		}

	case "Issue Hook":
		if err := op.handleIssueHook(ctx, params); err != nil {
			return nil, fmt.Errorf("handling issue hook: %w", err)
		}

	case "Note Hook":
		if err := op.handleNoteHook(ctx, params); err != nil {
			return nil, fmt.Errorf("handling note hook: %w", err)
		}

	default:
		// All other event types — acknowledge and drop.
		return &sdk.WebhookResult{Acknowledged: true}, nil
	}

	return &sdk.WebhookResult{Acknowledged: true}, nil
}

// handlePushHook fans out each commit in a Push Hook as an individual commit
// record, keyed like FetchCommitsOp: "<path_with_namespace>#<sha>". A hook with
// no commits (a tag push or a ref deletion) is stored raw as one push record.
func (op *WebhookEventsOp) handlePushHook(ctx context.Context, params *sdk.WebhookParams, eventType string) error {
	var hook glPushHook
	if err := json.Unmarshal(params.Body, &hook); err != nil {
		return fmt.Errorf("webhook: parse push hook: %w", err)
	}

	projectPath := hook.Project.PathWithNamespace

	// A hook with no commits — store the raw payload as a push record. It is the
	// ref's latest push, overwritten by the next one, with no event time in the
	// body, so it has no DataDate.
	if len(hook.Commits) == 0 {
		if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
			hook.TagExternalID(), params.Body, withRecordKind(map[string]string{"event_type": eventType}, webhookKindPush), time.Time{}); err != nil {
			return fmt.Errorf("emitting tag record: %w", err)
		}
		return nil
	}

	for _, commit := range hook.Commits {
		commitJSON, err := json.Marshal(commit)
		if err != nil {
			return fmt.Errorf("webhook: marshal commit: %w", err)
		}
		externalID := glCommit{ID: commit.ID}.ExternalID(projectPath)
		if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
			externalID, commitJSON, withRecordKind(map[string]string{"repo": projectPath}, webhookKindCommit), commit.Timestamp); err != nil {
			return fmt.Errorf("webhook: emit commit: %w", err)
		}
	}
	return nil
}

// handleMRHook emits merge request lifecycle records keyed like FetchPRsOp's:
// #opened on the open action, #snapshot always (no DataDate: it is current
// state), and #merged or #closed dated by their transition.
func (op *WebhookEventsOp) handleMRHook(ctx context.Context, params *sdk.WebhookParams) error {
	var hook glMRHook
	if err := json.Unmarshal(params.Body, &hook); err != nil {
		return fmt.Errorf("webhook: parse mr hook: %w", err)
	}
	externalID := hook.ExternalID()
	oa := hook.ObjectAttributes

	// "#opened" — the open action is the creation event. A reopen is action
	// "reopen", so it does not re-date the opening.
	if oa.Action == "open" {
		if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
			externalID+"#opened", params.Body, mrHookMeta("opened"), oa.CreatedAt); err != nil {
			return fmt.Errorf("emitting opened mr record: %w", err)
		}
	}

	// "#snapshot" — always emitted; the latest hook's state.
	if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
		externalID+"#snapshot", params.Body, mrHookMeta("snapshot"), time.Time{}); err != nil {
		return fmt.Errorf("emitting mr snapshot record: %w", err)
	}

	// "#merged" — only when mergedAt is set.
	if oa.MergedAt != nil {
		if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
			externalID+"#merged", params.Body, mrHookMeta("merged"), *oa.MergedAt); err != nil {
			return fmt.Errorf("emitting merged mr record: %w", err)
		}
	}

	// "#closed" — only for MRs closed without merge.
	if oa.State == stateClosed && oa.MergedAt == nil {
		closedAt := oa.CreatedAt
		if oa.ClosedAt != nil {
			closedAt = *oa.ClosedAt
		}
		if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
			externalID+"#closed", params.Body, mrHookMeta(stateClosed), closedAt); err != nil {
			return fmt.Errorf("emitting closed mr record: %w", err)
		}
	}

	return nil
}

func mrHookMeta(eventType string) map[string]string {
	return withRecordKind(map[string]string{"event_type": eventType}, webhookKindMergeRequest)
}

func issueHookMeta(eventType string) map[string]string {
	return withRecordKind(map[string]string{"event_type": eventType}, webhookKindIssue)
}

// handleIssueHook emits issue #snapshot (no DataDate: current state) and
// #closed records keyed like FetchIssuesOp's — not #opened; FetchIssuesOp's
// poll emits that.
func (op *WebhookEventsOp) handleIssueHook(ctx context.Context, params *sdk.WebhookParams) error {
	var hook glIssueHook
	if err := json.Unmarshal(params.Body, &hook); err != nil {
		return fmt.Errorf("webhook: parse issue hook: %w", err)
	}
	externalID := hook.ExternalID()
	oa := hook.ObjectAttributes

	// Reshape the hook body into the REST fetch_issues shape parseGitLab
	// expects: the hook's fields live under object_attributes/project/labels/
	// assignees (no top-level iid, no references.full), whereas the poll op
	// emits the REST issue object verbatim. Without this transform the
	// consumer drops every webhook-sourced issue as "missing iid" (mirrors
	// the reshape handleNoteHook already does for fetch_reviews below).
	payload, err := json.Marshal(buildRestIssuePayload(hook))
	if err != nil {
		return fmt.Errorf("webhook: marshal issue payload: %w", err)
	}

	// "#snapshot" — always emitted; the latest hook's state.
	if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
		externalID+"#snapshot", payload, issueHookMeta("snapshot"), time.Time{}); err != nil {
		return fmt.Errorf("emitting issue snapshot record: %w", err)
	}

	// "#closed" — only when issue is closed.
	if oa.State == stateClosed && oa.ClosedAt != nil {
		if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
			externalID+"#closed", payload, issueHookMeta(stateClosed), *oa.ClosedAt); err != nil {
			return fmt.Errorf("emitting closed issue record: %w", err)
		}
	}

	return nil
}

// buildRestIssuePayload reshapes a glIssueHook into the same map shape the
// REST fetch_issues op emits (gitlabIssuePayload in
// the consumer's GitLab issue reader: iid, title, state, issue_type
// (lowercased — the hook's own `type` field is Title-cased), created_at,
// closed_at, labels ([]string of titles, not the hook's label objects),
// author/assignees ({id, username}), and references.full (built from
// project.path_with_namespace, since the hook carries no references field).
//
// The hook's top-level assignees array carries display names with no numeric
// id, and object_attributes.assignee_ids carries ids with no name — GitLab
// gives no shared key to pair them by, and does not guarantee matching order,
// so a positional zip can attribute the WRONG display name to a real user, and
// a consumer may persist whatever display-name hint it is given. Emit ids only, matching the author field
// below — the poll path's authoritative id+username pairs fill the name in
// on the next sync.
func buildRestIssuePayload(hook glIssueHook) map[string]any {
	oa := hook.ObjectAttributes

	labels := make([]string, 0, len(hook.Labels))
	for _, l := range hook.Labels {
		if l.Title != "" {
			labels = append(labels, l.Title)
		}
	}

	assignees := make([]map[string]any, 0, len(oa.AssigneeIDs))
	for _, id := range oa.AssigneeIDs {
		assignees = append(assignees, map[string]any{"id": id, "username": ""})
	}

	payload := map[string]any{
		"iid":        oa.IID,
		"title":      oa.Title,
		"state":      oa.State,
		"issue_type": normalizeGitLabIssueType(oa.Type),
		"created_at": oa.CreatedAt.UTC().Format(time.RFC3339Nano),
		"labels":     labels,
		"assignees":  assignees,
		"references": map[string]any{
			"full": fmt.Sprintf("%s#%d", hook.Project.PathWithNamespace, oa.IID),
		},
	}
	if oa.ClosedAt != nil {
		payload["closed_at"] = oa.ClosedAt.UTC().Format(time.RFC3339Nano)
	}
	if oa.AuthorID != 0 {
		payload["author"] = map[string]any{"id": oa.AuthorID, "username": ""}
	}
	// due_date is the issue's own due date (distinct from any milestone's
	// deadline) and an unconditionally-overwritten consumer column (not
	// mask-gated); omitting it here would NULL a poll-set due
	// date on every webhook delivery.
	if oa.DueDate != "" {
		payload["due_date"] = oa.DueDate
	}
	return payload
}

// normalizeGitLabIssueType maps the hook's Title-cased issue type ("Issue",
// "Incident", "Test Case", "Task") to the snake_case form the REST
// fetch_issues payload emits verbatim ("issue", "incident", "test_case",
// "task" — see gitlabIssuePayload.IssueType's doc comment in
// the consumer's GitLab issue reader. A plain ToLower is not
// sufficient for the two-word "Test Case" case; an unrecognized value falls
// back to lowercase-with-underscores rather than silently dropping it.
func normalizeGitLabIssueType(t string) string {
	switch t {
	case "Issue":
		return "issue"
	case "Incident":
		return "incident"
	case "Test Case":
		return "test_case"
	case "Task":
		return "task"
	default:
		return strings.ReplaceAll(strings.ToLower(t), " ", "_")
	}
}

// handleNoteHook emits a note record keyed like FetchReviewsOp's notes, dated by createdAt.
// Emits six metadata fields (pr_key, mr_iid, pr_author_login, pr_author_id,
// platform_repo_id, reviewer_is_bot) matching the poll path's note emission.
// pr_key and mr_iid are required for canonicalPRKey: note record keys are
// "note-{project_id}-{note_id}" and don't embed the IID, so without these
// fields a consumer cannot recover the canonical "mr-{project_id}!{iid}"
// PR key for distinct-PR set membership.
func (op *WebhookEventsOp) handleNoteHook(ctx context.Context, params *sdk.WebhookParams) error {
	var hook glNoteHook
	if err := json.Unmarshal(params.Body, &hook); err != nil {
		return fmt.Errorf("webhook: parse note hook: %w", err)
	}
	// Skip system-generated notes — assignments, label changes, "merged", etc.
	// Mirror the poll path's note.System guard (FetchReviewsOp): a system note
	// must not be emitted as a review, or it counts as reviewer activity and can
	// become an MR's "first review", corrupting Review-Started TAT.
	if hook.ObjectAttributes.System {
		return nil
	}
	// Reshape the webhook body into the REST notes shape ParseNote expects:
	// the hook puts the commenter under top-level `user` whereas ParseNote
	// reads `author.username` / `author.id` / `created_at`. Without this
	// transform a consumer drops the record as "note missing author.username".
	payload, err := json.Marshal(map[string]any{
		"id": hook.ObjectAttributes.ID,
		"author": map[string]any{
			"id":       hook.User.ID,
			"username": hook.User.Username,
		},
		"created_at": hook.ObjectAttributes.CreatedAt,
	})
	if err != nil {
		return fmt.Errorf("webhook: marshal note payload: %w", err)
	}
	meta := map[string]string{
		"pr_key":           fmt.Sprintf("mr-%d!%d", hook.Project.ID, hook.MergeRequest.IID),
		"mr_iid":           strconv.Itoa(hook.MergeRequest.IID),
		"pr_author_login":  hook.MergeRequest.Author.Username,
		"pr_author_id":     formatAuthorID(hook.MergeRequest.Author.ID),
		"platform_repo_id": strconv.FormatInt(hook.Project.ID, 10),
		metaKeyPRNumber:    strconv.Itoa(hook.MergeRequest.IID),
		"review_type":      "note",
		"reviewer_is_bot":  detectIsBot(hook.User.Bot),
		"record_kind":      webhookKindNote,
	}
	if err := emitRecord(ctx, params.Sink, opWebhookEvents, params.Config.OrganizationID,
		hook.ExternalID(), payload, meta, hook.ObjectAttributes.CreatedAt); err != nil {
		return fmt.Errorf("emitting note record: %w", err)
	}
	return nil
}
