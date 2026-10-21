// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestEmitChangesRequestedReviews verifies the connector emits a CHANGES_REQUESTED
// review record for a participant in the "changes_requested" state, in the exact
// shape a consumer counts, and does NOT emit one for an
// approving/participating reviewer.
func TestEmitChangesRequestedReviews(t *testing.T) {
	t.Parallel()
	prUpdatedOn := time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC)
	prCreatedOn := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)

	pr := bbPullRequest{
		ID:        42,
		UpdatedOn: prUpdatedOn,
		CreatedOn: prCreatedOn,
		Author:    &bbUser{AccountID: "author-acc", DisplayName: "PR Author"},
		Destination: &bbPRRef{
			Repository: &bbRepository{UUID: "{repo-uuid-1}"},
		},
		Participants: []bbPRParticipant{
			{
				User:  &bbUser{AccountID: "reviewer-cr", DisplayName: "Change Requester", Nickname: "cr"},
				Role:  "REVIEWER",
				State: "changes_requested",
			},
			{
				// An approving reviewer must NOT produce a changes-requested record.
				User:     &bbUser{AccountID: "reviewer-ok", DisplayName: "Approver", Nickname: "ok"},
				Role:     "REVIEWER",
				Approved: true,
				State:    "approved",
			},
			{
				// A participant with no review state must be ignored.
				User: &bbUser{AccountID: "reviewer-none", DisplayName: "Lurker"},
				Role: "PARTICIPANT",
			},
		},
	}

	info := bbPRInfo{
		ID:                pr.ID,
		AuthorAccountID:   pr.Author.AccountID,
		AuthorDisplayName: pr.Author.DisplayName,
		PlatformRepoID:    pr.Destination.Repository.UUID,
		CreatedOn:         pr.CreatedOn,
	}

	sink := testkit.NewChannelSink()
	repoFullName := "workspace/repo"

	err := emitChangesRequestedReviews(context.Background(), sink, "fetch_reviews", "org-test", repoFullName, info, pr)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "exactly one changes-requested record (only the changes_requested participant)")

	rec := records[0]

	// External ID: unique per reviewer, distinct /changes-requested- segment so it
	// never collides with an /approval- record for the same reviewer.
	assert.Equal(t, "workspace/repo#PR-42/changes-requested-reviewer-cr", rec.ExternalID)

	// dataDate anchored to pr.CreatedOn — a STABLE per-PR value, NOT the mutable
	// pr.UpdatedOn. This is load-bearing: data_date is part of the record
	// uniqueness key, so anchoring to UpdatedOn would insert a new row (and
	// double-count the review) on every unrelated PR update.
	assert.Equal(t, prCreatedOn, rec.DataDate.UTC())
	assert.NotEqual(t, prUpdatedOn, rec.DataDate.UTC(), "must not anchor to the mutable UpdatedOn")

	// Payload: state drives a consumer count (ParseReview reads payload.state).
	var payload struct {
		User struct {
			AccountID   string `json:"account_id"`
			DisplayName string `json:"display_name"`
			Nickname    string `json:"nickname"`
		} `json:"user"`
		State     string `json:"state"`
		PRID      string `json:"pr_id"`
		CreatedOn string `json:"created_on"`
	}
	require.NoError(t, json.Unmarshal(rec.Payload, &payload))
	assert.Equal(t, "CHANGES_REQUESTED", payload.State)
	assert.Equal(t, "reviewer-cr", payload.User.AccountID)
	assert.Equal(t, "Change Requester", payload.User.DisplayName)
	assert.Equal(t, "cr", payload.User.Nickname)
	assert.Equal(t, "42", payload.PRID)

	// Metadata mirrors the APPROVED emission (drives self-PR / bot gates + PR-key set).
	assert.Equal(t, repoFullName, rec.Metadata["repo"])
	assert.Equal(t, "changes_requested", rec.Metadata["review_type"])
	assert.Equal(t, "42", rec.Metadata[metaKeyPRID])
	assert.Equal(t, "workspace/repo#PR-42", rec.Metadata["pr_key"])
	assert.Equal(t, "PR Author", rec.Metadata["pr_author_login"])
	assert.Equal(t, "author-acc", rec.Metadata["pr_author_id"])
	assert.Equal(t, "{repo-uuid-1}", rec.Metadata["platform_repo_id"])
	assert.Equal(t, prCreatedOn.Format(time.RFC3339), rec.Metadata["pr_created_at"])
	assert.Equal(t, "false", rec.Metadata["reviewer_is_bot"])
}

// TestEmitChangesRequestedReviews_StableDataDateAcrossUpdates pins that
// re-emitting the same still-open changes-requested
// state after pr.UpdatedOn has advanced (an unrelated PR touch) must produce a record
// with the IDENTICAL externalID and DataDate, so the record upsert collapses the
// two into one row rather than counting the review on two different days.
func TestEmitChangesRequestedReviews_StableDataDateAcrossUpdates(t *testing.T) {
	t.Parallel()
	createdOn := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	mkPR := func(updatedOn time.Time) bbPullRequest {
		return bbPullRequest{
			ID:          42,
			UpdatedOn:   updatedOn,
			CreatedOn:   createdOn,
			Author:      &bbUser{AccountID: "author-acc", DisplayName: "PR Author"},
			Destination: &bbPRRef{Repository: &bbRepository{UUID: "{repo-uuid-1}"}},
			Participants: []bbPRParticipant{{
				User:  &bbUser{AccountID: "reviewer-cr", DisplayName: "Change Requester"},
				Role:  "REVIEWER",
				State: "changes_requested",
			}},
		}
	}
	info := bbPRInfo{ID: 42, AuthorAccountID: "author-acc", AuthorDisplayName: "PR Author", PlatformRepoID: "{repo-uuid-1}", CreatedOn: createdOn}

	emitOnce := func(updatedOn time.Time) *sdk.RawRecord {
		sink := testkit.NewChannelSink()
		require.NoError(t, emitChangesRequestedReviews(context.Background(), sink, "fetch_reviews", "org-test", "workspace/repo", info, mkPR(updatedOn)))
		recs := sink.Records()
		require.Len(t, recs, 1)
		return recs[0]
	}

	day1 := emitOnce(time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC))
	day5 := emitOnce(time.Date(2026, 7, 25, 15, 0, 0, 0, time.UTC)) // PR touched again later

	assert.Equal(t, day1.ExternalID, day5.ExternalID, "externalID must be stable across re-syncs")
	assert.Equal(t, day1.DataDate.UTC(), day5.DataDate.UTC(), "dataDate must be stable across re-syncs (else the upsert double-counts)")
	assert.Equal(t, createdOn, day5.DataDate.UTC(), "dataDate must track the stable CreatedOn, not the advancing UpdatedOn")
}

// TestEmitChangesRequestedReviews_NoCreatedOn asserts a PR with no created_on is
// skipped (no stable anchor → an un-dedupable record would double-count).
func TestEmitChangesRequestedReviews_NoCreatedOn(t *testing.T) {
	t.Parallel()
	pr := bbPullRequest{
		ID:        99,
		UpdatedOn: time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
		// CreatedOn deliberately zero.
		Participants: []bbPRParticipant{{
			User:  &bbUser{AccountID: "reviewer-cr"},
			Role:  "REVIEWER",
			State: "changes_requested",
		}},
	}
	sink := testkit.NewChannelSink()
	require.NoError(t, emitChangesRequestedReviews(context.Background(), sink, "fetch_reviews", "org-test", "workspace/repo", bbPRInfo{ID: 99}, pr))
	assert.Empty(t, sink.Records(), "a PR with no created_on must not emit an un-dedupable changes-requested record")
}

// TestEmitChangesRequestedReviews_NoParticipants asserts a PR with no
// changes-requested participant emits nothing.
func TestEmitChangesRequestedReviews_NoParticipants(t *testing.T) {
	t.Parallel()
	pr := bbPullRequest{
		ID:        7,
		UpdatedOn: time.Now().UTC(),
		Participants: []bbPRParticipant{
			{User: &bbUser{AccountID: "a"}, State: "approved", Approved: true},
		},
	}
	info := bbPRInfo{ID: pr.ID}

	sink := testkit.NewChannelSink()
	err := emitChangesRequestedReviews(context.Background(), sink, "fetch_reviews", "org-test", "workspace/repo", info, pr)
	require.NoError(t, err)
	assert.Empty(t, sink.Records())
}
