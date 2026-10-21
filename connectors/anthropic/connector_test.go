// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestOperationsDescriptorsAreWellFormed guards the declaration the scheduler reads. Every
// field here is consumed outside this package and a wrong value fails at runtime, in a
// scheduled run, on someone else's clock: an unparseable cron never fires, a duplicate id
// makes two operations share one cursor row, and a nil factory panics the worker.
func TestOperationsDescriptorsAreWellFormed(t *testing.T) {
	t.Parallel()

	ops := (&Connector{}).Operations()
	require.NotEmpty(t, ops)

	// The same five-field set the scheduler side parses with (see the sibling's
	// classifyCronParser) — parsing with a wider field set here would accept an expression
	// production rejects.
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	seenID := map[sdk.OperationID]bool{}
	// Firing instants over a probe span, so a weekly schedule is compared against the daily
	// ones on the day it actually fires rather than never.
	seenFiring := map[time.Time]sdk.OperationID{}
	staggerFrom := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	for _, op := range ops {
		t.Run(string(op.ID), func(t *testing.T) {
			assert.False(t, seenID[op.ID], "a duplicate operation id makes two operations share one cursor row")
			seenID[op.ID] = true

			assert.NotEmpty(t, op.DisplayName)
			assert.NotEmpty(t, op.Description)
			assert.Positive(t, op.ExecutionTimeout, "the scheduler default would cancel a first sync partway")
			assert.Equal(t, sdk.IngestionPull, op.IngestionType)

			_, err := parser.Parse(op.DefaultSchedule)
			require.NoError(t, err, "an unparseable schedule never fires and reports no error at boot")

			require.NotNil(t, op.Factory, "a nil factory panics the worker rather than failing to configure")
			assert.NotNil(t, op.Factory(), "the factory must build an operation, not return a nil interface")
		})
		// Staggered, not stacked, for the reason Operations documents.
		//
		// Compared by FIRING INSTANT, not by expression. Two different strings can fire at
		// the same moment ("0 12 * * 1" and "0 12 * * *" collide every Monday), which a
		// string comparison cannot see — and the string form is what a reader checks by eye,
		// so it is exactly the collision that survives review.
		sched, err := parser.Parse(op.DefaultSchedule)
		require.NoError(t, err)
		for i := range staggerProbeDays {
			at := sched.Next(staggerFrom.AddDate(0, 0, i))
			if prev, dup := seenFiring[at]; dup && prev != op.ID {
				t.Errorf("operations %s and %s both fire at %v — they would compete for one rate budget",
					prev, op.ID, at)
			}
			seenFiring[at] = op.ID
		}
	}
}

// staggerProbeDays spans more than a week, so a weekly schedule is compared against the daily
// ones on the day it fires.
const staggerProbeDays = 9

// TestValidateConfigRefusesWhatAnOperationCannotRunWith is why ValidateConfig exists: the
// same configuration failure either lands at connect time, where a human is watching, or
// inside a scheduled run hours later.
func TestValidateConfigRefusesWhatAnOperationCannotRunWith(t *testing.T) {
	t.Parallel()

	c := &Connector{}
	valid := []byte(`{"api_key":"sk-ant-api01-x"}`)

	for _, tt := range []struct {
		name string
		cfg  *sdk.ConnectorConfig
		ok   bool
	}{
		{"a stored key with no settings is valid — settings are all optional", &sdk.ConnectorConfig{Credentials: valid}, true},
		{"a stored key with settings is valid", &sdk.ConnectorConfig{
			Credentials: valid, Settings: []byte(`{"anthropic_organization_id":"org_1","lookback_days":30}`),
		}, true},
		{"nil config", nil, false},
		{"no credentials", &sdk.ConnectorConfig{}, false},
		{"unparseable credentials", &sdk.ConnectorConfig{Credentials: []byte(`{`)}, false},
		{
			"an empty key would reach the vendor as a rejected key, not as a config problem",
			&sdk.ConnectorConfig{Credentials: []byte(`{"api_key":""}`)}, false,
		},
		{"unparseable settings", &sdk.ConnectorConfig{Credentials: valid, Settings: []byte(`{`)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := c.ValidateConfig(context.Background(), tt.cfg)
			if tt.ok {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "claude-code:", "the connector must be named, or an operator cannot tell which one refused")
		})
	}
}

// TestCategoriesStayAnalytics pins the classification, for the reason Categories documents.
func TestCategoriesStayAnalytics(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []sdk.ConnectorCategory{sdk.CategoryAnalytics}, (&Connector{}).Categories())
}
