// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	mock_adapter "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/schedule/connectorsource"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestMain(m *testing.M) {
	logger.Init("test", nil)
	os.Exit(testguard.Run(m))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func validPayload(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(ConnectorSyncPayload{
		OrgID:       "org-1",
		ConnectorID: "github",
		OperationID: "fetch_prs",
	})
	require.NoError(t, err)
	return b
}

// ---------------------------------------------------------------------------
// Tests: ProcessTask
// ---------------------------------------------------------------------------

func TestProcessTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		payload   []byte
		runnerErr error
		wantErr   bool
	}{
		{
			name:    "successful execution",
			payload: nil, // set below
		},
		{
			name:    "invalid payload JSON",
			payload: []byte(`{invalid`),
			wantErr: true,
		},
		{
			name:      "runner error",
			payload:   nil,
			runnerErr: errors.New("operation failed"),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			runner := mock_adapter.NewMockOperationRunner(ctrl)

			payload := tt.payload
			if payload == nil {
				payload = validPayload(t)
			}

			// Only expect RunOperation if payload is valid
			if tt.payload == nil {
				runner.EXPECT().RunOperation(
					gomock.Any(),
					"org-1",
					sdk.ConnectorID("github"),
					sdk.OperationID("fetch_prs"),
					sdk.TenantKindUnspecified,
					"scheduled",
				).Return(tt.runnerErr)
			}

			h := NewConnectorSyncHandler(runner)
			task := asynq.NewTask("connector.sync", payload)
			err := h.ProcessTask(context.Background(), task)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestProcessTask_SkipRetryClassification verifies the handler maps a runner
// error to asynq.SkipRetry exactly when the error chain carries a Terminal
// SyncError. Non-terminal errors — including config_invalid faults that are not
// flagged Terminal — must keep asynq's MaxRetry loop.
func TestProcessTask_SkipRetryClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		runnerErr     error
		wantSkipRetry bool
	}{
		{
			name: "terminal guard error skips retry",
			runnerErr: fmt.Errorf("guard: %w", &sdk.SyncError{
				Kind:     sdk.ErrKindConfigInvalid,
				Terminal: true,
				Cause:    errors.New("tenant_type UNSPECIFIED"),
			}),
			wantSkipRetry: true,
		},
		{
			// A transient config_invalid fault (the common connector case) is not
			// Terminal, so it keeps retrying — guards against keying on Kind.
			name: "non-terminal config_invalid error keeps retry",
			runnerErr: &sdk.SyncError{
				Kind:  sdk.ErrKindConfigInvalid,
				Cause: errors.New("bad settings JSON"),
			},
			wantSkipRetry: false,
		},
		{
			name: "retryable rate-limit error keeps retry",
			runnerErr: &sdk.SyncError{
				Kind:      sdk.ErrKindRateLimit,
				Retryable: true,
				Cause:     errors.New("429"),
			},
			wantSkipRetry: false,
		},
		{
			// A bare auth error without Terminal keeps retrying — proves the
			// handler keys on Terminal, not Kind.
			name: "non-terminal auth error keeps retry",
			runnerErr: &sdk.SyncError{
				Kind:  sdk.ErrKindAuth,
				Cause: errors.New("401"),
			},
			wantSkipRetry: false,
		},
		{
			// The classifier stamps Terminal on a dead-credential 401/403, so a
			// terminal auth error must SkipRetry rather than burn MaxRetry.
			name: "terminal auth error skips retry",
			runnerErr: &sdk.SyncError{
				Kind:     sdk.ErrKindAuth,
				Terminal: true,
				Cause:    errors.New("401 dead credential"),
			},
			wantSkipRetry: true,
		},
		{
			name:          "plain error keeps retry",
			runnerErr:     errors.New("boom"),
			wantSkipRetry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			runner := mock_adapter.NewMockOperationRunner(ctrl)
			runner.EXPECT().RunOperation(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			).Return(tt.runnerErr)

			h := NewConnectorSyncHandler(runner)
			task := asynq.NewTask("connector.sync", validPayload(t))
			err := h.ProcessTask(context.Background(), task)

			require.Error(t, err)
			assert.Equal(t, tt.wantSkipRetry, errors.Is(err, asynq.SkipRetry),
				"errors.Is(err, asynq.SkipRetry) mismatch")
		})
	}
}

func TestProcessTask_PayloadFieldsPassedCorrectly(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	runner := mock_adapter.NewMockOperationRunner(ctrl)

	payload, err := json.Marshal(ConnectorSyncPayload{
		OrgID:       "acme-corp",
		ConnectorID: "jira",
		OperationID: "fetch_issues",
		TenantType:  sdk.TenantKindIndividual,
	})
	require.NoError(t, err)

	// The payload's stamped tenant_type must thread through unchanged to the
	// runner — the handler is pure transport, not a place intent is re-derived.
	runner.EXPECT().RunOperation(
		gomock.Any(),
		"acme-corp",
		sdk.ConnectorID("jira"),
		sdk.OperationID("fetch_issues"),
		sdk.TenantKindIndividual,
		"scheduled",
	).Return(nil)

	h := NewConnectorSyncHandler(runner)
	task := asynq.NewTask("connector.sync", payload)
	assert.NoError(t, h.ProcessTask(context.Background(), task))
}

func TestProcessTask_TriggerIsScheduled(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	runner := mock_adapter.NewMockOperationRunner(ctrl)

	runner.EXPECT().RunOperation(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		"scheduled", // verify trigger
	).Return(nil)

	h := NewConnectorSyncHandler(runner)
	task := asynq.NewTask("connector.sync", validPayload(t))
	assert.NoError(t, h.ProcessTask(context.Background(), task))
}

func TestProcessTask_ContextPropagated(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "test-value")

	ctrl := gomock.NewController(t)
	runner := mock_adapter.NewMockOperationRunner(ctrl)

	runner.EXPECT().RunOperation(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(receivedCtx context.Context, _ string, _ sdk.ConnectorID, _ sdk.OperationID, _ sdk.TenantKind, _ string) error {
		assert.Equal(t, "test-value", receivedCtx.Value(ctxKey{}))
		return nil
	})

	h := NewConnectorSyncHandler(runner)
	task := asynq.NewTask("connector.sync", validPayload(t))
	assert.NoError(t, h.ProcessTask(ctx, task))
}
