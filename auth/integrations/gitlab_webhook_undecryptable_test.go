// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// One GitLab row that does not decrypt (org B, written under another key)
// does not fail org A's correctly signed hook: the row is skipped.
func TestGitLabWebhookSkipsARowThatDoesNotDecrypt(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("gitlab webhook as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	encA, _ := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{1}, 32))
	encB, _ := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{2}, 32))
	repoA, repoB := store.NewConnectorConfigRepo(db, encA), store.NewConnectorConfigRepo(db, encB)
	tx := store.PostgresTxRunner{}
	save := func(r *store.ConnectorConfigRepo, org, secret string) {
		creds, _ := json.Marshal(map[string]string{"webhook_secret": secret})
		c := &sdk.ConnectorConfig{ConnectorID: "gitlab", OrganizationID: org, Credentials: creds, Settings: []byte(`{}`), Enabled: true}
		require.NoError(t, tx.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error { return r.Save(s, c) }))
	}
	save(repoA, "00000000-0000-4000-8000-0000000c2a01", "secret-a")
	save(repoB, "00000000-0000-4000-8000-0000000c2a02", "secret-b")

	svc := NewService(Deps{Tx: tx, Cfg: webhookConfig(), Registry: testRegistry(), Sink: testkit.NewChannelSink(),
		ConnectorDAO: repoA, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db)})
	_, err = svc.HandleGitLabWebhook(ctx, &connintv1.HandleGitLabWebhookRequest{TokenHeader: "secret-a", EventType: "Pipeline Hook", Body: []byte("{}")})
	t.Logf("org A's hook: err=%v", err)
	require.NoError(t, err, "org B's row that does not decrypt must not fail org A's hook")
}
