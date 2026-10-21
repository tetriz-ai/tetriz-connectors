// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ListEnabledConfigs and ListEnabledOrganizations return only organization
// and connector ids, so they read no credentials: a row that does not decrypt
// is still listed and fails neither.
func TestListRPCsReadNoCredentials(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("list RPCs as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	encA, _ := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{1}, 32))
	encB, _ := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{2}, 32))
	tx := store.PostgresTxRunner{}
	for _, s := range []struct {
		org string
		r   *store.ConnectorConfigRepo
	}{{"00000000-0000-4000-8000-0000000c2b01", store.NewConnectorConfigRepo(db, encA)}, {"00000000-0000-4000-8000-0000000c2b02", store.NewConnectorConfigRepo(db, encB)}} {
		require.NoError(t, tx.RunInScope(ctx, s.org, store.ScopeEnforced, func(c context.Context) error {
			return s.r.Save(c, &sdk.ConnectorConfig{OrganizationID: s.org, ConnectorID: "github", Settings: []byte(`{}`), Credentials: []byte(`{"personal_access_token":"x"}`), Enabled: true})
		}))
	}
	svc := NewService(tx, store.NewConnectorConfigRepo(db, encA), store.NewSyncRunRepo(db), store.NewScheduleRepo(db), app.NewRegistry(nil), nil, nil, nil)
	l, err1 := svc.ListEnabledConfigs(ctx, &connectorpb.ListEnabledConfigsRequest{ConnectorId: "github"})
	o, err2 := svc.ListEnabledOrganizations(ctx, &connectorpb.ListEnabledOrganizationsRequest{ConnectorId: "github"})
	t.Logf("ListEnabledConfigs: %v err=%v", l.GetConfigs(), err1)
	t.Logf("ListEnabledOrganizations: %v err=%v", o.GetOrganizationIds(), err2)
	require.NoError(t, err1)
	require.NoError(t, err2)
	want := []string{"00000000-0000-4000-8000-0000000c2b01", "00000000-0000-4000-8000-0000000c2b02"}
	got := make([]string, 0, len(l.GetConfigs()))
	for _, c := range l.GetConfigs() {
		got = append(got, c.GetOrganizationId())
	}
	require.Equal(t, want, got)
	require.Equal(t, want, o.GetOrganizationIds())

	// The lists that do return credentials leave the row out and say so.
	var cfgs []*sdk.ConnectorConfig
	err = tx.RunInScope(ctx, "", store.ScopeSystem, func(s context.Context) error {
		var listErr error
		cfgs, listErr = store.NewConnectorConfigRepo(db, encA).ListEnabledByConnector(s, "github")
		return listErr
	})
	var undecryptable *store.UndecryptableRowsError
	require.ErrorAs(t, err, &undecryptable)
	require.Equal(t, []string{"00000000-0000-4000-8000-0000000c2b02"}, undecryptable.OrgIDs)
	require.Len(t, cfgs, 1)
	require.Equal(t, "00000000-0000-4000-8000-0000000c2b01", cfgs[0].OrganizationID)
}
