// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command embedgithub syncs the repositories a GitHub personal access token
// can see from a Go program of its own, with the built-in GitHub connector
// and gateway and no Postgres or Redis: an in-memory store, the built-in
// gateway factory on in-process stores (rate limits, budgets and token cache
// live in this process), and a channel sink.
//
//	GITHUB_TOKEN=github_pat_... go run ./examples/embedgithub
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/tetriz-ai/tetriz-connectors/connectors/github"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/inprocess"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// org is the organization the config is stored under: any UUID.
const org = "00000000-0000-4000-8000-0000000e4b01"

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		_, _ = fmt.Fprintln(os.Stderr, "set GITHUB_TOKEN to a personal access token")
		os.Exit(2)
	}
	repos, err := SyncRepos(context.Background(), token)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	for _, r := range repos {
		fmt.Println(r)
	}
}

// SyncRepos runs GitHub's fetch_repos once for token and returns the full
// name of each repository it emitted.
func SyncRepos(ctx context.Context, token string) ([]string, error) {
	// The built-in gateways on in-process stores: no Redis.
	gateways, err := app.NewGatewayFactory(app.InProcessGatewayDeps(app.Config{}, inprocess.New()))
	if err != nil {
		return nil, err
	}

	registry := sdk.NewConnectorRegistry()
	registry.Register(github.NewConnector(nil, app.GitHubCheckpointReserve))

	store := memstore.New()
	creds, err := json.Marshal(map[string]string{"personal_access_token": token})
	if err != nil {
		return nil, fmt.Errorf("encode credentials: %w", err)
	}
	if err := store.Save(ctx, &sdk.ConnectorConfig{
		OrganizationID: org,
		ConnectorID:    "github",
		Settings:       json.RawMessage(`{"include_private":true}`),
		Credentials:    creds,
		Enabled:        true,
	}); err != nil {
		return nil, fmt.Errorf("save config: %w", err)
	}

	records := sink.NewChannelSink()
	orch := runner.NewOrchestrator(registry, store, store, store, records, gateways)
	if err := orch.RunOperation(ctx, org, "github", "fetch_repos", sdk.TenantKindOrganization, "embedgithub"); err != nil {
		return nil, fmt.Errorf("fetch_repos: %w", err)
	}
	var names []string
	for _, r := range records.Records() {
		var repo struct {
			FullName string `json:"fullName"` // the protojson name of RepositoryRecord.full_name
		}
		if err := json.Unmarshal(r.Payload, &repo); err != nil || repo.FullName == "" {
			names = append(names, r.ExternalID)
			continue
		}
		names = append(names, repo.FullName)
	}
	return names, nil
}
