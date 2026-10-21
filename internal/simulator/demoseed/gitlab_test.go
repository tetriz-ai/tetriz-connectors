// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package demoseed

import (
	"encoding/json"
	"testing"
	"time"
)

func TestGitLabGroupID_CatalogOrgs(t *testing.T) {
	tests := []struct {
		name    string
		org     string
		want    int64
		project int64
		user    int64
	}{
		{name: "regulated worked example", org: "demo-gl-regulated", want: 91001, project: 9100101, user: 91001001},
		{name: "scaleup", org: "demo-gl-scaleup", want: 91002, project: 9100201, user: 91002001},
		{name: "solo", org: "demo-gl-solo", want: 91003, project: 9100301, user: 91003001},
		{name: "unknown is 0", org: "demo-gl-acme", want: 0, project: 1, user: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GitLabGroupID(tt.org); got != tt.want {
				t.Errorf("GitLabGroupID(%q) = %d, want %d", tt.org, got, tt.want)
			}
			if got := GitLabProjectID(tt.org, 1); got != tt.project {
				t.Errorf("GitLabProjectID(%q, 1) = %d, want %d", tt.org, got, tt.project)
			}
			if got := GitLabUserID(tt.org, 1); got != tt.user {
				t.Errorf("GitLabUserID(%q, 1) = %d, want %d", tt.org, got, tt.user)
			}
		})
	}
}

func TestGitLabCatalogRepoName_MapsProjectID(t *testing.T) {
	org := catalogOrg(t, "demo-gl-regulated")
	if got := gitLabCatalogRepoName(org, 9100101); got != "ledger" {
		t.Errorf("9100101 = %q, want ledger", got)
	}
	if got := gitLabCatalogRepoName(org, 9100103); got != "reporting" {
		t.Errorf("9100103 = %q, want reporting", got)
	}
	if got := gitLabCatalogRepoName(org, 0); got != "" {
		t.Errorf("unknown project_id = %q, want empty", got)
	}

	raw, err := json.Marshal(map[string]any{"project_id": int64(9100102), "iid": 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := gitLabCatalogRepoName(org, projectIDFromPayload(raw)); got != "compliance" {
		t.Errorf("payload project_id grouped as %q, want compliance", got)
	}
}

func catalogOrg(t *testing.T, name string) OrgCatalog {
	t.Helper()
	for _, org := range DefaultCatalog {
		if org.OrgName == name {
			return org
		}
	}
	t.Fatalf("catalog missing %s", name)
	return OrgCatalog{}
}

func TestBuildGitLabDirectPushCommits_SHAIncludesWindow(t *testing.T) {
	org := catalogOrg(t, "demo-gl-solo")
	repo := org.Repos[0]
	members := BuildGitLabMembers(org)
	w1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w2 := w1.Add(7 * 24 * time.Hour)
	first := BuildGitLabDirectPushCommits(org, repo, 1, members, 1, w1, w1.Add(24*time.Hour))
	second := BuildGitLabDirectPushCommits(org, repo, 1, members, 1, w2, w2.Add(24*time.Hour))
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("got %d/%d commits, want 1/1", len(first), len(second))
	}
	sha1, _ := first[0][glKeyID].(string)
	sha2, _ := second[0][glKeyID].(string)
	if sha1 == "" || sha1 == sha2 {
		t.Errorf("windowed SHAs collided: %q vs %q", sha1, sha2)
	}
}

func TestBuildGitLabIssues_ContinuesFromStartIID(t *testing.T) {
	org := catalogOrg(t, "demo-gl-scaleup")
	repo := org.Repos[0]
	members := BuildGitLabMembers(org)
	start, end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 2, 8, 0, 0, 0, 0, time.UTC)
	issues := BuildGitLabIssues(org, repo, 1, members, 2, 10, start, end)
	if len(issues) != 2 {
		t.Fatalf("got %d issues, want 2", len(issues))
	}
	if asInt(issues[0], glKeyIID) != 11 || asInt(issues[1], glKeyIID) != 12 {
		t.Errorf("iids = %d, %d, want 11, 12", asInt(issues[0], glKeyIID), asInt(issues[1], glKeyIID))
	}
}
