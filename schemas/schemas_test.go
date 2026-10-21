// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/schemas"
)

func TestValidateRejectsAWrongType(t *testing.T) {
	if err := schemas.Validate("jira", "fetch_sprints", []byte(`{"id":900103,"state":"active"}`)); err != nil {
		t.Fatalf("a valid sprint: %v", err)
	}
	err := schemas.Validate("jira", "fetch_sprints", []byte(`{"id":"900103"}`))
	if err == nil || !strings.Contains(err.Error(), "jira/fetch_sprints") {
		t.Fatalf("a string sprint id: got %v, want a validation error naming the schema", err)
	}
}

func TestValidateRejectsAMissingRequiredKey(t *testing.T) {
	if err := schemas.Validate("jira", "fetch_issues_data", []byte(`{"s3_key":"descriptions/o/jira/issues/1/A-1","issue_key":"A-1"}`)); err == nil {
		t.Fatal("no platform_project_id: want an error")
	}
}

func TestValidateAssertsFormatsAndContent(t *testing.T) {
	blob := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	good := `{"node_id":"R_1","homepage":"","allow_forking":true,"web_commit_signoff_required":false,"allow_squash_merge":true,"allow_merge_commit":true,"allow_rebase_merge":true}`
	repo := func(b64, created string) []byte {
		return []byte(`{"platformRepoId":"1","name":"w","fullName":"a/w","ownerLogin":"a","vendorCreatedAt":"` + created + `","vendorBlobJson":"` + b64 + `"}`)
	}
	if err := schemas.Validate("github", "fetch_repos", repo(blob(good), "2026-01-02T03:04:05Z")); err != nil {
		t.Fatalf("a valid repository: %v", err)
	}
	if err := schemas.Validate("github", "fetch_repos", repo(blob(good), "yesterday")); err == nil {
		t.Error("a date-time that is not RFC 3339: want an error")
	}
	if err := schemas.Validate("github", "fetch_repos", repo(blob(`{"github":{}}`), "2026-01-02T03:04:05Z")); err == nil {
		t.Error("a vendor blob that misses its keys: want an error")
	}
	if err := schemas.Validate("github", "fetch_repos", repo("not base64!", "2026-01-02T03:04:05Z")); err == nil {
		t.Error("a vendor blob that is not base64: want an error")
	}
}

func TestValidateUnknownSchema(t *testing.T) {
	err := schemas.Validate("github", "no_such_op", []byte(`{}`))
	if !errors.Is(err, schemas.ErrNoSchema) {
		t.Fatalf("got %v, want ErrNoSchema", err)
	}
	if _, ok := schemas.Schema("github", "no_such_op"); ok {
		t.Fatal("Schema reports a schema that does not exist")
	}
}

func TestValidateRejectsInvalidJSON(t *testing.T) {
	if err := schemas.Validate("jira", "fetch_sprints", []byte(`{`)); err == nil {
		t.Fatal("want an error")
	}
}

func TestListIsSortedAndSkipsSharedShapes(t *testing.T) {
	refs := schemas.List()
	if len(refs) == 0 {
		t.Fatal("no schemas")
	}
	for i, r := range refs {
		if r.Connector == "common" {
			t.Errorf("List returns the shared shape %s", r.Path())
		}
		if i > 0 && refs[i-1].String() >= r.String() {
			t.Errorf("List is not sorted: %s before %s", refs[i-1], r)
		}
	}
}
