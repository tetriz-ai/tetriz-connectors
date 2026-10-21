// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

type valuesResp struct {
	Values     []json.RawMessage `json:"values"`
	Total      int               `json:"total"`
	IsLast     bool              `json:"isLast"`
	StartAt    int               `json:"startAt"`
	MaxResults int               `json:"maxResults"`
}

type worklogResp struct {
	Worklogs   []json.RawMessage `json:"worklogs"`
	Total      int               `json:"total"`
	StartAt    int               `json:"startAt"`
	MaxResults int               `json:"maxResults"`
}

func getJSON(t *testing.T, url string, out any) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode == http.StatusOK && out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}
	return resp
}

// --- worklog ---

func issueRowWithWorklogs(key string, nWorklogs int) *dao.Row {
	worklogs := make([]map[string]any, nWorklogs)
	for i := range worklogs {
		worklogs[i] = map[string]any{"id": fmt.Sprintf("%d", 10000+i), "timeSpentSeconds": 3600}
	}
	payload, _ := json.Marshal(map[string]any{
		"key":    key,
		"fields": map[string]any{"worklogs": worklogs},
	})
	return &dao.Row{EntityType: "issue", Payload: payload, CursorVal: time.Now().UTC()}
}

func TestWorklog_Windowed(t *testing.T) {
	srv := newTestServer(&mockStore{byKey: map[string]*dao.Row{"PLAT-1": issueRowWithWorklogs("PLAT-1", 3)}})
	defer srv.Close()
	var wr worklogResp
	resp := getJSON(t, srv.URL+"/jira/ex/jira/demo-gh-enterprise/rest/api/3/issue/PLAT-1/worklog?startAt=1&maxResults=1", &wr)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(wr.Worklogs) != 1 || wr.Total != 3 || wr.StartAt != 1 {
		t.Errorf("worklog window = %+v, want 1 of total 3 at startAt 1", wr)
	}
}

func TestWorklog_UnknownKey(t *testing.T) {
	srv := newTestServer(&mockStore{byKey: map[string]*dao.Row{}})
	defer srv.Close()
	var wr worklogResp
	resp := getJSON(t, srv.URL+"/jira/ex/jira/demo-gh-enterprise/rest/api/3/issue/NOPE-1/worklog", &wr)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (unknown key is empty, not 404)", resp.StatusCode)
	}
	if len(wr.Worklogs) != 0 || wr.Total != 0 {
		t.Errorf("unknown-key worklog = %+v, want empty", wr)
	}
}

func TestWorklog_PermDenied(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()
	resp := getJSON(t, srv.URL+"/jira/ex/jira/acme-sim-perm-denied/rest/api/3/issue/PLAT-1/worklog", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

// --- boards (derived from projects) ---

func projectRow(id, key string) dao.Row {
	payload, _ := json.Marshal(map[string]any{"id": id, "key": key, "name": key + " Project"})
	return dao.Row{EntityType: "project", Payload: payload}
}

func TestBoardList_DerivedFromProjects(t *testing.T) {
	srv := newTestServer(&mockStore{projects: []dao.Row{projectRow("10000", "PLAT"), projectRow("10001", "PAYM")}})
	defer srv.Close()
	var vr valuesResp
	resp := getJSON(t, srv.URL+"/jira/ex/jira/demo-gh-enterprise/rest/agile/1.0/board?type=scrum", &vr)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(vr.Values) != 2 || vr.Total != 2 || !vr.IsLast {
		t.Fatalf("board list = %+v, want 2 boards", vr)
	}
	var board struct {
		ID       int64  `json:"id"`
		Type     string `json:"type"`
		Location struct {
			ProjectKey string `json:"projectKey"`
		} `json:"location"`
	}
	if err := json.Unmarshal(vr.Values[0], &board); err != nil {
		t.Fatalf("decode board: %v", err)
	}
	if board.Type != "scrum" || board.Location.ProjectKey == "" {
		t.Errorf("board = %+v, want scrum type + a location.projectKey", board)
	}
	if board.ID != deriveBoardID("10000") {
		t.Errorf("board id = %d, want deriveBoardID(10000)=%d", board.ID, deriveBoardID("10000"))
	}
}

func TestBoardList_NonScrumTypeYieldsNone(t *testing.T) {
	srv := newTestServer(&mockStore{projects: []dao.Row{projectRow("10000", "PLAT")}})
	defer srv.Close()
	var vr valuesResp
	getJSON(t, srv.URL+"/jira/ex/jira/demo-gh-enterprise/rest/agile/1.0/board?type=kanban", &vr)
	if len(vr.Values) != 0 {
		t.Errorf("kanban board query = %d values, want 0 (only scrum synthesized)", len(vr.Values))
	}
}

func TestBoardList_EmptyRule(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()
	var vr valuesResp
	resp := getJSON(t, srv.URL+"/jira/ex/jira/org-sim-empty/rest/agile/1.0/board", &vr)
	if resp.StatusCode != http.StatusOK || len(vr.Values) != 0 || !vr.IsLast {
		t.Errorf("sim-empty board = status %d %+v, want 200 empty isLast", resp.StatusCode, vr)
	}
}

// --- per-board sprints ---

func sprintRow(id, originBoardID int64) dao.Row {
	payload, _ := json.Marshal(map[string]any{"id": id, "originBoardId": originBoardID, "state": "closed"})
	return dao.Row{EntityType: "sprint", Payload: payload}
}

func TestBoardSprints_FilterByBoard(t *testing.T) {
	board := deriveBoardID("10000")
	other := deriveBoardID("10001")
	srv := newTestServer(&mockStore{sprints: []dao.Row{
		sprintRow(1, board), sprintRow(2, board), sprintRow(3, other),
	}})
	defer srv.Close()
	var vr valuesResp
	resp := getJSON(t, srv.URL+fmt.Sprintf("/jira/ex/jira/demo-gh-enterprise/rest/agile/1.0/board/%d/sprint", board), &vr)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(vr.Values) != 2 || vr.Total != 2 {
		t.Errorf("board %d sprints = %+v, want the 2 sprints on that board only", board, vr)
	}
}
