// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

func TestMRDiffs_JSONArray(t *testing.T) {
	pr := row("pr", `{"iid":1,"project_id":9100101,"diffs":[
		{"old_path":"settle/service.go","new_path":"settle/service.go","diff":"@@ -1,0 +1,10 @@\n+package settle\n","new_file":false}
	]}`)
	empty := row("pr", `{"iid":2,"project_id":9100101}`)
	srv := newTestServer(&mockDAO{prByKey: map[string]*dao.Row{
		"9100101/1": &pr,
		"9100101/2": &empty,
	}})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/merge_requests/1/diffs")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	arr := decodeArray(t, body)
	require.Len(t, arr, 1)
	var d struct {
		NewPath string `json:"new_path"`
		Diff    string `json:"diff"`
	}
	require.NoError(t, json.Unmarshal(arr[0], &d))
	assert.Equal(t, "settle/service.go", d.NewPath)
	assert.Contains(t, d.Diff, "package settle")

	emptyResp, emptyBody := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/merge_requests/2/diffs")
	require.Equal(t, http.StatusOK, emptyResp.StatusCode)
	assert.Empty(t, decodeArray(t, emptyBody))
}

func TestBranches_XNextPage(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("repo", `{
			"id":9100101,
			"default_branch":"main",
			"branches":[
				{"name":"main","default":true,"commit":{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
				{"name":"develop","default":false,"commit":{"id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
				{"name":"release","default":false,"commit":{"id":"cccccccccccccccccccccccccccccccccccccccc"}}
			]
		}`),
	}})
	defer srv.Close()

	p1, body1 := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/branches?page=1&per_page=2")
	require.Equal(t, http.StatusOK, p1.StatusCode)
	assert.Equal(t, "2", p1.Header.Get("X-Next-Page"))
	assert.Len(t, decodeArray(t, body1), 2)

	p2, body2 := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/branches?page=2&per_page=2")
	require.Equal(t, http.StatusOK, p2.StatusCode)
	assert.Empty(t, p2.Header.Get("X-Next-Page"))
	assert.Len(t, decodeArray(t, body2), 1)

	single, singleBody := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/branches/main")
	require.Equal(t, http.StatusOK, single.StatusCode)
	var b struct {
		Name   string `json:"name"`
		Commit struct {
			ID string `json:"id"`
		} `json:"commit"`
	}
	require.NoError(t, json.Unmarshal(singleBody, &b))
	assert.Equal(t, "main", b.Name)
	assert.Len(t, b.Commit.ID, 40)
}

func TestFileRaw_Body(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	goFile, goBody := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/files/main.go/raw")
	require.Equal(t, http.StatusOK, goFile.StatusCode)
	assert.Contains(t, goFile.Header.Get("Content-Type"), "text/plain")
	assert.Equal(t, "package main\n", string(goBody))

	readme, readmeBody := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/files/README.md/raw")
	require.Equal(t, http.StatusOK, readme.StatusCode)
	assert.Equal(t, "# README\n", string(readmeBody))

	empty, _ := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/files//raw")
	assert.Equal(t, http.StatusNotFound, empty.StatusCode)
}

func TestRepoTree_CannedListing(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/tree")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	arr := decodeArray(t, body)
	require.Len(t, arr, 3)
	names := map[string]string{}
	for _, raw := range arr {
		var e struct {
			Name string `json:"name"`
			Type string `json:"type"`
			Path string `json:"path"`
			Mode string `json:"mode"`
		}
		require.NoError(t, json.Unmarshal(raw, &e))
		names[e.Name] = e.Type
		assert.NotEmpty(t, e.Path)
		assert.NotEmpty(t, e.Mode)
	}
	assert.Equal(t, "blob", names["README.md"])
	assert.Equal(t, "blob", names["main.go"])
	assert.Equal(t, "tree", names["src"])
}

func TestFileRaw_EncodedPath(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/files/src%2Fmain.go/raw")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "package main\n", string(body))
	assert.NotContains(t, string(body), "{")
}

func TestBranches_DBErrorIs500NotSyntheticMain(t *testing.T) {
	srv := newTestServer(&mockDAO{err: errors.New("db down")})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/branches")
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assertGitLabError(t, body)

	single, singleBody := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/branches/main")
	assert.Equal(t, http.StatusInternalServerError, single.StatusCode)
	assertGitLabError(t, singleBody)
}
