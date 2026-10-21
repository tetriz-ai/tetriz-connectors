// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

// groupIDs maps synthetic GitLab group_id → catalog org name. GitLabSettings.GroupID
// is int64 and connector URLs are /groups/%d, so the simulator cannot key
// sim_records on the catalog name the way GitHub/Bitbucket do — this map is
// the only translation. Rules match the catalog name, not the integer, so a
// numeric path can only fire a rule when the lookup yields a name containing
// the keyword (the 9190x entries).
var groupIDs = map[int64]string{
	91001: "demo-gl-regulated",
	91002: "demo-gl-scaleup",
	91003: "demo-gl-solo",
	91901: "demo-gl-sim-perm-denied",
	91902: "demo-gl-sim-rate-limit",
	91903: "demo-gl-sim-server-error",
	91904: "demo-gl-sim-not-found",
	91905: "demo-gl-sim-empty",
}

// orgNameForGroup returns the catalog org name for a synthetic group_id.
// Unknown ids are not a default org — the caller must 404.
func orgNameForGroup(groupID int64) (string, bool) {
	name, ok := groupIDs[groupID]
	return name, ok
}

// splitProjectID decodes project_id = group_id*100 + repo_index (1-based).
// repo_index 0 is not a valid encoding and the caller must 404.
func splitProjectID(projectID int64) (groupID int64, repoIndex int) {
	return projectID / 100, int(projectID % 100)
}

// orgNameForProject recovers the catalog org from an encoded project_id.
// Unknown group or a zero repo_index is not found.
func orgNameForProject(projectID int64) (string, bool) {
	groupID, repoIndex := splitProjectID(projectID)
	if repoIndex < 1 {
		return "", false
	}
	return orgNameForGroup(groupID)
}
