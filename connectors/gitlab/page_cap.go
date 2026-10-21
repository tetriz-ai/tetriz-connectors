// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import "fmt"

// maxListPages bounds every offset-paginated list loop that follows the gateway's
// next_page_url (projects, merge requests, commits, issues, notes, members). At 100
// items a page it is 100,000 items, twice the 50,000-record ceiling GitLab puts on
// offset pagination, so no real listing reaches it. It is the same posture as
// maxBranchPages and maxTreePages: a vendor or proxy whose X-Next-Page never clears
// must not run the sync indefinitely.
const maxListPages = 1000

// checkPageCap reports an error once page passes maxListPages. Call it after the
// page counter advances, before the next request.
func checkPageCap(what string, page int32) error {
	if page > maxListPages {
		return fmt.Errorf("%s exceeded %d pages, possibly malformed pagination header", what, maxListPages)
	}
	return nil
}
