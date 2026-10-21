// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package tenantlabel

// SetLiveIndividualSetForTest replaces the package-global live-individual set and
// returns a restore function that reinstates the previous value. It exists so
// emit-site tests in other packages can exercise the individual-vs-org branch
// (skip / collapse) that keys off the global, which is otherwise only writable by
// a Source refresh. Test-only: pair every call with `defer restore()`.
func SetLiveIndividualSetForTest(set map[string]struct{}) (restore func()) {
	prev := LiveIndividualSet()
	publishSet(set)
	return func() { liveIndividualSet.Store(prev) }
}
