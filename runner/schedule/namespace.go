// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

// Namespaced returns name in namespace ns: "ns:name", or name itself when ns
// is empty. The scheduler applies it to its queue names and its leader lock
// key, so deployments that share one Redis under different namespaces never
// run each other's tasks or contend for one lock.
func Namespaced(ns, name string) string {
	if ns == "" {
		return name
	}
	return ns + ":" + name
}

// namespacedQueues returns queues with each name in namespace ns.
func namespacedQueues(ns string, queues map[string]int) map[string]int {
	if ns == "" || queues == nil {
		return queues
	}
	out := make(map[string]int, len(queues))
	for q, w := range queues {
		out[Namespaced(ns, q)] = w
	}
	return out
}
