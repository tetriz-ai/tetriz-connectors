// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter

type Priority int

var (
	priorities          = [3]Priority{PriorityHigh, PriorityMedium, PriorityLow}
	priorityToStringMap = map[Priority]string{
		PriorityHigh:   "0",
		PriorityMedium: "1",
		PriorityLow:    "2",
	}
)

const (
	PriorityHigh Priority = iota
	PriorityMedium
	PriorityLow
)

func (p Priority) String() string {
	return priorityToStringMap[p]
}
