#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT

# Acceptance run against the docker-compose stack (docker compose up --wait
# first). It connects the demo organization's GitHub connector to the
# simulator, syncs repositories and pull requests, runs the pull-request sync
# a second time to resume from the stored cursor, and checks what landed in
# the record file and the store.
set -eu

ORG=00000000-0000-4000-8000-00000000d3e0
COMPOSE=${COMPOSE:-docker compose}

tc() { $COMPOSE exec -T runner tetriz-connectors "$@"; }

fail() {
	echo "acceptance: FAIL: $*" >&2
	exit 1
}

# status_of prints the "status" member of a sync result on stdin.
status_of() { sed -n 's/^  "status": "\([a-z]*\)",$/\1/p'; }

# records_of prints the "records_emitted" member of a sync result on stdin.
records_of() { sed -n 's/^  "records_emitted": \([0-9]*\),*$/\1/p'; }

tc config put --org "$ORG" --connector github --settings '{"organization":"demo-gh-startup"}' --credentials '{}' >/dev/null

repos=$(tc sync --org "$ORG" --connector github --op fetch_repos 2>/dev/null)
[ "$(echo "$repos" | status_of)" = completed ] || fail "fetch_repos did not complete: $repos"
echo "acceptance: fetch_repos completed"

# A first run reads every pull request; one after an earlier sync (the quick
# start's) already resumes, and then only the second run's count is bounded.
fresh=no
[ "$(tc cursor show --org "$ORG" --connector github --op fetch_prs)" = "no cursor" ] && fresh=yes
first=$(tc sync --org "$ORG" --connector github --op fetch_prs 2>/dev/null)
[ "$(echo "$first" | status_of)" = completed ] || fail "fetch_prs did not complete: $first"
echo "$first" | grep -q '"demo-gh-startup/' || fail "fetch_prs stored no per-repository cursor"
echo "acceptance: fetch_prs completed and stored a cursor"

second=$(tc sync --org "$ORG" --connector github --op fetch_prs 2>/dev/null)
[ "$(echo "$second" | status_of)" = completed ] || fail "second fetch_prs did not complete: $second"
n1=$(echo "$first" | records_of)
n2=$(echo "$second" | records_of)
[ -n "$n1" ] && [ -n "$n2" ] || fail "a fetch_prs result has no records_emitted"
if [ "$fresh" = yes ]; then
	[ "$n2" -lt "$n1" ] || fail "the second fetch_prs emitted $n2 records, not fewer than the first's $n1: it did not resume"
else
	[ "$n2" -le "$n1" ] || fail "the second fetch_prs emitted $n2 records, more than the resumed first's $n1"
fi
echo "acceptance: fetch_prs resumed from its cursor and completed ($n1 records, then $n2)"

cursor=$(tc cursor show --org "$ORG" --connector github --op fetch_prs)
echo "$cursor" | grep -q '"demo-gh-startup/' || fail "cursor show printed no cursor"

prs=$($COMPOSE exec -T runner sh -c 'grep -c "\"operationId\":\"fetch_prs\"" /data/records.jsonl')
[ "$prs" -gt 0 ] || fail "no pull request records in /data/records.jsonl"
repos_n=$($COMPOSE exec -T runner sh -c 'grep -c "\"operationId\":\"fetch_repos\"" /data/records.jsonl')
[ "$repos_n" -gt 0 ] || fail "no repository records in /data/records.jsonl"
echo "acceptance: $repos_n repository records and $prs pull request records written"

# The serving process is the leader: it builds the periodic task set from
# the stored configs and exports it as a metric.
$COMPOSE exec -T runner wget -q -O - http://127.0.0.1:9090/metrics | grep -q '^tetriz_scheduler_periodic_tasks ' || fail "scheduler metrics not exported"
echo "acceptance: ok"
