#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT

# Runs every example the way its README says and compares the output.
#
# For each examples/<name>/README.md it runs the README's sh block from the
# repository root and compares standard output with the README's last text
# block. The duration `go test` prints ("ok <package> 0.41s") becomes "<time>".
# It fails when examples/README.md lists a folder that does not exist or misses
# one that does. scripts/ci.sh runs it.
set -eu
cd "$(dirname "$0")/.."

go vet ./examples/...

listed=$(sed -n 's/.*](\([a-z0-9-]*\)\/).*/\1/p' examples/README.md | sort)
present=$(find examples -mindepth 2 -maxdepth 2 -name README.md | sed 's#examples/\([^/]*\)/README.md#\1#' | sort)
if [ "$listed" != "$present" ]; then
	printf 'examples/README.md lists [%s] but the folders with a README are [%s]\n' "$listed" "$present" >&2
	exit 1
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
status=0
for name in $present; do
	readme="examples/$name/README.md"
	awk '/^```sh$/ {b=1; next} b && /^```$/ {exit} b {print}' "$readme" >"$tmp/cmd"
	awk '/^```text$/ {b=1; out=""; next} b && /^```$/ {b=0; last=out; next} b {out = out $0 "\n"} END {printf "%s", last}' "$readme" >"$tmp/want"
	if [ ! -s "$tmp/cmd" ] || [ ! -s "$tmp/want" ]; then
		echo "$readme: needs one sh block and a text block with its output" >&2
		status=1
		continue
	fi
	if ! bash -o pipefail "$tmp/cmd" >"$tmp/out"; then
		echo "$name: the README's command failed" >&2
		status=1
		continue
	fi
	sed -E 's/^(ok[[:space:]]+[^[:space:]]+[[:space:]]+)[0-9.]+s$/\1<time>/' "$tmp/out" >"$tmp/got"
	if diff -u "$tmp/want" "$tmp/got"; then
		echo "$name: ok"
	else
		echo "$name: the output differs from $readme" >&2
		status=1
	fi
done
exit $status
